// =============================================================================
// fill_merge_check.cpp —— 成交/报单双通道融合的**可执行**验证台 (独立单文件, 不入 CMake)
//
// 为什么需要它: 本机 (macOS) **编不了 tb** (缺 fmt/simdjson/boost/redis),
// 而"增量 ⇒ 累计"融合最危险的失效模式 (重复计数 / 均价算飞) 恰恰是纯算术问题,
// 完全可以脱离项目单独验证。这个文件把 `OrderManager::onOrderUpdate` 里
// **成交量那一段原样搬出来**(见下面 MERGE REGION), 用桩类型包住, 然后:
//   · 造一批"真实成交" (qty, px)
//   · 让"报单通道(累计)"与"成交通道(增量)"按**任意交错**投递 (含跳号/先到/后到/空)
//   · 断言 ① 推送出去的 tradeDiff 之和 == 真实成交量
//          ② 由推送序列重建出的累计均价 == 交易所口径的累计均价
//
// 编译运行:
//     clang++ -std=c++17 -O1 -Wall -o /tmp/fill_merge_check fill_merge_check.cpp && /tmp/fill_merge_check
//
// ⚠️ **MERGE REGION 必须与 OrderManager.cpp 保持逐字一致**。改了那边, 用这条命令重新同步:
//     sed -n "$(grep -n 'bool statusAdvanced = false;' src/oms/OrderManager.cpp | cut -d: -f1),\
//   $(awk 'NR>L && /^    \}$/{print NR; exit}' L=$(grep -n '如果没有新的成交' src/oms/OrderManager.cpp | cut -d: -f1) \
//   src/oms/OrderManager.cpp)p" src/oms/OrderManager.cpp
//   (更简单: 直接肉眼比对; 段落边界是 "bool statusAdvanced = false;" 到成交量 else 块的收尾 "}")
//
// 反证(确认这个测试有区分度): 把 `max(trade_sum, old_qty)` 换成朴素 `old_qty + fill_qty`,
// 应当看到大量 FAIL —— 实测 4800 组里失败 2758 组。
// =============================================================================

#include <cstdio>
#include <cstring>
#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <random>
#include <algorithm>
#include <cmath>

#define ZERO_NUM 0.0000000001
#define LOG_ERROR(...) do {} while (0)

enum ExchangeType { BINANCE, OKX, GATEIO };
enum InstType { SPOT, MARGIN, USDT_SWAP, C_SWAP, USDT_FUTURES, C_FUTURES };
enum OrderStatus { OS_NEW, OS_PARTFILLED, OS_FILLED, OS_CANCELED, OS_FAILED };
enum ApiSource { AS_WEBSOCKET, AS_CANCEL_ORDER };

namespace pubsub {
    enum CommandType { CMD_RPT_ORDER_RESPONSE, CMD_RPT_TRADE, CMD_RPT_QUERY_ORDER };
    struct OrderResponse {
        InstType instTypeEnum;
        OrderStatus orderStatus;
        ApiSource apiSourceEnum;
        // ★ 成交通道融合游标 —— 与真头文件 include/pubsub_protocol.h 同位置同类型。
        //   (这里只求"字段存在 + 类型对", 布局不影响本验证台的算术断言)
        double tradeSum;
        ExchangeType exchangeTypeEnum;
        char orderSysId[64];
        double volumeTotal, volumeTraded, tradePrice, tradeDiff, fillPrice;
        int64_t updateTime;
    };
    struct CommandBody { OrderResponse orderResponse; };
    struct RCommand { CommandType cmdTypeEnum; CommandBody body; };
}
namespace crypto { inline int64_t getCurrentTime() { return 0; } }

// ===== 下面这段是从 tb/src/oms/OrderManager.cpp 的 onOrderUpdate **逐字**抽出的 =====
static bool merge_block(pubsub::RCommand& rcmd, pubsub::RCommand& op) {
    bool statusAdvanced = false;
    bool volumeIncreased = false;

    // ==================================================================
    // ★ 成交通道 → 报单通道 的形态归一 (增量 ⇒ 累计)
    //
    //   报单通道 (orders) 给的是 **累计**成交量 accFillSz + 累计均价 avgPx;
    //   成交通道 (fills)  给的是 **单笔增量** fillSz + 该笔成交价 fillPx。
    //   两者描述的是同一批成交 —— 直接相加必然重复计数, 所以不能各推各的。
    //
    //   这里把增量折成累计, 然后交给**下面原有的**那套 merge 统一处理:
    //       tradeSum = 本单在成交通道上**已经看过的**成交量之和 (该通道自己的游标)
    //       target   = max(op.volumeTraded, tradeSum)   ← 两个通道谁先到谁算数
    //       delta    = target - op.volumeTraded         ← 本次真正要补的量
    //   再按 delta / fillPx 把 (volumeTraded, tradePrice) 重算成一个**自洽的**
    //   (累计量, 累计均价) 对。原 merge 从这两个点反推出的 fillPrice 恰好等于本笔 fillPx:
    //       线性合约 (SPOT/MARGIN/USDT_SWAP…): 均价 = Σ(量×价)/Σ量
    //           fillPrice = (target·新均价 − old_qty·old_avg) / delta = delta·fill_px/delta ✓
    //       币本位合约 (C_SWAP/C_FUTURES): 均价是**倒数加权**口径 (原 merge 的反推式就是
    //           这么写的: fillPrice = delta / (target/新均价 − old_qty/old_avg)), 所以
    //           新均价必须按 1/均价 加权: target/新均价 = old_qty/old_avg + delta/fill_px ✓
    //
    //   ★ 游标**就存在报单体自己的字段里** (`OrderResponse::tradeSum`, 见 pubsub_protocol.h)。
    //     这个函数本来就要从 shm 取出这张单的上一个报单体 (`cached` → `op`), 改完再整份写回
    //     (`oms_flush_slot(omsShm_, op)`) —— 游标搭这趟车就够了, 零额外查找/分配/写。
    //   ★ 为什么 target 要跟 op.volumeTraded 取 max, 而不是直接 tradeSum 累加:
    //     报单通道可能**先**把这一笔报过来 (此时 tradeSum 还落后), 取 max 后 delta=0
    //     → 不会把同一笔成交推两次。
    //   ★ 为什么 tradeSum 可以放心累加、不用按 tradeId 去重:
    //     fills 频道的 tradeId 在聚合时是"最后一条成交的 id", 不是可去重的唯一键; 而
    //     单个 WS 连接内 TCP 有序、OKX 也不重放历史, 所以同一连接内不会重复投递。
    //   ★ 成交价异常 (<=0) 时只累加游标、不推进累计量, 避免污染均价 —— 报单通道会带上
    //     正确的累计均价兜底。
    // ==================================================================
    if (rcmd.cmdTypeEnum == pubsub::CMD_RPT_TRADE) {
        const double fill_qty = rcmd.body.orderResponse.tradeDiff;   // fillSz (增量)
        const double fill_px  = rcmd.body.orderResponse.fillPrice;   // fillPx (本笔价)
        const double old_qty  = op.body.orderResponse.volumeTraded;
        const double old_avg  = op.body.orderResponse.tradePrice;

        // ★ 游标直接读写**报单体自己的字段** (随 op 一起写回 shm, 见上面的说明)。
        double& trade_sum = op.body.orderResponse.tradeSum;
        trade_sum += fill_qty;

        const double target = (trade_sum > old_qty) ? trade_sum : old_qty;

        if (fill_px > ZERO_NUM && target > old_qty + ZERO_NUM) {
            const double delta = target - old_qty;
            const InstType it = op.body.orderResponse.instTypeEnum;

            if (old_qty > ZERO_NUM && old_avg > ZERO_NUM) {
                if (it == C_SWAP || it == C_FUTURES) {
                    rcmd.body.orderResponse.tradePrice = target / (old_qty / old_avg + delta / fill_px);
                }
                else {
                    rcmd.body.orderResponse.tradePrice = (old_qty * old_avg + delta * fill_px) / target;
                }
            }
            else {
                // 本单第一次成交: 累计均价就是本笔价
                rcmd.body.orderResponse.tradePrice = fill_px;
            }

            rcmd.body.orderResponse.volumeTraded = target;
        }
        else {
            // 这一笔报单通道已经报过 (或成交价不可用) → 归一成"没有新成交", 走原 merge 的 else 分支
            rcmd.body.orderResponse.volumeTraded = old_qty;
            rcmd.body.orderResponse.tradePrice   = old_avg;
        }

        // 成交通道**不掌握订单状态**, 只声明"至少部分成交"。
        // 原 merge 的状态优先级推进 (getOrderStatusPriority) 会做 OS_NEW(3) → OS_PARTFILLED(4),
        // 并且**不会**覆盖终态 —— 所以这里给 PARTFILLED 是安全的, 不会把 FILLED/CANCELED 拉回来。
        rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
    }

    if (rcmd.body.orderResponse.volumeTraded > op.body.orderResponse.volumeTotal + ZERO_NUM) {
        LOG_ERROR("Overfilled order! orderSysId: {} volumeTraded: {} volumeTotal: {}", op.body.orderResponse.orderSysId, rcmd.body.orderResponse.volumeTraded, op.body.orderResponse.volumeTotal);
    }

    if (rcmd.body.orderResponse.orderStatus == OS_CANCELED) {
        if (rcmd.body.orderResponse.apiSourceEnum == AS_CANCEL_ORDER && rcmd.body.orderResponse.exchangeTypeEnum == BINANCE) {
            if (op.body.orderResponse.volumeTraded < rcmd.body.orderResponse.volumeTraded) {
                return false;   // ← 此处尚未修改 op, 不需要写回
            }
        }    
    }

    if (rcmd.body.orderResponse.volumeTraded > op.body.orderResponse.volumeTraded) {
        op.body.orderResponse.tradeDiff = rcmd.body.orderResponse.volumeTraded - op.body.orderResponse.volumeTraded;
        op.body.orderResponse.fillPrice = 0.0;

        if (rcmd.body.orderResponse.instTypeEnum == C_SWAP || rcmd.body.orderResponse.instTypeEnum == C_FUTURES) {
            if (op.body.orderResponse.volumeTraded > ZERO_NUM) {
                op.body.orderResponse.fillPrice = op.body.orderResponse.tradeDiff / (rcmd.body.orderResponse.volumeTraded / rcmd.body.orderResponse.tradePrice - op.body.orderResponse.volumeTraded / op.body.orderResponse.tradePrice);
            }
            else {
                op.body.orderResponse.fillPrice = rcmd.body.orderResponse.tradePrice;
            }
        }
        else {
            op.body.orderResponse.fillPrice = (rcmd.body.orderResponse.volumeTraded * rcmd.body.orderResponse.tradePrice - op.body.orderResponse.volumeTraded * op.body.orderResponse.tradePrice) / op.body.orderResponse.tradeDiff;

        }

        op.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTraded;
        op.body.orderResponse.tradePrice = rcmd.body.orderResponse.tradePrice;
        op.body.orderResponse.updateTime = crypto::getCurrentTime();
        volumeIncreased = true;
    }
    else { // 如果没有新的成交，本次成交量和成交价设为0
        op.body.orderResponse.tradeDiff = 0.0;
        op.body.orderResponse.fillPrice = 0.0;
    }
    (void)statusAdvanced;   // 本验证台只关心成交量, 不复制后面的状态推进/发布判定
    return volumeIncreased;
}
// ==================================================================================

// ---- 真值模型 ----
struct Fill { double q, p; };

static double true_vwap(const std::vector<Fill>& f, int k, bool inv) {
    double q = 0, n = 0;
    for (int i = 0; i < k; i++) { q += f[i].q; n += inv ? f[i].q / f[i].p : f[i].q * f[i].p; }
    return inv ? q / n : n / q;
}
static double true_cum(const std::vector<Fill>& f, int k) {
    double q = 0; for (int i = 0; i < k; i++) q += f[i].q; return q;
}

int main() {
    std::mt19937 rng(7);
    long trials = 0, bad = 0;
    for (int mode = 0; mode < 2; mode++) {
        const bool inv = (mode == 1);
        const InstType it = inv ? C_SWAP : USDT_SWAP;
        for (int t = 0; t < 400; t++) {
            int n = 1 + (int)(rng() % 5);
            std::vector<Fill> tf;
            double qs[4] = {1, 2, 5, 10};
            for (int i = 0; i < n; i++) tf.push_back({qs[rng() % 4], 95.0 + (double)(rng() % 1000) / 100.0});
            double total = true_cum(tf, n);

            // 报单通道报哪几个 k (可跳号、可为空)
            std::vector<int> ks;
            for (int k = 1; k <= n; k++) if (rng() % 2) ks.push_back(k);

            for (int rep = 0; rep < 6; rep++) {
                struct Msg { int kind; int idx; double v; double p; };
                std::vector<Msg> fm, om;
                for (int i = 0; i < n; i++) fm.push_back({0, i, tf[i].q, tf[i].p});
                for (int k : ks)      om.push_back({1, k, true_cum(tf, k), true_vwap(tf, k, inv)});

                pubsub::RCommand op; memset(&op, 0, sizeof(op));
                op.body.orderResponse.instTypeEnum = it;
                op.body.orderResponse.volumeTotal = total;
                // ★ 游标就在 op 自己的报单体里 (op.body.orderResponse.tradeSum), 无需外部容器。

                std::vector<std::pair<double,double>> pub;
                size_t i = 0, j = 0;
                while (i < fm.size() || j < om.size()) {
                    bool take_f = (j >= om.size()) || (i < fm.size() && (rng() % 2));
                    pubsub::RCommand rcmd; memset(&rcmd, 0, sizeof(rcmd));
                    rcmd.body.orderResponse.instTypeEnum = it;
                    rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
                    if (take_f) {
                        rcmd.cmdTypeEnum = pubsub::CMD_RPT_TRADE;
                        rcmd.body.orderResponse.tradeDiff = fm[i].v;   // fillSz
                        rcmd.body.orderResponse.fillPrice = fm[i].p;   // fillPx
                        i++;
                    } else {
                        rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                        rcmd.body.orderResponse.volumeTraded = om[j].v;   // 累计量
                        rcmd.body.orderResponse.tradePrice   = om[j].p;   // 累计均价
                        j++;
                    }
                    bool pubd = merge_block(rcmd, op);
                    if (pubd) pub.push_back({op.body.orderResponse.tradeDiff, op.body.orderResponse.fillPrice});
                }

                double sq = 0, sn = 0;
                for (auto& pr : pub) { sq += pr.first; sn += inv ? pr.first / pr.second : pr.first * pr.second; }
                double got_vwap = inv ? sq / sn : sn / sq;
                double want_vwap = true_vwap(tf, n, inv);

                bool ok = std::fabs(sq - total) < 1e-6 &&
                          std::fabs(got_vwap - want_vwap) / want_vwap < 1e-9;
                if (!ok) {
                    bad++;
                    if (bad <= 3)
                        printf("FAIL mode=%s n=%d 推送量=%.6f (真实 %.6f) 重建均价=%.9f (真实 %.9f)\n",
                               inv ? "C_SWAP" : "LINEAR", n, sq, total, got_vwap, want_vwap);
                }
                trials++;
            }
        }
    }
    printf("\n逐字跑真实 C++ 代码: %ld 组交错, 失败 %ld\n", trials, bad);
    printf(bad == 0 ? "OK: 推送增量之和 == 真实成交量, 重建均价 == 交易所均价 (无重复计数/无丢量)\n"
                    : "!!! 有失败\n");
    return bad == 0 ? 0 : 1;
}
