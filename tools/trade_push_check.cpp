// ============================================================================
// trade_push_check.cpp —— Bybit / Gate.io 成交通道解析规则的**可执行验证台**
//
//   背景: OKX fills 通道已全部落地(见 clordid_check.cpp / §12)。本轮把同样的
//         "回填 client-id ⇒ 发 CMD_RPT_TRADE 增量" 模式复制到三条新通道:
//           - Bybit  `execution`            → handleExecutionUpdate
//           - Gate US `futures.usertrades`  → GateioUsWsTradeUnit::handleTradesUpdate
//           - Gate SP `spot.usertrades`     → GateioSpotWsTradeUnit::handleTradesUpdate
//         三条通道都**不改 OMS 侧一行代码**(融合逻辑本来就是通用的)。
//
//   本验证台钉死三件事, 它们都是"错了就静默出错"的类型:
//     ① Bybit execType 过滤 —— execution 频道**不只有成交**, 还夹带
//        Funding(资金费结算)/AdlTrade/BustTrade/Delivery/Settle/BlockTrade/
//        MovePosition/… 共 13 种非成交。放进来的话:
//          - Funding 的 execQty 是**仓位规模**、execPrice 是**标记价**,
//            且**不进** order 频道的 cumExecQty ⇒ 会凭空造出交易所从未确认的成交量。
//        ⇒ 只认 execType == "Trade"。
//     ② Gate text 过滤 —— usertrades 的 text 会回填我们下单时填的 orderSysId,
//        但也会回填 Gate 的**保留值**(web/api/app/liquidation/auto_deleveraging/…)。
//        不过滤的话, 一次强平成交可能被挂到恰好存在的同名字段上。
//     ③ orderSysId 长度/字符集硬约束 —— 超限时**下单阶段就被交易所拒单**(响亮),
//        但"strategyId 多长才安全"必须写死, 否则配置一改就踩雷。
//
//   与生产代码的关系: 下面的 mirror_* 函数是三个 handler 里对应判断的**逐字拷贝**
//   (只包了一层函数壳)。改了生产代码就要同步改这里 —— 这是本验证台的**已知局限**
//   (本机编不了 tb: 缺 fmt / simdjson / boost / redis, 只能靠"人工同步 + 逐字拷贝")。
//
//   编译运行 (不需要 fmt / simdjson / 任何第三方库):
//     clang++ -std=c++17 -O1 -Wall -Wextra -o /tmp/trade_push_check tb/tools/trade_push_check.cpp
//     /tmp/trade_push_check
//
//   非生产代码, 不入 CMake, **不需要可以直接删**。
// ============================================================================
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>

// ===========================================================================
// ① Bybit: 只认真正的成交
//    逐字取自 BybitWsTrade.cpp::handleExecutionUpdate
//      if (execType_sv != "Trade") { ...计数+限流日志...; continue; }
// ===========================================================================
static bool mirror_bybit_accept_exec_type(std::string_view execType_sv) {
    return execType_sv == "Trade";
}

// ===========================================================================
// ② Gate: text 必须是**我们自己的** orderSysId
//    逐字取自 GateioUSWsTrade.cpp / GateioSpotWsTrade.cpp::handleTradesUpdate
//      if (text_sv.size() < 2 || text_sv[0] != 't' || text_sv[1] != '-') continue;
// ===========================================================================
static bool mirror_gate_accept_text(std::string_view text_sv) {
    return !(text_sv.size() < 2 || text_sv[0] != 't' || text_sv[1] != '-');
}

// ===========================================================================
// ③ 下单时的 orderSysId 生成规则
//    逐字取自 OrderManager.cpp::getOrderSysId(ExchangeType, const char*)
//      GATEIO: fmt::format("t-{}{}",  strategyId, rdtsc)
//      BYBIT : fmt::format("by{}{}",  strategyId, rdtsc)
//    (rdtsc 是 get_rdtsc_timestamp() 的十进制字符串)
// ===========================================================================
static std::string mirror_gate_order_sys_id(const char* sid, const char* rdtsc) {
    return std::string("t-") + sid + rdtsc;
}
static std::string mirror_bybit_order_sys_id(const char* sid, const char* rdtsc) {
    return std::string("by") + sid + rdtsc;
}

// 交易所对这两个字段的硬约束 (见设计文档 §14)
static constexpr size_t kGateTextMaxBytes    = 28;   // 去掉 "t-" 之后 ≤ 28 字节
static constexpr size_t kBybitLinkIdMaxBytes = 36;   // 含前缀, 总长 ≤ 36 字节
static constexpr size_t kOrderSysIdBufBytes  = 64;   // pubsub::OrderResponse::orderSysId[64]
static constexpr size_t kStrategyIdBufBytes  = 32;   // pubsub::OrderResponse::strategyId[32]

// Bybit orderLinkId 允许的字符: [A-Za-z0-9_-]
static bool bybit_charset_ok(std::string_view s) {
    if (s.empty()) {
        return false;
    }
    for (const char c : s) {
        const bool ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-';
        if (!ok) {
            return false;
        }
    }
    return true;
}

// Gate text 去掉 "t-" 后允许的字符: 数字/字母/_/-/.
static bool gate_charset_ok(std::string_view s) {
    if (s.size() < 2 || s[0] != 't' || s[1] != '-') {
        return false;
    }
    const std::string_view body = s.substr(2);
    if (body.empty()) {
        return false;
    }
    for (const char c : body) {
        const bool ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
                        c == '_' || c == '-' || c == '.';
        if (!ok) {
            return false;
        }
    }
    return true;
}

// ===========================================================================
// ④ 数量口径一致性 —— 成交通道的 tradeDiff 必须和**同频道的报单路径**用同一套换算
//    (口径不一致的话, OMS 侧 max(op.volumeTraded, tradeSum) 就是在比不同单位)
// ===========================================================================
// Gate US: 报单路径 volumeTotal = fabs(size)                     (不缩放)
//          成交路径 tradeDiff = fabs(size)                        (不缩放)
static double mirror_gate_us_orders_volume_total(double size) { return std::fabs(size); }
static double mirror_gate_us_trade_diff(double size)          { return std::fabs(size); }

// Gate SP: 报单路径 volumeTotal = fabs(amount) * info.magnifyNumber
//          成交路径 tradeDiff = fabs(amount) * info.magnifyNumber
static double mirror_gate_sp_orders_volume_total(double amount, double mag) { return std::fabs(amount) * mag; }
static double mirror_gate_sp_trade_diff(double amount, double mag)          { return std::fabs(amount) * mag; }

// ---------------------------------------------------------------------------
static int failures = 0;
static int total    = 0;

static void expect_bool(const char* what, const char* in, bool got, bool want) {
    ++total;
    if (got != want) {
        ++failures;
        std::printf("FAIL %-40s in=%-26s got=%-7s want=%s\n",
                    what, in, got ? "accept" : "reject", want ? "accept" : "reject");
    }
}

static void expect_true(const char* what, bool cond) {
    ++total;
    if (!cond) {
        ++failures;
        std::printf("FAIL %-40s (condition false)\n", what);
    }
}

static void expect_eq_d(const char* what, double got, double want) {
    ++total;
    if (got != want) {
        ++failures;
        std::printf("FAIL %-40s got=%.10g want=%.10g\n", what, got, want);
    }
}
// ---------------------------------------------------------------------------

int main() {
    // =======================================================================
    // ① Bybit execType: 14 种取值, 只有 1 种是成交
    // =======================================================================
    std::printf("-- ① Bybit execType 过滤 --\n");

    // 唯一接受
    expect_bool("bybit Trade", "Trade", mirror_bybit_accept_exec_type("Trade"), true);

    // 🔴 载重用例: Funding 不是成交。它的 execQty 是仓位规模、execPrice 是标记价,
    //    且不进 order 频道的 cumExecQty ⇒ 接受它就会凭空造量。
    expect_bool("bybit Funding (LOAD-BEARING)", "Funding", mirror_bybit_accept_exec_type("Funding"), false);

    // 其余 12 种非成交, 全部必须拒绝
    expect_bool("bybit AdlTrade",     "AdlTrade",     mirror_bybit_accept_exec_type("AdlTrade"),     false);
    expect_bool("bybit BustTrade",    "BustTrade",    mirror_bybit_accept_exec_type("BustTrade"),    false);
    expect_bool("bybit Delivery",     "Delivery",     mirror_bybit_accept_exec_type("Delivery"),     false);
    expect_bool("bybit Settle",       "Settle",       mirror_bybit_accept_exec_type("Settle"),       false);
    expect_bool("bybit BlockTrade",   "BlockTrade",   mirror_bybit_accept_exec_type("BlockTrade"),   false);
    expect_bool("bybit MovePosition", "MovePosition", mirror_bybit_accept_exec_type("MovePosition"), false);
    expect_bool("bybit FutureSpread", "FutureSpread", mirror_bybit_accept_exec_type("FutureSpread"), false);
    expect_bool("bybit FwdSplitSettle", "ForwardSplitSettle", mirror_bybit_accept_exec_type("ForwardSplitSettle"), false);
    expect_bool("bybit FwdSplitOpen",   "ForwardSplitOpen",   mirror_bybit_accept_exec_type("ForwardSplitOpen"),   false);
    expect_bool("bybit RevSplitSettle", "ReverseSplitSettle", mirror_bybit_accept_exec_type("ReverseSplitSettle"), false);
    expect_bool("bybit RevSplitOpen",   "ReverseSplitOpen",   mirror_bybit_accept_exec_type("ReverseSplitOpen"),   false);
    expect_bool("bybit UNKNOWN",        "UNKNOWN",            mirror_bybit_accept_exec_type("UNKNOWN"),            false);

    // 大小写敏感: 交易所恒发 "Trade" 这个精确拼写; 别名不该被放进来
    expect_bool("bybit lowercase trade", "trade", mirror_bybit_accept_exec_type("trade"), false);
    expect_bool("bybit TRADE",           "TRADE", mirror_bybit_accept_exec_type("TRADE"), false);
    // 字段缺失 (execType 没解析出来 ⇒ 空串) 也必须拒绝
    expect_bool("bybit missing execType", "", mirror_bybit_accept_exec_type(""), false);

    // =======================================================================
    // ② Gate text: 只接受 "t-" 前缀
    // =======================================================================
    std::printf("-- ② Gate text 过滤 --\n");

    // 接受: 我们自己的单号
    expect_bool("gate our id", "t-ABC123456789012345678", mirror_gate_accept_text("t-ABC123456789012345678"), true);
    expect_bool("gate minimal", "t-1", mirror_gate_accept_text("t-1"), true);

    // 🔴 载重用例: Gate 的保留值 —— 这些是"不是我们下的单"的成交
    expect_bool("gate web (LOAD-BEARING)",         "web",                  mirror_gate_accept_text("web"),                  false);
    expect_bool("gate api (LOAD-BEARING)",         "api",                  mirror_gate_accept_text("api"),                  false);
    expect_bool("gate app",                        "app",                  mirror_gate_accept_text("app"),                  false);
    expect_bool("gate auto_deleveraging",          "auto_deleveraging",    mirror_gate_accept_text("auto_deleveraging"),    false);
    expect_bool("gate liquidation",                "liquidation",          mirror_gate_accept_text("liquidation"),          false);
    expect_bool("gate liq-xxx",                    "liq-12345",            mirror_gate_accept_text("liq-12345"),            false);
    expect_bool("gate hedge-liq-xxx",              "hedge-liq-12345",      mirror_gate_accept_text("hedge-liq-12345"),      false);
    expect_bool("gate pm_liquidate",               "pm_liquidate",         mirror_gate_accept_text("pm_liquidate"),         false);
    expect_bool("gate comb_margin_liquidate",      "comb_margin_liquidate",mirror_gate_accept_text("comb_margin_liquidate"),false);
    expect_bool("gate scm_liquidate",              "scm_liquidate",        mirror_gate_accept_text("scm_liquidate"),        false);
    expect_bool("gate insurance",                  "insurance",            mirror_gate_accept_text("insurance"),            false);
    expect_bool("gate clear",                      "clear",                mirror_gate_accept_text("clear"),                false);

    // 边界: 空 / 单字符 / 前缀大小写 / 别的交易所的前缀
    expect_bool("gate missing text", "",        mirror_gate_accept_text(""),        false);
    expect_bool("gate single t",     "t",       mirror_gate_accept_text("t"),       false);
    expect_bool("gate upper T-",     "T-12345", mirror_gate_accept_text("T-12345"), false);
    expect_bool("gate t_ (not t-)",  "t_12345", mirror_gate_accept_text("t_12345"), false);
    expect_bool("gate bybit-style",  "by12345", mirror_gate_accept_text("by12345"), false);
    expect_bool("gate binance-style","x-12345", mirror_gate_accept_text("x-12345"), false);
    expect_bool("gate bare digits",  "12345",   mirror_gate_accept_text("12345"),   false);

    // =======================================================================
    // ③ 长度 / 字符集硬约束
    // =======================================================================
    std::printf("-- ③ orderSysId 长度/字符集约束 --\n");

    // rdtsc 十进制位数: 13 (短开机) ~ 19 (int64 上限量级)
    const char* tsc13 = "1234567890123";                  // 13 位
    const char* tsc19 = "4733640000000000000";            // 19 位 (最坏情况)
    const size_t n13 = std::string(tsc13).size();
    const size_t n19 = std::string(tsc19).size();
    expect_true("tsc13 is 13 digits", n13 == 13);
    expect_true("tsc19 is 19 digits", n19 == 19);

    // ---- Gate: "t-{sid}{rdtsc}" 去掉 "t-" 后 ≤ 28 字节 ----
    //      ⇒ 2 + len(sid) + 19 ≤ 28  ⇒ len(sid) ≤ 7?? 不, 28 是"去掉 t- 后"的口径:
    //        len(sid) + 19 ≤ 28  ⇒ len(sid) ≤ 9
    constexpr size_t kGateSidMax = kGateTextMaxBytes - 19;      // 9
    expect_true("gate sid max == 9", kGateSidMax == 9);

    // 9 字符 strategyId: 恰好卡线, 必须通过
    {
        const std::string id = mirror_gate_order_sys_id("ABCDEFGHI", tsc19);
        expect_true("gate sid=9 total==30", id.size() == 30);            // 2 (t-) + 9 + 19
        expect_true("gate sid=9 body==28 (at limit)", id.size() - 2 == kGateTextMaxBytes);
        expect_true("gate sid=9 fits 28-byte limit", id.size() - 2 <= kGateTextMaxBytes);
        expect_true("gate sid=9 fits our orderSysId[64]", id.size() < kOrderSysIdBufBytes);
        expect_true("gate sid=9 charset ok", gate_charset_ok(id));
        expect_true("gate sid=9 accepted by filter", mirror_gate_accept_text(id));
    }
    // 10 字符 strategyId: 超限 —— 交易所会**拒单**(响亮), 所以这里断言它确实超了
    {
        const std::string id = mirror_gate_order_sys_id("ABCDEFGHIJ", tsc19);
        expect_true("gate sid=10 total==31", id.size() == 31);
        expect_true("gate sid=10 EXCEEDS 28-byte limit", id.size() - 2 > kGateTextMaxBytes);
    }
    // 用 13 位 rdtsc 时, 余量更大 (上限变成 15)
    {
        const std::string id = mirror_gate_order_sys_id("ABCDEFGHIJKLMNO", tsc13);   // 15 字符 sid
        expect_true("gate sid=15 with tsc13 fits", id.size() - 2 <= kGateTextMaxBytes);
    }

    // ---- Bybit: "by{sid}{rdtsc}" 总长 ≤ 36 字节 ----
    //      ⇒ 2 + len(sid) + 19 ≤ 36  ⇒ len(sid) ≤ 15
    constexpr size_t kBybitSidMax = kBybitLinkIdMaxBytes - 2 - 19;   // 15
    expect_true("bybit sid max == 15", kBybitSidMax == 15);
    {
        const std::string id = mirror_bybit_order_sys_id("ABCDEFGHIJKLMNO", tsc19);   // 15 字符
        expect_true("bybit sid=15 total==36", id.size() == 36);
        expect_true("bybit sid=15 fits 36-byte limit", id.size() <= kBybitLinkIdMaxBytes);
        expect_true("bybit sid=15 fits our orderSysId[64]", id.size() < kOrderSysIdBufBytes);
        expect_true("bybit sid=15 charset ok", bybit_charset_ok(id));
    }
    {
        const std::string id = mirror_bybit_order_sys_id("ABCDEFGHIJKLMNOP", tsc19);  // 16 字符
        expect_true("bybit sid=16 total==37", id.size() == 37);
        expect_true("bybit sid=16 EXCEEDS 36-byte limit", id.size() > kBybitLinkIdMaxBytes);
    }

    // ---- 两边的 strategyId 都远小于 pubsub 的 strategyId[32] 容量, 所以缓冲不是瓶颈 ----
    expect_true("gate sid max < strategyId[32]", kGateSidMax + 1 < kStrategyIdBufBytes);
    expect_true("bybit sid max < strategyId[32]", kBybitSidMax + 1 < kStrategyIdBufBytes);

    // ---- 字符集: Bybit 不允许 '.', Gate 允许 ----
    expect_true("bybit charset rejects dot",  !bybit_charset_ok("byABC.DEF"));
    expect_true("bybit charset accepts -/_",   bybit_charset_ok("byABC-DEF_123"));
    expect_true("bybit charset rejects space", !bybit_charset_ok("byABC DEF"));
    expect_true("gate charset accepts dot",    gate_charset_ok("t-ABC.DEF"));
    expect_true("gate charset rejects space",  !gate_charset_ok("t-ABC DEF"));
    expect_true("gate charset rejects empty body", !gate_charset_ok("t-"));

    // ---- 两条通道的前缀天然互斥 (跨所不会撞键) ----
    expect_true("gate id rejected by bybit prefix-check",  mirror_gate_order_sys_id("S1", tsc19).rfind("by", 0) != 0);
    expect_true("bybit id rejected by gate filter",        !mirror_gate_accept_text(mirror_bybit_order_sys_id("S1", tsc19)));

    // =======================================================================
    // ④ 数量口径一致性
    // =======================================================================
    std::printf("-- ④ 数量口径一致性 --\n");

    // Gate US: 报单路径与成交路径必须给同一单位 (不缩放)
    for (const double sz : {0.001, 1.0, 12.5, -12.5, 1e6}) {
        expect_eq_d("gate US volumeTotal == tradeDiff",
                    mirror_gate_us_orders_volume_total(sz), mirror_gate_us_trade_diff(sz));
    }
    // Gate SP: 两条路径都必须带 magnifyNumber (含 mag != 1 的情形 —— 证明它们会一致)
    for (const double mag : {1.0, 1000.0, 0.001}) {
        for (const double amt : {0.001, 1.0, 12.5, 1e6}) {
            expect_eq_d("gate SP volumeTotal == tradeDiff (scaled)",
                        mirror_gate_sp_orders_volume_total(amt, mag), mirror_gate_sp_trade_diff(amt, mag));
        }
    }
    // 反证: 如果成交路径漏了 magnifyNumber, 在 mag != 1 时就会错 —— 这正是要防的回归
    {
        const double amt = 12.5, mag = 1000.0;
        expect_true("counter-proof: dropping magnifyNumber diverges",
                    mirror_gate_sp_orders_volume_total(amt, mag) != std::fabs(amt));
    }
    // Gate US 路径**故意不缩放**: 它的报单路径也不缩放, 所以两边一致;
    // 反证: 若单方面加上 magnifyNumber 就会和报单路径脱节
    {
        const double sz = 12.5, mag = 1000.0;
        expect_true("counter-proof: scaling gate US would diverge",
                    mirror_gate_us_orders_volume_total(sz) != std::fabs(sz) * mag);
    }

    // =======================================================================
    // ⑤ 三条通道的结构同构 (文档表, 也是"OMS 零改动"的依据)
    // =======================================================================
    std::printf("\n-- ⑤ 三条通道结构同构表 --\n");
    std::printf("%-12s %-22s %-16s %-22s %s\n", "exchange", "locate key", "cmdType", "tradeDiff", "orderStatus/offsetFlag");
    std::printf("%-12s %-22s %-16s %-22s %s\n", "OKX",  "clOrdId (裸数字)", "CMD_RPT_TRADE", "fillSz  (增量)", "PARTFILLED / OF_OPEN");
    std::printf("%-12s %-22s %-16s %-22s %s\n", "Bybit", "orderLinkId", "CMD_RPT_TRADE", "execQty (增量)", "PARTFILLED / OF_OPEN");
    std::printf("%-12s %-22s %-16s %-22s %s\n", "GateUS", "text (t-前缀)", "CMD_RPT_TRADE", "fabs(size) (增量)", "PARTFILLED / OF_OPEN");
    std::printf("%-12s %-22s %-16s %-22s %s\n", "GateSP", "text (t-前缀)", "CMD_RPT_TRADE", "fabs(amount)*mag", "PARTFILLED / OF_OPEN");

    std::printf("\n%d/%d passed, %d failed\n", total - failures, total, failures);
    return failures ? 1 : 0;
}
