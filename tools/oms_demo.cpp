// =============================================================================
// oms_demo.cpp — OMS SHM 使用示例
//
// 演示完整的 writer + reader 工作流:
//   1. writer 打开 SHM (不存在则创建)
//   2. 插入 5 条 LIVE 单
//   3. 更新 2 条到 PARTFILLED, 1 条到 FILLED (终结)
//   4. 用不同 key (orderSysId / clientOrderId int64 / orderId) 查询
//   5. 遍历 LIVE / FINISHED, 打印统计
//   6. 演示崩溃恢复 (人为把某 slot 置 RECLAIMING, 重开触发 recover)
//
// 用法:
//   oms_demo [--shm=<path>] [--reset]
// =============================================================================

#include "oms/OmsShm.h"
#include <cstdio>
#include <cstring>
#include <string>

using namespace oms::shm;

static void make_order(pubsub::RCommand& r,
                       const char* sysId, int64_t cid, const char* orderId,
                       const char* instId, OrderStatus st, double vol, double filled)
{
    std::memset(&r, 0, sizeof(r));
    r.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
    auto& o = r.body.orderResponse;
    o.exchangeTypeEnum = BINANCE;
    o.instTypeEnum     = SPOT;
    std::snprintf(o.accountId,  sizeof(o.accountId),  "demo_acc");
    std::snprintf(o.strategyId, sizeof(o.strategyId), "demo_strat");
    std::snprintf(o.instId,     sizeof(o.instId),     "%s", instId);
    std::snprintf(o.orderSysId, sizeof(o.orderSysId), "%s", sysId);
    std::snprintf(o.orderId,    sizeof(o.orderId),    "%s", orderId);
    o.clientOrderId = cid;
    o.direction     = DT_LONG;
    o.orderType     = OT_LIMIT;
    o.orderStatus   = st;
    o.volumeTotal   = vol;
    o.volumeTraded  = filled;
    o.limitPrice    = 50000.0;
    o.tradePrice    = filled > 0 ? 49950.0 : 0.0;
    o.updateTime    = 0;
}

static void print_rcmd(const char* label, const pubsub::RCommand& r) {
    const auto& o = r.body.orderResponse;
    std::printf("  [%s] sysId=%s cid=%lld oid=%s status=%d filled=%.2f/%.2f\n",
                label,
                o.orderSysId,
                static_cast<long long>(o.clientOrderId),
                o.orderId,
                o.orderStatus,
                o.volumeTraded, o.volumeTotal);
}

int main(int argc, char** argv) {
    std::string shm_path = "/dev/shm/tb_oms_demo.dat";
    bool do_reset = false;

    for (int i = 1; i < argc; ++i) {
        std::string_view a(argv[i]);
        if (a.rfind("--shm=", 0) == 0) shm_path = std::string(a.substr(6));
        else if (a == "--reset") do_reset = true;
    }

    std::printf("=== OmsShm demo ===\n");
    std::printf("shm path: %s\n\n", shm_path.c_str());

    // ---- 1. Writer 打开 (自动创建 / 崩溃恢复) ----
    OmsShmWriter w;
    try {
        w.open(shm_path, /*capacity=*/1024);   // demo 用小容量便于观察
    } catch (const std::exception& e) {
        std::fprintf(stderr, "writer open failed: %s\n", e.what());
        return 1;
    }
    std::printf("[1] Writer opened (created=%s capacity=%u)\n",
                w.created_new() ? "new" : "existing", w.slot_capacity());
    if (do_reset) {
        w.reset_all();
        std::printf("    (reset_all cleared all slots)\n");
    }

    // ---- 2. 插入 5 条 LIVE 单 ----
    pubsub::RCommand r;
    make_order(r, "demo-sys-1001", 10001, "ex-8001001", "BTC-USDT", OS_NEW, 100.0, 0.0);
    uint32_t s1 = w.upsert(r);
    make_order(r, "demo-sys-1002", 10002, "ex-8001002", "ETH-USDT", OS_NEW, 50.0, 0.0);
    uint32_t s2 = w.upsert(r);
    make_order(r, "demo-sys-1003", 10003, "ex-8001003", "SOL-USDT", OS_NEW, 200.0, 0.0);
    uint32_t s3 = w.upsert(r);
    make_order(r, "demo-sys-1004", 10004, "ex-8001004", "BNB-USDT", OS_NEW, 30.0, 0.0);
    uint32_t s4 = w.upsert(r);
    make_order(r, "demo-sys-1005", 10005, "ex-8001005", "XRP-USDT", OS_NEW, 5000.0, 0.0);
    uint32_t s5 = w.upsert(r);
    std::printf("[2] Inserted 5 LIVE orders → slots %u/%u/%u/%u/%u\n\n",
                s1, s2, s3, s4, s5);

    // ---- 3. 部分成交 / 完全成交 ----
    make_order(r, "demo-sys-1001", 10001, "ex-8001001", "BTC-USDT", OS_PARTFILLED, 100.0, 40.0);
    w.upsert(r);
    make_order(r, "demo-sys-1002", 10002, "ex-8001002", "ETH-USDT", OS_PARTFILLED, 50.0, 25.0);
    w.upsert(r);
    make_order(r, "demo-sys-1003", 10003, "ex-8001003", "SOL-USDT", OS_FILLED,      200.0, 200.0);
    w.upsert(r);   // → FINISHED
    std::printf("[3] Updated: 1001/1002 → PARTFILLED, 1003 → FILLED (terminal)\n\n");

    // ---- 4. Reader 打开, 用三种 key 查 ----
    OmsShmReader rd;
    rd.open(shm_path);

    pubsub::RCommand out;
    std::printf("[4] Lookups:\n");

    if (rd.lookup_by_orderSysId("demo-sys-1001", out))
        print_rcmd("by orderSysId", out);
    else
        std::printf("  by orderSysId    NOT FOUND\n");

    // 主接口: 复合查, 需 strategyId + cid
    if (rd.lookup_by_client(std::string_view("demo_strat"), static_cast<int64_t>(10002), out))
        print_rcmd("by client (strat,cid)", out);

    // 底层: 直接用复合字符串
    if (rd.lookup_by_clientOrderId(std::string_view("demo_strat10002"), out))
        print_rcmd("by composed key      ", out);

    if (rd.lookup_by_orderId("ex-8001003", out))
        print_rcmd("by orderId       ", out);

    if (rd.lookup_by_orderSysId("demo-sys-nonexistent", out))
        print_rcmd("  ", out);
    else
        std::printf("  by orderSysId=nonexistent  → NOT FOUND (expected)\n");
    std::printf("\n");

    // ---- 5. 遍历 LIVE / FINISHED ----
    std::printf("[5] iterate_live():\n");
    rd.iterate_live([](const pubsub::RCommand& r){
        std::printf("  LIVE  sysId=%s status=%d filled=%.0f/%.0f\n",
                    r.body.orderResponse.orderSysId,
                    r.body.orderResponse.orderStatus,
                    r.body.orderResponse.volumeTraded,
                    r.body.orderResponse.volumeTotal);
    });
    std::printf("[5] iterate_finished():\n");
    rd.iterate_finished([](const pubsub::RCommand& r){
        std::printf("  FIN   sysId=%s status=%d filled=%.0f/%.0f\n",
                    r.body.orderResponse.orderSysId,
                    r.body.orderResponse.orderStatus,
                    r.body.orderResponse.volumeTraded,
                    r.body.orderResponse.volumeTotal);
    });
    std::printf("\n");

    // ---- 6. Stats ----
    auto st = rd.stats();
    std::printf("[6] Stats:\n");
    std::printf("  capacity=%u empty=%u live=%u (fresh=%u stale=%u) finished=%u\n",
                st.capacity, st.empty, st.live, st.live_fresh, st.live_stale, st.finished);
    std::printf("  total_inserts=%llu updates=%llu reclaims=%llu stale_live_reclaims=%llu\n",
                (unsigned long long)st.total_inserts,
                (unsigned long long)st.total_updates,
                (unsigned long long)st.total_reclaims,
                (unsigned long long)st.total_stale_live_reclaims);
    std::printf("\n");

    // ---- 7. 演示 crash recovery: 手动把 slot s4 置 RECLAIMING (模拟 tb 崩在 alloc 之后)
    std::printf("[7] Simulating crash: forcing slot %u state=RECLAIMING\n", s4);
    w.slots()[s4].state.store(SLOT_RECLAIMING, std::memory_order_release);
    w.close();
    // 重新打开, 应该自动 recover
    std::printf("    reopening writer → should auto-recover orphan slot\n");
    OmsShmWriter w2;
    w2.open(shm_path, 1024);
    // 检查 s4 是否被归位
    auto st2 = w2.stats();
    std::printf("    after open: empty=%u live=%u (slot %u should be EMPTY now)\n",
                st2.empty, st2.live, s4);
    std::printf("    slot %u state now = %u (%s)\n",
                s4,
                w2.slots()[s4].state.load(std::memory_order_relaxed),
                w2.slots()[s4].state.load(std::memory_order_relaxed) == SLOT_EMPTY
                    ? "EMPTY ✓" : "not empty!");
    return 0;
}
