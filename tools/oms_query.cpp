// =============================================================================
// oms_query.cpp — OMS SHM 查询工具
//
// 用法:
//   oms_query --shm=/dev/shm/tb_oms.dat --sys=<orderSysId>
//   oms_query --shm=/dev/shm/tb_oms.dat --cid=<clientOrderId>
//   oms_query --shm=/dev/shm/tb_oms.dat --oid=<exchangeOrderId>
//   oms_query --shm=/dev/shm/tb_oms.dat --list-live
//   oms_query --shm=/dev/shm/tb_oms.dat --list-finished
//   oms_query --shm=/dev/shm/tb_oms.dat --stats
//
// 编译:
//   g++ -std=c++17 -O2 -I../../include -I../include \
//       oms_query.cpp -o oms_query
// =============================================================================

#include "oms/OmsShm.h"
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>

using namespace oms::shm;

static const char* state_name(uint32_t s) {
    switch (s) {
        case SLOT_EMPTY:      return "EMPTY";
        case SLOT_LIVE:       return "LIVE";
        case SLOT_FINISHED:   return "FIN";
        case SLOT_RECLAIMING: return "RECL";
        default:              return "?";
    }
}

static const char* order_status_name(int s) {
    switch (s) {
        case OS_MIN:          return "MIN";
        case OS_PEND:         return "PEND";
        case OS_PENDING_NEW:  return "PENDING_NEW";
        case OS_NEW:          return "NEW";
        case OS_PARTFILLED:   return "PARTFILLED";
        case OS_FILLED:       return "FILLED";
        case OS_REJECTED:     return "REJECTED";
        case OS_CANCEL:       return "CANCEL";
        case OS_CANCELLING:   return "CANCELLING";
        case OS_CANCELED:     return "CANCELED";
        case OS_UNKNOWN:      return "UNKNOWN";
        case OS_FAILED:       return "FAILED";
        default:              return "?";
    }
}

static void print_rcmd(const pubsub::RCommand& r) {
    const auto& o = r.body.orderResponse;
    std::printf("  orderSysId    : %s\n", o.orderSysId);
    std::printf("  clientOrderId : %lld\n", static_cast<long long>(o.clientOrderId));
    std::printf("  orderId       : %s\n", o.orderId);
    std::printf("  instId        : %s\n", o.instId);
    std::printf("  status        : %s (%d)\n", order_status_name(o.orderStatus), o.orderStatus);
    std::printf("  direction     : %d\n", o.direction);
    std::printf("  volumeTotal   : %.6f\n", o.volumeTotal);
    std::printf("  volumeTraded  : %.6f\n", o.volumeTraded);
    std::printf("  limitPrice    : %.6f\n", o.limitPrice);
    std::printf("  tradePrice    : %.6f\n", o.tradePrice);
    std::printf("  errorId       : %d\n", o.errorId);
    std::printf("  originMsg     : %s\n", o.originMsg);
    std::printf("  updateTime    : %lld\n", static_cast<long long>(o.updateTime));
}

static void print_header_row() {
    std::printf("%-6s  %-6s  %-24s  %-12s  %-12s  %-12s\n",
                "SLOT", "STATE", "orderSysId", "orderId", "status", "instId");
    std::printf("%-6s  %-6s  %-24s  %-12s  %-12s  %-12s\n",
                "----", "-----", "----------", "-------", "------", "------");
}

static void print_slot_row(uint32_t idx, const OmsSlot& s) {
    std::printf("%-6u  %-6s  %-24s  %-12s  %-12s  %-12s\n",
                idx,
                state_name(s.state.load(std::memory_order_relaxed)),
                s.orderSysId,
                s.orderId,
                order_status_name(s.order.body.orderResponse.orderStatus),
                s.order.body.orderResponse.instId);
}

static void usage() {
    std::fprintf(stderr,
        "Usage:\n"
        "  oms_query --shm=<path> --sys=<orderSysId>\n"
        "  oms_query --shm=<path> --strategy=<sid> --cid=<cid>   (推荐, 复合查)\n"
        "  oms_query --shm=<path> --cid=<composed_string>        (兜底, 已复合)\n"
        "  oms_query --shm=<path> --oid=<exchangeOrderId>\n"
        "  oms_query --shm=<path> --list-live\n"
        "  oms_query --shm=<path> --list-finished\n"
        "  oms_query --shm=<path> --list-stale\n"
        "  oms_query --shm=<path> --stats\n");
}

int main(int argc, char** argv) {
    std::string shm_path = "/dev/shm/tb_oms.dat";
    std::string key_sys, key_cid, key_oid, key_strategy;
    bool do_list_live = false, do_list_finished = false, do_list_stale = false, do_stats = false;

    for (int i = 1; i < argc; ++i) {
        std::string_view a(argv[i]);
        if      (a.rfind("--shm=", 0) == 0)      shm_path     = std::string(a.substr(6));
        else if (a.rfind("--sys=", 0) == 0)      key_sys      = std::string(a.substr(6));
        else if (a.rfind("--cid=", 0) == 0)      key_cid      = std::string(a.substr(6));
        else if (a.rfind("--strategy=", 0) == 0) key_strategy = std::string(a.substr(11));
        else if (a.rfind("--oid=", 0) == 0)      key_oid      = std::string(a.substr(6));
        else if (a == "--list-live")             do_list_live = true;
        else if (a == "--list-finished")         do_list_finished = true;
        else if (a == "--list-stale")            do_list_stale = true;
        else if (a == "--stats")                 do_stats     = true;
        else if (a == "-h" || a == "--help") { usage(); return 0; }
        else { std::fprintf(stderr, "unknown arg: %s\n", argv[i]); usage(); return 1; }
    }

    OmsShmReader r;
    try {
        r.open(shm_path);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "open %s failed: %s\n", shm_path.c_str(), e.what());
        return 2;
    }

    // 单条查询
    if (!key_sys.empty() || !key_cid.empty() || !key_oid.empty()) {
        pubsub::RCommand out;
        bool ok = false;
        const char* kind = "";
        if (!key_sys.empty()) { ok = r.lookup_by_orderSysId(key_sys, out); kind = "orderSysId"; }
        if (!ok && !key_cid.empty()) {
            // 优先: --strategy=X --cid=N → 复合 key 查
            // 兜底: --cid=<已复合字符串> 直接查 (e.g. --cid=strat110002)
            char* endp = nullptr;
            long long v = std::strtoll(key_cid.c_str(), &endp, 10);
            bool is_int = (endp && *endp == '\0' && !key_cid.empty());
            if (is_int && !key_strategy.empty()) {
                ok = r.lookup_by_client(std::string_view(key_strategy),
                                        static_cast<int64_t>(v), out);
            }
            if (!ok) ok = r.lookup_by_clientOrderId(std::string_view(key_cid), out);
            kind = "clientOrderId";
        }
        if (!ok && !key_oid.empty()) { ok = r.lookup_by_orderId(key_oid, out); kind = "orderId"; }
        if (!ok) { std::printf("NOT FOUND\n"); return 3; }
        std::printf("Found by %s:\n", kind);
        print_rcmd(out);
        return 0;
    }

    // 列表
    if (do_list_live) {
        print_header_row();
        uint32_t cap = r.slot_capacity();
        size_t n = 0;
        for (uint32_t i = 0; i < cap; ++i) {
            const OmsSlot& s = r.slots()[i];
            if (s.state.load(std::memory_order_relaxed) != SLOT_LIVE) continue;
            print_slot_row(i, s);
            ++n;
        }
        std::printf("\n%zu live order(s)\n", n);
        return 0;
    }
    if (do_list_finished) {
        print_header_row();
        uint32_t cap = r.slot_capacity();
        size_t n = 0;
        for (uint32_t i = 0; i < cap; ++i) {
            const OmsSlot& s = r.slots()[i];
            if (s.state.load(std::memory_order_relaxed) != SLOT_FINISHED) continue;
            print_slot_row(i, s);
            ++n;
        }
        std::printf("\n%zu finished order(s)\n", n);
        return 0;
    }

    // --list-stale: 打印所有已达 max_live_stale 阈值、下次 alloc 可能被强制回收的 LIVE 单
    if (do_list_stale) {
        uint64_t now = OmsShmSegment::now_ns();
        uint64_t max_live_stale = r.header()->max_live_stale_ns;
        std::printf("threshold: max_live_stale = %.1f s\n", max_live_stale / 1e9);
        std::printf("current:   %llu ns\n", (unsigned long long)now);
        std::printf("\n");
        std::printf("%-6s  %-24s  %-12s  %-12s  %-12s\n",
                    "SLOT", "orderSysId", "instId", "status", "idle_seconds");
        std::printf("%-6s  %-24s  %-12s  %-12s  %-12s\n",
                    "----", "----------", "------", "------", "------------");
        uint32_t cap = r.slot_capacity();
        size_t n = 0;
        for (uint32_t i = 0; i < cap; ++i) {
            const OmsSlot& s = r.slots()[i];
            if (s.state.load(std::memory_order_relaxed) != SLOT_LIVE) continue;
            uint64_t idle = now - s.last_update_time_ns;
            if (max_live_stale == 0 || idle < max_live_stale) continue;
            std::printf("%-6u  %-24s  %-12s  %-12s  %.1f\n",
                        i, s.orderSysId,
                        s.order.body.orderResponse.instId,
                        order_status_name(s.order.body.orderResponse.orderStatus),
                        idle / 1e9);
            ++n;
        }
        std::printf("\n%zu stale LIVE order(s) — 若在有新单落地时会被强制回收\n", n);
        return 0;
    }

    // stats
    if (do_stats) {
        auto s = r.stats();
        std::printf("SHM: %s\n", shm_path.c_str());
        std::printf("  capacity                   : %u\n", s.capacity);
        std::printf("  empty                      : %u (%.1f%%)\n", s.empty,      100.0 * s.empty      / s.capacity);
        std::printf("  live (total)               : %u (%.1f%%)\n", s.live,       100.0 * s.live       / s.capacity);
        std::printf("    ├─ fresh                 : %u\n", s.live_fresh);
        std::printf("    └─ stale (卡单, 待强制回收): %u\n", s.live_stale);
        std::printf("  finished                   : %u (%.1f%%)\n", s.finished,   100.0 * s.finished   / s.capacity);
        std::printf("  reclaiming                 : %u\n", s.reclaiming);
        std::printf("  --\n");
        std::printf("  min_reclaim_age            : %.1f s (FINISHED 最小 TTL)\n", s.min_reclaim_age_ns / 1e9);
        std::printf("  max_live_stale             : %.1f s (LIVE 无更新阈值, 0=禁用)\n", s.max_live_stale_ns / 1e9);
        std::printf("  --\n");
        std::printf("  total_inserts              : %llu\n", (unsigned long long)s.total_inserts);
        std::printf("  total_updates              : %llu\n", (unsigned long long)s.total_updates);
        std::printf("  total_reclaims             : %llu\n", (unsigned long long)s.total_reclaims);
        std::printf("  total_stale_live_reclaims  : %llu  ← 非零说明有卡单被强制回收, 排查!\n",
                                                             (unsigned long long)s.total_stale_live_reclaims);
        std::printf("  total_alloc_failures       : %llu\n", (unsigned long long)s.total_alloc_failures);
        return 0;
    }

    usage();
    return 1;
}
