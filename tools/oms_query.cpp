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
        std::printf("  index_capacity             : %u (每个索引 = next_pow2(4*slot_cap))\n",
                    s.index_capacity);
        // ★ 数值行**必须**以数字结尾 (doctor 用 `grep -oE '[0-9]+' | head -1` 取第一个数字)。
        //   这里原来是 `probe_max : 32 (索引 open-addressing 探测步数上限)` —— 行尾挂了说明,
        //   只是碰巧那段中文里没有数字才没出事。按约定把提示挪到下一行。
        std::printf("  probe_max                  : %u\n", s.probe_max);
        std::printf("      ← 索引 open-addressing 探测步数上限\n");
        {
            uint32_t tight = 0;
            uint32_t worst_run = 0;
            for (uint32_t k = 0; k < oms::shm::IDX_COUNT; ++k) {
                const auto& o = s.index_occupancy[k];
                std::printf("  index %-14s       : live=%-8u tomb=%-8u empty=%-8u 占用 %.2f%%\n",
                            oms::shm::index_kind_name(k), o.live, o.tomb, o.empty,
                            s.index_capacity ? 100.0 * o.used() / s.index_capacity : 0.0);
                std::printf("      └─ 最长 live 连续段 %u / probe 上限 %u (余量 %.2fx)\n",
                            o.max_live_run, s.probe_max,
                            o.max_live_run ? static_cast<double>(s.probe_max) / o.max_live_run : 0.0);
                if (o.empty == 0) ++tight;
                if (o.max_live_run > worst_run) worst_run = o.max_live_run;
            }
            // 注意: slot 环填满后 live≈slot_cap、tomb≈slot_cap, 于是 empty→0 是**稳态正常现象**
            // (slot_cap 是 2 的幂时更是恰好 empty==0)。所以这里只做说明, 不当告警。
            if (tight) {
                std::printf("  [i] %u/%u 个索引 empty=0 —— slot 环填满后这是正常现象\n"
                            "      (live≈slot_cap, tomb≈slot_cap)。环满后 empty 必然排干到 0,\n"
                            "      插入能否成功只看最长 live 连续段与 probe 上限的比值 (见上一行)\n",
                            tight, oms::shm::IDX_COUNT);
            }
            // ★ 探测余量判读 (B6, OMS_SHM_REVIEW.md §2.5): 环满后 empty 必然为 0, 插入必须
            //   走到某个可复用 bucket, 而连续 live 段一个都不提供 —— 所以"最长连续 live 段
            //   >= probe 上限"就等于**已经在丢单** (total_alloc_failures 会随之增长)。
            if (worst_run >= s.probe_max) {
                std::printf("  [!] 最长 live 连续段 %u >= probe 上限 %u —— 已经在丢单\n"
                            "      加大 slot_cap 没用 (empty 照样排干到 0); 要加大 index_capacity 的倍数\n",
                            worst_run, s.probe_max);
            } else if (worst_run * 3 >= s.probe_max * 2) {
                std::printf("  [i] 最长 live 连续段 %u, 对 probe 上限 %u 余量不足 1.5 倍 —— 留意\n",
                            worst_run, s.probe_max);
            }
            // ★ 数值行以数字结尾 (oms_shm.sh doctor 取值用); 提示写在 key 里, 行尾不挂文字。
            std::printf("  worst_live_run (最长 live 连续段) : %u\n", worst_run);
        }
        std::printf("  --\n");
        std::printf("  total_inserts              : %llu\n", (unsigned long long)s.total_inserts);
        std::printf("  total_updates              : %llu\n", (unsigned long long)s.total_updates);
        std::printf("  total_reclaims             : %llu\n", (unsigned long long)s.total_reclaims);
        // ★ 数值行**必须**以数字结尾: oms_shm.sh doctor 用 awk '{print $NF}' 取值,
        //   行尾挂中文提示会让它取到提示文字, 检查静默失效 (见 OMS_SHM_REVIEW.md §0.4)。
        //   提示一律另起一行。
        std::printf("  total_stale_live_reclaims  : %llu\n",
                    (unsigned long long)s.total_stale_live_reclaims);
        if (s.total_stale_live_reclaims) {
            std::printf("      ← 非零: 有卡单被强制回收, 排查!\n");
        }
        std::printf("  total_alloc_failures       : %llu\n",
                    (unsigned long long)s.total_alloc_failures);
        if (s.total_alloc_failures) {
            std::printf("      ← 非零: 有单没写进 SHM! 看上面的 index live/tomb/empty 与 index_capacity\n");
        }
        // ★ ③ (alloc 快/慢路径) 与 ② (key 同步失败) 的专属计数。数值行同样以数字结尾。
        std::printf("  total_alloc_slowpath       : %llu\n",
                    (unsigned long long)s.total_alloc_slowpath);
        if (s.total_alloc_slowpath) {
            std::printf("      ← 非零: 快路径 128 步落空、被全表扫描**救回来**的次数。单没丢,\n"
                        "        但说明 hint 前方有 >= 128 个连续不可回收 slot —— 在途单过于集中,\n"
                        "        要加大 slot_capacity, 或让上层及时 finalize 订单。\n");
        }
        std::printf("  total_alloc_exhausted      : %llu\n",
                    (unsigned long long)s.total_alloc_exhausted);
        if (s.total_alloc_exhausted) {
            std::printf("      ← 非零: 快慢两遍扫描都没找到可回收 slot → **真·环满, 单丢了**。\n"
                        "        看上面的 live / finished / reclaiming 与 min_reclaim_age。\n");
        }
        std::printf("  total_key_sync_failures    : %llu\n",
                    (unsigned long long)s.total_key_sync_failures);
        if (s.total_key_sync_failures) {
            std::printf("      ← 非零: key 变更时索引插入失败, 已**保持旧 key 不变** (副本与索引仍一致)。\n"
                        "        说明索引饱和, 看上面的 index live/tomb/empty 与 index_capacity。\n");
        }
        std::printf("  total_alias_insert_failures: %llu\n",
                    (unsigned long long)s.total_alias_insert_failures);
        if (s.total_alias_insert_failures) {
            std::printf("      ← 非零: 主索引 OK 但 clientOrderId / orderId 别名插入失败。\n"
                        "        单**在** SHM 里 (按 orderSysId 查得到), 但用该别名查不到 ——\n"
                        "        策略侧按 clientOrderId 查活单会落空 (漏撤单 / 重复下单)。\n"
                        "        不计入 total_alloc_failures; 同样说明索引饱和。\n");
        }
        return 0;
    }

    usage();
    return 1;
}