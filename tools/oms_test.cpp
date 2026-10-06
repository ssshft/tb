// =============================================================================
// oms_test.cpp — OMS SHM 功能 / 边界 / 并发 断言集
//
// 这是 tb/tools 下**唯一**需要长期保留的测试入口 (替代此前散落在 /tmp 的
// verify / sync_invariant / alloc_scan / race 等一次性 harness)。
//
// 用法:
//   ./oms_test [scratch_dir]        # 默认 /tmp/oms_test
//   ./oms_shm.sh test [scratch_dir] # 编译 + 跑本程序 + 解析契约检查
//
// 退出码: 0 = 全部通过; 1 = 有 FAIL。
//
// 覆盖范围 (每一节对应一类"曾经出过 / 差点出过"的问题):
//   [1]  布局与常量         — sizeof / kVersion / next_pow2 / 探测序列满射性 (P1-1)
//   [2]  三层索引往返       — orderSysId / 复合 clientOrderId / orderId
//   [3]  orderId 生命周期   — "" → 首次回报设定 → 再变 (user-taught 语义)
//   [4]  生命周期与回收     — LIVE → FINISHED → reclaim, 索引 tombstone
//   [5]  绝不覆盖 LIVE      — 环满时新单失败而不是踩掉活单 (③ 真·环满)
//   [6]  ③ 慢路径救回       — 128 步窗口落空但全表扫有 → 救回 + 计数
//   [7]  ② key 同步失败     — 索引饱和时保持旧 key (新旧都不丢, 不是"新 key 丢")
//   [8]  B4 别名插入失败    — 主索引 OK + 别名失败 → 单在 SHM 里, 别名计数 +1
//   [9]  B5 副本==索引      — key 迁移后旧 key 不再命中, 新 key 命中
//   [10] 卡单强制回收       — LIVE 超 max_live_stale → 强制回收 + 计数
//   [11] 稳态 tombstone     — 环满后 empty→0 是设计如此, 不是泄漏
//   [12] B6 探测余量        — max_live_run < kMaxProbeIndex, 且不丢单
//   [13] A2 iterate 跳过计数— 对账 API 必须能报告"跳过了几条"
//   [14] 版本 / 容量拒绝    — 老文件 / 非 2 的幂 / 2N 索引 / 越界 capacity
//   [15] 崩溃恢复           — RECLAIMING 残留 slot 归位 EMPTY + tombstone
//   [16] reset_all          — 清零**全部**计数 (含 v4 补漏的那几个)
//   [17] 并发 seqlock       — 1 writer + N reader, 断言 0 撕裂读
//   [18] 其余公开接口       — remove / recover_orphan_slots / is_open / created_new / close
//   [19] TTL 语义           — FINISHED 的回收门槛 = min_reclaim_age_ns (环满事故的机理)
//   [20] B3 lookup 三态     — OK / NOT_FOUND / BUSY, 且 NOT_FOUND 不得计成 BUSY
//
// 编译:
//   g++ -std=c++17 -O2 -pthread -I../../include -I../include \
//       oms_test.cpp -o oms_test
// =============================================================================

#include "oms/OmsShm.h"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace oms::shm;

// -----------------------------------------------------------------------------
// 极简断言框架
// -----------------------------------------------------------------------------
static int  g_pass = 0;
static int  g_fail = 0;
static const char* g_section = "";

static void section(const char* s) {
    g_section = s;
    std::printf("\n[%s]\n", s);
}

static void check(bool ok, const std::string& what, const std::string& detail = "") {
    const std::string tail = detail.empty() ? std::string() : ("   — " + detail);
    if (ok) { ++g_pass; std::printf("  [PASS] %s%s\n", what.c_str(), tail.c_str()); }
    else    { ++g_fail; std::printf("  [FAIL] %s%s\n", what.c_str(), tail.c_str()); }
}

static std::string u(uint64_t v) { return std::to_string((unsigned long long)v); }

// -----------------------------------------------------------------------------
// 构造测试用 RCommand
// -----------------------------------------------------------------------------
static pubsub::RCommand mk(const char* sysid, const char* oid, OrderStatus st,
                           int64_t cid, const char* strategy = "sss_test1") {
    pubsub::RCommand c{};
    c.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
    auto& o = c.body.orderResponse;
    std::snprintf(o.orderSysId, sizeof(o.orderSysId), "%s", sysid);
    std::snprintf(o.orderId,    sizeof(o.orderId),    "%s", oid);
    std::snprintf(o.strategyId, sizeof(o.strategyId), "%s", strategy);
    std::snprintf(o.instId,     sizeof(o.instId),     "BTC-USDT");
    o.clientOrderId = cid;
    o.orderStatus   = st;
    o.volumeTotal   = 100.0;
    o.volumeTraded  = (st == OS_FILLED) ? 100.0 : 0.0;
    o.limitPrice    = 50000.0;
    return c;
}

static std::string j(const std::string& dir, const char* name) {
    return dir + "/" + name;
}

// 删掉旧文件再以 cap 新建 (每次测试都从干净状态开始)
static void fresh(OmsShmWriter& w, const std::string& dir, const char* name, uint32_t cap) {
    const std::string p = j(dir, name);
    ::unlink(p.c_str());
    w.open(p, cap);
}

// -----------------------------------------------------------------------------
// 索引"阻塞器": 把 key 的整条探测序列 (kMaxProbeIndex 个 bucket) 全部占成
// "指向 target_slot 的其它 key"。insert_index 走满 32 步都拿不到可复用 bucket
// → 返回 false。用来确定性地制造"索引饱和", 不用靠碰运气凑 hash 聚集。
//   target_slot 的该 kind key 副本必须**非空且不等于 key** (否则算 kStale, 会被复用)。
// -----------------------------------------------------------------------------
static void plant_blockers(OmsShmWriter& w, IndexKind kind,
                           std::string_view key, uint32_t target_slot) {
    IndexEntry* arr = w.index(kind);
    const uint32_t cap = w.index_capacity();
    const uint32_t mask = cap - 1;
    const uint64_t h = fnv1a(key);
    for (uint32_t i = 0; i < kMaxProbeIndex; ++i) {
        const uint32_t b = index_probe_bucket(h, i, cap, mask, is_pow2(cap));
        IndexEntry& e = arr[b];
        std::memset(e.key_prefix, 0, sizeof(e.key_prefix));
        e.key_prefix[0] = '\x01';            // 与任何真实 key 前缀都不同
        e.slot_idx.store(target_slot, std::memory_order_relaxed);
        e.key_hash.store(0x00DEAD000000ULL + i, std::memory_order_release);
    }
}

static void unplant(OmsShmWriter& w, IndexKind kind, std::string_view key) {
    IndexEntry* arr = w.index(kind);
    const uint32_t cap = w.index_capacity();
    const uint32_t mask = cap - 1;
    const uint64_t h = fnv1a(key);
    for (uint32_t i = 0; i < kMaxProbeIndex; ++i) {
        const uint32_t b = index_probe_bucket(h, i, cap, mask, is_pow2(cap));
        arr[b].key_hash.store(kHashEmpty, std::memory_order_release);
        arr[b].slot_idx.store(kInvalidSlot, std::memory_order_release);
    }
}

// 判断 bucket b 是否落在 blocker_key 的 32 步阻塞窗口内
static bool in_blocked_window(uint32_t b, uint64_t hb, uint32_t mask) {
    for (uint32_t i = 0; i < kMaxProbeIndex; ++i) {
        if (static_cast<uint32_t>((hb + i) & mask) == b) return true;
    }
    return false;
}

// 选一个 orderId, 使它的索引条目**不落在** blocker_key 的阻塞窗口里。
//   ★ 为什么需要: plant_blockers 会直接覆写那 32 个 bucket。如果"旧 key"的条目
//     正好在其中, 它就被顺手抹掉了 —— 那是**测试自身的构造缺陷**, 不是被测代码的
//     问题 (第一版就是这么误报的)。这里避开窗口, 才能真的验到"旧 key 仍在"。
static std::string pick_key_outside(const std::string& blocker_key, uint32_t cap) {
    const uint32_t mask = cap - 1;
    const uint64_t hb = fnv1a(std::string_view(blocker_key));
    for (int t = 0; t < 100000; ++t) {
        char buf[48];
        std::snprintf(buf, sizeof buf, "EX-OLD-%d", t);
        const uint64_t ho = fnv1a(std::string_view(buf));
        // 该 key 插入时可能落在 j = 0..3 任一个 bucket (取决于前面已有多少条目)
        bool clash = false;
        for (uint32_t j = 0; j < 4 && !clash; ++j) {
            if (in_blocked_window(static_cast<uint32_t>((ho + j) & mask), hb, mask)) {
                clash = true;
            }
        }
        if (!clash) return std::string(buf);
    }
    return std::string("EX-OLD");
}

// 直接改写文件头, 用来伪造"老版本 / 容量不对"的 shm 文件
static void patch_header(const std::string& path, uint32_t cap, uint32_t idx, uint32_t ver) {
    const int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) return;
    const size_t sz = OmsShmLayout::compute_total_size(cap, idx);
    if (::ftruncate(fd, static_cast<off_t>(sz)) != 0) { ::close(fd); return; }
    void* p = ::mmap(nullptr, sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (p != MAP_FAILED) {
        auto* h = reinterpret_cast<OmsShmHeader*>(p);
        h->magic          = kMagic;
        h->version        = ver;
        h->slot_capacity  = cap;
        h->index_capacity = idx;
        ::munmap(p, sz);
    }
    ::close(fd);
}

static bool open_throws(const std::string& path, uint32_t cap) {
    try {
        OmsShmWriter w;
        w.open(path, cap);
        return false;
    } catch (const std::exception&) {
        return true;
    }
}

// 按 key 副本反查 slot 下标。★ 不要硬编码 "第 N 次 upsert 就落在 slot N" ——
//   alloc_slot 每次探测都会推进 next_slot_hint, 顺序是实现的细节, 不是契约。
//   硬编码曾经让我误报过一次 (见 §7 的 pick_key_outside)。
static uint32_t find_slot_of(OmsShmWriter& w, const char* sysid) {
    for (uint32_t i = 0; i < w.slot_capacity(); ++i) {
        if (std::strcmp(w.slots()[i].orderSysId, sysid) == 0) return i;
    }
    return kInvalidSlot;
}

// =============================================================================
int main(int argc, char** argv) {
    const std::string dir = (argc > 1) ? argv[1] : "/tmp/oms_test";

    std::printf("=== oms_test — OMS SHM 断言集 ===\n");
    std::printf("scratch dir: %s\n", dir.c_str());

    // -------------------------------------------------------------------------
    section("1. 布局与常量");
    // -------------------------------------------------------------------------
    check(sizeof(OmsShmHeader) == 4096, "sizeof(OmsShmHeader) == 4096", u(sizeof(OmsShmHeader)));
    check(sizeof(IndexEntry) == 32, "sizeof(IndexEntry) == 32", u(sizeof(IndexEntry)));
    check(sizeof(OmsSlot) == kSlotSize, "sizeof(OmsSlot) == kSlotSize", u(sizeof(OmsSlot)));
    check(kVersion == 3, "kVersion == 3 (B6 把索引改成 next_pow2(4N) 时 bump 过)", u(kVersion));
    check(IDX_COUNT == 3, "IDX_COUNT == 3", u(IDX_COUNT));
    check(kMaxProbeIndex == 32, "kMaxProbeIndex == 32", u(kMaxProbeIndex));
    check(kMaxProbeSlots == 128, "kMaxProbeSlots == 128", u(kMaxProbeSlots));

    // next_pow2 / is_pow2
    check(!is_pow2(0) && is_pow2(1) && is_pow2(2) && !is_pow2(3) && is_pow2(4),
          "is_pow2: 0→false, 1→true, 2→true, 3→false, 4→true");
    check(next_pow2(0) == 1 && next_pow2(1) == 1 && next_pow2(3) == 4 && next_pow2(4) == 4,
          "next_pow2: 0→1, 1→1, 3→4, 4→4");
    // 关键: 100000 * 4 = 400000 → 524288, 不是 400000 (P1-1 / B6 的容量口径)
    check(next_pow2(400000) == 524288, "next_pow2(4 * 100000) == 524288", u(next_pow2(400000)));

    {
        // P1-1 回归: 非 2 的幂 cap 下, `% cap` 满射, `& (cap-1)` 不是。
        //   这正是"索引容量必须是 2 的幂"的根据 —— 用 mask 走非 pow2 cap 会提前饱和。
        const uint32_t cap = 100;                 // 非 2 的幂
        const uint32_t mask = cap - 1;
        std::vector<char> seen_mod(cap, 0), seen_mask(cap, 0);
        for (uint32_t i = 0; i < cap; ++i) {
            seen_mod [index_probe_bucket(0, i, cap, mask, false)] = 1;
            seen_mask[index_probe_bucket(0, i, cap, mask, true )] = 1;
        }
        uint32_t n_mod = 0, n_mask = 0;
        for (uint32_t i = 0; i < cap; ++i) { n_mod += seen_mod[i] ? 1u : 0u; n_mask += seen_mask[i] ? 1u : 0u; }
        check(n_mod == cap, "非 pow2 cap: `% cap` 探测序列覆盖全部 bucket", u(n_mod) + "/" + u(cap));
        check(n_mask < cap, "非 pow2 cap: `& (cap-1)` 覆盖不全 (这就是必须拒绝的原因)",
              u(n_mask) + "/" + u(cap));
    }

    // -------------------------------------------------------------------------
    section("2. 三层索引往返");
    // -------------------------------------------------------------------------
    {
        OmsShmWriter w;
        fresh(w, dir, "t02.dat", 1024);
        check(w.index_capacity() == next_pow2(4 * 1024),
              "index_capacity == next_pow2(4 * slot_cap)", u(w.index_capacity()));

        pubsub::RCommand r = mk("SYS-1", "EX-1", OS_NEW, 1001);
        const uint32_t idx = w.upsert(r);
        check(idx != kInvalidSlot, "upsert 成功", u(idx));

        pubsub::RCommand out;
        check(w.lookup_by_orderSysId("SYS-1", out), "按 orderSysId 查得到");
        check(w.lookup_by_client("sss_test1", 1001, out), "按 (strategyId, cid) 复合查得到");
        check(w.lookup_by_orderId("EX-1", out), "按 orderId 查得到");
        check(w.exists_by_orderSysId("SYS-1"), "exists_by_orderSysId == true");

        // 复合 key 的字符串形态必须与写入时一致
        char ck[64];
        const int n = OmsShmSegment::compose_client_key(ck, sizeof(ck), "sss_test1", 1001);
        check(n > 0 && std::string(ck, static_cast<size_t>(n)) == "sss_test11001",
              "compose_client_key == strategyId + 十进制 cid", std::string(ck));
        check(w.lookup_by_clientOrderId(std::string_view(ck, static_cast<size_t>(n)), out),
              "按**已复合**字符串也能查到 (CLI 兜底路径)");

        // 查不到必须是查不到 (三态: 不是"命中了别人")
        check(!w.lookup_by_orderSysId("SYS-NOPE", out), "不存在的 orderSysId 查不到");
        check(!w.lookup_by_orderId("EX-NOPE", out), "不存在的 orderId 查不到");
        check(!w.lookup_by_client("sss_test1", 9999, out), "不存在的 cid 查不到");
        // 同 cid 不同 strategyId 不能互相命中 (复合 key 的意义)
        check(!w.lookup_by_client("OTHER_STRAT", 1001, out),
              "同 cid 不同 strategyId **不**命中 (复合 key 防跨策略撞车)");

        // 负数 / 边界 cid
        char nb[64];
        check(OmsShmSegment::compose_client_key(nb, sizeof(nb), "s", -1) > 0 &&
              std::string(nb) == "s-1", "compose_client_key 处理负 cid", std::string(nb));
        check(OmsShmSegment::compose_client_key(nb, sizeof(nb), "s", 0) > 0 &&
              std::string(nb) == "s0", "compose_client_key 处理 cid=0", std::string(nb));
        check(OmsShmSegment::compose_client_key(nb, sizeof(nb), "s",
                  std::numeric_limits<int64_t>::min()) > 0,
              "compose_client_key 处理 int64 最小值 (20 位十进制)");
        check(OmsShmSegment::compose_client_key(nb, 8, "s", 1) <= 0,
              "compose_client_key 缓冲区过小 → 返回 <=0 (不静默截断)");
    }

    // -------------------------------------------------------------------------
    section("3. orderId 生命周期 (\"\" → 首次回报设定 → 再变)");
    // -------------------------------------------------------------------------
    {
        OmsShmWriter w;
        fresh(w, dir, "t03.dat", 64);
        pubsub::RCommand out;

        // 建单时交易所还没给 orderId
        w.upsert(mk("A1", "", OS_NEW, 1));
        check(!w.lookup_by_orderId("", out), "orderId 为空时不建 orderId 索引 (查 \"\" 查不到)");
        check(w.index_occupancy_of(IDX_EXCHANGE_ID).live == 0,
              "orderId 索引 live == 0 (没写空 key)",
              u(w.index_occupancy_of(IDX_EXCHANGE_ID).live));
        check(w.lookup_by_orderSysId("A1", out), "orderId 为空不影响主索引");

        // 首次回报带上 orderId
        w.upsert(mk("A1", "EX-A1", OS_NEW, 1));
        check(w.lookup_by_orderId("EX-A1", out), "首次设定 orderId 后可查");
        check(w.index_occupancy_of(IDX_EXCHANGE_ID).live == 1,
              "orderId 索引 live == 1 (没有多插一条)",
              u(w.index_occupancy_of(IDX_EXCHANGE_ID).live));

        // 再变一次 (异常路径, 但必须自洽)
        w.upsert(mk("A1", "EX-A1B", OS_NEW, 1));
        check(!w.lookup_by_orderId("EX-A1", out), "orderId 变更后**旧值**查不到");
        check(w.lookup_by_orderId("EX-A1B", out), "orderId 变更后**新值**查得到");
        check(w.index_occupancy_of(IDX_EXCHANGE_ID).live == 1,
              "变更后 live 仍为 1 (旧条目被 tombstone, 不是泄漏成 2)",
              u(w.index_occupancy_of(IDX_EXCHANGE_ID).live));
        check(std::strcmp(w.slots()[0].orderId, "EX-A1B") == 0,
              "slot 的 orderId 副本已跟上", w.slots()[0].orderId);

        // 报单体里字段为空 = "这次没带这个字段", 不能把已建立的 key 抹掉
        w.upsert(mk("A1", "", OS_NEW, 1));
        check(w.lookup_by_orderId("EX-A1B", out),
              "回报里 orderId 为空**不抹掉**已建立的 orderId 索引");
    }

    // -------------------------------------------------------------------------
    section("4. 生命周期: LIVE → FINISHED → reclaim");
    // -------------------------------------------------------------------------
    {
        OmsShmWriter w;
        fresh(w, dir, "t04.dat", 8);
        w.set_min_reclaim_age_ns(0);                 // FINISHED 立即可回收
        w.set_max_live_stale_ns(24ULL * 3600 * 1'000'000'000);

        for (uint32_t i = 0; i < 8; ++i) {
            char s[32], o[32];
            std::snprintf(s, sizeof s, "S%02u", i);
            std::snprintf(o, sizeof o, "E%02u", i);
            const uint32_t idx = w.upsert(mk(s, o, OS_NEW, 100 + i));
            check(idx == i, std::string("第 ") + u(i) + " 单落在 slot " + u(i), u(idx));
        }
        {
            const auto st = w.stats();
            check(st.live == 8 && st.empty == 0 && st.finished == 0,
                  "8 单全 LIVE, 环已满", "live=" + u(st.live) + " empty=" + u(st.empty));
        }

        // 0 号单成交
        w.upsert(mk("S00", "E00", OS_FILLED, 100));
        {
            const auto st = w.stats();
            check(st.finished == 1 && st.live == 7, "S00 → FINISHED",
                  "finished=" + u(st.finished) + " live=" + u(st.live));
        }

        // 新单必须回收 S00 的 slot
        const uint32_t ni = w.upsert(mk("NEW", "ENEW", OS_NEW, 999));
        check(ni == 0, "新单回收了 slot 0 (FINISHED 且 age >= min_reclaim_age)", u(ni));
        {
            const auto st = w.stats();
            check(st.total_reclaims >= 1, "total_reclaims 增加", u(st.total_reclaims));
            check(st.live == 8, "回收后仍 8 单 LIVE", u(st.live));
            check(st.total_alloc_failures == 0, "过程中没有丢单", u(st.total_alloc_failures));
        }
        pubsub::RCommand out;
        check(!w.lookup_by_orderSysId("S00", out), "被回收的单按 orderSysId 查不到了");
        check(w.lookup_by_orderSysId("NEW", out), "新单查得到");
        check(!w.lookup_by_orderId("E00", out), "被回收单的别名索引也 tombstone 了");
        check(w.index_occupancy_of(IDX_EXCHANGE_ID).tomb >= 1,
              "回收留下了 tombstone (不是删 bucket)",
              u(w.index_occupancy_of(IDX_EXCHANGE_ID).tomb));
    }

    // -------------------------------------------------------------------------
    section("5. 绝不覆盖 LIVE: 环满时失败而不是踩活单 (③ 真·环满)");
    // -------------------------------------------------------------------------
    {
        OmsShmWriter w;
        fresh(w, dir, "t05.dat", 8);
        w.set_min_reclaim_age_ns(60ULL * 1'000'000'000);   // FINISHED 60s 内不可回收
        w.set_max_live_stale_ns(24ULL * 3600 * 1'000'000'000);

        for (uint32_t i = 0; i < 8; ++i) {
            char s[32], o[32];
            std::snprintf(s, sizeof s, "S%02u", i);
            std::snprintf(o, sizeof o, "E%02u", i);
            w.upsert(mk(s, o, OS_NEW, 100 + i));
        }
        const uint32_t r = w.upsert(mk("OVERFLOW", "EOVF", OS_NEW, 777));
        check(r == kInvalidSlot, "环满且无可回收 → upsert 失败 (返回 kInvalidSlot)");
        {
            const auto st = w.stats();
            check(st.total_alloc_exhausted == 1, "total_alloc_exhausted == 1", u(st.total_alloc_exhausted));
            check(st.total_alloc_failures == 1, "total_alloc_failures == 1", u(st.total_alloc_failures));
            check(st.total_alloc_slowpath == 0, "total_alloc_slowpath == 0 (这是真环满, 不是慢路径)", u(st.total_alloc_slowpath));
            check(st.live == 8, "8 张活单一个都没被踩", u(st.live));
        }
        pubsub::RCommand out;
        bool all_alive = true;
        for (uint32_t i = 0; i < 8; ++i) {
            char s[32];
            std::snprintf(s, sizeof s, "S%02u", i);
            if (!w.lookup_by_orderSysId(s, out)) all_alive = false;
        }
        check(all_alive, "失败之后 8 张活单全部仍可查 (绝不覆盖 LIVE)");
        check(!w.lookup_by_orderSysId("OVERFLOW", out), "失败的单没有进 SHM");
    }

    // -------------------------------------------------------------------------
    section("6. ③ 慢路径救回 (128 步窗口落空, 全表扫有)");
    // -------------------------------------------------------------------------
    {
        OmsShmWriter w;
        const uint32_t cap = 512;
        fresh(w, dir, "t06.dat", cap);
        w.set_min_reclaim_age_ns(0);
        w.set_max_live_stale_ns(24ULL * 3600 * 1'000'000'000);

        // 填满整个环 (全 LIVE, 不可回收) → hint 回到 0
        for (uint32_t i = 0; i < cap; ++i) {
            char s[32], o[32];
            std::snprintf(s, sizeof s, "F%04u", i);
            std::snprintf(o, sizeof o, "G%04u", i);
            w.upsert(mk(s, o, OS_NEW, 200000 + i));
        }
        // 终结 slot 200 的单 —— 它在快路径窗口 (hint=512 → idx 0..127) **之外**
        w.upsert(mk("F0200", "G0200", OS_FILLED, 200200));

        const uint32_t idx = w.upsert(mk("SLOW", "GSLOW", OS_NEW, 999999));
        check(idx == 200, "慢路径在窗口外找到了可回收 slot 200", u(idx));
        {
            const auto st = w.stats();
            check(st.total_alloc_slowpath == 1, "total_alloc_slowpath == 1 (单被救回)", u(st.total_alloc_slowpath));
            check(st.total_alloc_exhausted == 0, "total_alloc_exhausted == 0", u(st.total_alloc_exhausted));
            check(st.total_alloc_failures == 0, "total_alloc_failures == 0 (**没有丢单**)", u(st.total_alloc_failures));
        }
        pubsub::RCommand out;
        check(w.lookup_by_orderSysId("SLOW", out), "救回的单查得到");
        check(!w.lookup_by_orderSysId("F0200", out), "被回收的 F0200 查不到了");
        check(std::strcmp(w.slots()[200].orderSysId, "SLOW") == 0,
              "slot 200 已被新单占用", w.slots()[200].orderSysId);
    }

    // -------------------------------------------------------------------------
    section("7. ② key 同步失败 → 保持旧 key (不是新旧都丢)");
    // -------------------------------------------------------------------------
    {
        OmsShmWriter w;
        fresh(w, dir, "t07.dat", 64);
        pubsub::RCommand out;

        w.upsert(mk("A1", "BLOCK", OS_NEW, 1));       // slot 0, orderId 副本非空
        // 旧 orderId 必须选在 "EX-NEW" 的阻塞窗口之外 (否则会被 plant_blockers 覆盖)
        const std::string old_oid = pick_key_outside("EX-NEW", w.index_capacity());
        w.upsert(mk("A2", old_oid.c_str(), OS_NEW, 2));      // slot 1
        check(w.lookup_by_orderId(old_oid, out), "前置: A2 的旧 orderId 可查", old_oid);

        // 把 "EX-NEW" 的整条探测序列占满 → 插入必然失败
        plant_blockers(w, IDX_EXCHANGE_ID, "EX-NEW", /*target_slot=*/0);
        check(w.lookup_by_orderId(old_oid, out),
              "前置: 阻塞窗口没有波及旧 key 的条目", old_oid);
        w.upsert(mk("A2", "EX-NEW", OS_NEW, 2));      // 触发 sync_one_key, 插入失败

        {
            const auto st = w.stats();
            check(st.total_key_sync_failures == 1, "total_key_sync_failures == 1", u(st.total_key_sync_failures));
            check(st.total_alloc_failures == 0, "不计入 total_alloc_failures (这不是丢单)", u(st.total_alloc_failures));
        }
        check(std::strcmp(w.slots()[1].orderId, old_oid.c_str()) == 0,
              "失败时**不动** slot 的 key 副本 (副本与索引仍一致)", w.slots()[1].orderId);
        check(w.lookup_by_orderId(old_oid, out),
              "★ 旧 key 仍然查得到 (旧实现会把旧条目删掉 → 新旧都查不到)");
        check(!w.lookup_by_orderId("EX-NEW", out), "新 key 查不到 (如实反映失败)");
        check(w.lookup_by_orderSysId("A2", out), "主索引不受影响");

        // 放开阻塞 → 再同步一次应当成功
        unplant(w, IDX_EXCHANGE_ID, "EX-NEW");
        w.upsert(mk("A2", "EX-NEW", OS_NEW, 2));
        check(std::strcmp(w.slots()[1].orderId, "EX-NEW") == 0,
              "解除饱和后副本更新成功", w.slots()[1].orderId);
        check(w.lookup_by_orderId("EX-NEW", out), "成功路径: 新 key 查得到");
        check(!w.lookup_by_orderId(old_oid, out), "成功路径: 旧 key 已 tombstone");
        check(w.stats().total_key_sync_failures == 1, "计数不被后续成功清零");
    }

    // -------------------------------------------------------------------------
    section("8. B4 别名插入失败 (主索引 OK + 别名失败)");
    // -------------------------------------------------------------------------
    {
        OmsShmWriter w;
        fresh(w, dir, "t08.dat", 64);
        pubsub::RCommand out;

        w.upsert(mk("B0", "BLOCK", OS_NEW, 1));       // slot 0
        plant_blockers(w, IDX_EXCHANGE_ID, "NEWOID", /*target_slot=*/0);

        const uint32_t idx = w.upsert(mk("B1", "NEWOID", OS_NEW, 2));
        check(idx != kInvalidSlot, "主索引成功 → 单进了 SHM", u(idx));
        {
            const auto st = w.stats();
            check(st.total_alias_insert_failures == 1, "total_alias_insert_failures == 1", u(st.total_alias_insert_failures));
            check(st.total_alloc_failures == 0, "不计入 total_alloc_failures (单确实进了 SHM)", u(st.total_alloc_failures));
            check(st.total_inserts == 2, "total_inserts 正常 +1", u(st.total_inserts));
        }
        check(w.lookup_by_orderSysId("B1", out), "按 orderSysId 查得到 (主索引正常)");
        check(!w.lookup_by_orderId("NEWOID", out), "按 orderId 查不到 (别名确实丢了)");
        check(w.lookup_by_client("sss_test1", 2, out), "另一个别名 clientOrderId 不受影响");
    }

    // -------------------------------------------------------------------------
    section("9. B5 副本 == 索引: key 迁移后旧 key 不再命中");
    // -------------------------------------------------------------------------
    {
        OmsShmWriter w;
        fresh(w, dir, "t09.dat", 64);
        pubsub::RCommand out;

        w.upsert(mk("C1", "EO", OS_NEW, 500, "STRAT_A"));
        check(w.lookup_by_client("STRAT_A", 500, out), "前置: 原复合 key 可查");
        const uint32_t before = w.index_occupancy_of(IDX_CLIENT_ORDER).live;

        // strategyId 变了 → 复合 key 跟着变 (必须迁移索引, 否则查 A 会命中 B)
        w.upsert(mk("C1", "EO", OS_NEW, 500, "STRAT_B"));
        check(w.lookup_by_client("STRAT_B", 500, out), "变更后新复合 key 可查");
        check(!w.lookup_by_client("STRAT_A", 500, out), "变更后旧复合 key 不再命中");
        check(w.index_occupancy_of(IDX_CLIENT_ORDER).live == before,
              "live 条数不变 (旧条目 tombstone, 不是新增一条)",
              u(w.index_occupancy_of(IDX_CLIENT_ORDER).live));
        check(std::strcmp(w.slots()[0].clientOrderId, "STRAT_B500") == 0,
              "slot 的复合 key 副本已跟上", w.slots()[0].clientOrderId);
        check(w.lookup_by_orderSysId("C1", out), "主索引不受影响");
    }

    // -------------------------------------------------------------------------
    section("10. 卡单 (stale LIVE) 强制回收");
    // -------------------------------------------------------------------------
    {
        OmsShmWriter w;
        fresh(w, dir, "t10.dat", 4);
        w.set_min_reclaim_age_ns(60ULL * 1'000'000'000);

        for (uint32_t i = 0; i < 4; ++i) {
            char s[32], o[32];
            std::snprintf(s, sizeof s, "Z%u", i);
            std::snprintf(o, sizeof o, "Y%u", i);
            w.upsert(mk(s, o, OS_NEW, 300 + i));
        }
        // 把所有 LIVE 都变成"超时未更新" → 下一次 alloc 只能强制回收
        w.set_max_live_stale_ns(1);
        const uint32_t idx = w.upsert(mk("FRESH", "YFRESH", OS_NEW, 999));
        check(idx != kInvalidSlot, "有卡单时新单仍能落地 (兜底回收)", u(idx));
        {
            const auto st = w.stats();
            check(st.total_stale_live_reclaims == 1, "total_stale_live_reclaims == 1", u(st.total_stale_live_reclaims));
            check(st.total_reclaims >= 1, "total_reclaims 也 +1", u(st.total_reclaims));
            check(st.total_alloc_failures == 0, "没有丢单", u(st.total_alloc_failures));
        }
        // 卡单检测基准是 last_update_time_ns: 关掉阈值后不应再有卡单
        w.set_max_live_stale_ns(24ULL * 3600 * 1'000'000'000);
        {
            const auto st = w.stats();
            check(st.live_stale == 0, "恢复阈值后 live_stale == 0", u(st.live_stale));
        }
    }

    // -------------------------------------------------------------------------
    section("11. 稳态 tombstone: 环满后 empty→0 是设计如此, 不是泄漏");
    // -------------------------------------------------------------------------
    {
        OmsShmWriter w;
        const uint32_t cap = 64;
        fresh(w, dir, "t11.dat", cap);
        w.set_min_reclaim_age_ns(0);
        w.set_max_live_stale_ns(24ULL * 3600 * 1'000'000'000);

        // 反复: 填满 → 全部终结 → 再填满 (每轮都走 reclaim + tombstone)
        const uint32_t rounds = 40;
        for (uint32_t r = 0; r < rounds; ++r) {
            for (uint32_t i = 0; i < cap; ++i) {
                char s[48], o[48];
                std::snprintf(s, sizeof s, "R%03u_%03u", r, i);
                std::snprintf(o, sizeof o, "Q%03u_%03u", r, i);
                w.upsert(mk(s, o, OS_NEW, r * 1000u + i));
            }
            for (uint32_t i = 0; i < cap; ++i) {
                char s[48], o[48];
                std::snprintf(s, sizeof s, "R%03u_%03u", r, i);
                std::snprintf(o, sizeof o, "Q%03u_%03u", r, i);
                w.upsert(mk(s, o, OS_FILLED, r * 1000u + i));
            }
        }
        const auto st = w.stats();
        const uint32_t icap = w.index_capacity();
        check(st.total_alloc_failures == 0,
              "反复饱和 " + u(rounds) + " 轮**一次都没丢单** (B6 的收益)",
              "failures=" + u(st.total_alloc_failures));
        for (uint32_t k = 0; k < IDX_COUNT; ++k) {
            const auto& occ = st.index_occupancy[k];
            const std::string nm = index_kind_name(k);
            check(occ.live + occ.tomb + occ.empty == icap,
                  nm + ": live+tomb+empty == index_capacity",
                  u(occ.live) + "+" + u(occ.tomb) + "+" + u(occ.empty) + " vs " + u(icap));
            check(occ.live <= cap,
                  nm + ": live <= slot_cap (没有陈旧条目累积)",
                  u(occ.live) + " <= " + u(cap));
            check(occ.empty <= icap / 8,
                  nm + ": empty 已被排干到接近 0 (证明 tombstone 被复用, 不是泄漏)",
                  u(occ.empty) + " <= " + u(icap / 8));
        }
    }

    // -------------------------------------------------------------------------
    section("12. B6 探测余量 (max_live_run < kMaxProbeIndex)");
    // -------------------------------------------------------------------------
    for (uint32_t cap : {1024u, 4096u}) {
        OmsShmWriter w;
        const std::string nm = "t12_" + u(cap) + ".dat";
        fresh(w, dir, nm.c_str(), cap);
        w.set_min_reclaim_age_ns(0);
        w.set_max_live_stale_ns(24ULL * 3600 * 1'000'000'000);

        // 先填满一次, 让索引进入"环满"稳态 (empty 排干到 0), 这是 B6 真正要看的场景
        for (uint32_t i = 0; i < cap; ++i) {
            char s[48], o[48];
            std::snprintf(s, sizeof s, "H%06u", i);
            std::snprintf(o, sizeof o, "I%06u", i);
            w.upsert(mk(s, o, OS_NEW, 700000 + i));
        }
        for (uint32_t i = 0; i < cap; ++i) {
            char s[48], o[48];
            std::snprintf(s, sizeof s, "H%06u", i);
            std::snprintf(o, sizeof o, "I%06u", i);
            w.upsert(mk(s, o, OS_FILLED, 700000 + i));
        }
        const auto st = w.stats();
        check(w.index_capacity() == next_pow2(4 * cap),
              "cap=" + u(cap) + ": index_capacity == next_pow2(4*cap)", u(w.index_capacity()));
        uint32_t worst = 0;
        for (uint32_t k = 0; k < IDX_COUNT; ++k) {
            const uint32_t run = st.index_occupancy[k].max_live_run;
            if (run > worst) worst = run;
        }
        check(worst < kMaxProbeIndex,
              "cap=" + u(cap) + ": worst max_live_run < kMaxProbeIndex (有余量)",
              u(worst) + " < " + u(kMaxProbeIndex));
        check(st.total_alloc_failures == 0,
              "cap=" + u(cap) + ": 无丢单", u(st.total_alloc_failures));
    }

    // -------------------------------------------------------------------------
    section("13. A2 iterate_live / iterate_finished 跳过计数");
    // -------------------------------------------------------------------------
    {
        OmsShmWriter w;
        fresh(w, dir, "t13.dat", 64);
        for (uint32_t i = 0; i < 10; ++i) {
            char s[32], o[32];
            std::snprintf(s, sizeof s, "L%02u", i);
            std::snprintf(o, sizeof o, "K%02u", i);
            w.upsert(mk(s, o, OS_NEW, 400 + i));
        }
        for (uint32_t i = 10; i < 15; ++i) {
            char s[32], o[32];
            std::snprintf(s, sizeof s, "L%02u", i);
            std::snprintf(o, sizeof o, "K%02u", i);
            w.upsert(mk(s, o, OS_FILLED, 400 + i));
        }
        size_t skipped = 999, nl = 0, nf = 0;
        nl = w.iterate_live([&](const pubsub::RCommand&){ });
        check(nl == 10, "iterate_live 遍历到 10 条", u(nl));
        nf = w.iterate_finished([&](const pubsub::RCommand&){ });
        check(nf == 5, "iterate_finished 遍历到 5 条", u(nf));
        skipped = 999;
        nl = w.iterate_live([&](const pubsub::RCommand&){ }, &skipped);
        check(nl == 10 && skipped == 0,
              "无并发时 skipped == 0 (A2: 跳过条数可见, 不再是静默丢弃)",
              "n=" + u(nl) + " skipped=" + u(skipped));
        // 不带 out 参数也要能编译/调用 (向后兼容)
        const size_t n2 = w.iterate_live([&](const pubsub::RCommand&){ });
        check(n2 == 10, "不传 skipped_out 的旧调用方式仍然可用", u(n2));
    }

    // -------------------------------------------------------------------------
    section("14. 版本 / 容量 拒绝 (不能带病运行)");
    // -------------------------------------------------------------------------
    {
        // 14a: 版本不匹配
        const std::string p1 = j(dir, "t14a.dat");
        {
            OmsShmWriter w; ::unlink(p1.c_str()); w.open(p1, 64);
        }
        patch_header(p1, 64, next_pow2(4 * 64), kVersion + 1);
        check(open_throws(p1, 64), "version 不匹配 → open 抛异常 (不静默按新版本跑)");

        // 14b: index_capacity 不是 2 的幂 (老 P1-1 建的文件)
        const std::string p2 = j(dir, "t14b.dat");
        {
            OmsShmWriter w; ::unlink(p2.c_str()); w.open(p2, 64);
        }
        patch_header(p2, 64, 100, kVersion);          // 100 不是 2 的幂
        check(open_throws(p2, 64), "index_capacity 非 2 的幂 → open 抛异常");

        // 14c: 2N 索引 (老 B6 之前的文件) —— 是 2 的幂, 所以必须另有一条倍数检查
        const std::string p3 = j(dir, "t14c.dat");
        {
            OmsShmWriter w; ::unlink(p3.c_str()); w.open(p3, 64);
        }
        patch_header(p3, 64, next_pow2(2 * 64), kVersion);   // 128 < 4*64=256
        check(open_throws(p3, 64), "index_capacity < 4*slot_cap → open 抛异常 (老 2N 文件)");

        // 14d: 合法文件必须能打开 (反向对照, 防止上面三条是靠"总是抛异常"蒙对的)
        const std::string p4 = j(dir, "t14d.dat");
        {
            OmsShmWriter w; ::unlink(p4.c_str()); w.open(p4, 64);
        }
        check(!open_throws(p4, 64), "对照: 正常文件 open 不抛异常");

        // 14e: 新建时 slot_cap 越界
        const std::string p5 = j(dir, "t14e.dat");
        ::unlink(p5.c_str());
        check(open_throws(p5, 0), "新建 slot_cap = 0 → 抛异常");
        ::unlink(p5.c_str());
        check(open_throws(p5, (1u << 29)), "新建 slot_cap = 2^29 (> 2^28) → 抛异常");

        // 14f: 只读打开空文件
        const std::string p6 = j(dir, "t14f.dat");
        ::unlink(p6.c_str());
        {
            FILE* f = std::fopen(p6.c_str(), "wb");   // 造一个 0 字节文件
            if (f) std::fclose(f);
        }
        bool ro_throws = false;
        try { OmsShmReader rr; rr.open(p6); } catch (const std::exception&) { ro_throws = true; }
        check(ro_throws, "只读打开 0 字节文件 → 抛异常");
    }

    // -------------------------------------------------------------------------
    section("15. 崩溃恢复: RECLAIMING 残留 slot");
    // -------------------------------------------------------------------------
    {
        const std::string p = j(dir, "t15.dat");
        {
            OmsShmWriter w; ::unlink(p.c_str()); w.open(p, 64);
            w.upsert(mk("CR1", "CE1", OS_NEW, 1));
            // 伪造"writer 崩在 CAS 之后、写入完成之前"的中间态
            w.slots()[0].seq.store(7, std::memory_order_relaxed);          // odd
            w.slots()[0].state.store(SLOT_RECLAIMING, std::memory_order_relaxed);
        }   // 析构 → munmap (MAP_SHARED 写回文件)
        {
            OmsShmWriter w2; w2.open(p, 64);      // open 时自动扫残留
            check(w2.slots()[0].state.load() == SLOT_EMPTY,
                  "残留 RECLAIMING slot 归位 EMPTY");
            check(w2.slots()[0].orderSysId[0] == '\0',
                  "残留 slot 的 key 副本已清空");
            check((w2.slots()[0].seq.load() & 1ULL) == 0,
                  "残留 slot 的 seq 被刷成偶数 (reader 不会一直重试)",
                  u(w2.slots()[0].seq.load()));
            pubsub::RCommand out;
            check(!w2.lookup_by_orderSysId("CR1", out),
                  "残留 slot 关联的索引项已 tombstone (查不到了)");
            check(w2.stats().reclaiming == 0, "stats 里 reclaiming == 0");
        }
    }

    // -------------------------------------------------------------------------
    section("16. reset_all 清零全部计数 (含 v4 补漏)");
    // -------------------------------------------------------------------------
    {
        OmsShmWriter w;
        fresh(w, dir, "t16.dat", 64);
        w.upsert(mk("X1", "XO1", OS_NEW, 1));
        // 直接把每个计数都写成非零, 再 reset_all, 逐个断言清零。
        //   ★ 这条测试的价值在于**枚举**: 以后新增计数器如果忘了在 reset_all 里清零,
        //     这里补一行就会红 —— 上一轮 total_stale_live_reclaims 就是这么漏的。
        w.header()->total_inserts.store(7);
        w.header()->total_updates.store(7);
        w.header()->total_reclaims.store(7);
        w.header()->total_alloc_failures.store(7);
        w.header()->total_stale_live_reclaims.store(7);
        w.header()->total_alloc_slowpath.store(7);
        w.header()->total_alloc_exhausted.store(7);
        w.header()->total_key_sync_failures.store(7);
        w.header()->total_alias_insert_failures.store(7);
        w.reset_all();

        const auto st = w.stats();
        check(st.total_inserts == 0, "reset_all: total_inserts == 0", u(st.total_inserts));
        check(st.total_updates == 0, "reset_all: total_updates == 0", u(st.total_updates));
        check(st.total_reclaims == 0, "reset_all: total_reclaims == 0", u(st.total_reclaims));
        check(st.total_alloc_failures == 0, "reset_all: total_alloc_failures == 0", u(st.total_alloc_failures));
        check(st.total_stale_live_reclaims == 0, "reset_all: total_stale_live_reclaims == 0 (v4 补漏)", u(st.total_stale_live_reclaims));
        check(st.total_alloc_slowpath == 0, "reset_all: total_alloc_slowpath == 0", u(st.total_alloc_slowpath));
        check(st.total_alloc_exhausted == 0, "reset_all: total_alloc_exhausted == 0", u(st.total_alloc_exhausted));
        check(st.total_key_sync_failures == 0, "reset_all: total_key_sync_failures == 0", u(st.total_key_sync_failures));
        check(st.total_alias_insert_failures == 0, "reset_all: total_alias_insert_failures == 0", u(st.total_alias_insert_failures));
        check(st.empty == st.capacity && st.live == 0 && st.finished == 0,
              "reset_all: 所有 slot 回到 EMPTY",
              "empty=" + u(st.empty) + " live=" + u(st.live));
        check(st.probe_max == kMaxProbeIndex, "Stats::probe_max == kMaxProbeIndex", u(st.probe_max));
        uint32_t total_used = 0;
        for (uint32_t k = 0; k < IDX_COUNT; ++k) total_used += st.index_occupancy[k].used();
        check(total_used == 0, "reset_all: 三个索引全部清空", u(total_used));
        pubsub::RCommand out;
        check(!w.lookup_by_orderSysId("X1", out), "reset_all 后旧单查不到");
    }

    // -------------------------------------------------------------------------
    section("17. 并发 seqlock: 1 writer + N reader, 0 撕裂读");
    // -------------------------------------------------------------------------
    {
        const std::string p = j(dir, "t17.dat");
        const uint32_t cap = 8192;
        const uint64_t N   = 200000;
        ::unlink(p.c_str());
        { OmsShmWriter w; w.open(p, cap); }        // 先建文件, reader 才能 open

        std::atomic<bool>     stop{false};
        std::atomic<uint64_t> hits{0}, torn{0};

        std::thread wt([&]{
            OmsShmWriter w;
            w.open(p, cap);
            // ★ 必须让它"有出有进": 只 insert 不 finalize 的话, 8192 个 slot 填满 LIVE 之后
            //   就没有任何可回收 slot, 后面 N-8192 次 upsert 全部合法地失败 ——
            //   那是测试设计问题, 不是产品问题 (第一版就是这么误报的)。
            w.set_min_reclaim_age_ns(0);                              // FINISHED 立即可回收
            w.set_max_live_stale_ns(24ULL * 3600 * 1'000'000'000);
            pubsub::RCommand r;
            for (uint64_t i = 0; i < N; ++i) {
                char s[48], o[48];
                std::snprintf(s, sizeof s, "R%08llu", (unsigned long long)i);
                std::snprintf(o, sizeof o, "O%08llu", (unsigned long long)i);
                r = mk(s, o, OS_NEW, static_cast<int64_t>(i));
                w.upsert(r);
                r = mk(s, o, OS_FILLED, static_cast<int64_t>(i));
                w.upsert(r);          // 终结 → 环可以继续回收
            }
            stop.store(true, std::memory_order_release);
        });

        std::vector<std::thread> rts;
        for (int t = 0; t < 4; ++t) {
            rts.emplace_back([&]{
                OmsShmReader rr;
                rr.open(p);
                pubsub::RCommand out;
                char key[48];
                uint64_t k = 0;
                while (!stop.load(std::memory_order_acquire)) {
                    const uint64_t id = (k++ * 7919ULL) % N;   // 打散, 避免只查最近写的
                    const int n = std::snprintf(key, sizeof key, "R%08llu",
                                                (unsigned long long)id);
                    if (rr.lookup_by_orderSysId(std::string_view(key, static_cast<size_t>(n)), out)) {
                        hits.fetch_add(1, std::memory_order_relaxed);
                        // 命中即意味着快照里的 key 副本 == 查询 key; 报单体若与它不一致
                        // 就是 seqlock 撕裂 (读到了半截写入)。
                        if (std::strncmp(out.body.orderResponse.orderSysId, key,
                                         static_cast<size_t>(n)) != 0) {
                            torn.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                    std::this_thread::yield();
                }
            });
        }
        wt.join();
        for (auto& t : rts) t.join();

        const uint64_t h = hits.load(), tr = torn.load();
        check(tr == 0, "并发下 0 次撕裂读 (命中时报单体 orderSysId == 查询 key)",
              "hits=" + u(h) + " torn=" + u(tr));
        check(h > 0, "reader 确实命中过 (测试有效, 不是空跑)", "hits=" + u(h));
        {
            OmsShmWriter w; w.open(p, cap);
            const auto st = w.stats();
            check(st.total_alloc_failures == 0,
                  "并发写入过程中无丢单", u(st.total_alloc_failures));
        }
    }

    // -------------------------------------------------------------------------
    section("18. 其余公开接口: remove / recover_orphan_slots / is_open / created_new / close");
    // -------------------------------------------------------------------------
    {
        pubsub::RCommand out;

        // ---- 18a/18b/18c: remove() ----------------------------------------
        OmsShmWriter w;
        fresh(w, dir, "t18a.dat", 8);
        check(!w.remove("NO-SUCH-KEY"), "remove(不存在的 key) == false");

        w.upsert(mk("RM1", "RE1", OS_NEW, 11));
        {
            const auto st0 = w.stats();
            check(st0.live == 1, "前置: 1 条 LIVE", u(st0.live));
            check(st0.index_occupancy[IDX_ORDER_SYS_ID].live == 1 &&
                  st0.index_occupancy[IDX_CLIENT_ORDER].live == 1 &&
                  st0.index_occupancy[IDX_EXCHANGE_ID].live  == 1,
                  "前置: 三个索引各 1 条 live 条目");
        }
        check(w.remove("RM1"), "remove(LIVE 单) == true");
        {
            const auto st = w.stats();
            check(st.live == 0 && st.empty == 8,
                  "remove 后 slot 回到 EMPTY", "live=" + u(st.live) + " empty=" + u(st.empty));
            check(st.index_occupancy[IDX_ORDER_SYS_ID].live == 0 &&
                  st.index_occupancy[IDX_CLIENT_ORDER].live == 0 &&
                  st.index_occupancy[IDX_EXCHANGE_ID].live  == 0,
                  "remove 后三个索引的 live 条目都清空");
        }
        check(!w.lookup_by_orderSysId("RM1", out),      "remove 后按 orderSysId 查不到");
        check(!w.lookup_by_orderId("RE1", out),          "remove 后按 orderId 查不到");
        check(!w.lookup_by_client("sss_test1", 11, out), "remove 后按复合 clientOrderId 查不到");
        check(!w.remove("RM1"), "重复 remove 同一个 key == false");

        // remove 腾出来的 slot 必须能立刻复用, 否则等于"删了反而丢单"
        for (uint32_t i = 0; i < 8; ++i) {
            char s[32];
            std::snprintf(s, sizeof s, "F%02u", i);
            w.upsert(mk(s, s, OS_NEW, 100 + i));
        }
        check(w.stats().live == 8, "环再次填满 8 条 LIVE", u(w.stats().live));
        check(w.remove("F03"), "remove 环内的一条");
        check(w.upsert(mk("F99", "F99", OS_NEW, 999)) != kInvalidSlot,
              "remove 腾出的 slot 立刻被复用 (不丢单)");
        check(w.lookup_by_orderSysId("F99", out), "复用后新单查得到");
        check(w.stats().total_alloc_failures == 0,
              "整个 remove 流程 0 次 alloc 失败", u(w.stats().total_alloc_failures));

        // ---- 18d: recover_orphan_slots() ----------------------------------
        // 显式调用 (不是靠 open() 的自动恢复), 断言返回值 + 幂等 + 不误伤别人。
        OmsShmWriter w2;
        fresh(w2, dir, "t18d.dat", 8);
        w2.upsert(mk("OR1", "OE1", OS_NEW, 21));   // 将被伪造成"崩在中间"的 slot
        w2.upsert(mk("OR2", "OE2", OS_NEW, 22));   // 必须毫发无损
        const uint32_t s_orphan = find_slot_of(w2, "OR1");
        const uint32_t s_alive  = find_slot_of(w2, "OR2");
        check(s_orphan != kInvalidSlot && s_alive != kInvalidSlot && s_orphan != s_alive,
              "前置: 两条单落在不同的 slot", "orphan=" + u(s_orphan) + " alive=" + u(s_alive));
        w2.slots()[s_orphan].seq.store(7, std::memory_order_relaxed);       // odd = 写者停在中间
        w2.slots()[s_orphan].state.store(SLOT_RECLAIMING, std::memory_order_relaxed);

        const size_t r1 = w2.recover_orphan_slots();
        check(r1 == 1, "recover_orphan_slots() 返回 1 (只回收残留那条)", u(r1));
        check(w2.slots()[s_orphan].state.load() == SLOT_EMPTY, "残留 slot 归位 EMPTY");
        check(w2.slots()[s_orphan].orderSysId[0] == '\0' &&
              w2.slots()[s_orphan].clientOrderId[0] == '\0' &&
              w2.slots()[s_orphan].orderId[0] == '\0',
              "残留 slot 的三个 key 副本都清空");
        check((w2.slots()[s_orphan].seq.load() & 1ULL) == 0,
              "残留 slot 的 seq 被刷成偶数", u(w2.slots()[s_orphan].seq.load()));
        check(!w2.lookup_by_orderSysId("OR1", out), "残留单的索引项已 tombstone");
        check(w2.stats().reclaiming == 0, "stats.reclaiming == 0");

        check(w2.slots()[s_alive].state.load() == SLOT_LIVE, "非 RECLAIMING 的 slot 未被误伤");
        check(w2.lookup_by_orderSysId("OR2", out) &&
              out.body.orderResponse.clientOrderId == 22,
              "非残留单仍可查到且内容不变");
        check(w2.recover_orphan_slots() == 0, "再调一次 recover_orphan_slots() == 0 (幂等)");

        // ---- 18e: is_open / created_new / close ---------------------------
        OmsShmWriter w3;
        const std::string p = j(dir, "t18e.dat");
        ::unlink(p.c_str());
        check(!w3.is_open(), "未 open 时 is_open() == false");

        w3.open(p, 8);
        check(w3.is_open(), "open 后 is_open() == true");
        check(w3.created_new(), "首次创建文件 → created_new() == true");
        w3.upsert(mk("IO1", "IE1", OS_NEW, 31));

        w3.close();
        check(!w3.is_open(), "close 后 is_open() == false");
        w3.close();                                   // 幂等, 不能崩
        check(!w3.is_open(), "close() 幂等 (再调一次仍为 false, 不崩)");

        w3.open(p, 8);
        check(w3.is_open() && !w3.created_new(),
              "复用已有文件 → is_open() == true 且 created_new() == false");
        check(w3.lookup_by_orderSysId("IO1", out), "close/open 往返后数据仍在");

        OmsShmReader rr;
        rr.open(p);
        check(rr.is_open(), "Reader: open 后 is_open() == true");
        rr.close();
        check(!rr.is_open(), "Reader: close 后 is_open() == false");
    }

    // -------------------------------------------------------------------------
    section("19. TTL 语义: FINISHED 的回收门槛 = min_reclaim_age_ns");
    // -------------------------------------------------------------------------
    {
        // 钉住第 23 轮事故的机理: 环里全是"FINISHED 但还太年轻"的 slot 时, 新单**必然**
        // 写不进去 —— 这是 TTL 的设计, 不是 bug; 把 TTL 调成 0 后同一批 slot 立刻可回收,
        // 同一张单马上就写进去了。oms_bench 的 mixed 相位刷 [ERROR] 就是这个原因:
        //   可持续写入速率上限 = slot_cap / min_reclaim_age (131072 / 60s ≈ 2185 单/秒),
        //   而 bench 压到 ~70 万单/秒 → 0.2 秒写满。
        OmsShmWriter w;
        fresh(w, dir, "t19.dat", 8);
        w.set_min_reclaim_age_ns(60ULL * 1'000'000'000);              // 默认 60s
        w.set_max_live_stale_ns(24ULL * 3600 * 1'000'000'000);

        for (uint32_t i = 0; i < 8; ++i) {
            char s[32];
            std::snprintf(s, sizeof s, "T%02u", i);
            w.upsert(mk(s, s, OS_NEW, 700 + i));
            w.upsert(mk(s, s, OS_FILLED, 700 + i));                   // → FINISHED
        }
        {
            const auto st = w.stats();
            check(st.finished == 8 && st.empty == 0,
                  "前置: 8 个 slot 全部 FINISHED 且 empty == 0",
                  "finished=" + u(st.finished) + " empty=" + u(st.empty));
            check(st.total_reclaims == 0, "TTL 内一个都没被回收", u(st.total_reclaims));
        }

        pubsub::RCommand out;
        check(w.upsert(mk("NEW1", "NEW1", OS_NEW, 900)) == kInvalidSlot,
              "TTL 内新单写不进去 (真·环满)");
        check(!w.lookup_by_orderSysId("NEW1", out), "失败的新单确实没进 SHM");
        check(w.stats().total_alloc_exhausted == 1,
              "total_alloc_exhausted == 1", u(w.stats().total_alloc_exhausted));

        // ★ 把 TTL 调成 0 → 同一批 FINISHED slot 立刻变成可回收
        w.set_min_reclaim_age_ns(0);
        check(w.upsert(mk("NEW1", "NEW1", OS_NEW, 900)) != kInvalidSlot,
              "TTL 调成 0 后同一张单立刻写得进去 (回收了 FINISHED)");
        check(w.lookup_by_orderSysId("NEW1", out), "新单可查");
        check(w.stats().total_reclaims >= 1, "确实发生了回收", u(w.stats().total_reclaims));
        check(w.stats().total_alloc_exhausted == 1,
              "exhausted 计数没有继续涨 (仍是 1)", u(w.stats().total_alloc_exhausted));
    }

    // -------------------------------------------------------------------------
    section("20. B3 lookup 三态: OK / NOT_FOUND / BUSY (对账不能把活单判死)");
    // -------------------------------------------------------------------------
    {
        // 背景: 旧实现 lookup_* 只返回 bool, 把"**确认**不存在"和"索引里有这条、但这一瞬间
        // 读不到一致快照 (写者忙 / slot 正在被回收)"混成同一个 false。重启对账据此会把
        // **活单判成死单** → 重复下单 / 误平仓。
        // 本节的判据有两条, 缺一不可:
        //   ① BUSY 必须能和 NOT_FOUND 分开 (三态 API 存在且正确);
        //   ② NOT_FOUND **绝不能**被计成 BUSY —— 否则 total_lookup_busy 只是个噪声计数。
        // ★ BUSY 用**直接改 slot 状态**构造, 不靠并发碰运气 (否则测试会随机红/绿)。
        OmsShmWriter w;
        fresh(w, dir, "t20.dat", 64);
        w.set_min_reclaim_age_ns(0);      // 本节点不测 TTL, 免得上层的回收干扰

        pubsub::RCommand out;
        using LS = OmsShmSegment::LookupStatus;

        // 单次 lookup, 返回 (三态, BUSY 计数增量)。
        //   ★ 为什么按**增量**断言而不是"总数 == N": bool 版 lookup_by_orderSysId 走的是
        //     同一条路径, 也会计数。把"我调用了几次"写死进断言, 一改测试就假红。
        auto q = [&](const char* key) -> std::pair<LS, uint64_t> {
            const uint64_t b = w.local_lookup_busy();
            const LS st = w.lookup_by_orderSysId_ex(key, out);
            return { st, w.local_lookup_busy() - b };
        };

        check(std::string(OmsShmSegment::to_string(LS::OK))        == "OK" &&
              std::string(OmsShmSegment::to_string(LS::NOT_FOUND)) == "NOT_FOUND" &&
              std::string(OmsShmSegment::to_string(LS::BUSY))      == "BUSY",
              "to_string(三态) 可读 (供日志/对账打点)");

        check(w.upsert(mk("B3-1", "B3O-1", OS_NEW, 1001)) != kInvalidSlot,
              "前置: 写入 1 张活单");
        const uint32_t slot = find_slot_of(w, "B3-1");
        check(slot != kInvalidSlot, "前置: 找得到它的 slot", u(slot));
        check(w.local_lookup_busy() == 0, "前置: BUSY 计数为 0");

        // --- ① OK: 三个索引都走通 ---
        check(w.lookup_by_orderSysId_ex("B3-1", out) == LS::OK, "主索引 (orderSysId) → OK");
        check(w.lookup_by_orderSysId("B3-1", out), "bool 版: 活单 → true");
        check(w.lookup_by_client_ex("sss_test1", 1001, out) == LS::OK, "复合 client key → OK");
        check(w.lookup_by_orderId_ex("B3O-1", out) == LS::OK, "orderId → OK");

        // --- ② NOT_FOUND: 而且**不能**计成 BUSY ---
        {
            const auto r1 = q("NOPE");
            const auto r2 = q("");
            check(r1.first == LS::NOT_FOUND && r1.second == 0,
                  "不存在的 key → NOT_FOUND 且不计 BUSY", u(r1.second));
            check(r2.first == LS::NOT_FOUND && r2.second == 0,
                  "空 key → NOT_FOUND 且不计 BUSY", u(r2.second));
            check(w.lookup_by_client_ex("sss_test1", 999999, out) == LS::NOT_FOUND,
                  "不存在的 client → NOT_FOUND");
            check(w.lookup_by_orderId_ex("NOPE-OID", out) == LS::NOT_FOUND,
                  "不存在的 orderId → NOT_FOUND");
            check(w.local_lookup_busy() == 0,
                  "★ 一路 NOT_FOUND 下来 BUSY 计数仍是 0",
                  u(w.local_lookup_busy()));
        }

        // --- ③ BUSY: slot 正在被回收 (确定性构造) ---
        w.slots()[slot].state.store(SLOT_RECLAIMING, std::memory_order_release);
        {
            const auto r = q("B3-1");
            check(r.first == LS::BUSY, "slot 处于 RECLAIMING → BUSY");
            check(r.second == 1, "BUSY 计数恰好 +1 (单次调用)", u(r.second));
        }
        {
            const uint64_t b = w.local_lookup_busy();
            check(!w.lookup_by_orderSysId("B3-1", out), "bool 版语义不变: BUSY 也是 false");
            check(w.local_lookup_busy() == b + 1,
                  "bool 版走同一路径, 也计一次 BUSY",
                  u(w.local_lookup_busy() - b));
        }

        // 状态恢复 → 又是 OK, 且计数不再涨 (证明 BUSY 是**暂时**的, 重试有效)
        w.slots()[slot].state.store(SLOT_LIVE, std::memory_order_release);
        {
            const auto r = q("B3-1");
            check(r.first == LS::OK && r.second == 0,
                  "状态恢复 → 回到 OK 且不再计 BUSY", u(r.second));
        }

        // --- ④ 空 slot ≠ BUSY: 索引条目陈旧 → NOT_FOUND ---
        w.slots()[slot].state.store(SLOT_EMPTY, std::memory_order_release);
        {
            const auto r = q("B3-1");
            check(r.first == LS::NOT_FOUND,
                  "★ slot 已空 (索引条目陈旧) → NOT_FOUND, **不是** BUSY");
            check(r.second == 0, "空 slot 不计入 BUSY", u(r.second));
        }
        w.slots()[slot].state.store(SLOT_LIVE, std::memory_order_release);

        // --- ⑤ 索引条目被破坏 (slot_idx 非法) 不会崩、也不会误报 OK ---
        //   注意语义: 直接篡改 slot_idx 时, `index_probe_find` → `entry_key_match` 会先把它
        //   判成 kStale, 于是 found 根本不成立 → 结果是 NOT_FOUND。代码里那条
        //   "slot_idx 非法 → BUSY" 是给**撕裂读**兜底的 (probe 判 found 之后、重读 slot_idx
        //   之前被并发改掉), 无法确定性构造, 所以这里只钉住**可观测**的降级行为。
        {
            IndexEntry* arr = w.index(IDX_ORDER_SYS_ID);
            const uint32_t icap = w.index_capacity();
            uint32_t hit = kInvalidSlot;
            for (uint32_t b = 0; b < icap; ++b) {
                const uint64_t h = arr[b].key_hash.load(std::memory_order_acquire);
                if (h != kHashEmpty && h != kHashTombstone &&
                    arr[b].slot_idx.load(std::memory_order_acquire) == slot) { hit = b; break; }
            }
            check(hit != kInvalidSlot, "前置: 找得到该 slot 的主索引条目", u(hit));
            if (hit != kInvalidSlot) {
                arr[hit].slot_idx.store(kInvalidSlot, std::memory_order_release);
                const auto r = q("B3-1");
                check(r.first == LS::NOT_FOUND,
                      "索引 slot_idx 非法 → 降级为 NOT_FOUND (不崩、不误报 OK)");
                check(r.second == 0, "该路径不计入 BUSY (被 probe 判为陈旧条目)", u(r.second));
                arr[hit].slot_idx.store(slot, std::memory_order_release);   // 复原
                check(w.lookup_by_orderSysId_ex("B3-1", out) == LS::OK, "索引复原 → 回到 OK");
            }
        }

        // --- ⑥ 本节存在的理由: 对账必须能**区分**活单与死单 ---
        w.slots()[slot].state.store(SLOT_RECLAIMING, std::memory_order_release);
        const LS live = w.lookup_by_orderSysId_ex("B3-1", out);   // 活单, 只是暂时读不到
        const LS gone = w.lookup_by_orderSysId_ex("GONE", out);   // 真的没有
        check(live == LS::BUSY && gone == LS::NOT_FOUND,
              "★ 对账判据: 活单 BUSY / 死单 NOT_FOUND (可区分)",
              std::string("live=") + OmsShmSegment::to_string(live) +
              " gone=" + OmsShmSegment::to_string(gone));
        check(w.lookup_by_orderSysId("B3-1", out) == w.lookup_by_orderSysId("GONE", out),
              "反证: 旧 bool API 下这两者**无法**区分 (都是 false) —— 这就是 B3");
        w.slots()[slot].state.store(SLOT_LIVE, std::memory_order_release);

        // --- ⑦ ★ 只读 reader 走 BUSY 路径绝不能写 SHM (本轮抓到的真 bug) ---
        //   第一版把 BUSY 计数放进了 SHM header。reader 的映射是 PROT_READ
        //   (open(): read_only ? PROT_READ : PROT_READ|PROT_WRITE), 于是那次 fetch_add
        //   撞上写保护页 → SIGBUS。症状是**偶发**, 极易被当成"测试不稳定"放过去:
        //   实测 §17 并发用例 20 次挂 2 次, EXC_BAD_ACCESS code=2, 崩在 ldadd 指令上。
        //   这里用一个**只读 reader** 明确钉住: BUSY 路径在只读映射上必须能安全走完,
        //   而且计数只记在它自己身上 (进程内/实例内), 不碰 SHM。
        {
            w.slots()[slot].state.store(SLOT_RECLAIMING, std::memory_order_release);
            const uint64_t w_before = w.local_lookup_busy();

            OmsShmReader rr;
            rr.open(j(dir, "t20.dat"));
            check(rr.is_open(), "reader 以只读方式打开同一个文件");
            check(rr.local_lookup_busy() == 0, "reader 自己的 BUSY 计数从 0 起");

            pubsub::RCommand ro;
            const LS rst = rr.lookup_by_orderSysId_ex("B3-1", ro);
            check(rst == LS::BUSY, "★ 只读 reader: RECLAIMING slot → BUSY (没有 SIGBUS)");
            check(rr.local_lookup_busy() == 1,
                  "只读 reader 的 BUSY 计在自己身上", u(rr.local_lookup_busy()));

            for (int i = 0; i < 8; ++i) (void)rr.lookup_by_orderSysId_ex("B3-1", ro);
            check(rr.local_lookup_busy() == 9,
                  "只读 reader 连续 9 次 BUSY 都安全", u(rr.local_lookup_busy()));

            check(w.local_lookup_busy() == w_before,
                  "reader 的 BUSY 不计入 writer 实例 (计数不共享, 也没写 SHM)",
                  u(w.local_lookup_busy()));

            w.slots()[slot].state.store(SLOT_LIVE, std::memory_order_release);
            check(rr.lookup_by_orderSysId_ex("B3-1", ro) == LS::OK,
                  "只读 reader: 状态恢复 → OK");
        }
    }

    // -------------------------------------------------------------------------
    // 给 oms_shm.sh test 用的解析契约 fixture: 一个有内容、计数非零的 shm,
    // 供 oms_query --stats 输出后校验"数值行以数字结尾"。
    // -------------------------------------------------------------------------
    {
        OmsShmWriter w;
        fresh(w, dir, "parser_fixture.dat", 64);
        w.set_min_reclaim_age_ns(0);
        for (uint32_t i = 0; i < 8; ++i) {
            char s[32], o[32];
            std::snprintf(s, sizeof s, "P%02u", i);
            std::snprintf(o, sizeof o, "N%02u", i);
            w.upsert(mk(s, o, OS_NEW, 600 + i));
        }
        w.upsert(mk("P00", "N00", OS_FILLED, 600));    // 制造 1 个 FINISHED
        w.upsert(mk("PZZ", "NZZ", OS_NEW, 999));       // 触发一次 reclaim
        // ★ 把 TTL 恢复成默认值再交付 fixture: 这样 `oms_query --stats` 打出的
        //   sustainable_insert_rate 是**非零**的, 解析契约才算真的验到那个数
        //   (全是 0 的话, 计算写错也看不出来)。
        w.set_min_reclaim_age_ns(kDefaultMinReclaimAgeNs);
    }

    // -------------------------------------------------------------------------
    std::printf("\n========================================\n");
    std::printf("PASS %d   FAIL %d\n", g_pass, g_fail);
    std::printf("========================================\n");
    if (g_fail == 0) std::printf("ALL TESTS PASSED\n");
    else             std::printf("THERE ARE FAILURES\n");
    return g_fail == 0 ? 0 : 1;
}
