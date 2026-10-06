// =============================================================================
// oms_bench.cpp — OMS SHM 性能测试
//
// 测量指标:
//   - insert 吞吐 (单线程 writer 每秒能写入多少单)
//   - update 吞吐 (对已存在单更新)
//   - lookup 吞吐 (单线程 reader 每秒能查多少单)
//   - 各操作延迟分位数 (p50 / p95 / p99 / p999 / max)
//   - 混合读写场景 (1 writer + N reader 并行, 模拟策略进程)
//
// 用法:
//   oms_bench --shm=/dev/shm/tb_bench.dat --capacity=100000 \
//             --iters=1000000 --readers=4 --reset
//
// 编译:
//   g++ -std=c++17 -O2 -pthread -I../../include -I../include \
//       oms_bench.cpp -o oms_bench
// =============================================================================

#include "oms/OmsShm.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace oms::shm;

// -----------------------------------------------------------------------------
// 计时辅助
// -----------------------------------------------------------------------------
using Clock = std::chrono::steady_clock;
static inline uint64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        Clock::now().time_since_epoch()).count();
}

// -----------------------------------------------------------------------------
// 分位数
// -----------------------------------------------------------------------------
struct LatSummary {
    uint64_t n = 0;
    uint64_t p50 = 0, p95 = 0, p99 = 0, p999 = 0, mn = 0, mx = 0;
    double   mean_ns = 0;
    double   ops_per_sec = 0;
};

static LatSummary summarize(std::vector<uint64_t>& lat_ns, uint64_t total_wall_ns) {
    LatSummary s;
    s.n = lat_ns.size();
    if (s.n == 0) return s;
    std::sort(lat_ns.begin(), lat_ns.end());
    auto pick = [&](double q){
        size_t i = static_cast<size_t>(q * (s.n - 1));
        return lat_ns[i];
    };
    s.mn   = lat_ns.front();
    s.mx   = lat_ns.back();
    s.p50  = pick(0.50);
    s.p95  = pick(0.95);
    s.p99  = pick(0.99);
    s.p999 = pick(0.999);
    uint64_t sum = 0;
    for (auto v : lat_ns) sum += v;
    s.mean_ns = static_cast<double>(sum) / s.n;
    if (total_wall_ns > 0) {
        s.ops_per_sec = static_cast<double>(s.n) * 1e9 / total_wall_ns;
    }
    return s;
}

static void print_summary(const char* name, const LatSummary& s) {
    std::printf("\n=== %s ===\n", name);
    std::printf("  ops         : %llu\n", (unsigned long long)s.n);
    std::printf("  throughput  : %.0f ops/sec\n", s.ops_per_sec);
    std::printf("  mean        : %.0f ns\n", s.mean_ns);
    std::printf("  min         : %llu ns\n", (unsigned long long)s.mn);
    std::printf("  p50         : %llu ns\n", (unsigned long long)s.p50);
    std::printf("  p95         : %llu ns\n", (unsigned long long)s.p95);
    std::printf("  p99         : %llu ns\n", (unsigned long long)s.p99);
    std::printf("  p999        : %llu ns\n", (unsigned long long)s.p999);
    std::printf("  max         : %llu ns\n", (unsigned long long)s.mx);
}

// -----------------------------------------------------------------------------
// 生成测试用 RCommand
//
// ★ prefix / oid_base 必须让**各相位之间的 key 空间互不相交**:
//   - orderSysId = "<prefix>-<seq>"  → 主索引
//   - strategyId = "<prefix>"        → 复合 clientOrderId 索引 (避免不同 prefix 撞 key)
//   - orderId    = "<oid_base>+<seq>"→ orderId 索引
//   否则后一个相位会**更新**前一个相位留下的单, 而不是新建 —— 测出来的东西
//   跟注释写的完全不是一回事 (第 23 轮踩过: MIXED 相位复用 "bench-<i>" 且 i 从 0
//   开始, 于是 i < insert_iters 全是 update, 只有 i >= insert_iters 才真的新建)。
// -----------------------------------------------------------------------------
static void make_rcmd(pubsub::RCommand& r, uint64_t seq, OrderStatus st,
                      const char* prefix = "bench", uint64_t oid_base = 1000000) {
    std::memset(&r, 0, sizeof(r));
    r.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
    auto& o = r.body.orderResponse;
    // orderSysId: <prefix>-<seq>
    std::snprintf(o.orderSysId, sizeof(o.orderSysId), "%s-%llu", prefix,
                  (unsigned long long)seq);
    o.clientOrderId = static_cast<int64_t>(seq);
    std::snprintf(o.orderId, sizeof(o.orderId), "%llu",
                  (unsigned long long)(oid_base + seq));
    std::snprintf(o.strategyId, sizeof(o.strategyId), "%s", prefix);
    std::snprintf(o.instId, sizeof(o.instId), "BTC-USDT");
    o.exchangeTypeEnum = BINANCE;
    o.instTypeEnum     = SPOT;
    o.direction        = DT_LONG;
    o.orderType        = OT_LIMIT;
    o.orderStatus      = st;
    o.volumeTotal      = 100.0;
    o.volumeTraded     = (st == OS_FILLED) ? 100.0 : (st == OS_PARTFILLED ? 50.0 : 0.0);
    o.limitPrice       = 50000.0;
    o.tradePrice       = (o.volumeTraded > 0) ? 50000.0 : 0.0;
    o.updateTime       = static_cast<long>(seq);
}

// -----------------------------------------------------------------------------
// 各测试
// -----------------------------------------------------------------------------

// 1) 顺序 insert (全新 slot 分配)
static void bench_insert(OmsShmWriter& w, uint64_t iters) {
    std::vector<uint64_t> lat; lat.reserve(iters);
    uint64_t t0 = now_ns();
    pubsub::RCommand r;
    for (uint64_t i = 0; i < iters; ++i) {
        make_rcmd(r, i, OS_NEW);
        uint64_t s = now_ns();
        uint32_t idx = w.upsert(r);
        uint64_t e = now_ns();
        if (idx == kInvalidSlot) {
            std::fprintf(stderr, "insert failed at i=%llu (ring exhausted)\n", (unsigned long long)i);
            break;
        }
        lat.push_back(e - s);
    }
    uint64_t t1 = now_ns();
    auto s = summarize(lat, t1 - t0);
    print_summary("INSERT (new orderSysId each)", s);
}

// 2) update: 对上一步 insert 过的单更新到 FILLED
static void bench_update(OmsShmWriter& w, uint64_t iters) {
    std::vector<uint64_t> lat; lat.reserve(iters);
    uint64_t t0 = now_ns();
    pubsub::RCommand r;
    for (uint64_t i = 0; i < iters; ++i) {
        make_rcmd(r, i, OS_FILLED);   // 同 orderSysId, 状态改
        uint64_t s = now_ns();
        w.upsert(r);
        uint64_t e = now_ns();
        lat.push_back(e - s);
    }
    uint64_t t1 = now_ns();
    auto s = summarize(lat, t1 - t0);
    print_summary("UPDATE (same orderSysId → FILLED)", s);
}

// 3) lookup: 单线程 reader
static void bench_lookup(OmsShmReader& r, uint64_t iters, uint64_t max_seq) {
    std::vector<uint64_t> lat; lat.reserve(iters);
    pubsub::RCommand out;
    uint64_t t0 = now_ns();
    uint64_t hit = 0;
    char key[64];
    for (uint64_t i = 0; i < iters; ++i) {
        uint64_t idx = i % max_seq;
        int n = std::snprintf(key, sizeof(key), "bench-%llu", (unsigned long long)idx);
        std::string_view sv(key, n);
        uint64_t s = now_ns();
        bool ok = r.lookup_by_orderSysId(sv, out);
        uint64_t e = now_ns();
        if (ok) ++hit;
        lat.push_back(e - s);
    }
    uint64_t t1 = now_ns();
    auto s = summarize(lat, t1 - t0);
    print_summary("LOOKUP by orderSysId", s);
    std::printf("  hit_rate    : %.2f%%\n", 100.0 * hit / iters);
}

// 4) 混合: 1 writer + N reader 并行
//
// ★ 这个相位必须是**可持续**的流, 否则它测的不是 store 的容量, 而是自己的建模错误。
//   两个前提 (第 23 轮修正, 之前两个都不成立):
//
//   (a) key 空间与前面的 insert / update 相位**不重叠**。
//       旧版复用 "bench-<i>" 且 i 从 0 开始 → i < insert_iters 的调用其实是在
//       **更新**前面插入的单, 只有 i >= insert_iters 才真的新建 slot。
//       实测 (cap=4096, iters=20000): upd += 4095, ins += 1 —— 注释里写的
//       "一半 insert 一半 update" 从来没有成立过。现在用独立的 "mx-" 前缀。
//
//   (b) 新建单的数量必须能被环容纳。稳态占用 ≈ **写入速率 × TTL**:
//       TTL = min_reclaim_age_ns (默认 60s) 时, 本机 ~70 万单/秒 → 需要约
//       4200 万个 slot, 而 131072 的环 0.2 秒就满, 之后**每一张单都失败**
//       (实测 100 万次迭代里 868928 次失败, 日志刷屏)。
//       所以这里把 TTL 调成 **--mixed-ttl-ms (默认 0 = 立刻可回收)**:
//       回收始终可用 → 无论机器多快、容量多小都不会写满, 而且回收路径被真实压到。
//       想复现"环接近写满"的行为, 显式给一个大 TTL 即可 (此时必须满足
//       capacity >= 速率 × TTL, 否则必然丢单 —— 这正是那次事故的成因)。
//
//   每个单走两步: 偶数次迭代 insert(NEW), 紧接着奇数次迭代 update(FILLED),
//   所以每张单都会变成 FINISHED, TTL 过后可被回收 —— insert / update /
//   状态迁移 / 回收 四条路径全部被覆盖。
static void bench_mixed(const std::string& shm_path, uint64_t writer_iters,
                        uint64_t reader_iters, int n_readers, uint64_t min_reclaim_ms)
{
    std::atomic<bool>     stop{false};
    std::atomic<uint64_t> orders_done{0};     // 已推进到终态的单数 (reader 的查询上界)
    std::vector<uint64_t> wlat;
    uint64_t              wwall = 0;

    // Writer
    std::thread wt([&]{
        OmsShmWriter w;
        w.open(shm_path);
        // ★ 见上面 (b): 不调小 TTL, 这个相位必然把环写满。
        w.set_min_reclaim_age_ns(min_reclaim_ms * 1'000'000ULL);
        wlat.reserve(writer_iters);
        pubsub::RCommand r;
        const uint64_t t0 = now_ns();
        for (uint64_t i = 0; i < writer_iters; ++i) {
            const uint64_t    ord = i >> 1;                       // 每个单占两次迭代
            const OrderStatus st  = (i & 1) ? OS_FILLED : OS_NEW;  // insert → 终态
            make_rcmd(r, ord, st, "mx", 5'000'000);
            const uint64_t s = now_ns();
            w.upsert(r);
            const uint64_t e = now_ns();
            wlat.push_back(e - s);
            if (i & 1) orders_done.fetch_add(1, std::memory_order_relaxed);
        }
        wwall = now_ns() - t0;
        stop.store(true, std::memory_order_release);
    });

    // Readers
    std::vector<std::thread> rts;
    std::vector<LatSummary>  rsummary(n_readers);
    std::vector<std::vector<uint64_t>> rlats(n_readers);
    std::vector<uint64_t> rwall(n_readers);
    for (int t = 0; t < n_readers; ++t) {
        rts.emplace_back([&, t]{
            OmsShmReader r;
            r.open(shm_path);
            std::vector<uint64_t>& lat = rlats[t];
            lat.reserve(reader_iters);
            uint64_t t0 = now_ns();
            pubsub::RCommand out;
            char key[64];
            uint64_t i = 0;
            while (i < reader_iters && !stop.load(std::memory_order_acquire)) {
                uint64_t done = orders_done.load(std::memory_order_relaxed);
                if (done == 0) { std::this_thread::yield(); continue; }
                uint64_t pick = i % done;
                int n = std::snprintf(key, sizeof(key), "mx-%llu", (unsigned long long)pick);
                uint64_t s = now_ns();
                r.lookup_by_orderSysId(std::string_view(key, n), out);
                uint64_t e = now_ns();
                lat.push_back(e - s);
                ++i;
            }
            uint64_t t1 = now_ns();
            rwall[t] = t1 - t0;
        });
    }
    wt.join();
    for (auto& t : rts) t.join();

    // writer 的结果也要打 —— 旧版只打 reader, "1 writer + N readers" 里的 writer
    // 吞吐完全没被报告, 而它才是这个相位真正想测的东西。
    print_summary("MIXED-WRITER", summarize(wlat, wwall));
    for (int t = 0; t < n_readers; ++t) {
        char name[64];
        std::snprintf(name, sizeof(name), "MIXED-READER-%d", t);
        auto s = summarize(rlats[t], rwall[t]);
        print_summary(name, s);
    }

    // 自检: 可持续的流不该丢单, 且应该真的发生过回收。
    OmsShmReader rc;
    rc.open(shm_path);
    const auto st = rc.stats();
    std::printf("\n  [MIXED 自检] ttl=%llums  inserts=%llu  updates=%llu  reclaims=%llu  "
                "alloc_fail=%llu\n",
                (unsigned long long)min_reclaim_ms,
                (unsigned long long)st.total_inserts,
                (unsigned long long)st.total_updates,
                (unsigned long long)st.total_reclaims,
                (unsigned long long)st.total_alloc_failures);
    std::printf("    empty=%u live=%u finished=%u (slot_cap=%u)\n",
                st.empty, st.live, st.finished, st.capacity);
    if (st.total_alloc_failures == 0) {
        std::printf("    ✓ 可持续: 0 丢单\n");
    } else {
        std::printf("    ✗ 不可持续: 丢了 %llu 单 —— TTL 太大或 capacity 太小 "
                    "(可持续速率上限 ≈ slot_cap / TTL = %llu 单/秒)\n",
                    (unsigned long long)st.total_alloc_failures,
                    (unsigned long long)(st.min_reclaim_age_ns
                        ? (uint64_t)st.capacity * 1'000'000'000ULL / st.min_reclaim_age_ns : 0));
    }
}

// -----------------------------------------------------------------------------
// main
// -----------------------------------------------------------------------------
static void usage() {
    std::fprintf(stderr,
        "Usage:\n"
        "  oms_bench [--shm=<path>] [--capacity=<N>] [--iters=<N>]\n"
        "            [--readers=<N>] [--mixed-ttl-ms=<N>] [--reset]\n"
        "\n"
        "  --shm       path (default: /dev/shm/tb_bench.dat)\n"
        "  --capacity  slot count (default: 100000, range 1..2^28;\n"
        "              index_capacity = next_pow2(4*slot_cap) is computed for you,\n"
        "              slot_cap itself does NOT need to be a power of two)\n"
        "  --iters     iterations per test (default: 500000)\n"
        "  --readers   concurrent reader threads in mixed test (default: 4)\n"
        "  --mixed-ttl-ms  FINISHED 最小 TTL (default: 0) 只在 mixed 相位生效。\n"
        "              可持续写入速率上限 ≈ capacity / TTL。生产默认 TTL 是 60s:\n"
        "              131072 slot 只够约 2185 单/秒, 而本机实测 70 万单/秒 ——\n"
        "              环 0.2 秒就满, 之后每一张单都失败。所以 mixed 相位默认用\n"
        "              0 (= 立刻可回收), 保证任何机器/容量都不会写满; 想复现\n"
        "              「环写满」的行为就显式给一个大值。\n"
        "  --reset     zero-init the shm at start (safe, DO NOT run on production shm)\n");
}

int main(int argc, char** argv) {
    std::string shm_path = "/dev/shm/tb_bench.dat";
    uint32_t capacity = 100'000;
    uint64_t iters = 500'000;
    int n_readers = 4;
    uint64_t mixed_ttl_ms = 0;       // 见 bench_mixed 的说明: 0 = 回收始终可用, 任何容量都不会写满
    bool do_reset = false;

    for (int i = 1; i < argc; ++i) {
        std::string_view a(argv[i]);
        if      (a.rfind("--shm=", 0) == 0)      shm_path  = std::string(a.substr(6));
        else if (a.rfind("--capacity=", 0) == 0) capacity  = std::stoul(std::string(a.substr(11)));
        else if (a.rfind("--iters=", 0) == 0)    iters     = std::stoull(std::string(a.substr(8)));
        else if (a.rfind("--readers=", 0) == 0)  n_readers = std::stoi (std::string(a.substr(10)));
        else if (a.rfind("--mixed-ttl-ms=", 0) == 0)
                                                 mixed_ttl_ms = std::stoull(std::string(a.substr(15)));
        else if (a == "--reset")                 do_reset  = true;
        else if (a == "-h" || a == "--help") { usage(); return 0; }
        else { std::fprintf(stderr, "unknown arg: %s\n", argv[i]); usage(); return 1; }
    }

    // 打开 (writer 建/清)
    OmsShmWriter w;
    try {
        w.open(shm_path, capacity);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "writer open failed: %s\n", e.what());
        return 2;
    }
    if (do_reset) {
        // ★ 打开**已存在**的文件时, 容量一律从 header 读, 命令行 --capacity 被忽略
        //   (OmsShm.h 的既定语义: 容量归 owner 所有)。于是 "./oms_shm.sh bench"
        //   传的 --capacity=131072 会被一个遗留的 16k 文件悄悄吃掉, 而 --reset 只清零
        //   不改变容量 —— 测出来的东西和以为的完全不是一回事。这里显式重建。
        if (w.slot_capacity() != capacity) {
            std::fprintf(stderr,
                "[reset] capacity mismatch: file=%u requested=%u → unlink & recreate\n",
                w.slot_capacity(), capacity);
            w.close();
            if (::unlink(shm_path.c_str()) != 0 && errno != ENOENT) {
                std::fprintf(stderr, "unlink failed: errno=%d\n", errno);
                return 4;
            }
            try {
                w.open(shm_path, capacity);
            } catch (const std::exception& e) {
                std::fprintf(stderr, "writer re-open failed: %s\n", e.what());
                return 2;
            }
        }
        std::fprintf(stderr, "[reset] wiping shm ...\n");
        w.reset_all();
    }
    // ★ 打**文件里实际的** slot_capacity, 而不是命令行请求的那个: 打开已存在的文件时
    //   容量一律从 header 读, 请求值被忽略 —— 旧版打的是请求值, 容易看错。
    std::printf("SHM ready: path=%s capacity=%u (requested=%u, slot_size=%zuB, total=%.1fMB)\n",
                shm_path.c_str(), w.slot_capacity(), capacity,
                sizeof(OmsSlot),
                (double)OmsShmLayout::compute_total_size(w.slot_capacity(),
                                                         w.index_capacity()) / (1024.0 * 1024.0));

    // 单线程 insert / update / lookup
    // insert iters 上限 = capacity (超了会开始 reclaim, 干扰纯 insert 语义)
    uint64_t insert_iters = std::min<uint64_t>(iters, capacity - 1);
    bench_insert(w, insert_iters);
    bench_update(w, insert_iters);

    // Reader (read-only mmap 同一份)
    OmsShmReader r;
    try {
        r.open(shm_path);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "reader open failed: %s\n", e.what());
        return 3;
    }
    bench_lookup(r, iters, insert_iters);

    // 混合读写
    w.close();
    r.close();
    std::printf("\n=== MIXED: 1 writer + %d readers (mixed TTL = %llu ms) ===\n",
                n_readers, (unsigned long long)mixed_ttl_ms);
    bench_mixed(shm_path, iters, iters, n_readers, mixed_ttl_ms);

    // 把 TTL 恢复成默认值 —— mixed 相位为了可持续把它调小了, 不该留给下一次运行。
    {
        OmsShmWriter wr;
        wr.open(shm_path);
        wr.set_min_reclaim_age_ns(kDefaultMinReclaimAgeNs);
    }

    // 最终 stats
    OmsShmReader r2;
    r2.open(shm_path);
    auto s = r2.stats();
    std::printf("\n=== FINAL STATS ===\n");
    std::printf("  capacity=%u  empty=%u live=%u finished=%u\n",
                s.capacity, s.empty, s.live, s.finished);
    std::printf("  total_inserts=%llu  updates=%llu  reclaims=%llu  alloc_fail=%llu\n",
                (unsigned long long)s.total_inserts,
                (unsigned long long)s.total_updates,
                (unsigned long long)s.total_reclaims,
                (unsigned long long)s.total_alloc_failures);
    return 0;
}