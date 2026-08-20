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
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <thread>
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
// -----------------------------------------------------------------------------
static void make_rcmd(pubsub::RCommand& r, uint64_t seq, OrderStatus st) {
    std::memset(&r, 0, sizeof(r));
    r.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
    auto& o = r.body.orderResponse;
    // orderSysId: bench-<seq>
    std::snprintf(o.orderSysId, sizeof(o.orderSysId), "bench-%llu", (unsigned long long)seq);
    o.clientOrderId = static_cast<int64_t>(seq);
    std::snprintf(o.orderId, sizeof(o.orderId), "%llu", (unsigned long long)(1000000 + seq));
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
static void bench_mixed(const std::string& shm_path, uint64_t writer_iters,
                        uint64_t reader_iters, int n_readers)
{
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> writer_ops{0};

    // Writer
    std::thread wt([&](){
        OmsShmWriter w;
        w.open(shm_path);
        pubsub::RCommand r;
        for (uint64_t i = 0; i < writer_iters; ++i) {
            // 一半 insert 一半 update (触发状态迁移 + 部分 reclaim)
            OrderStatus st = (i % 3 == 2) ? OS_FILLED : ((i % 3 == 1) ? OS_PARTFILLED : OS_NEW);
            make_rcmd(r, i, st);
            w.upsert(r);
            writer_ops.fetch_add(1, std::memory_order_relaxed);
        }
        stop.store(true);
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
                uint64_t seq = writer_ops.load(std::memory_order_relaxed);
                if (seq == 0) { std::this_thread::yield(); continue; }
                uint64_t pick = i % seq;
                int n = std::snprintf(key, sizeof(key), "bench-%llu", (unsigned long long)pick);
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

    // 汇总每个 reader
    for (int t = 0; t < n_readers; ++t) {
        char name[64];
        std::snprintf(name, sizeof(name), "MIXED-READER-%d", t);
        auto s = summarize(rlats[t], rwall[t]);
        print_summary(name, s);
    }
}

// -----------------------------------------------------------------------------
// main
// -----------------------------------------------------------------------------
static void usage() {
    std::fprintf(stderr,
        "Usage:\n"
        "  oms_bench [--shm=<path>] [--capacity=<N>] [--iters=<N>]\n"
        "            [--readers=<N>] [--reset]\n"
        "\n"
        "  --shm       path (default: /dev/shm/tb_bench.dat)\n"
        "  --capacity  slot count (default: 100000, must be power of 2 for speed)\n"
        "  --iters     iterations per test (default: 500000)\n"
        "  --readers   concurrent reader threads in mixed test (default: 4)\n"
        "  --reset     zero-init the shm at start (safe, DO NOT run on production shm)\n");
}

int main(int argc, char** argv) {
    std::string shm_path = "/dev/shm/tb_bench.dat";
    uint32_t capacity = 100'000;
    uint64_t iters = 500'000;
    int n_readers = 4;
    bool do_reset = false;

    for (int i = 1; i < argc; ++i) {
        std::string_view a(argv[i]);
        if      (a.rfind("--shm=", 0) == 0)      shm_path  = std::string(a.substr(6));
        else if (a.rfind("--capacity=", 0) == 0) capacity  = std::stoul(std::string(a.substr(11)));
        else if (a.rfind("--iters=", 0) == 0)    iters     = std::stoull(std::string(a.substr(8)));
        else if (a.rfind("--readers=", 0) == 0)  n_readers = std::stoi (std::string(a.substr(10)));
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
        std::fprintf(stderr, "[reset] wiping shm ...\n");
        w.reset_all();
    }
    std::printf("SHM ready: path=%s capacity=%u (slot_size=%zuB, total=%.1fMB)\n",
                shm_path.c_str(), capacity,
                sizeof(OmsSlot),
                (double)OmsShmLayout::compute_total_size(capacity, capacity * 2) / (1024.0 * 1024.0));

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
    std::printf("\n=== MIXED: 1 writer + %d readers ===\n", n_readers);
    bench_mixed(shm_path, iters, iters, n_readers);

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
