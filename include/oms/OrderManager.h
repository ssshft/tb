#pragma once

#include "data_struct.h"
#include "pubsub_protocol.h"
#include "log_engine.h"
#include "time_util.h"
#include "oms/AccountManager.h"
#include "oms/OmsShm.h"                 // ← 取代 <tbb/concurrent_unordered_map.h>
#include "crypto_exception.h"
#include "utils/order_util.h"
#include "utils/tb_global.h"


namespace om {

    // OmsShm 段的配置。
    //
    // ★ 容量和 TTL **必须一起规划**, 只调一个会持续静默丢单:
    //       稳态占用 ≈ 写入速率 × min_reclaim_age
    //    ⇒  **可持续写入速率上限 = slot_capacity / min_reclaim_age**
    //
    //   默认 (100000 slot + 60s) ⇒ 只有 **约 1666 单/秒**。超过这条线环必然写满,
    //   `upsert` 返回 kInvalidSlot → `processTcmd` 会**拒绝该单**(不发给交易所)。
    //
    //   例: 峰值 20000 单/秒、想留 60s 历史 ⇒ slot_capacity 至少 1_200_000。
    //       内存 ≈ slot_capacity × 1KB + 3 × next_pow2(4×slot_capacity) × 32B。
    //       (1200000 slot ⇒ 约 1.2 GB + 1.6 GB, 需要确认 /dev/shm 够大)
    //
    //   如果只想要"够用就行"而不留历史, 把 min_reclaim_age_ns 调小即可 (回收更激进),
    //   但**不能**调到 0: 0 意味着刚 FINISHED 就被复用, 上层按 orderId 反查会落空。
    struct OmsShmConfig {
        std::string path               = "/dev/shm/tb_order";
        uint32_t    slot_capacity      = 100000;
        uint64_t    min_reclaim_age_ns = 60ULL * 1000000000ULL;              // FINISHED 多久后可回收
        uint64_t    max_live_stale_ns  = 24ULL * 3600ULL * 1000000000ULL;    // LIVE 僵尸兜底

        // 可持续写入速率上限 (单/秒); 0 = 不限制
        uint64_t sustainable_insert_rate() const noexcept {
            return min_reclaim_age_ns
                 ? static_cast<uint64_t>(slot_capacity) * 1000000000ULL / min_reclaim_age_ns
                 : 0;
        }
    };

    class OrderManager{
    public:
        OrderManager();
        ~OrderManager();

        void preStart(const OmsShmConfig& cfg = OmsShmConfig());
        void run();
        void preStop();

        bool processTcmd(pubsub::TCommand& tcmd);
        bool processRcmd(pubsub::RCommand& rcmd);
        bool onOrderUpdate(pubsub::RCommand& rcmd);

        // 测试 / 工具用: 直接访问底层 shm 段 (只读统计、对账遍历)
        oms::shm::OmsShmWriter& omsShm() { return omsShm_; }

    private:
        // 一次查询拿到**完整报单体** (三态)。
        //     OmsShm 的三张索引都指向同一个 slot, slot 里就是完整 RCommand, 所以**一次就够**。
        //   ★ 返回三态而不是 bool: BUSY(索引里有、这一瞬间读不到) 与 NOT_FOUND(真的没有)
        //     会导致**完全不同的动作**, 合并成 false 会把活单回成 OS_REJECTED。
        oms::shm::OmsShmSegment::LookupStatus getOrderSysId(const int64_t clientOrderId, const char* strategyId, pubsub::RCommand& out, const char* orderId="");
        std::string getOrderSysId(ExchangeType exchangeTypeEnum, const char* strategyId);

    protected:
        // ★ 单写者共享内存订单存储 —— 取代原先的三张 tbb::concurrent_unordered_map:
        //     clientOrderId2OrderSysIdMap / orderId2OrderSysIdMap / orderSysId2OrderResponseMap
        //
        //   三张表的内容现在都在这一个段里:
        //     - 报单体 (pubsub::RCommand) → slot
        //     - orderSysId / 复合 clientOrderId / orderId → 三张 hash 索引, 全部指向 slot
        //   复合 client key 的格式与老代码 `fmt::format("{}{}", strategyId, clientOrderId)`
        //   一致 (OmsShm 的 compose_client_key 就是照它写的)。
        //
        //   ⚠ OmsShmWriter 的契约是**单线程串行调用** (见 OmsShm.h 的类注释)。
        //     tb 侧由 `TbOperation::execute()` 这**一个**线程串行驱动
        //     processTcmd / processRcmd, 满足该契约。
        //     如果将来把 tcmd 和 rcmd 拆到不同线程, **必须**在这里加锁 (或每线程一个段),
        //     否则会破坏 next_slot_hint 和 slot 状态机。
        oms::shm::OmsShmWriter omsShm_;

        // 生效的配置 (preStart 时确定), 供日志/诊断直接引用, 避免热路径去算 stats()
        OmsShmConfig cfg_;

        // 丢单计数 —— 用于限流告警。
        //   ★ 为什么不直接把失败逐条打日志: 环写满时**每一单**都会失败。20k 单/秒就是
        //     每秒 20k 条 ERROR —— 和 OMS_SHM_REVIEW.md §2.9 里那个"刷屏"事故同一类问题。
        //     OmsShm 内部的 alloc_exhausted_report 已经限流并打出完整现场, 这里只补
        //     "被上层拒掉的单"这个视角, 同样必须限流。
        uint64_t shm_reject_count_ = 0;
        uint64_t shm_busy_count_   = 0;

        // 限流规则与 OmsShm 一致: 前 8 次 + 之后每 4096 次
        static bool should_log_limited(uint64_t n) noexcept {
            return n <= 8 || (n % 4096) == 0;
        }

        am::AccountManager accountManager;
    };
}
