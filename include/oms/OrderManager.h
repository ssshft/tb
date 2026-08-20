#pragma once

#include "data_struct.h"
#include "pubsub_protocol.h"
#include "log_engine.h"
#include "time_util.h"
#include "oms/AccountManager.h"
#include "oms/OmsShm.h"
#include "crypto_exception.h"
#include "utils/order_util.h"
#include "utils/tb_global.h"

#include <string>
#include <unordered_map>


#define OneDayMicroSeconds 86400 * 1e6
#define OneHourMicroSeconds 3600 * 1e6

namespace om {

    struct PushState {
        double lastVolume{0.0};
        OrderStatus lastStatus{OS_MIN};
    };

    class OrderManager{
    public:
        OrderManager();
        ~OrderManager();

        void preStart();
        void run();
        void preStop();

        bool processTcmd(pubsub::TCommand& tcmd);
        bool processRcmd(pubsub::RCommand& rcmd);
        bool onOrderUpdate(pubsub::RCommand& rcmd);


    private:
        void load_data();
        void store_rcmd_data(vector<pubsub::RCommand> &rcmdVec);
        // 用 (strategyId, clientOrderId) 或 orderId 反查完整报单。
        // 主查失败 (client key) 时退回 orderId 兜底, 都失败返 false。
        // ★ 返回**完整 RCommand**, 避免 caller 再调一次 lookup_by_orderSysId (老代码是 2 次查询)。
        bool findOrder(int64_t clientOrderId, const char* strategyId, const char* orderId,
                       pubsub::RCommand& out);
        // 生成新 orderSysId (交易所前缀 + 时间戳), 与存储无关
        std::string getOrderSysId(ExchangeType exchangeTypeEnum, const char* strategyId);

    protected:
        // ============================================================
        // 报单存储: 共享内存环形数组, 替代原 3 张 tbb map。
        //   - orderSysId → RCommand      (主索引, 直接查完整报单)
        //   - clientOrderId → RCommand   (复合 key: strategyId + int64 cid)
        //   - orderId → RCommand         (交易所返回的 id 别名)
        //   - 内存永远封顶 (~120MB @ 131072 slot), 环形覆盖 FINISHED,
        //     卡单 24h 强制回收, seqlock 无锁并发 read
        //   - 跨进程持久, tb crash 后 restart 自动 recover orphan
        // ============================================================
        oms::shm::OmsShmWriter shm_;
        // SHM 配置 (config 未指定时的默认)
        std::string shm_path_       = "/dev/shm/tb_oms.dat";
        uint32_t    shm_capacity_   = 131072;   // 2^17

        std::unordered_map<std::string, PushState> pushStateMap;
        am::AccountManager accountManager;
        // tb_sqlite::SqliteStorage storage;
    };
}
