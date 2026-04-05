#pragma once

#include "data_struct.h"
#include "pubsub_protocol.h"
#include "log_engine.h"
#include "time_util.h"
#include "oms/AccountManager.h"
#include <oneapi/tbb/concurrent_unordered_map.h>
#include "crypto_exception.h"
#include "utils/order_util.h"
#include "utils/tb_global.h"


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
        bool getOrderSysId(const int64_t clientOrderId, const char* strategyId, char* orderSysId, const char* orderId="");
        std::string getOrderSysId(ExchangeType exchangeTypeEnum, const char* strategyId);

    protected:
        oneapi::tbb::concurrent_unordered_map<std::string, std::string> clientOrderId2OrderSysIdMap;
        oneapi::tbb::concurrent_unordered_map<std::string, std::string> orderId2OrderSysIdMap;
        oneapi::tbb::concurrent_unordered_map<std::string, pubsub::RCommand> orderSysId2OrderResponseMap;

        std::unordered_map<std::string, PushState> pushStateMap;
        am::AccountManager accountManager;
        // tb_sqlite::SqliteStorage storage;
    };
}