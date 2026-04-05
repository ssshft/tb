#pragma once

#include "data_struct.h"
#include "pubsub_protocol.h"
#include "log_engine.h"
#include "time_util.h"
#include "command_helper.h"
#include "key_util.h"
#include "precision_util.h"
#include "utils/tb_global.h"


namespace am {
    constexpr auto kPushZeroPositionSeconds = 60;

    class AccountManager{
    public:
        AccountManager();
        ~AccountManager();

        void preStart();
     
        void preStop();

        bool processRcmd(pubsub::RCommand& rcmd);
       
        void load_data();
        void store_data();

    protected:

        std::unordered_map<std::string, pubsub::RCommand> balanceMap;
        std::unordered_map<std::string, pubsub::RCommand> positionMap;
        std::unordered_map<std::string, std::unordered_map<std::string, pubsub::RCommand>> currentPositionMap;
        std::unordered_map<std::string, long> lastPubZeroPositionMap;
        
        // tb_sqlite::SqliteStorage *storage;
    };
}