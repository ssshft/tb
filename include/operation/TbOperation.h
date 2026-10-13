#pragma once

#include <unordered_map>
#include "program_util.h"
#include "log_engine.h"
#include "securitymanager.h"
#include "crypto_exception.h"
#include "config.h"
#include "key_util.h"
#include "command_helper.h"
#include "shm_global.h"
#include "base/BaseTrade.h"
#include "oms/OrderManager.h"
#include "utils/tb_global.h"



class TbOperation {

public:
    TbOperation();
    ~TbOperation();
    bool preStart(Config* config);
    void run();
    void execute();
    void executeTcmd();
    void preStop();
    void processTcmd(pubsub::TCommand& tcmd);
    void processRcmd(pubsub::RCommand& rcmd);
    bool rptNewOrder(pubsub::RCommand& rcmd);

protected:
    sm::SecurityManager* smc{nullptr};
    std::vector<Utrade2TbTcmd*> vTcmd;
    Tb2UtradeRcmd* tb2TradeRcmd{nullptr};
    TcmdInnerQueue tcmdInnerQueue;
    std::unordered_map<std::string, BaseTradeClient*> mTradeClient;
    om::OrderManager orderManager;
};