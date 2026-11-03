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
#include "SpinBackoff.h"



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

    // 线程绑核：只给**需要独占**的线程开键，-1 = 不绑。preStart 里从 tb.json 读。
    //   cpuExecute     : execute()，OMS 串行线程，自旋
    //   cpuExecuteTcmd : executeTcmd()，下单发送线程，自旋
    // 账号级 REST worker 不在 tb.json 里，逐个读 trade_config.json 的 accounts[i].cpuCore
    // （BaseTradeClient 构造时填进 AccountCfg::cpuCore）。
    int cpuExecute{-1};
    int cpuExecuteTcmd{-1};
};