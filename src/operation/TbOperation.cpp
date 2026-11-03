#include "operation/TbOperation.h"
#include "binance/BinanceTradeClient.h"
#include "Gateio/GateioTradeClient.h"
#include "okx/OkxTradeClient.h"
#include "bybit/BybitTradeClient.h"
#include "hft_metrics.h"


#define PUBLISH_RCMD(rcmd) \
    if (rcmd.cmdTypeEnum == pubsub::CMD_RPT_ORDER_RESPONSE) { \
        if (rcmd.body.orderResponse.clientOrderId != TESTCLIENTORDERID) { \
            tb2TradeRcmd->push(rcmd); \
        } \
    } \
    else { \
        tb2TradeRcmd->push(rcmd); \
    } \


TbOperation::TbOperation(){

}

TbOperation::~TbOperation() {
    if (smc) {
        delete smc;
        smc = nullptr;
    }

    if (tb2TradeRcmd) {
        delete tb2TradeRcmd;
        tb2TradeRcmd = nullptr;
    }

    for (auto& t : vTcmd) {
        if (t) {
            delete t;
            t = nullptr;
        }
    }

    vTcmd.clear();
}

bool TbOperation::preStart(Config* config) {
    std::string tradeConfigStr = crypto::read_file("/inc/trade_config.json");
    rapidjson::Document d;
    rapidjson::Value& tradeConfig = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(tradeConfigStr.c_str());
    if (d.HasParseError() || !tradeConfig.IsObject()) {
        cryptothrow("error occurs when reading /inc/trade_config.json, please check it", -1);
    }

    std::string host = "localhost";
    int port = 9379;
    std::string password = "";
    if (tradeConfig.HasMember("SMC")) {
        host = tradeConfig["SMC"]["host"].GetString();
        port = std::stoi(tradeConfig["SMC"]["port"].GetString());
        if(tradeConfig["SMC"].HasMember("password")){
            password = tradeConfig["SMC"]["password"].GetString();
        }
    }
    else {
        cryptothrow("trade_config.json not found SMC configuration!!", -1);
    }
    smc = new sm::SecurityManager(host.c_str(), port, password.c_str(), true);

    std::string utrade2TbTCommandSHM = "";
    if (tradeConfig.HasMember("OMS")) {
        utrade2TbTCommandSHM = tradeConfig["OMS"]["Utrade2TbTCommandSHM"].GetString();
        tb2TradeRcmd = new Tb2UtradeRcmd(tradeConfig["OMS"]["Tb2UtradeRCommandSHM"].GetString());
    }
    else {
        cryptothrow("trade_config.json not found OMS fields configuration!!", -1);
    }

    
    std::string configStr = config->get_document_str();
    rapidjson::Document d1;
    rapidjson::Value& configValue = d1.Parse<rapidjson::kParseNumbersAsStringsFlag>(configStr.c_str());

    // ---- 线程绑核（可选，缺省 -1 = 不绑）----
    if (configValue.HasMember("cpuexecute")) {
        cpuExecute = std::stoi(configValue["cpuexecute"].GetString());
    }
    if (configValue.HasMember("cpuexecutetcmd")) {
        cpuExecuteTcmd = std::stoi(configValue["cpuexecutetcmd"].GetString());
    }
    LOG_INFO("cpu pin: execute={} executeTcmd={} (-1 = not pinned)", cpuExecute, cpuExecuteTcmd);

    const rapidjson::Value &tbAccounts = configValue["tb_accounts"].GetArray();
    for(rapidjson::SizeType i = 0; i < tbAccounts.Size(); i++){
        std::string accountName = tbAccounts[i].GetString();
        if(tradeConfig["TB"]["tb_accounts"].HasMember(accountName.c_str())){
            rapidjson::Value &accountConfig = tradeConfig["TB"]["tb_accounts"][accountName.c_str()][0];
            std::string exchId = accountConfig["exchId"].GetString();
            std::string strategyId = accountConfig["strategyId"].GetString();
            std::string exchIdAccountKey = crypto::get_tradeclient_key(exchId.c_str(), strategyId.c_str());

            std::string strategyTcmdShm = crypto::get_strategy_shm(utrade2TbTCommandSHM, strategyId);
            Utrade2TbTcmd* shm = new Utrade2TbTcmd(strategyTcmdShm);
            vTcmd.push_back(shm);

            if (crypto::str_cmp(exchId.c_str(), "BINANCE")) {
               BaseTradeClient* trade = new BinanceTradeClient(accountConfig, smc);
               mTradeClient[exchIdAccountKey] = trade;
            }
            else if(crypto::str_cmp(exchId.c_str(), "GATEIO") == true){
                BaseTradeClient* client = new GateioTradeClient(accountConfig, smc);
                mTradeClient[exchIdAccountKey] = client;
            }
            else if(crypto::str_cmp(exchId.c_str(), "OKX") == true){
                BaseTradeClient* client = new OkxTradeClient(accountConfig, smc);
                mTradeClient[exchIdAccountKey] = client;
            }
            else if(crypto::str_cmp(exchId.c_str(), "BYBIT") == true){
                BaseTradeClient* client = new BybitTradeClient(accountConfig, smc);
                mTradeClient[exchIdAccountKey] = client;
            }    
            else {
               LOG_ERROR("not implemented exchId: {}", exchId);
            }
        }
        else {
            LOG_ERROR("not found accountName: {} configuration in trade_config.json. please add it first", accountName);
        }
    }

   orderManager.preStart();
   return true;
}


void TbOperation::run() {
    for (auto iter = mTradeClient.begin(); iter != mTradeClient.end(); ++iter) {
        LOG_INFO("{} client start", iter->first);
        iter->second->start();
    }

    LOG_INFO("start execute tcmd");
    
    std::thread executeThread(&TbOperation::execute, this);
    executeThread.detach();

    std::thread executeTcmdThread(&TbOperation::executeTcmd, this);
    executeTcmdThread.detach();
    
}

void TbOperation::execute() {
    // 绑核放在线程体第一行：run() 里是 detach 出去的，外面拿不到 std::thread 对象，
    // 而且"创建完到钉上"那段窗口线程已经在烧核了。
    if (cpuExecute >= 0 && crypto::set_cpu_self(cpuExecute) != 0) {
        LOG_ERROR("tb execute: pin to core {} failed, running unpinned", cpuExecute);
    }
    pubsub::TCommand tcmd;
    pubsub::RCommand rcmd;
    unsigned step = 0;
    while (1) {
        bool idle = true; 
        for (auto& t : vTcmd) {
            if (t->tryPop(tcmd)) { // 策略端的请求
                idle = false;
                // 这三处日志是本文件里唯一的"每帧固定成本"，而且它在**唯一的串行线程**上：
                // getString() 会 fmt 二十多个字段 + 5 次 unordered_map 查表 + 一次堆分配。
                // 单独量出来，是为了判断 tb.oms / tb.tcmd 之外到底有多少是花在打日志上。
                // 三个站点共用同一个名字（registry 按名字去重，不会变成三个指标）。
                {
                    HFT_SCOPE("tb.log");
                    LOG_INFO("{}", tcmd.getString());
                }
                processTcmd(tcmd);
            }
        }

        if (rcmdInnerQueue.pop(rcmd)) {  // 系统内部的回报
            idle = false;
            {
                HFT_SCOPE("tb.log");
                LOG_INFO("{}", rcmd.getString());
            }
            PUBLISH_RCMD(rcmd);
        }

        if (tb2OmsRCommandInnerQueue.pop(rcmd)) { // 交易所返回的回报
            idle = false;
            {
                HFT_SCOPE("tb.log");
                LOG_INFO("{}", rcmd.getString());
            }
            processRcmd(rcmd);
        }

        if (idle) {
            // 空转轮数。这个数除以时间是**轮询频率**，它的倒数就是
            // "一条消息从进队到被 pop"的上界 —— execute() 是纯轮询，没有事件唤醒。
            // 频率突然掉下来，说明这一轮里有东西阻塞了（日志、shm 写、或线程被抢占）。
            HFT_CNT("tb.exec.idle");
            spin::backoff(step);
        }
        else {
            step = 0;
        }
    }
}

void TbOperation::preStop() {
    orderManager.preStop();
}

void TbOperation::processTcmd(pubsub::TCommand& tcmd) {
    // 策略请求进 OMS 的处理（资金/仓位/风控检查 + 落 shm），不含真正的下单发送 ——
    // 发送在另一个线程 executeTcmd() 里，见 tb.send。
    HFT_SCOPE("tb.tcmd");
    if (orderManager.processTcmd(tcmd)) {
        tcmdInnerQueue.push(tcmd);
    }
}

void TbOperation::executeTcmd() {
    if (cpuExecuteTcmd >= 0 && crypto::set_cpu_self(cpuExecuteTcmd) != 0) {
        LOG_ERROR("tb executeTcmd: pin to core {} failed, running unpinned", cpuExecuteTcmd);
    }
    pubsub::TCommand tcmd;
    unsigned step = 0;
    while (1) {
        bool idle = true; 
        if (tcmdInnerQueue.pop(tcmd)) {
            idle = false;
            // 一条 tcmd 从出队到交给交易所客户端：建 JSON + send_text（或签名 + 投递到 io_context）。
            // **不含网络往返** —— 那一段只能从回报的时间戳里看，这里量不到。
            HFT_SCOPE("tb.send");
            switch (tcmd.cmdTypeEnum) {
                case pubsub::CMD_NEW_ORDER: {
                    const std::string& exchId = ExchangeTypeEnum2StrMap[tcmd.body.newOrder.exchangeTypeEnum];
                    const std::string& exchIdAccountKey = crypto::get_tradeclient_key(exchId.c_str(), tcmd.body.newOrder.strategyId);
                    auto iter = mTradeClient.find(exchIdAccountKey);
                    if (iter != mTradeClient.end()) {
                        iter->second->add_new_order(tcmd);
                    }
                    else {
                        LOG_ERROR("not found exchIdAccountKey: {}", exchIdAccountKey);
                    }
                    break;
                }
                case pubsub::CMD_CANCEL_ORDER: {
                    const std::string& exchId = ExchangeTypeEnum2StrMap[tcmd.body.cancelOrder.exchangeTypeEnum];
                    const std::string& exchIdAccountKey = crypto::get_tradeclient_key(exchId.c_str(), tcmd.body.cancelOrder.strategyId);
                    auto iter = mTradeClient.find(exchIdAccountKey);
                    if (iter != mTradeClient.end()) {
                        iter->second->cancel_order(tcmd);
                    }
                    else {
                        LOG_ERROR("not found exchIdAccountKey: {}", exchIdAccountKey);
                    }
                    break;
                }

                case pubsub::CMD_QUERY_ORDER: {
                    const std::string& exchId = ExchangeTypeEnum2StrMap[tcmd.body.queryOrder.exchangeTypeEnum];
                    const std::string& exchIdAccountKey = crypto::get_tradeclient_key(exchId.c_str(), tcmd.body.queryOrder.strategyId);
                    auto iter = mTradeClient.find(exchIdAccountKey);
                    if (iter != mTradeClient.end()) {
                        iter->second->query_order(tcmd);
                    }
                    else {
                        LOG_ERROR("not found exchIdAccountKey: {}", exchIdAccountKey);
                    }
                    break;
                }
                case pubsub::CMD_QUERY_ACCOUNT: {
                    const std::string& exchId = ExchangeTypeEnum2StrMap[tcmd.body.queryAccount.exchangeTypeEnum];
                    const std::string& exchIdAccountKey = crypto::get_tradeclient_key(exchId.c_str(), tcmd.body.queryAccount.strategyId);
                    auto iter = mTradeClient.find(exchIdAccountKey);
                    if (iter != mTradeClient.end()) {
                        iter->second->query_account(tcmd);
                    }
                    else {
                        LOG_ERROR("not found exchIdAccountKey: {}", exchIdAccountKey);
                    }
                    break;
                }
                case pubsub::CMD_QUERY_BALANCE: {
                    const std::string& exchId = ExchangeTypeEnum2StrMap[tcmd.body.queryBalance.exchangeTypeEnum];
                    const std::string& exchIdAccountKey = crypto::get_tradeclient_key(exchId.c_str(), tcmd.body.queryBalance.strategyId);
                    auto iter = mTradeClient.find(exchIdAccountKey);
                    if (iter != mTradeClient.end()) {
                        iter->second->query_balance(tcmd);
                    }
                    else {
                        LOG_ERROR("not found exchIdAccountKey: {}", exchIdAccountKey);
                    }
                    break;
                }
                case pubsub::CMD_QUERY_POSITION: {
                    const std::string& exchId = ExchangeTypeEnum2StrMap[tcmd.body.queryPosition.exchangeTypeEnum];
                    const std::string& exchIdAccountKey = crypto::get_tradeclient_key(exchId.c_str(), tcmd.body.queryPosition.strategyId);
                    auto iter = mTradeClient.find(exchIdAccountKey);
                    if (iter != mTradeClient.end()) {
                        iter->second->query_position(tcmd);
                    }
                    else {
                        LOG_ERROR("not found exchIdAccountKey: {}", exchIdAccountKey);
                    }
                    break;
                }
                default: {
                    LOG_ERROR("not support this tcmd: {} now",tcmd.getString());
                }
            }
        }

        if (idle) {
            // 发送线程的空转轮数。除以时间 = 发送线程的轮询频率。
            // 它跟 tb.exec.idle 是两条独立的心跳：一条掉了就是那一条线程有问题。
            HFT_CNT("tb.send.idle");
            spin::backoff(step);
        }
        else {
            step = 0;
        }
    }
}

void TbOperation::processRcmd(pubsub::RCommand& rcmd) {
    // 交易所回报进 OMS 的**整段**：onOrderUpdate 的查单（orderSysId 索引）+ 状态优先级推进 +
    // 成交量差分 + 整份报单体写回 /dev/shm（oms_flush_slot）+ 再推给策略（PUBLISH_RCMD）。
    // 这一段跑在 execute() 的串行线程上，所以它的 p99 就是"一条回报被 OMS 阻塞多久"。
    //
    // ⚠️ 这里**不测**报单的网络往返：下单走 REST，ack 就是 REST 的响应体，
    //    往返在 BaseTradeUnit::asyncRequest 的闭包里直接量（tb.rest.rtt.*）。
    //    用 WS 回报去反查"是哪一单"既多一层匹配、又要处理本地回环，没有必要。
    HFT_SCOPE("tb.oms");
    if (orderManager.processRcmd(rcmd)) {
        PUBLISH_RCMD(rcmd);
    }
    else {
        LOG_INFO("tb will not publish rcmd: {}", rcmd.getString());
    }
}
