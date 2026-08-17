#include "operation/TbOperation.h"
#include "binance/BinanceTradeClient.h"
#include "Gateio/GateioTradeClient.h"


#define PUBLISH_RCMD(rcmd) \
    if (rcmd.cmdTypeEnum == pubsub::CMD_RPT_ORDER_RESPONSE) { \
        if (rcmd.body.orderResponse.clientOrderId != TESTCLIENTORDERID) { \
            tb2TradeRCommandPubSHM->push(rcmd); \
        } \
    } \
    else { \
        tb2TradeRCommandPubSHM->push(rcmd); \
    } \


TbOperation::TbOperation(){

}

TbOperation::~TbOperation(){
 
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

    if (tradeConfig.HasMember("OMS")) {
        utrade2TbTCommandShm = new Utrade2TbTCommandSHM(std::stoi(tradeConfig["OMS"]["Utrade2TbTCommandSHM"].GetString()));
        tb2TradeRCommandPubSHM = new Tb2TradeRCommandPubSHM(tradeConfig["OMS"]["Tb2UtradeRCommandSHM"].GetString());
    }
    else {
        cryptothrow("trade_config.json not found OMS fields configuration!!", -1);
    }

    
    std::string configStr = config->get_document_str();
    rapidjson::Document d1;
    rapidjson::Value& configValue = d1.Parse<rapidjson::kParseNumbersAsStringsFlag>(configStr.c_str());

    const rapidjson::Value &tbAccounts = configValue["tb_accounts"].GetArray();
    for(rapidjson::SizeType i = 0; i < tbAccounts.Size(); i++){
        std::string accountId = tbAccounts[i].GetString();
        if(tradeConfig["TB"]["tb_accounts"].HasMember(accountId.c_str())){
            rapidjson::Value &accountConfig = tradeConfig["TB"]["tb_accounts"][accountId.c_str()][0];
            std::string exchId = accountConfig["exchId"].GetString();
            std::string strategyId = accountConfig["strategyId"].GetString();
            std::string exchIdAccountKey = crypto::get_tradeclient_key(exchId.c_str(), strategyId.c_str());

            std::cout << exchId << " " << strategyId << " " << exchIdAccountKey << std::endl;

            if (crypto::str_cmp(exchId.c_str(), "BINANCE")) {
               BaseTradeClient* trade = new BinanceTradeClient(accountConfig, smc);
               mTradeClient[exchIdAccountKey] = trade;
            }
            else if(crypto::str_cmp(exchId.c_str(), "GATEIO") == true){
                BaseTradeClient *client = new GateioTradingClient(accountConfig , smc);
                mTradeClient[exchIdAccountKey] = client;
            }
            // else if(crypto::str_cmp(exchId.c_str(), "OKX") == true){
            //     BaseTradeClient *client = new OKXTradingClient(accountConfig , smc);
            //     mTradeClient[exchIdAccountKey] = client;
            // }
            // else if(crypto::str_cmp(exchId.c_str(), "BYBIT") == true){
            //     BaseTradeClient *client = new BybitTradingClient(accountConfig , smc);
            //     mTradeClient[exchIdAccountKey] = client;
            // }    
            //else {
            //    LOG_ERROR("not implemented exchId: {}", exchId);
            //}
        }
        else {
            LOG_ERROR("not found accountId: {} configuration in trade_config.json. please add it first", accountId);
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
    pubsub::TCommand tcmd;
    pubsub::RCommand rcmd;
    while (1) {
        if (utrade2TbTCommandShm->pop(tcmd)) { // 策略端的请求
            LOG_INFO("{}", tcmd.getString());
            processTcmd(tcmd);
        }

        if (rcmdInnerQueue.pop(rcmd)) {  // 系统内部的回报
            LOG_INFO("{}", rcmd.getString());
            PUBLISH_RCMD(rcmd);
        }

        if (tb2OmsRCommandInnerQueue.pop(rcmd)) { // 交易所返回的回报
            LOG_INFO("{}", rcmd.getString());
            processRcmd(rcmd);
        }

    #ifdef NEED_SLEEP
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    #endif
    }
}

void TbOperation::preStop() {
    orderManager.preStop();
}

void TbOperation::processTcmd(pubsub::TCommand& tcmd) {
    if (orderManager.processTcmd(tcmd)) {
        tcmdInnerQueue.push(tcmd);
    }
}

void TbOperation::executeTcmd() {
    pubsub::TCommand tcmd;
    while (1) {
        if (tcmdInnerQueue.pop(tcmd)) {
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

    #ifdef NEED_SLEEP
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    #endif
    }
}

void TbOperation::processRcmd(pubsub::RCommand& rcmd) {
    if (orderManager.processRcmd(rcmd)) {
        PUBLISH_RCMD(rcmd);
    }
    else {
        LOG_INFO("tb will not publish rcmd: {}", rcmd.getString());
    }
}
