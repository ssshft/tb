#include "api/Bybit/BybitTradingClient.h"


BybitTradingClient::BybitTradingClient(rapidjson::Value &accountConfig, sm::SecurityManager *smc){
    this->smc = smc;
    string exchId = accountConfig["exchId"].GetString();
    string strategyId = accountConfig["strategyId"].GetString();
    string accountId = accountConfig["accountId"].GetString();
    // string userId = accountConfig["passphrase"].GetString();
//    rapidjson::Value &accounts = accountConfig["accounts"];
    for(rapidjson::SizeType i = 0; i < accountConfig["accounts"].Size(); i++){
        rapidjson::Value &account  = accountConfig["accounts"][i];

        bool use = account["use"].GetBool();
        if(use == false){
            continue;
        }
        AccountCfg accCfg;
        strcpy(accCfg.apiKey, account["apiKey"].GetString());
        strcpy(accCfg.apiSecret, account["apiSecret"].GetString());
        strcpy(accCfg.strategyId, strategyId.c_str());
        strcpy(accCfg.accountId, accountId.c_str());
        accCfg.restBaseUrl = account["restBaseUrl"].GetString();
        accCfg.wsBaseUrl   = account["wsBaseUrl"].GetString();
        accCfg.exchangeTypeEnum = ExchangeType_BYBIT;
        
        // string instType    = account["instType"].GetString();
        // if(crypto::str_cmp(instType.c_str(), "InstType_USDT_SWAP")
        // || crypto::str_cmp(instType.c_str(), "InstType_USDT_FUTURES")
        // ){
        //     bybitSwapTradingClient = new BybitSwapTradingClient();
        //     bybitSwapTradingClient->Initialize(accCfg, smc);
        // }
        // else{
        //     string errmsg = "TB " + exchId + " not implemented instType:" + instType;
        //     LOG_ERROR("%s", errmsg.c_str());
        // }

        bybitSwapTradingClient = new BybitSwapTradingClient();
        bybitSwapTradingClient->Initialize(accCfg, smc);
    }
    start();
}

BybitTradingClient::~BybitTradingClient(){
    if(nullptr != bybitSwapTradingClient){
        delete bybitSwapTradingClient;
    }
}

void BybitTradingClient::start(){
    if(nullptr != bybitSwapTradingClient){
        bybitSwapTradingClient->Run();
    }

    Initial();
}

void BybitTradingClient::add_new_order(pubsub::TCommand &tcmd) {
    bybitSwapTradingClient->add_new_order(tcmd);
}

void BybitTradingClient::cancel_order(pubsub::TCommand &tcmd) {
    bybitSwapTradingClient->cancel_order(tcmd);
}

void BybitTradingClient::query_order(pubsub::TCommand &tcmd) {
    bybitSwapTradingClient->query_order(tcmd);
}

void BybitTradingClient::query_account(pubsub::TCommand &tcmd){
    get_balances_and_positions();
}

void BybitTradingClient::query_position(pubsub::TCommand &tcmd){
    if(nullptr != bybitSwapTradingClient){
        bybitSwapTradingClient->get_positions(tcmd);
    }
}

void BybitTradingClient::query_balance(pubsub::TCommand &tcmd){
    if(nullptr != bybitSwapTradingClient){
        bybitSwapTradingClient->get_balances(tcmd);
    }
}


void BybitTradingClient::get_balances_and_positions() {
    if(nullptr != bybitSwapTradingClient){
        pubsub::TCommand tcmd;
        bybitSwapTradingClient->get_balances(tcmd);
        bybitSwapTradingClient->get_positions(tcmd);
    }
}

void BybitTradingClient::Initial(){
    // 订阅的时候会推送，所以这里不再需要主动获取
    get_balances_and_positions();
}