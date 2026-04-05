#include "api/OKX/OKXTradingClient.h"


OKXTradingClient::OKXTradingClient(rapidjson::Value &accountConfig, sm::SecurityManager *smc){
    this->smc = smc;
    string exchId = accountConfig["exchId"].GetString();
    string strategyId = accountConfig["strategyId"].GetString();
    string accountId = accountConfig["accountId"].GetString();
    string userId = accountConfig["passphrase"].GetString();
//    rapidjson::Value &accounts = accountConfig["accounts"];
    for(rapidjson::SizeType i = 0; i < accountConfig["accounts"].Size(); i++){
        rapidjson::Value &account  = accountConfig["accounts"][i];
        AccountCfg accCfg;
        strcpy(accCfg.apiKey, account["apiKey"].GetString());
        strcpy(accCfg.apiSecret, account["apiSecret"].GetString());
        strcpy(accCfg.strategyId, strategyId.c_str());
        strcpy(accCfg.accountId, accountId.c_str());
        strcpy(accCfg.userId, userId.c_str());
        // string instType = account["instType"].GetString();
        accCfg.exchangeTypeEnum = ExchangeType_OKX;

        if(account.HasMember("simulatedTrading")
        && crypto::str_cmp(account["simulatedTrading"].GetString(),"") == false){
            accCfg.simulatedTrading = account["simulatedTrading"].GetBool();
        }
        accCfg.restBaseUrl = account["restBaseUrl"].GetString();
        accCfg.wsBaseUrl = account["wsBaseUrl"].GetString();

        okxSpotSwapFuturesTradingClient = new OKXSpotSwapFuturesTradingClient();
        okxSpotSwapFuturesTradingClient->Initialize(accCfg, smc);
    }
    start();
}

OKXTradingClient::~OKXTradingClient(){
    if(nullptr != okxSpotSwapFuturesTradingClient){
        delete okxSpotSwapFuturesTradingClient;
    }
}

void OKXTradingClient::start(){
    if(nullptr != okxSpotSwapFuturesTradingClient){
        okxSpotSwapFuturesTradingClient->Run();
    }

    Initial();
}

void OKXTradingClient::add_new_order(pubsub::TCommand &tcmd) {
    okxSpotSwapFuturesTradingClient->add_new_order(tcmd);
}

void OKXTradingClient::cancel_order(pubsub::TCommand &tcmd) {
    okxSpotSwapFuturesTradingClient->cancel_order(tcmd);
}

void OKXTradingClient::query_order(pubsub::TCommand &tcmd) {
    okxSpotSwapFuturesTradingClient->query_order(tcmd);
}

void OKXTradingClient::query_account(pubsub::TCommand &tcmd){
    get_balances_and_positions();
}

void OKXTradingClient::query_position(pubsub::TCommand &tcmd){
    if(nullptr != okxSpotSwapFuturesTradingClient){
        okxSpotSwapFuturesTradingClient->get_positions();
    }
}

void OKXTradingClient::query_balance(pubsub::TCommand &tcmd){
    if(nullptr != okxSpotSwapFuturesTradingClient){
        okxSpotSwapFuturesTradingClient->get_balances();
    }
}

void OKXTradingClient::get_balances_and_positions() {
    if(nullptr != okxSpotSwapFuturesTradingClient){
        okxSpotSwapFuturesTradingClient->get_balances();
        okxSpotSwapFuturesTradingClient->get_positions();
    }
}

void OKXTradingClient::Initial(){
    //okx订阅的时候会推送，所以这里不再需要主动获取
    //get_balances_and_positions();
}