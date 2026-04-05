#include "XTTradingClient.h"

#if 0
XTTradingClient::XTTradingClient(const char *apiKey,const char *apiSecret,
                    const char *accountId, sm::SecurityManager *smc){
    this->smc = smc;
    AccountCfg spotAccCfg;//apiKey, apiSecret, accountId
    strcpy(spotAccCfg.apiKey, apiKey);
    strcpy(spotAccCfg.apiSecret, apiSecret);
    strcpy(spotAccCfg.accountId, accountId);
    spotAccCfg.restBaseUrl = XT_SPOT_REST_HOST;
//    spotAccCfg.wsBaseUrl = XT_SPOT_WEBSOCKET_HOST;
    xtSpotTradingClient =  new XTSpotTradingClient();
    xtSpotTradingClient->Initialize(spotAccCfg, smc);

    AccountCfg uswapAccCfg;//apiKey, apiSecret, accountId,userId
    strcpy(uswapAccCfg.apiKey, apiKey);
    strcpy(uswapAccCfg.apiSecret, apiSecret);
    strcpy(uswapAccCfg.accountId, accountId);
    strcpy(uswapAccCfg.userId, userId);
    uswapAccCfg.restBaseUrl = XT_U_SWAP_REST_HOST;
    uswapAccCfg.wsBaseUrl = XT_U_SWAP_WEBSOCKET_HOST;
    xtUSwapTradingClient =  new XTUSwapTradingClient();
    xtUSwapTradingClient->Initialize(uswapAccCfg, smc);

    AccountCfg cswapAccCfg;//apiKey, apiSecret, accountId,userId
    strcpy(cswapAccCfg.apiKey, apiKey);
    strcpy(cswapAccCfg.apiSecret, apiSecret);
    strcpy(cswapAccCfg.accountId, accountId);
    strcpy(cswapAccCfg.userId, userId);
    cswapAccCfg.restBaseUrl = XT_C_SWAP_REST_HOST;
    cswapAccCfg.wsBaseUrl = XT_C_SWAP_WEBSOCKET_HOST;
    xtCSwapTradingClient =  new XTCSwapTradingClient();
    xtCSwapTradingClient->Initialize(cswapAccCfg, smc);

//    xtSpotTradingClient->start();
    start();
}
#endif

XTTradingClient::XTTradingClient(rapidjson::Value &accountConfig, sm::SecurityManager *smc){
    this->smc = smc;
    string exchId = accountConfig["exchId"].GetString();
    string accountId = accountConfig["accountId"].GetString();
//    string userId = accountConfig["userId"].GetString();
//    rapidjson::Value &accounts = accountConfig["accounts"];
    for(rapidjson::SizeType i = 0; i < accountConfig["accounts"].Size(); i++){
//        continue;
        rapidjson::Value &account  = accountConfig["accounts"][i];

        AccountCfg accCfg;
        strcpy(accCfg.apiKey, account["apiKey"].GetString());
        strcpy(accCfg.apiSecret, account["apiSecret"].GetString());
        strcpy(accCfg.accountId, accountId.c_str());
        accCfg.exchangeTypeEnum = ExchangeType_XT;

        accCfg.restBaseUrl = account["restBaseUrl"].GetString();
//        accCfg.wsBaseUrl = account["wsBaseUrl"].GetString();
        string instType = account["instType"].GetString();
        if(crypto::str_cmp(instType.c_str(), "SPOT")
           || crypto::str_cmp(instType.c_str(), "InstType_SPOT")){
            xtSpotTradingClient =  new XTSpotTradingClient();
            xtSpotTradingClient->Initialize(accCfg, smc);
        }
        else{
            string errmsg = "TB "+ exchId + " not implemented instType:" + instType;
            LOG_ERROR("%s", errmsg.c_str());
        }
    }
    start();
}

XTTradingClient::~XTTradingClient(){
    if(nullptr != xtSpotTradingClient){
        delete xtSpotTradingClient;
    }
}

void XTTradingClient::start(){
    if(nullptr != xtSpotTradingClient){
        xtSpotTradingClient->Run();
    }
    Initial();
}

void XTTradingClient::add_new_order(pubsub::TCommand &cmd) {
    if(cmd.header.instTypeEnum == InstType_SPOT){
        if(nullptr != xtSpotTradingClient){
            xtSpotTradingClient->add_new_order(cmd);
        }
    }
    else{
        LOG_ERROR("not support instType:%s",cmd.header.getInstTypeStr().c_str());
    }
}

void XTTradingClient::cancel_order(pubsub::TCommand &cmd) {
    if(cmd.header.instTypeEnum == InstType_SPOT){
        if(nullptr != xtSpotTradingClient){
            xtSpotTradingClient->cancel_order(cmd);
        }
    }
    else{
        LOG_ERROR("not support instType:%s",cmd.header.getInstTypeStr().c_str());
    }
}

void XTTradingClient::query_order(pubsub::TCommand &cmd) {
    if(cmd.header.instTypeEnum == InstType_SPOT){
        if(nullptr != xtSpotTradingClient){
            xtSpotTradingClient->query_order(cmd);
        }
    }
    else{
        LOG_ERROR("not support instType:%s",cmd.header.getInstTypeStr().c_str());
    }
}

void XTTradingClient::get_balances_and_positions() {
    if(nullptr != xtSpotTradingClient){
        xtSpotTradingClient->get_balances();
    }
}

void XTTradingClient::Initial(){
    if(nullptr != xtSpotTradingClient){
        get_balances_and_positions();
    }
}