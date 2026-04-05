#include "api/Gateio/GateioTradingClient.h"
//#include "data_struct.h"

GateioTradingClient::GateioTradingClient(rapidjson::Value &accountConfig, sm::SecurityManager *smc){
    this->smc = smc;
    string exchId = accountConfig["exchId"].GetString();
    string strategyId = accountConfig["strategyId"].GetString();
    string accountId = accountConfig["accountId"].GetString();
    string userId = accountConfig["userId"].GetString();
//    rapidjson::Value &accounts = accountConfig["accounts"];
    for(rapidjson::SizeType i = 0; i < accountConfig["accounts"].Size(); i++){
        rapidjson::Value &account  = accountConfig["accounts"][i];
        AccountCfg accCfg;
        strcpy(accCfg.apiKey, account["apiKey"].GetString());
        strcpy(accCfg.apiSecret, account["apiSecret"].GetString());
        strcpy(accCfg.strategyId, strategyId.c_str());
        strcpy(accCfg.accountId, accountId.c_str());
        strcpy(accCfg.userId, userId.c_str());
        string instType = account["instType"].GetString();
        accCfg.exchangeTypeEnum = ExchangeType_GATEIO;
        // accCfg.instTypeEnum = InstTypeStr2EnumMap[instType];

        // string restBaseUrl = account["restBaseUrl"].GetString();
        // string wsBaseUrl = account["wsBaseUrl"].GetString();
        accCfg.restBaseUrl = account["restBaseUrl"].GetString();
        accCfg.wsBaseUrl = account["wsBaseUrl"].GetString();

        bool use = account["use"].GetBool();
        if(use == false){
            continue;
        }
        if(crypto::str_cmp(instType.c_str(), "SPOT")
        || crypto::str_cmp(instType.c_str(), "InstType_SPOT")){
            gateioSpotTradingClient = new GateioSpotTradingClient();
            gateioSpotTradingClient->Initialize(accCfg, smc);
        }
        else if(crypto::str_cmp(instType.c_str(), "InstType_USDT_SWAP") ){// crypto::str_cmp(instType.c_str(), "InstType_BTC_SWAP")

            gateioUSwapTradingClient =  new GateioUSwapTradingClient();
            gateioUSwapTradingClient->Initialize(accCfg, smc);
        }
        // else if(crypto::str_cmp(instType.c_str(), "InstType_USDT_SWAP")){
        //     gateioUSwapTradingClient =  new GateioUSwapTradingClient();
        //     gateioUSwapTradingClient->Initialize(accCfg, smc);
        // }
        // else if(crypto::str_cmp(instType.c_str(), "InstType_BTC_SWAP")){
        //     gateioCSwapTradingClient =  new GateioCSwapTradingClient();
        //     gateioCSwapTradingClient->Initialize(accCfg, smc);
        // }
        else{
            string errmsg = "TB "+ exchId + " not implemented instType:" + instType;
            LOG_ERROR("%s", errmsg.c_str());
//            cryptothrow(errmsg.c_str(), -1);
        }
    }
    start();
}

GateioTradingClient::~GateioTradingClient(){
    if(nullptr != gateioSpotTradingClient){
        delete gateioSpotTradingClient;
    }
    if(nullptr != gateioUSwapTradingClient){
        delete gateioUSwapTradingClient;
    }
    // if(nullptr != gateioCSwapTradingClient){
    //     delete gateioCSwapTradingClient;
    // }
}

void GateioTradingClient::start(){
//    gateioSpotTradingClient->get_balance();
    if(nullptr != gateioSpotTradingClient){
        gateioSpotTradingClient->Run();
    }
    if(nullptr != gateioUSwapTradingClient) {
        gateioUSwapTradingClient->Run();
    }
    // if(nullptr != gateioCSwapTradingClient){
    //     gateioCSwapTradingClient->Run();
    // }
    Initial();
}

void GateioTradingClient::add_new_order(pubsub::TCommand &tcmd) {
    
    
    if(tcmd.header.instTypeEnum == InstType_SPOT){
        if(nullptr != gateioSpotTradingClient){
            #ifdef USE_WEBSOCKET_API
            gateioSpotTradingClient->ws_add_new_order(tcmd);
            #else
            gateioSpotTradingClient->add_new_order(tcmd);
            #endif
        }
    }
    else if(tcmd.header.instTypeEnum == InstType_USDT_SWAP){
        if(nullptr != gateioUSwapTradingClient){  
        #ifdef USE_WEBSOCKET_API
            gateioUSwapTradingClient->ws_add_new_order(tcmd);
        #else
            gateioUSwapTradingClient->add_new_order(tcmd);
        #endif
        }
    }
    // else if(cmd.header.instTypeEnum == InstType_BTC_SWAP){
    //     if(nullptr != gateioCSwapTradingClient){
    //         gateioCSwapTradingClient->add_new_order(cmd);
    //     }
    // }
    else{
        LOG_ERROR("not support instType:%s",tcmd.header.getInstTypeStr().c_str());
    }
}

void GateioTradingClient::cancel_order(pubsub::TCommand &cmd) {
    if(cmd.header.instTypeEnum == InstType_SPOT){
        if(nullptr != gateioSpotTradingClient){
            #ifdef USE_WEBSOCKET_API
            gateioSpotTradingClient->ws_cancel_order(cmd);
            #else
            gateioSpotTradingClient->cancel_order(cmd);
            #endif
        }
    }
    else if(cmd.header.instTypeEnum == InstType_USDT_SWAP){
        if(nullptr != gateioUSwapTradingClient){
        #ifdef USE_WEBSOCKET_API
            gateioUSwapTradingClient->ws_cancel_order(cmd);
        #else
            gateioUSwapTradingClient->cancel_order(cmd);
        #endif
        }
    }
    // else if(cmd.header.instTypeEnum == InstType_BTC_SWAP){
    //     if(nullptr != gateioCSwapTradingClient){
    //         gateioCSwapTradingClient->cancel_order(cmd);
    //     }
    // }
    else{
        LOG_ERROR("not support instType:%s",cmd.header.getInstTypeStr().c_str());
    }
}

void GateioTradingClient::query_order(pubsub::TCommand &cmd) {
    if(cmd.header.instTypeEnum == InstType_SPOT){
        if(nullptr != gateioSpotTradingClient){
            #ifdef USE_WEBSOCKET_API
            gateioSpotTradingClient->ws_query_order(cmd);
            #else
            gateioSpotTradingClient->query_order(cmd);
            #endif
        }
    }
    else if(cmd.header.instTypeEnum == InstType_USDT_SWAP){
        if(nullptr != gateioUSwapTradingClient){
        #ifdef USE_WEBSOCKET_API
            gateioUSwapTradingClient->ws_query_order(cmd);
        #else
            gateioUSwapTradingClient->query_order(cmd);
        #endif
        }
    }
    else{
        LOG_ERROR("not support instType:%s",cmd.header.getInstTypeStr().c_str());
    }
}

void GateioTradingClient::query_account(pubsub::TCommand &cmd) {
    if(cmd.header.instTypeEnum == InstType_SPOT){
        if(nullptr != gateioSpotTradingClient){
        #ifdef USE_UNIFIED
            gateioSpotTradingClient->get_unified_account();
        #else
            gateioSpotTradingClient->get_balances();
        #endif
        }
    }
    else if(cmd.header.instTypeEnum == InstType_USDT_SWAP){
        if(nullptr != gateioUSwapTradingClient){
            pubsub::TCommand tcmd;
            memset(&tcmd, 0, sizeof(tcmd));
            tcmd.header.cmdTypeEnum = CMD_QUERY_ACCOUNT;
            gateioUSwapTradingClient->get_positions();
            #ifdef USE_UNIFIED
            #else
            gateioUSwapTradingClient->get_account(tcmd);
            #endif
        }
    }
    else{
        LOG_ERROR("not support instType:%s",cmd.header.getInstTypeStr().c_str());
    }
}

void GateioTradingClient::query_position(pubsub::TCommand &tcmd) {
    if(tcmd.header.instTypeEnum == InstType_USDT_SWAP){
        if(nullptr != gateioUSwapTradingClient){
            gateioUSwapTradingClient->get_position(tcmd);
        }
    }
    else{
        LOG_ERROR("not support instType:%s",tcmd.header.getInstTypeStr().c_str());
    }
}

void GateioTradingClient::query_balance(pubsub::TCommand &tcmd) {
    if(tcmd.header.instTypeEnum == InstType_USDT_SWAP){
        if(nullptr != gateioUSwapTradingClient){
            #ifdef USE_UNIFIED
            #else
            gateioUSwapTradingClient->get_account(tcmd);
            #endif
        }
    }
    else{
        LOG_ERROR("not support instType:%s",tcmd.header.getInstTypeStr().c_str());
    }
}

void GateioTradingClient::get_balances_and_positions() {
    pubsub::TCommand tcmd;
    memset(&tcmd, 0, sizeof(tcmd));
    tcmd.header.cmdTypeEnum = CMD_QUERY_ACCOUNT;
    if(nullptr != gateioSpotTradingClient){
    #ifdef USE_UNIFIED
        gateioSpotTradingClient->get_unified_account();
    #else
        gateioSpotTradingClient->get_balances();
    #endif    
    }
    if(nullptr != gateioUSwapTradingClient){
        gateioUSwapTradingClient->get_positions();
        #ifdef USE_UNIFIED
        #else
        gateioUSwapTradingClient->get_account(tcmd);
        #endif
    }
    // if(nullptr != gateioCSwapTradingClient){
    //     gateioCSwapTradingClient->get_positions();
    //     gateioCSwapTradingClient->get_accounts();
    // }
}

void GateioTradingClient::Initial(){
//    std::vector<om::Balance> balanceVec;
    get_balances_and_positions();//balanceVec
}