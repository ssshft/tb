#include "binance/BinanceTradeClient.h"


BinanceTradeClient::BinanceTradeClient(rapidjson::Value& accCfg, sm::SecurityManager* s) : BaseTradeClient(accCfg, s) {
    for (size_t i = 0; i < vAccount.size(); ++i) {
        auto& acc = vAccount[i];

    #ifdef USE_BINANCE_UNIFIED
        unifiedTradeUnit = new BinanceUnifiedTradeUnit(acc, smc);
    #else
        
        if (acc.instTypeEnum == SPOT) {
            std::cout << "----------spot created-------" << acc.accountName << " " << acc.strategyId << std::endl;
            if (acc.apiMode == AM_REST) {
                spotTradeUnit = new BinanceSpotTradeUnit(acc, smc);
            }
            else if (acc.apiMode == AM_WS) {
                spotTradeUnit = new BinanceSpotWsTradeUnit(acc, smc);
            }
        }
        else if (acc.instTypeEnum == USDT_SWAP) {
            if (acc.apiMode == AM_REST) {
                ufTradeUnit = new BinanceUFTradeUnit(acc, smc);
            }
            else if (acc.apiMode == AM_WS) {
                ufTradeUnit = new BinanceUFWsTradeUnit(acc, smc);
            }
        }
    #endif
    
    }
}

BinanceTradeClient::~BinanceTradeClient(){
    if (spotTradeUnit) {
        delete spotTradeUnit;
        spotTradeUnit = nullptr;
    }

    if(ufTradeUnit){
        delete ufTradeUnit;
        ufTradeUnit = nullptr;
    }
    // if(nullptr != binanceCFTradingClient ){
    //     delete binanceCFTradingClient;
    // }
    if (unifiedTradeUnit) {
        delete unifiedTradeUnit;
        unifiedTradeUnit = nullptr;
    }
}

void BinanceTradeClient::start() {
    if (spotTradeUnit) {
        spotTradeUnit->start();
    }

    if (ufTradeUnit) {
        ufTradeUnit->start();
    }
    // if(nullptr != binanceCFTradingClient ){
    //     binanceCFTradingClient->start();
    // }
    if (unifiedTradeUnit) {
        unifiedTradeUnit->start();
    }
    initial();
}

void BinanceTradeClient::initial() {
    pubsub::TCommand tcmd;
    memset(&tcmd, 0, sizeof(tcmd));
    tcmd.cmdTypeEnum = pubsub::CMD_QUERY_ACCOUNT;

    tcmd.body.queryAccount.instTypeEnum = SPOT;
    query_account(tcmd);

#ifdef USE_BINANCE_UNIFIED
#else
    tcmd.body.queryAccount.instTypeEnum = USDT_SWAP;
    query_account(tcmd);
#endif

}

void BinanceTradeClient::query_account(const pubsub::TCommand& tcmd) {
#ifdef USE_BINANCE_UNIFIED
        if (unifiedTradeClient) {
            unifiedTradeClient->query_account(tcmd);

            unifiedTradeClient->query_balance(tcmd);

            pubsub::TCommand tcmdUm = tcmd;
            tcmdUm.body.queryPosition.instTypeEnum == USDT_SWAP;
            unifiedTradeClient->query_position(tcmdUm);

            pubsub::TCommand tcmdCm = tcmd;
            tcmdCm.body.queryPosition.instTypeEnum == C_SWAP;
            unifiedTradeClient->query_position(tcmdCm);
        }
#else
    if (tcmd.body.queryAccount.instTypeEnum == SPOT) {
        if (spotTradeUnit){
            spotTradeUnit->query_balance(tcmd);
        }
    }
    else if (tcmd.body.queryAccount.instTypeEnum == USDT_SWAP || tcmd.body.queryAccount.instTypeEnum == USDT_FUTURES || tcmd.body.queryAccount.instTypeEnum == USDC_SWAP) {
        if (ufTradeUnit) {
            ufTradeUnit->query_account(tcmd);
        }
    }
    // else if(tcmd.queryAccount.instTypeEnum == C_SWAP || tcmd.queryAccount.instTypeEnum == C_FUTURES){
    //     if(nullptr != binanceCFTradingClient ){
    //         binanceCFTradingClient->get_balances();
    //         binanceCFTradingClient->get_positions();
    //         binanceCFTradingClient->get_adlquantile();
    //     }
    // }
    // else{
    //     LOG_ERROR("not support instType:%s",tcmd.header.getInstTypeStr().c_str());
    // }
#endif
}

void BinanceTradeClient::query_balance(const pubsub::TCommand& tcmd){
#ifdef USE_BINANCE_UNIFIED
        if (unifiedTradeClient) {
            unifiedTradeClient->query_balance(tcmd);
        }
#else

    if (tcmd.body.queryBalance.instTypeEnum == SPOT) {
        if (spotTradeUnit){
            spotTradeUnit->query_balance(tcmd);
        }
    }
    else if (tcmd.body.queryBalance.instTypeEnum == USDT_SWAP || tcmd.body.queryBalance.instTypeEnum == USDT_FUTURES || tcmd.body.queryBalance.instTypeEnum == USDC_SWAP) {
        if (ufTradeUnit) {
            ufTradeUnit->query_balance(tcmd);
        }
    }

    // if(nullptr != binanceCFTradingClient){
    //     // tcmd.header.instTypeEnum = InstType_C_SWAP;
    //     // query_account(tcmd);
    //     // binanceCFTradingClient->get_balances();
    //     // binanceCFTradingClient->get_positions();
    //     // binanceCFTradingClient->get_adlquantile();
    // }
#endif
}

void BinanceTradeClient::query_position(const pubsub::TCommand& tcmd){
#ifdef USE_BINANCE_UNIFIED
        if (unifiedTradeClient) {
            unifiedTradeClient->query_position(tcmd);
        }
#else
    if (ufTradeUnit) {
        ufTradeUnit->query_position(tcmd);
    }
    // if(nullptr != binanceCFTradingClient){
    //     // tcmd.header.instTypeEnum = InstType_C_SWAP;
    //     // query_account(tcmd);
    //     // binanceCFTradingClient->get_balances();
    //     // binanceCFTradingClient->get_positions();
    //     // binanceCFTradingClient->get_adlquantile();
    // }
#endif
}

void BinanceTradeClient::add_new_order(const pubsub::TCommand& tcmd) {
#ifdef USE_BINANCE_UNIFIED
        if (unifiedTradeClient) {
            unifiedTradeClient->add_new_order(tcmd);
        }
#else
    if (tcmd.body.newOrder.instTypeEnum == SPOT) {
        if (spotTradeUnit){
            spotTradeUnit->add_new_order(tcmd);
        }
    }
    else if (tcmd.body.newOrder.instTypeEnum == USDT_SWAP || tcmd.body.newOrder.instTypeEnum == USDT_FUTURES || tcmd.body.newOrder.instTypeEnum == USDC_SWAP) {
        if (ufTradeUnit) {
            ufTradeUnit->add_new_order(tcmd);
        }
    }
    // else if(tcmd.newOrder.instTypeEnum == InstType_C_SWAP || tcmd.newOrder.instTypeEnum == InstType_C_FUTURES){
    //     if(nullptr != binanceCFTradingClient ){
    //         return binanceCFTradingClient->add_new_order(tcmd);
    //     }
    // }
    // else{
    //     LOG_ERROR("not support instType:%s",tcmd.header.getInstTypeStr().c_str());
    // }
#endif
}

void BinanceTradeClient::cancel_order(const pubsub::TCommand& tcmd) {
#ifdef USE_BINANCE_UNIFIED
        if (unifiedTradeClient) {
            unifiedTradeClient->cancel_order(tcmd);
        }
#else

    if (tcmd.body.cancelOrder.instTypeEnum == SPOT) {
        if (spotTradeUnit){
            spotTradeUnit->cancel_order(tcmd);
        }
    }
    else if(tcmd.body.cancelOrder.instTypeEnum == USDT_SWAP || tcmd.body.cancelOrder.instTypeEnum == USDT_FUTURES || tcmd.body.cancelOrder.instTypeEnum == USDC_SWAP) {
        if (ufTradeUnit) {
            ufTradeUnit->cancel_order(tcmd);
        }
    }
    // else if(tcmd.cancelOrder.instTypeEnum == C_SWAP || tcmd.cancelOrder.instTypeEnum == C_FUTURES){
    //     if(nullptr != binanceCFTradingClient ){
    //         binanceCFTradingClient->cancel_order(tcmd);
    //     }
    // }
    // else{
    //     LOG_ERROR("not support instType:%s",tcmd.header.getInstTypeStr().c_str());
    // }
#endif
}

void BinanceTradeClient::query_order(const pubsub::TCommand& tcmd) {
#ifdef USE_BINANCE_UNIFIED
        if (unifiedTradeClient) {
            unifiedTradeClient->query_order(tcmd);
        }
#else
    if (tcmd.body.queryOrder.instTypeEnum == SPOT) {
        if (spotTradeUnit){
            spotTradeUnit->query_order(tcmd);
        }
    }
    else if (tcmd.body.queryOrder.instTypeEnum == USDT_SWAP || tcmd.body.queryOrder.instTypeEnum == USDT_FUTURES || tcmd.body.queryOrder.instTypeEnum == USDC_SWAP) {
        if (ufTradeUnit) {
            ufTradeUnit->query_order(tcmd);
        }
    }
    // else if(tcmd.queryOrder.instTypeEnum == C_SWAP || tcmd.queryOrder.instTypeEnum == C_FUTURES){
    //     if(nullptr != binanceCFTradingClient ){
    //         binanceCFTradingClient->query_order(tcmd);
    //     }
    // }
    // else{
    //     LOG_ERROR("not support instType:%s",tcmd.header.getInstTypeStr().c_str());
    // }
#endif
}
