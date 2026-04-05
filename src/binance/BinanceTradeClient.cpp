#include "binance/BinanceTradeClient.h"


BinanceTradeClient::BinanceTradeClient(rapidjson::Value& accCfg, sm::SecurityManager* s) : BaseTradeClient(accCfg, s) {
    for (size_t i = 0; i < vAccount.size(); ++i) {
        auto& acc = vAccount[i];

    #ifdef USE_BINANCE_UNIFIED
    #else
        
        if (acc.instTypeEnum == SPOT) {
            spotTradeUnit = new BinanceSpotTradeUnit(acc, smc);
        }
        else if (acc.instTypeEnum == USDT_SWAP) {

        }
    #endif
        

    }
}

BinanceTradeClient::~BinanceTradeClient(){
    if (spotTradeUnit) {
        delete spotTradeUnit;
        spotTradeUnit = nullptr;
    }

    // if(nullptr != binanceUFTradingClient){
    //     delete binanceUFTradingClient;
    // }
    // if(nullptr != binanceCFTradingClient ){
    //     delete binanceCFTradingClient;
    // }
    // if (nullptr != binanceUnifiedTradingClient) {
    //     delete binanceUnifiedTradingClient;
    // }
}

void BinanceTradeClient::start() {
    if (spotTradeUnit) {
        spotTradeUnit->start();
    }

    // if(nullptr != binanceUFTradingClient ){
    //     binanceUFTradingClient->start();
    // }
    // if(nullptr != binanceCFTradingClient ){
    //     binanceCFTradingClient->start();
    // }
    // if(nullptr != binanceUnifiedTradingClient ){
    //     binanceUnifiedTradingClient->start();
    // }
    initial();
}

void BinanceTradeClient::initial() {
    pubsub::TCommand tcmd;
    memset(&tcmd, 0, sizeof(tcmd));
    
#ifdef USE_BINANCE_UNIFIED
        if(nullptr != binanceUnifiedTradingClient ){
            // tcmd.header.instTypeEnum = InstType_SPOT;
            query_account(tcmd);
        }
#else
    if(spotTradeUnit){
        tcmd.body.queryAccount.instTypeEnum = SPOT;
        spotTradeUnit->query_balance(tcmd);
    }
    // if(nullptr != binanceUFTradingClient){
    //     tcmd.queryAccount.instTypeEnum = USDT_SWAP;
    //     query_account(tcmd);
    //     // binanceUFTradingClient->get_balances();
    //     // binanceUFTradingClient->get_positions(tcmd);
    //     // binanceUFTradingClient->get_adlquantile();
    // }
    // if(nullptr != binanceCFTradingClient){
    //     // tcmd.queryAccount.instTypeEnum = C_SWAP;
    //     // query_account(tcmd);
    //     // binanceCFTradingClient->get_balances();
    //     // binanceCFTradingClient->get_positions();
    //     // binanceCFTradingClient->get_adlquantile();
    // }
#endif
}


void BinanceTradeClient::query_account(const pubsub::TCommand& tcmd){
#ifdef USE_BINANCE_UNIFIED
        if(nullptr != binanceUnifiedTradingClient ){
            binanceUnifiedTradingClient->get_balances(tcmd);
            binanceUnifiedTradingClient->get_account(tcmd);

            pubsub::TCommand tcmdUm = tcmd;
            tcmdUm.header.instTypeEnum == USDT_SWAP;
            pubsub::TCommand tcmdCm = tcmd;
            tcmdCm.header.instTypeEnum == C_SWAP;
            binanceUnifiedTradingClient->get_adlquantile(tcmdUm);
            binanceUnifiedTradingClient->get_positions(tcmdUm);
            binanceUnifiedTradingClient->get_adlquantile(tcmdCm);
            binanceUnifiedTradingClient->get_positions(tcmdCm);
        }
#else
    if (tcmd.body.queryAccount.instTypeEnum == SPOT) {
        if (spotTradeUnit){
            spotTradeUnit->query_balance(tcmd);
        }
    }
    // else if(tcmd.queryAccount.instTypeEnum == USDT_SWAP || tcmd.queryAccount.instTypeEnum == USDT_FUTURES) {
    //     if(nullptr != binanceUFTradingClient ){
    //         binanceUFTradingClient->get_adlquantile(tcmd);
    //         binanceUFTradingClient->get_balances(tcmd);
    //         binanceUFTradingClient->get_positions(tcmd);
    //     }
    // }
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
        if(nullptr != binanceUnifiedTradingClient ){
            query_account(tcmd);
        }
#else

    if (tcmd.body.queryAccount.instTypeEnum == SPOT) {
        if (spotTradeUnit){
            spotTradeUnit->query_balance(tcmd);
        }
    }
    // if(nullptr != binanceUFTradingClient){
    //      binanceUFTradingClient->get_balances(tcmd);
    //     // binanceUFTradingClient->get_positions(tcmd);
    //     // binanceUFTradingClient->get_adlquantile();
    // }
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
        if(nullptr != binanceUnifiedTradingClient ){
            query_account(tcmd);
        }
#else
    if (spotTradeUnit){
    }
    // if(nullptr != binanceUFTradingClient){
    //     binanceUFTradingClient->get_adlquantile(tcmd);
    //     binanceUFTradingClient->get_positions(tcmd);
    //     // binanceUFTradingClient->get_balances();
    //     // binanceUFTradingClient->get_positions(tcmd);
    //     // binanceUFTradingClient->get_adlquantile();
    // }
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
        if(nullptr != binanceUnifiedTradingClient ){
            return binanceUnifiedTradingClient->add_new_order(tcmd);
        }
#else

    if (tcmd.body.newOrder.instTypeEnum == SPOT) {
        if (spotTradeUnit){
            spotTradeUnit->add_new_order(tcmd);
        }
    }
    // else if(tcmd.newOrder.instTypeEnum == USDT_SWAP || tcmd.newOrder.instTypeEnum == InstType_USDT_FUTURES){
    //     if(nullptr != binanceUFTradingClient ){
    //         return binanceUFTradingClient->add_new_order(tcmd);
    //     }
    // }
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
        if(nullptr != binanceUnifiedTradingClient ){
            return binanceUnifiedTradingClient->cancel_order(tcmd);
        }
#else

    if (tcmd.body.cancelOrder.instTypeEnum == SPOT) {
        if (spotTradeUnit){
            spotTradeUnit->cancel_order(tcmd);
        }
    }
    // else if(tcmd.cancelOrder.instTypeEnum == USDT_SWAP || tcmd.cancelOrder.instTypeEnum == USDT_FUTURES){
    //     if(nullptr != binanceUFTradingClient ){
    //         binanceUFTradingClient->cancel_order(tcmd);
    //     }
    // }
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
        if(nullptr != binanceUnifiedTradingClient ){
            return binanceUnifiedTradingClient->query_order(tcmd);
        }
#else
    if (tcmd.body.queryOrder.instTypeEnum == SPOT) {
        if (spotTradeUnit){
            spotTradeUnit->query_order(tcmd);
        }
    }
    // else if(tcmd.queryOrder.instTypeEnum == USDT_SWAP || tcmd.queryOrder.instTypeEnum == USDT_FUTURES){
    //     if(nullptr != binanceUFTradingClient ){
    //         binanceUFTradingClient->query_order(tcmd);
    //     }
    // }
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
