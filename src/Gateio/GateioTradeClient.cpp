#include "Gateio/GateioTradeClient.h"


GateioTradeClient::GateioTradeClient(rapidjson::Value& accCfg, sm::SecurityManager* s) : BaseTradeClient(accCfg, s) {
    for (size_t i = 0; i < vAccount.size(); ++i) {
        auto& acc = vAccount[i];        
        if (acc.instTypeEnum == SPOT) {
            std::cout << "----------spot created-------" << acc.accountName << " " << acc.strategyId << std::endl;
            if (acc.apiMode == AM_REST) {
                spotTradeUnit = new GateioSpotTradeUnit(acc, smc);
            }
            else if (acc.apiMode == AM_WS) {
                spotTradeUnit = new GateioSpotWsTradeUnit(acc, smc);
            }
            
        }
        else if (acc.instTypeEnum == USDT_SWAP) {
            if (acc.apiMode == AM_REST) {
                usTradeClient = new GateioUSTradeUnit(acc, smc);
            }
            else if (acc.apiMode == AM_WS) {
                usTradeClient = new GateioUsWsTradeUnit(acc, smc);
            }
            
        }    
    }
}

GateioTradeClient::~GateioTradeClient(){
    if (spotTradeUnit) {
        delete spotTradeUnit;
        spotTradeUnit = nullptr;
    }

    if(usTradeClient){
        delete usTradeClient;
        usTradeClient = nullptr;
    }
}

void GateioTradeClient::start() {
    if (spotTradeUnit) {
        spotTradeUnit->start();
    }

    if (usTradeClient) {
        usTradeClient->start();
    }

    initial();
}

void GateioTradeClient::initial() {
    pubsub::TCommand tcmd;
    memset(&tcmd, 0, sizeof(tcmd));
    tcmd.cmdTypeEnum = pubsub::CMD_QUERY_ACCOUNT;

    tcmd.body.queryAccount.instTypeEnum = SPOT;
    query_account(tcmd);

#ifdef USE_GATEIO_UNIFIED  // 统一账户只需要查询一次
#else
    tcmd.body.queryAccount.instTypeEnum = USDT_SWAP;
    query_account(tcmd);
#endif
}


void GateioTradeClient::query_account(const pubsub::TCommand& tcmd) {
#ifdef USE_GATEIO_UNIFIED
    if (spotTradeUnit){
        spotTradeUnit->query_account(tcmd);
    }

    if (usTradeClient) {
        usTradeClient->query_position(tcmd);
    }
#else
    if (tcmd.body.queryAccount.instTypeEnum == SPOT) {
        if (spotTradeUnit){
            spotTradeUnit->query_balance(tcmd);
        }
    }
    else if (tcmd.body.queryAccount.instTypeEnum == USDT_SWAP) {
        if (usTradeClient) {
            usTradeClient->query_balance(tcmd);
            usTradeClient->query_position(tcmd);
        }
    }
#endif
}

void GateioTradeClient::query_balance(const pubsub::TCommand& tcmd) {
#ifdef USE_GATEIO_UNIFIED
    if (spotTradeUnit){
        spotTradeUnit->query_account(tcmd);
    }
#else
    if (tcmd.body.queryBalance.instTypeEnum == SPOT) {
        if (spotTradeUnit){
            spotTradeUnit->query_balance(tcmd);
        }
    }
    else if (tcmd.body.queryBalance.instTypeEnum == USDT_SWAP) {
        if (usTradeClient) {
            usTradeClient->query_balance(tcmd);
        }
    }
#endif
}

void GateioTradeClient::query_position(const pubsub::TCommand& tcmd){
    if (usTradeClient) {
        usTradeClient->query_position(tcmd);
    }
}

void GateioTradeClient::add_new_order(const pubsub::TCommand& tcmd) {
    if (tcmd.body.newOrder.instTypeEnum == SPOT) {
        if (spotTradeUnit){
            spotTradeUnit->add_new_order(tcmd);
        }
    }
    else if (tcmd.body.newOrder.instTypeEnum == USDT_SWAP) {
        if (usTradeClient) {
            usTradeClient->add_new_order(tcmd);
        }
    }
}

void GateioTradeClient::cancel_order(const pubsub::TCommand& tcmd) {
    if (tcmd.body.cancelOrder.instTypeEnum == SPOT) {
        if (spotTradeUnit){
            spotTradeUnit->cancel_order(tcmd);
        }
    }
    else if(tcmd.body.cancelOrder.instTypeEnum == USDT_SWAP) {
        if (usTradeClient) {
            usTradeClient->cancel_order(tcmd);
        }
    }
}

void GateioTradeClient::query_order(const pubsub::TCommand& tcmd) {
    if (tcmd.body.queryOrder.instTypeEnum == SPOT) {
        if (spotTradeUnit){
            spotTradeUnit->query_order(tcmd);
        }
    }
    else if (tcmd.body.queryOrder.instTypeEnum == USDT_SWAP) {
        if (usTradeClient) {
            usTradeClient->query_order(tcmd);
        }
    }
}
