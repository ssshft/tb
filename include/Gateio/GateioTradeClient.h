#pragma once
#include "base/BaseTrade.h"
#include "Gateio/GateioSpotTrade.h"
#include "Gateio/GateioUSTrade.h"



class GateioTradeClient : public BaseTradeClient {

public:
    GateioTradeClient(rapidjson::Value& accountConfig, sm::SecurityManager* smc);
    ~GateioTradeClient();

    virtual void start();
    virtual void initial();
    virtual void add_new_order(const pubsub::TCommand& tcmd);
    virtual void cancel_order(const pubsub::TCommand& tcmd);
    virtual void query_order(const pubsub::TCommand& tcmd);
    virtual void query_account(const pubsub::TCommand& tcmd);
    virtual void query_position(const pubsub::TCommand& tcmd);
    virtual void query_balance(const pubsub::TCommand& tcmd);

protected:
    GateioSpotTradeUnit* spotTradeUnit = nullptr;
    GateioUSTradeUnit* usTradeClient = nullptr;
};
