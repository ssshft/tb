#pragma once
#include "base/BaseTrade.h"
#include "bybit/BybitTrade.h"
#include "bybit/BybitWsTrade.h"


class BybitTradeClient : public BaseTradeClient{

public:
    BybitTradeClient(rapidjson::Value &accountConfig, sm::SecurityManager *smc);
    ~BybitTradeClient();

    virtual void start();
    virtual void initial();
    virtual void add_new_order(pubsub::TCommand &tcmd);
    virtual void cancel_order(pubsub::TCommand &tcmd);
    virtual void query_order(pubsub::TCommand &tcmd);
    virtual void query_account(pubsub::TCommand &tcmd);
    virtual void query_position(pubsub::TCommand &tcmd);
    virtual void query_balance(pubsub::TCommand &tcmd);

protected:

    BaseTradeUnit* tradeUnit = nullptr;
    sm::SecurityManager *smc;

};
