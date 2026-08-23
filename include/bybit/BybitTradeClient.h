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
    virtual void query_account(const pubsub::TCommand& tcmd);
    virtual void query_position(const pubsub::TCommand& tcmd);
    virtual void query_balance(const pubsub::TCommand& tcmd);
    virtual void add_new_order(const pubsub::TCommand& tcmd);
    virtual void cancel_order(const pubsub::TCommand& tcmd);
    virtual void query_order(const pubsub::TCommand& tcmd);

protected:

    BaseTradeUnit* tradeUnit = nullptr;
    sm::SecurityManager *smc;

};