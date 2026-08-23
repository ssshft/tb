#pragma once
#include "base/BaseTrade.h"
#include "bybit/BybitTrade.h"
#include "bybit/BybitWsTrade.h"


class BybitTradeClient : public BaseTradeClient{

public:
    BybitTradeClient(rapidjson::Value &accountConfig, sm::SecurityManager *smc);
    ~BybitTradeClient();

    void start();
    void Initial();
    void add_new_order(pubsub::TCommand &tcmd);
    void cancel_order(pubsub::TCommand &tcmd);
    void query_order(pubsub::TCommand &tcmd);
    void query_account(pubsub::TCommand &tcmd);
    void query_position(pubsub::TCommand &tcmd);
    void query_balance(pubsub::TCommand &tcmd);

protected:

    BaseTradeUnit* tradeUnit = nullptr;
    sm::SecurityManager *smc;

};
