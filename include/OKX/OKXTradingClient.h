#pragma once
#include "OKXSpotSwapFuturesTradingClient.h"
// #include "OKXSwapTradingClient.h"
// #include "OKXCSwapTradingClient.h"

class OKXTradingClient : public TradingClientBase{

public:
    OKXTradingClient(rapidjson::Value &accountConfig, sm::SecurityManager *smc);

    ~OKXTradingClient();
    OKXSpotSwapFuturesTradingClient *okxSpotSwapFuturesTradingClient = nullptr;
    // OKXSwapTradingClient *gateioSwapTradingClient = nullptr;
    // OKXCSwapTradingClient *gateioCSwapTradingClient = nullptr;

    void add_new_order(pubsub::TCommand &tcmd);
    void cancel_order(pubsub::TCommand &tcmd);
    void query_order(pubsub::TCommand &tcmd);
    void query_account(pubsub::TCommand &tcmd);
    void query_position(pubsub::TCommand &tcmd);
    void query_balance(pubsub::TCommand &tcmd);
    void get_balances_and_positions();
protected:
    sm::SecurityManager *smc;
    void start();
    //启动的时候获取一遍balance和持仓
    void Initial();
};
