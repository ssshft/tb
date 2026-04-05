#pragma once
#include "XTSpotTradingClient.h"

class XTTradingClient : public TradingClientBase{

public:
//    XTTradingClient(const char *apiKey, const char *apiSecret,
//                        const char *accountId, sm::SecurityManager *smc);
    XTTradingClient(rapidjson::Value &accountConfig, sm::SecurityManager *smc);
    virtual ~XTTradingClient();
    XTSpotTradingClient *xtSpotTradingClient=nullptr;
//    XTUSwapTradingClient *gateioUSwapTradingClient;
//    XTCSwapTradingClient *gateioCSwapTradingClient;

    virtual void add_new_order(pubsub::TCommand &cmd);
    virtual void cancel_order(pubsub::TCommand &cmd);
    virtual void query_order(pubsub::TCommand &cmd);
    virtual void get_balances_and_positions();
protected:
    sm::SecurityManager *smc;
    virtual void start();
    //启动的时候获取一遍balance和持仓
    virtual void Initial();
};
