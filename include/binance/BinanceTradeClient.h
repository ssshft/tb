#pragma once
#include "base/BaseTrade.h"
#include "binance/BinanceSpotTrade.h"
#include "binance/BinanceSpotWsTrade.h"
#include "binance/BinanceUFTrade.h"
#include "binance/BinanceUFWsTrade.h
// #include "binance/BinanceCFTradingClient.h"
#include "binance/BinanceUnifiedTrade.h"


class BinanceTradeClient : public BaseTradeClient {

public:
    BinanceTradeClient(rapidjson::Value& accountConfig, sm::SecurityManager *smc);
    ~BinanceTradeClient();

    virtual void start();
    virtual void initial();
    virtual void add_new_order(const pubsub::TCommand& tcmd);
    virtual void cancel_order(const pubsub::TCommand& tcmd);
    virtual void query_order(const pubsub::TCommand& tcmd);
    virtual void query_account(const pubsub::TCommand& tcmd);
    virtual void query_position(const pubsub::TCommand& tcmd);
    virtual void query_balance(const pubsub::TCommand& tcmd);

protected:
    BaseTradeUnit* spotTradeUnit = nullptr;
    BaseTradeUnit* ufTradeUnit = nullptr;
    BaseTradeUnit* unifiedTradeUnit = nullptr;
};