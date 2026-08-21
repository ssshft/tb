#pragma once
#include "base/BaseTrade.h"
#include "okx/OkxTrade.h"


class OkxTradeClient : public BaseTradeClient {

public:
    OkxTradeClient(rapidjson::Value& accCfg, sm::SecurityManager* s);
    ~OkxTradeClient();

    virtual void start() override;
    virtual void initial() override;
    virtual void query_account(const pubsub::TCommand& tcmd) override;
    virtual void query_balance(const pubsub::TCommand& tcmd) override;
    virtual void query_position(const pubsub::TCommand& tcmd) override;
    virtual void add_new_order(const pubsub::TCommand& tcmd) override;
    virtual void cancel_order(const pubsub::TCommand& tcmd) override;
    virtual void query_order(const pubsub::TCommand& tcmd) override;

protected:
    // OKX v5 API 是统一的 (SPOT/SWAP/FUTURES/MARGIN 走同一个 endpoint),
    // 所以每个 account 只需要一个 unit —— 用 unordered_map<accountId, unit>
    // 就够了。 但 tb 侧当前的 config 结构是 "一个 exchange config 只有一个 accountId,
    // 但 accounts[] 里可能是多个 instType 的子账号"; OKX 完全不需要, 直接用第一个 acc。
    BaseTradeUnit* tradeUnit = nullptr;
};