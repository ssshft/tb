#pragma once
//
// Gateio USDT-M Futures Trade Unit —— REST + WS。
// 与 GateioSpot 结构一致, 只是订阅 channel / URL / body 换成 futures 版。
//
#include "base/BaseTrade.h"

#include <simdjson.h>


class GateioUSTradeUnit : public BaseTradeUnit {

public:
    GateioUSTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~GateioUSTradeUnit();

    virtual void subWebsocekt() override;
    virtual void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t recv_ns) override;
    virtual void onOpen() override;

    virtual void query_account(const pubsub::TCommand& tcmd) override;
    virtual void query_balance(const pubsub::TCommand& tcmd) override;
    virtual void query_position(const pubsub::TCommand& tcmd) override;
    virtual void add_new_order(const pubsub::TCommand& tcmd) override;
    virtual void cancel_order(const pubsub::TCommand& tcmd) override;
    virtual void query_order(const pubsub::TCommand& tcmd) override;

private:
    // ---- subscribe builder ----
    std::string buildOrdersSubscribeJson() const;
    std::string buildBalancesSubscribeJson()  const;
    std::string buildPositionsSubscribeJson() const;

    // ---- msg 分派 ----
    void handleOrdersUpdate(simdjson::ondemand::array& arr);
    void handleBalancesUpdate(simdjson::ondemand::array& arr);
    void handlePositionsUpdate(simdjson::ondemand::array& arr);

private:
    std::string newOrderUrl = "/api/v4/futures/usdt/orders";
    std::string cancelOrderUrl = "/api/v4/futures/usdt/orders";
    std::string queryOrderUrl = "/api/v4/futures/usdt/orders";
    std::string balanceUrl = "/api/v4/futures/usdt/accounts";
    std::string positionUrl = "/api/v4/futures/usdt/positions";
};
