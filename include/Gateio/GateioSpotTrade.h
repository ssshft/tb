#pragma once
//
// Gateio Spot Trade Unit —— REST + WS。
//   REST : boost::beast (net::RestClient) — 全异步
//   WS   : boost::beast (net::WsClient)   — 自带 auto_reconnect
//   Parse: simdjson ondemand
//
// Gateio 特点:
//   1) REST 用 HMAC-SHA512 (getGateioSignatureRest), 三个 header: KEY, Timestamp, SIGN
//   2) WS 用 HMAC-SHA512 (getGateioSignatureWs), 订阅 JSON 里带 auth 字段
//   3) 应用层 ping —— 需要发 {"channel":"spot.ping"} 保活 (cfg.ping_mode = ClientPeriodicText)
//   4) 订阅 signature 有 TTL, 重连时不能复用旧 message —— 在 onOpen 里现场签发
//
#include "base/BaseTrade.h"

#include <simdjson.h>


class GateioSpotTradeUnit : public BaseTradeUnit {

public:
    GateioSpotTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~GateioSpotTradeUnit();

    virtual void subWebsocekt() override;
    virtual void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t recv_ns) override;
    virtual void onOpen() override;   // 每次连上现场签发 subscribe

    virtual void query_account (const pubsub::TCommand& tcmd) override;
    virtual void query_balance (const pubsub::TCommand& tcmd) override;
    virtual void query_position(const pubsub::TCommand& tcmd) override;
    virtual void add_new_order (const pubsub::TCommand& tcmd) override;
    virtual void cancel_order  (const pubsub::TCommand& tcmd) override;
    virtual void query_order   (const pubsub::TCommand& tcmd) override;

private:
    // ---- WS subscribe payload builder ----
    // spot.orders: payload=["BTC_USDT","!all"] 一次性订阅所有 pair 的订单流
    std::string buildOrdersSubscribeJson() const;
    // spot.balances: payload 可选, 一般订阅所有资产
    std::string buildBalancesSubscribeJson() const;

    // ---- WS msg 分派 ----
    void handleOrdersUpdate  (simdjson::ondemand::value& result);
    void handleBalancesUpdate(simdjson::ondemand::value& result);

private:
    std::string newOrderUrl    = "/api/v4/spot/orders";
    std::string cancelOrderUrl = "/api/v4/spot/orders";
    std::string queryOrderUrl  = "/api/v4/spot/orders";
    std::string balanceUrl     = "/api/v4/spot/accounts";
    std::string unifiedUrl     = "/api/v4/unified/accounts";
};