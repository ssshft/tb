#pragma once
//
// Bybit v5 Trade Unit —— 一个 unit 支持所有 Bybit instType (SPOT / USDT_SWAP /
// USDT_FUTURES / C_SWAP / C_FUTURES), 因为 Bybit v5 API 完全统一, 通过
// `category` 参数区分 (spot / linear / inverse):
//   REST: /v5/order/create, /v5/order/cancel, /v5/order/realtime,
//         /v5/account/wallet-balance, /v5/position/list
//   WS:   /v5/private → auth → subscribe order, wallet, position
//
// 认证:
//   X-BAPI-API-KEY, X-BAPI-TIMESTAMP (ms), X-BAPI-RECV-WINDOW,
//   X-BAPI-SIGN = hex(HMAC-SHA256(secret, timestamp + apiKey + recvWindow + payload))
//     其中 payload 是 GET 的 queryString 或 POST 的 body
//
#include "base/BaseTrade.h"

#include <simdjson.h>


class BybitTradeUnit : public BaseTradeUnit {

public:
    BybitTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~BybitTradeUnit();

    virtual void subWebsocekt() override;
    virtual void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t recv_ns) override;
    virtual void onOpen() override;   // auth + subscribe

    virtual void query_account(const pubsub::TCommand& tcmd) override;
    virtual void query_balance(const pubsub::TCommand& tcmd) override;
    virtual void query_position(const pubsub::TCommand& tcmd) override;
    virtual void add_new_order(const pubsub::TCommand& tcmd) override;
    virtual void cancel_order(const pubsub::TCommand& tcmd) override;
    virtual void query_order(const pubsub::TCommand& tcmd) override;

private:
    // ---- WS msg 分派 ----
    void handleOrdersUpdate(simdjson::ondemand::array& dataArr);
    void handleWalletUpdate(simdjson::ondemand::array& dataArr);
    void handlePositionUpdate(simdjson::ondemand::array& dataArr);

    // build WS auth / subscribe payload
    std::string buildAuthJson() const;
    std::string buildSubscribeJson() const;

private:
    std::string orderUrl = "/v5/order/create";
    std::string cancelOrderUrl = "/v5/order/cancel";
    std::string queryOrderUrl = "/v5/order/realtime";
    std::string balanceUrl = "/v5/account/wallet-balance";
    std::string positionUrl = "/v5/position/list";
};