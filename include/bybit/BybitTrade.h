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

    virtual void query_account (const pubsub::TCommand& tcmd) override;
    virtual void query_balance (const pubsub::TCommand& tcmd) override;
    virtual void query_position(const pubsub::TCommand& tcmd) override;
    virtual void add_new_order (const pubsub::TCommand& tcmd) override;
    virtual void cancel_order  (const pubsub::TCommand& tcmd) override;
    virtual void query_order   (const pubsub::TCommand& tcmd) override;

private:
    // ---- signature ----
    // 通用 signature = hex(HMAC-SHA256(secret, ts + apiKey + recvWindow + payload))
    std::string bybitSign(const std::string& timestamp,
                          const std::string& payload) const;

    // 构造 REST 的 3 个 auth header. GET 用 queryString 做 payload, POST 用 body。
    std::vector<std::pair<std::string, std::string>>
    bybitAuthHeaders(const std::string& payload) const;

    static const char* categoryOf(InstType t);   // "spot" / "linear" / "inverse" / nullptr

    // ---- WS msg 分派 ----
    void handleOrdersUpdate  (simdjson::ondemand::value& dataArr);
    void handleWalletUpdate  (simdjson::ondemand::value& dataArr);
    void handlePositionUpdate(simdjson::ondemand::value& dataArr);

    // Bybit 的 symbol 是 "BTCUSDT" 类风格, 我们把它跟 SecurityManager 里的 originInstId
    // 匹配 (contractinfo 侧已经把 Bybit 品种以原生 symbol 存)。
    bool lookupInstrument(const std::string& originInstId, const std::string& category,
                          md::InstrumentInfo& info, InstType& out) const;

    // build WS auth / subscribe payload
    std::string buildAuthJson() const;
    std::string buildSubscribeJson() const;

private:
    std::string orderUrl        = "/v5/order/create";
    std::string cancelOrderUrl  = "/v5/order/cancel";
    std::string queryOrderUrl   = "/v5/order/realtime";
    std::string balanceUrl      = "/v5/account/wallet-balance";
    std::string positionUrl     = "/v5/position/list";
    std::string wsPath          = "/v5/private";

    std::string recvWindow_     = "5000";
};