#pragma once
//
// OKX Trade Unit —— 一个 unit 支持所有 OKX instType (SPOT / USDT_SWAP / USDT_FUTURES /
// C_SWAP / C_FUTURES / MARGIN), 因为 OKX v5 API 是完全统一的:
//   REST: /api/v5/trade/order (POST/GET), /api/v5/trade/cancel-order,
//         /api/v5/account/balance, /api/v5/account/positions
//   WS:   /ws/v5/private → login → subscribe account / positions / orders
// 所有 instType 通过 `instType` 参数 / `instId` 前缀区分, 底层是一个连接。
//
// 认证:
//   OK-ACCESS-KEY, OK-ACCESS-SIGN (base64(HMAC-SHA256(secret, ts+method+path+body))),
//   OK-ACCESS-TIMESTAMP (ISO8601 with ms), OK-ACCESS-PASSPHRASE
//
#include "base/BaseTrade.h"

#include <simdjson.h>


class OkxTradeUnit : public BaseTradeUnit {

public:
    OkxTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~OkxTradeUnit();

    virtual void subWebsocekt() override;
    virtual void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t recv_ns) override;
    virtual void onOpen() override;   // login + subscribe

    virtual void query_account (const pubsub::TCommand& tcmd) override;
    virtual void query_balance (const pubsub::TCommand& tcmd) override;
    virtual void query_position(const pubsub::TCommand& tcmd) override;
    virtual void add_new_order (const pubsub::TCommand& tcmd) override;
    virtual void cancel_order  (const pubsub::TCommand& tcmd) override;
    virtual void query_order   (const pubsub::TCommand& tcmd) override;

private:
    // ---- signature ----
    // sign = base64(HMAC-SHA256(secret, timestamp + method + requestPath + body))
    std::string okxSign(const std::string& timestamp,
                        const std::string& method,
                        const std::string& requestPath,
                        const std::string& body) const;
    // OKX 需要 ISO8601 时间戳带毫秒, e.g. "2024-01-01T00:00:00.000Z"
    static std::string okxTimestamp();

    std::vector<std::pair<std::string, std::string>>
    okxAuthHeaders(const std::string& method,
                   const std::string& requestPath,
                   const std::string& body) const;

    // ---- WS msg 分派 ----
    // channel = "account" | "positions" | "orders" | "balance_and_position"
    void handleAccountUpdate  (simdjson::ondemand::value& dataArr);
    void handlePositionsUpdate(simdjson::ondemand::value& dataArr);
    void handleOrdersUpdate   (simdjson::ondemand::value& dataArr);

    // 根据 originInstId 遍历所有 OKX 品种查 SecurityManager, 找到即返回。
    // OKX 的 instType 字段和我们的 InstType 需要映射。
    bool lookupInstrument(const std::string& originInstId,
                          std::string_view okxInstType,
                          md::InstrumentInfo& info,
                          InstType& out) const;

    // build WS login / subscribe payload
    std::string buildLoginJson() const;
    std::string buildSubscribeJson() const;

private:
    std::string orderUrl        = "/api/v5/trade/order";
    std::string cancelOrderUrl  = "/api/v5/trade/cancel-order";
    std::string queryOrderUrl   = "/api/v5/trade/order";   // GET with ordId query
    std::string balanceUrl      = "/api/v5/account/balance";
    std::string positionUrl     = "/api/v5/account/positions";
    std::string wsPath          = "/ws/v5/private";
};