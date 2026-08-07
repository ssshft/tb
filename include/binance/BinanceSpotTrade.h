#pragma once
//
// Binance Spot Trade Unit (REST + userDataStream WS).
//   REST : boost::beast (net::RestClient) — 全异步, 不阻塞策略线程
//   WS   : boost::beast (net::WsClient)   — 自带 auto_reconnect + ping/pong
//   Parse: simdjson ondemand
//
// 不带 WS 下单 (ws-api order.place) —— 后续 Phase 会加, 先保持功能与老版本等价。
//
#include "base/BaseTrade.h"

#include <simdjson.h>


class BinanceSpotTradeUnit : public BaseTradeUnit {

public:
    BinanceSpotTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~BinanceSpotTradeUnit();

    // 建 REST + WS. WsClient 自动重连, on_open 里发一次 fresh-signed subscribe。
    virtual void subWebsocekt() override;

    // BeastWsClient message callback: (data, len, isBinary, recv_ns)
    virtual void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t recv_ns) override;

    // 每次重连都要发一条 fresh-signed userDataStream.subscribe.signature (timestamp 5s 内)。
    // 用 cfg.subscribe_messages 存不下 —— 会 stale。 所以在 on_open 时现场生成再 send_text。
    virtual void onOpen() override;

    virtual void query_account (const pubsub::TCommand& tcmd) override;
    virtual void query_balance (const pubsub::TCommand& tcmd) override;
    virtual void query_position(const pubsub::TCommand& tcmd) override;
    virtual void add_new_order (const pubsub::TCommand& tcmd) override;
    virtual void cancel_order  (const pubsub::TCommand& tcmd) override;
    virtual void query_order   (const pubsub::TCommand& tcmd) override;

private:
    // ---- 签名 / 构造 URL 助手 ----
    // 把 (kvs) 拼成 "k1=v1&k2=v2&..." 追加 signature, 返回 "path?...&signature=..."
    // Binance REST 契约: 按 caller 给的顺序拼 (顺序不必字母序, 只要 sign 的 query 跟 send 的 query 完全一致)。
    std::string buildSignedPath(std::string_view basePath,
                                const std::vector<std::pair<std::string, std::string>>& kvs) const;

    // WS ws-api userDataStream.subscribe.signature: kvs **必须字母序** 拼串再 HMAC。
    std::string buildWsSigPayload(std::vector<std::pair<std::string, std::string>> kvs) const;

    // 拼 JSON body {"id":"...","method":"...","params":{...}}, 手拼避免拉 rapidjson。
    std::string buildSubscribeJson(long ts_ms, const std::string& signature) const;

    // ---- 解析助手 (simdjson ondemand) ----
    // 走 outboundAccountPosition (余额 push) → PUSH_RCMD
    void handleAccountPosition(simdjson::ondemand::object& ev);
    // 走 executionReport (订单状态 push) → PUSH_RCMD
    void handleExecutionReport (simdjson::ondemand::object& ev);


private:
    // REST 相对路径
    std::string newOrderUrl    = "/api/v3/order";
    std::string cancelOrderUrl = "/api/v3/order";
    std::string queryOrderUrl  = "/api/v3/order";
    std::string balanceUrl     = "/api/v3/account";
};