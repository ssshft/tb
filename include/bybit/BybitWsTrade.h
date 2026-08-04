#pragma once
//
// Bybit v5 Trade Unit —— **WS 下单**版本 (v5/trade + v5/private 双连接)。
//
// 与 BybitTradeUnit (纯 REST + 单 private WS) 并列存在。
//
// Bybit 的 WS 下单跟 Binance / Gate 不同 —— 用两条独立连接:
//
//   1. wss://stream.bybit.com/v5/trade   —— hot path (order.create / order.cancel)
//      auth: {"op":"auth","args":[apiKey, expires_ms, hex_sig]}
//      order: {"reqId","header":{X-BAPI-TIMESTAMP:...},"op":"order.create","args":[{...}]}
//      响应: {"reqId","retCode":0,"retMsg":"OK","op":"...","data":{"orderId","orderLinkId"}}
//      ⚠️ ack 只有 orderId + orderLinkId, 无成交信息 —— 成交回报要靠下面 private 连接的 order 推送
//
//   2. wss://stream.bybit.com/v5/private —— 订阅 order/wallet/position 推送
//      auth: 同上 (op=auth)
//      subscribe: {"op":"subscribe","args":["order","wallet","position"]}
//
// 认证:
//   HMAC-SHA256 hex (跟 REST 一样), acc.secretKey 是原文 HMAC key。
//
// 无 REST 下单兜底:
//   trade WS 未 auth → REJECT (TBDisconnectError)。
//   query_balance / query_position / query_order 走 REST (HMAC 签名)。
//
#include "base/BaseTrade.h"

#include <simdjson.h>

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>


class BybitWsTradeUnit : public BaseTradeUnit {

public:
    BybitWsTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~BybitWsTradeUnit();

    // 覆盖 subWebsocekt: 自己起 **两条** WsClient, 不走 BaseTradeUnit::subWebsocketWithConfig
    virtual void subWebsocekt() override;

    // 这里的 onWebsocketMsg 只处理 trade WS (BaseTradeUnit 的默认 hook 走 pWsClient),
    // private WS 单独有 onPrivateWsMsg。
    virtual void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t recv_ns) override;
    virtual void onOpen() override;
    virtual void onCloseMsg(int code, const std::string& reason) override;

    virtual void query_account (const pubsub::TCommand& tcmd) override;
    virtual void query_balance (const pubsub::TCommand& tcmd) override;
    virtual void query_position(const pubsub::TCommand& tcmd) override;
    virtual void add_new_order (const pubsub::TCommand& tcmd) override;
    virtual void cancel_order  (const pubsub::TCommand& tcmd) override;
    virtual void query_order   (const pubsub::TCommand& tcmd) override;

private:
    enum class WsReqType : uint8_t { NEW_ORDER, CANCEL_ORDER };
    struct WsPending {
        pubsub::RCommand   rcmd;
        WsReqType          type;
        int64_t            ts_ms;
        md::InstrumentInfo info;
    };

    // Bybit reqId 是字符串, 我们用 "1"/"2"/"100"+ 递增
    static constexpr int kTradeAuthId = 1;
    static constexpr int kUserAuthId  = 1;   // 两条连接各自独立, id 空间不冲突
    static constexpr int kUserSubId   = 2;

    // ---- Bybit 签名 (WS auth) ----
    // sign = hex(HMAC-SHA256(secret, "GET/realtime" + expires_ms))
    std::string bybitWsAuthSign(long expires_ms) const;

    // REST 签名 (跟 BybitTradeUnit 一样, 复用逻辑)
    std::string bybitRestSign(const std::string& timestamp, const std::string& payload) const;
    std::vector<std::pair<std::string, std::string>> bybitRestHeaders(const std::string& payload) const;

    static const char* categoryOf(InstType t);

    // ---- WS JSON builders (trade endpoint) ----
    std::string buildTradeAuthJson(long expires_ms) const;
    std::string buildOrderPlaceJson(int reqId,
                                     const pubsub::TCommand& tcmd,
                                     const md::InstrumentInfo& info,
                                     const char* category,
                                     const char* side, const char* orderType,
                                     const char* tif,
                                     const std::string& qty,
                                     const std::string& price) const;
    std::string buildOrderCancelJson(int reqId,
                                      const pubsub::TCommand& tcmd,
                                      const md::InstrumentInfo& info,
                                      const char* category) const;

    // ---- WS JSON builders (private endpoint) ----
    std::string buildPrivateAuthJson(long expires_ms) const;
    std::string buildPrivateSubscribeJson() const;

    // ---- pending map ----
    void recordPending(int id, WsReqType type,
                       const pubsub::RCommand& rcmd,
                       const md::InstrumentInfo& info);
    bool takePending(int id, WsPending& out);
    void clearPending();
    void gcPendingLocked(int64_t now_ms);

    // ---- trade WS 分派 ----
    // 有 "op":"pong" 是 ping response
    // 有 "op":"auth" + retCode=0 是 auth ack
    // 有 "op":"order.create"/"order.cancel" 是订单响应
    void handleTradeWsMsg(simdjson::ondemand::document& doc);

    // ---- private WS 分派 ----
    // 有 "op":"auth" 是 auth ack
    // 有 "op":"subscribe" 是 subscribe ack
    // 有 "topic":"order"/"wallet"/"position" 是订阅推送
    void onPrivateWsMsg(const uint8_t* data, size_t len, bool isBinary, int64_t recv_ns);
    void handlePrivateAuthAck  (simdjson::ondemand::document& doc);
    void handleOrdersUpdate    (simdjson::ondemand::value& data);
    void handleWalletUpdate    (simdjson::ondemand::value& data);
    void handlePositionUpdate  (simdjson::ondemand::value& data);

    // ---- 订单响应处理 ----
    void onOrderPlaceResponse  (WsPending& pending, int retCode,
                                 std::string_view retMsg,
                                 simdjson::ondemand::document& doc);
    void onOrderCancelResponse (WsPending& pending, int retCode,
                                 std::string_view retMsg,
                                 simdjson::ondemand::document& doc);

    bool lookupInstrument(const std::string& originInstId, const std::string& category,
                          md::InstrumentInfo& info, InstType& out) const;

private:
    // Trade WS state
    std::atomic<bool>  tradeWsAuthed_{false};
    // Private WS state
    std::atomic<bool>  privateWsAuthed_{false};
    // 独立 io 连接
    std::shared_ptr<::net::WsClient> pPrivateWs_;

    std::atomic<int>   nextWsId_{100};

    std::mutex                                   pendingMtx_;
    std::unordered_map<int, WsPending>           pendingMap_;
    std::atomic<int64_t>                         pendingLastGcMs_{0};

    // URL 路径 (acc.wsUrl 传 "wss://stream.bybit.com" 之类的基地址)
    std::string tradeWsPath_   = "/v5/trade";
    std::string privateWsPath_ = "/v5/private";

    // REST 端点
    std::string balanceUrl    = "/v5/account/wallet-balance";
    std::string positionUrl   = "/v5/position/list";
    std::string queryOrderUrl = "/v5/order/realtime";

    // recv_window 字符串, 5000ms 默认
    std::string recvWindow_ = "5000";
};