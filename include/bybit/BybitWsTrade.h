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
#include <tbb/concurrent_hash_map.h>


class BybitWsTradeUnit : public BaseTradeUnit {

public:
    BybitWsTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~BybitWsTradeUnit();

    // 覆盖 subWebsocekt: 自己起 **两条** WsClient, 不走 BaseTradeUnit::subWebsocketWithConfig
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
    // ---- 请求类型 (pending map 里记类型分派响应) ----
    struct WsPending {
        pubsub::CommandType type;
        pubsub::RCommand rcmd;
        int64_t ts_ms;
    };

    // Bybit reqId 是字符串, 我们用 "1"/"2"/"100"+ 递增
    static constexpr int kTradeAuthId = 1;
    static constexpr int kUserAuthId = 1;   // 两条连接各自独立, id 空间不冲突
    static constexpr int kUserSubId = 2;

    // ---- WS JSON builders (trade endpoint) ----
    std::string buildTradeAuthJson() const;

    // ---- WS JSON builders (private endpoint) ----
    std::string buildPrivateAuthJson() const;
    std::string buildPrivateSubscribeJson() const;

    std::string buildOrderPlaceJson(int reqId,
                    const pubsub::TCommand& tcmd,
                    const md::InstrumentInfo& info,
                    const char* category,
                    const char* side, const char* orderType,
                    const char* tif,
                    const std::string& qty,
                    const std::string& price) const;
                    
    std::string buildOrderCancelJson(int reqId, const pubsub::TCommand& tcmd, const md::InstrumentInfo& info, const char* category) const;

    // ---- pending map ----
    void recordPending(int id, pubsub::CommandType type, const pubsub::RCommand& rcmd);
    bool takePending(int id, WsPending& out);
    void clearPending();

    // ---- trade WS 分派 ----
    // 有 "op":"pong" 是 ping response
    // 有 "op":"auth" + retCode=0 是 auth ack
    // 有 "op":"order.create"/"order.cancel" 是订单响应
    void onWsTradeMsg(const uint8_t* data, size_t len, bool isBinary, int64_t recv_ns);

    // ---- WS msg 分派 ----
    void handleOrdersUpdate(simdjson::ondemand::array& dataArr);
    void handleWalletUpdate(simdjson::ondemand::array& dataArr);
    void handlePositionUpdate(simdjson::ondemand::array& dataArr);

    // ---- 订单响应处理 ----
    void onOrderPlaceResponse  (WsPending& pending, int retCode,
                                 std::string_view retMsg,
                                 simdjson::ondemand::document& doc);
    void onOrderCancelResponse (WsPending& pending, int retCode,
                                 std::string_view retMsg,
                                 simdjson::ondemand::document& doc);

private:

    std::shared_ptr<net::WsClient> pWsTradeClient;
    std::atomic<bool> isTradeConnected{false};
    std::atomic<bool> tradeWsAuthed_{false};

    std::atomic<int>   nextWsId_{100};

    tbb::concurrent_hash_map<int, WsPending> pendingMap_;
    std::atomic<int64_t> pendingLastGcMs_{0};

    // REST 端点
    std::string balanceUrl = "/v5/account/wallet-balance";
    std::string positionUrl = "/v5/position/list";
    std::string queryOrderUrl = "/v5/order/realtime";

    int64_t kPendingTtlMs = 30 * 1000;
    size_t kPendingHardMax = 10000;
    int64_t kGcIntervalMs = 5000;
};