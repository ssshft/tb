#pragma once
//
// Gateio Spot Trade Unit —— **WS 下单**版本 (v4 WS RPC + session login)。
//
// 与 GateioSpotTradeUnit (纯 REST + 订阅推送 WS) 并列存在。
//
// Gate v4 WS 特色:
//   同一个 endpoint (wss://api.gateio.ws/ws/v4/) 既支持 subscribe/push, 也支持
//   spot.order_place / spot.order_cancel 这种 RPC 调用。
//
// 认证:
//   HMAC-SHA512 (跟 REST 一样, acc.secretKey 是 HMAC key), 不是 Ed25519。
//
// 会话:
//   1) 连上 → 发 spot.login (HMAC 签 "channel=spot.login&event=api&time=T")
//   2) login ack (ack=true, status=200) → wsLoggedIn_ = true
//   3) 发 spot.orders / spot.balances 订阅 (session-authed, payload 里不带 auth)
//   4) 后续 spot.order_place / spot.order_cancel 也免签
//
// 无 REST 下单兜底:
//   WS 未 login → REJECT。
//   query_balance / query_order 走 REST (HMAC-SHA512 签名, 3 个 header)。
//
// 响应关联:
//   req_id 是 string. 我们用 "100", "101", ... 递增, atoi 回来查表。
//
#include "base/BaseTrade.h"

#include <simdjson.h>

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>


class GateioSpotWsTradeUnit : public BaseTradeUnit {

public:
    GateioSpotWsTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~GateioSpotWsTradeUnit();

    virtual void subWebsocekt() override;
    virtual void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t recv_ns) override;
    virtual void onOpen() override;
    virtual void onCloseMsg(int code, const std::string& reason) override;

    virtual void query_account(const pubsub::TCommand& tcmd) override;
    virtual void query_balance(const pubsub::TCommand& tcmd) override;
    virtual void query_position(const pubsub::TCommand& tcmd) override;
    virtual void add_new_order(const pubsub::TCommand& tcmd) override;
    virtual void cancel_order(const pubsub::TCommand& tcmd) override;
    virtual void query_order(const pubsub::TCommand& tcmd) override;

private:
    struct WsPending {
        pubsub::CommandType type;
        pubsub::RCommand rcmd;
        int64_t ts_ms;
    };

    static constexpr int kLoginId = 1;
    static constexpr int kOrdersSubId = 2;
    static constexpr int kBalancesSubId = 3;

    // ---- WS RPC msg builders ----
    // spot.login 需要签名, 其他 (post-login) 无 auth 字段
    std::string buildLoginJson(long ts) const;
    std::string buildSubscribeJson(int reqId, const char* channel, const char* payload_first, const char* payload_second) const;
    std::string buildOrderPlaceJson(int reqId, const pubsub::TCommand& tcmd,
                                     const md::InstrumentInfo& info,
                                     const std::string& price, const std::string& amount,
                                     const char* side, const char* tif) const;
    std::string buildOrderCancelJson(int reqId, const pubsub::TCommand& tcmd,
                                      const md::InstrumentInfo& info) const;

    // ---- pending map ----
    void recordPending(int id, pubsub::CommandType type, const pubsub::RCommand& rcmd);
    bool takePending(int id, WsPending& out);
    void clearPending();

    // ---- msg 分派 ----
    void handleWsApiResponse(WsPending& pending, simdjson::ondemand::object& result);
    void handleWsApiError(WsPending& pending, simdjson::ondemand::object& err);
    void handleBalancesUpdate(simdjson::ondemand::array& arr);
    void handleOrdersUpdate(simdjson::ondemand::array& arr);

private:
    std::atomic<bool> wsLoggedIn_{false};
    std::atomic<int> nextWsId_{100};

    tbb::concurrent_hash_map<int, WsPending> pendingMap_;
    std::atomic<int64_t> pendingLastGcMs_{0};

    // REST 端点 (query_balance / query_order 走 REST)
    std::string balanceUrl = "/api/v4/spot/accounts";
    std::string queryOrderUrl = "/api/v4/spot/orders";

    int64_t kPendingTtlMs = 30 * 1000;
    size_t  kPendingHardMax = 10000;
    int64_t kGcIntervalMs = 5000;
};