#pragma once
//
// Binance Spot Trade Unit —— **WS 下单**版本 (ws-api + Ed25519)。
//
//   与 BinanceSpotTradeUnit (纯 REST) 并列存在, 交易所连接方式二选一。
//
// 认证:
//   Ed25519 私钥 (acc.secretKey 必须是 PEM). 启动时 init, 失败则所有请求 REJECT。
//
// 会话:
//   连上 ws-api endpoint (wss://ws-api.binance.com/ws-api/v3) 后:
//     1) 发 session.logon (Ed25519 签名 "apiKey=X&timestamp=T")
//     2) session.logon ack 200 → wsLoggedIn_ = true
//     3) 发 userDataStream.subscribe (session-authed, 无需 signature)
//   后续 order.place / order.cancel 也都是 session-authed, 省一次签名。
//
// 无 REST 下单兜底:
//   WS 未 logon → add_new_order/cancel_order 立即 REJECT (TBDisconnectError)。
//   query_balance / query_order 仍走 REST (Ed25519 签名 + URL-encode) —— 那是低频路径,
//   即使 WS 挂了也能查账。
//
// 响应关联:
//   pending_map<id → {rcmd, type, info, ts_ms}>, mutex 保护 (低 QPS 无锁不必要)。
//   TTL 30s + hard cap 10000 兜底 GC (WS 长时间挂起时避免累积泄漏)。
//
#include "base/BaseTrade.h"
#include "ed25519_signer.h"

#include <simdjson.h>

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <tbb/concurrent_hash_map.h>


class BinanceSpotWsTradeUnit : public BaseTradeUnit {

public:
    BinanceSpotWsTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~BinanceSpotWsTradeUnit();

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
    // ---- 请求类型 (pending map 里记类型分派响应) ----
    struct WsPending {
        pubsub::CommandType type;
        pubsub::RCommand rcmd;
        int64_t ts_ms;
    };

    // 固定 id (跟 order id 空间不冲突, 后者从 100 起递增)
    static constexpr int kSessionLogonId = 1;
    static constexpr int kUserStreamSubId = 2;

    // ---- REST helpers (Ed25519 签名 + URL-encode) ----
    std::string signPayloadForRest(const std::string& qs) const;
    std::string buildRestSignedPath(std::string_view basePath, const std::vector<std::pair<std::string, std::string>>& kvs) const;

    // ---- WS JSON builders ----
    std::string buildLogonJson();
    std::string buildUserSubscribeJson() const;
    std::string buildOrderPlaceJson(int wsId,
                    const pubsub::TCommand& tcmd,
                    const md::InstrumentInfo& info,
                    const std::string& price, const std::string& amount,
                    const char* side, const char* type,
                    const char* tif, const char* respType) const;

    std::string buildOrderCancelJson(int wsId,
                    const pubsub::TCommand& tcmd,
                    const md::InstrumentInfo& info) const;

    // ---- pending map ----
    void recordPending(int id, pubsub::CommandType type, const pubsub::RCommand& rcmd);
    bool takePending(int id, WsPending& out);
    void clearPending();

    // ---- msg 分派 ----
    void handleWsApiResponse(simdjson::ondemand::document& doc, int64_t recv_ns);
    void handleUserDataEvent(simdjson::ondemand::object& event);
    void handleAccountPosition(simdjson::ondemand::object& event);
    void handleExecutionReport(simdjson::ondemand::object& event);

    // ---- ws-api 响应处理 ----
    void onLogonResponse(int status, simdjson::ondemand::document& doc);
    void onOrderPlaceResponse(WsPending& pending, int status, simdjson::ondemand::document& doc, int64_t recv_ns);
    void onOrderCancelResponse (WsPending& pending, int status, simdjson::ondemand::document& doc, int64_t recv_ns);

private:
    crypto::Ed25519Signer signer_;

    // WS session state
    std::atomic<bool> wsLoggedIn_{false};
    std::atomic<int> nextWsId_{100};

    tbb::concurrent_hash_map<int, WsPending> pendingMap_;
    std::atomic<int64_t> pendingLastGcMs_{0};

    // REST 端点 (query 才走)
    std::string balanceUrl = "/api/v3/account";
    std::string queryOrderUrl = "/api/v3/order";

    int64_t kPendingTtlMs = 30 * 1000;
    size_t kPendingHardMax = 10000;
    int64_t kGcIntervalMs = 5000;

};