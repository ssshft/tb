#pragma once
//
// Binance USDT-M (FAPI) Trade Unit —— **WS 下单**版本 (ws-fapi + Ed25519)。
//
// 与 BinanceUFTradeUnit (纯 REST + fstream listenKey WS) 并列存在。
//
// 认证:
//   Ed25519 私钥 (acc.secretKey 是 PEM). 启动时 init, 失败则所有请求 REJECT。
//
// 会话:
//   连上 ws-fapi endpoint (wss://ws-fapi.binance.com/ws-fapi/v1) 后:
//     1) 发 session.logon (Ed25519 签 "apiKey=X&timestamp=T")
//     2) session.logon ack 200 → wsLoggedIn_ = true
//     3) 发 userDataStream.subscribe (session-authed, 无 signature)
//   后续 order.place / order.cancel 也都是 session-authed。
//
//   ws-fapi.binance.com **不需要 listenKey** —— 只有 legacy fstream.binance.com/ws/<listenKey>
//   才用 listenKey; ws-fapi 已经用 session.logon 认证。
//
// 无 REST 下单兜底:
//   WS 未 logon → add_new_order/cancel_order 立即 REJECT (TBDisconnectError)。
//   query_balance / query_position / query_order 走 REST (Ed25519 签名)。
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


class BinanceUFWsTradeUnit : public BaseTradeUnit {

public:
    BinanceUFWsTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~BinanceUFWsTradeUnit();

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

    static constexpr int kSessionLogonId  = 1;

    // ---- REST (Ed25519 签名 + URL-encode) ----
    std::string signPayloadForRest(const std::string& qs) const;
    std::string buildRestSignedPath(std::string_view basePath, const std::vector<std::pair<std::string, std::string>>& kvs) const;

    // ---- WS JSON builders ----
    std::string buildLogonJson();
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
    void handleWsApiResponse(WsPending& pending, simdjson::ondemand::object& result);
    void handleWsApiError(WsPending& pending, simdjson::ondemand::object& err);
    
    bool generateListenKeySync();
    void renewListenKeyAsync();
    void listenKeyRenewLoop();

    void onWsTradeMsg(const uint8_t* data, size_t len, bool isBinary, int64_t recv_ns);

    // ---- WS msg 分派 ----
    // "e":"ACCOUNT_UPDATE" → 余额 + 持仓 push
    void handleAccountUpdate(simdjson::ondemand::object& o);
    // "e":"ORDER_TRADE_UPDATE" → 订单回报
    void handleOrderUpdate(simdjson::ondemand::object& a);

private:
    crypto::Ed25519Signer signer_;

    std::atomic<bool> wsLoggedIn_{false};
    std::atomic<int> nextWsId_{100};

    tbb::concurrent_hash_map<int, WsPending> pendingMap_;
    std::atomic<int64_t> pendingLastGcMs_{0};

    // REST 端点
    std::string balanceUrl = "/fapi/v3/account";
    std::string positionUrl = "/fapi/v3/positionRisk";
    std::string queryOrderUrl = "/fapi/v1/order";
    std::string listenKeyUrl = "/fapi/v1/listenKey";

    std::string listenKey_;
    std::shared_ptr<net::WsClient> pWsTradeClient;
    std::atomic<bool> isTradeConnected{false};

    // ---- listenKey 续期后台线程 ----
    std::thread renewThread_;
    std::atomic<bool> renewStop_{false};
    std::mutex renewMtx_;
    std::condition_variable renewCv_;
    int kListenKeyRenewSec = 30 * 60;  // 30min

    int64_t kPendingTtlMs = 30 * 1000;
    size_t kPendingHardMax = 10000;
    int64_t kGcIntervalMs = 5000;
};