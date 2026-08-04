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


class BinanceUFWsTradeUnit : public BaseTradeUnit {

public:
    BinanceUFWsTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~BinanceUFWsTradeUnit();

    virtual void subWebsocekt() override;
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

    static constexpr int kSessionLogonId  = 1;
    static constexpr int kUserStreamSubId = 2;

    // ---- REST (Ed25519 签名 + URL-encode) ----
    std::string signPayloadForRest(const std::string& qs) const;
    std::string buildRestSignedPath(std::string_view basePath,
                                    const std::vector<std::pair<std::string, std::string>>& kvs) const;

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
    void recordPending(int id, WsReqType type,
                       const pubsub::RCommand& rcmd,
                       const md::InstrumentInfo& info);
    bool takePending(int id, WsPending& out);
    void clearPending();
    void gcPendingLocked(int64_t now_ms);

    // ---- msg 分派 ----
    void handleWsApiResponse (simdjson::ondemand::document& doc, int64_t recv_ns);
    void handleUserDataEvent (simdjson::ondemand::object& event);
    void handleAccountUpdate (simdjson::ondemand::object& event);
    void handleOrderUpdate   (simdjson::ondemand::object& event);

    // ---- ws-api 响应处理 ----
    void onLogonResponse       (int status, simdjson::ondemand::document& doc);
    void onOrderPlaceResponse  (WsPending& pending, int status,
                                 simdjson::ondemand::document& doc, int64_t recv_ns);
    void onOrderCancelResponse (WsPending& pending, int status,
                                 simdjson::ondemand::document& doc, int64_t recv_ns);

    // ---- helpers ----
    bool lookupInstrument(const std::string& originInstId, md::InstrumentInfo& info, InstType& out) const;

private:
    crypto::Ed25519Signer signer_;

    std::atomic<bool> wsLoggedIn_{false};
    std::atomic<int>  nextWsId_{100};

    std::mutex                                   pendingMtx_;
    std::unordered_map<int, WsPending>           pendingMap_;
    std::atomic<int64_t>                         pendingLastGcMs_{0};

    // REST 端点
    std::string balanceUrl    = "/fapi/v3/account";
    std::string positionUrl   = "/fapi/v3/positionRisk";
    std::string queryOrderUrl = "/fapi/v1/order";
};