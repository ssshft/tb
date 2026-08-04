#pragma once
//
// Gateio USDT-M Futures Trade Unit —— **WS 下单**版本 (v4 WS RPC + session login)。
// 与 GateioUSTradeUnit (纯 REST + 订阅 WS) 并列存在。
//
// Endpoint: wss://fx-ws.gateio.ws/v4/ws/usdt/
// 结构跟 GateioSpotWs 一模一样, 只是 channel 前缀 spot.* → futures.*
//
#include "base/BaseTrade.h"

#include <simdjson.h>

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>


class GateioUsWsTradeUnit : public BaseTradeUnit {

public:
    GateioUsWsTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~GateioUsWsTradeUnit();

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

    static constexpr int kLoginId          = 1;
    static constexpr int kOrdersSubId      = 2;
    static constexpr int kBalancesSubId    = 3;
    static constexpr int kPositionsSubId   = 4;

    std::string buildLoginJson(long ts) const;
    std::string buildSubscribeJson(int reqId, const char* channel) const;
    std::string buildOrderPlaceJson(int reqId, const pubsub::TCommand& tcmd,
                                     const md::InstrumentInfo& info,
                                     const std::string& price, double sizeSigned,
                                     const char* tif) const;
    std::string buildOrderCancelJson(int reqId, const pubsub::TCommand& tcmd,
                                      const md::InstrumentInfo& info) const;

    std::vector<std::pair<std::string, std::string>>
    gateAuthHeaders(const std::string& method, const std::string& path,
                    const std::string& query, const std::string& body,
                    const std::string& time_str) const;

    void recordPending(int id, WsReqType type,
                       const pubsub::RCommand& rcmd,
                       const md::InstrumentInfo& info);
    bool takePending(int id, WsPending& out);
    void clearPending();
    void gcPendingLocked(int64_t now_ms);

    void handleRpcResponse(simdjson::ondemand::document& doc);
    void handleSubUpdate  (simdjson::ondemand::document& doc);
    void handleOrdersUpdate   (simdjson::ondemand::value& result);
    void handleBalancesUpdate (simdjson::ondemand::value& result);
    void handlePositionsUpdate(simdjson::ondemand::value& result);

    void onLoginResponse       (bool ack, int status, simdjson::ondemand::document& doc);
    void onOrderPlaceResponse  (WsPending& pending, bool ack, int status,
                                 simdjson::ondemand::document& doc);
    void onOrderCancelResponse (WsPending& pending, bool ack, int status,
                                 simdjson::ondemand::document& doc);

    static int mapAdlRanking(int r);

private:
    std::atomic<bool> wsLoggedIn_{false};
    std::atomic<int>  nextWsId_{100};

    std::mutex                                   pendingMtx_;
    std::unordered_map<int, WsPending>           pendingMap_;
    std::atomic<int64_t>                         pendingLastGcMs_{0};

    std::string balanceUrl    = "/api/v4/futures/usdt/accounts";
    std::string positionUrl   = "/api/v4/futures/usdt/positions";
    std::string queryOrderUrl = "/api/v4/futures/usdt/orders";
};