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
#include <tbb/concurrent_hash_map.h>


class GateioUsWsTradeUnit : public BaseTradeUnit {

public:
    GateioUsWsTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~GateioUsWsTradeUnit();

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
    static constexpr int kPositionsSubId = 4;

    std::string buildLoginJson(long ts) const;
    std::string buildSubscribeJson(int reqId, const char* channel) const;
    std::string buildOrderPlaceJson(int reqId, const pubsub::TCommand& tcmd,
                                     const md::InstrumentInfo& info,
                                     const std::string& price, double sizeSigned,
                                     const char* tif) const;
    std::string buildOrderCancelJson(int reqId, const pubsub::TCommand& tcmd, const md::InstrumentInfo& info) const;

    // ---- pending map ----
    void recordPending(int id, pubsub::CommandType type, const pubsub::RCommand& rcmd);
    bool takePending(int id, WsPending& out);
    void clearPending();

    struct OrderResultFields {
        std::string_view id_sv;
        std::string_view size_sv;
        std::string_view left_sv;
        std::string_view fill_sv;
        std::string_view status_sv;
        std::string_view finish_sv;
    };

    struct ErrorFields {
        std::string_view label_sv;
        std::string_view message_sv;
    };

    // ---- msg 分派 ----

    void handleWsApiResponse(WsPending& pending, const OrderResultFields& fields);
    void handleWsApiError(WsPending& pending, const ErrorFields& fields);

    void handleOrdersUpdate(simdjson::ondemand::array& arr);
    void handleBalancesUpdate(simdjson::ondemand::array& arr);
    void handlePositionsUpdate(simdjson::ondemand::array& arr);

private:
    std::atomic<bool> wsLoggedIn_{false};
    std::atomic<int> nextWsId_{100};

    tbb::concurrent_hash_map<int, WsPending> pendingMap_;
    std::atomic<int64_t> pendingLastGcMs_{0};

    std::string balanceUrl = "/api/v4/futures/usdt/accounts";
    std::string positionUrl = "/api/v4/futures/usdt/positions";
    std::string queryOrderUrl = "/api/v4/futures/usdt/orders";

    int64_t kPendingTtlMs = 30 * 1000;
    size_t kPendingHardMax = 10000;
    int64_t kGcIntervalMs = 5000;
};