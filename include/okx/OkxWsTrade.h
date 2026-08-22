#pragma once
//
// OKX v5 Trade Unit —— **WS 下单**版本 (op:order over /ws/v5/private).
//
// 与 OkxTradeUnit (REST 下单 + WS 订阅) 并列存在。
//
// OKX 的 v5 WS 是**最简单**的一档 —— 单连接, session-based, 一个 endpoint 全包:
//   1. 连上 wss://ws.okx.com:8443/ws/v5/private
//   2. 发 op:login (HMAC-SHA256+base64 签 "timestamp + GET + /users/self/verify")
//   3. login ack 后 session 就 authenticated 了, 后续所有操作免签:
//      - op:subscribe 订阅 account/positions/orders 推送
//      - op:order      下单 (跟 REST /api/v5/trade/order 参数一致)
//      - op:cancel-order 撤单
//      - op:batch-orders 批量 (未实现)
//   4. Ping: 发原始文本 "ping" (不是 JSON), server 回 "pong"
//
// 认证:
//   HMAC-SHA256 + base64 (跟 REST 一样, acc.secretKey 是 HMAC key), 不是 Ed25519。
//
// 无 REST 下单兜底:
//   WS 未 login → REJECT。 query_balance/query_position/query_order 走 REST。
//
#include "base/BaseTrade.h"
#include <simdjson.h>
#include <atomic>
#include <string>
#include <unordered_map>
#include <tbb/concurrent_hash_map.h>


class OkxWsTradeUnit : public BaseTradeUnit {

public:
    OkxWsTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~OkxWsTradeUnit();

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

    // ---- WS JSON builders ----
    // op:login (timestamp 秒), 单条消息里签名
    std::string buildLoginJson();
    // op:subscribe [account, positions, orders]
    std::string buildSubscribeJson() const;
    // op:order  args=[{...}]
    std::string buildOrderPlaceJson(int reqId, const pubsub::TCommand& tcmd,
                                     const md::InstrumentInfo& info,
                                     const std::string& price, const std::string& amount,
                                     const char* side, const char* ordType) const;
    // op:cancel-order  args=[{...}]
    std::string buildOrderCancelJson(int reqId, const pubsub::TCommand& tcmd, const md::InstrumentInfo& info) const;

    // ---- pending map ----
    void recordPending(int id, pubsub::CommandType type, const pubsub::RCommand& rcmd);
    bool takePending(int id, WsPending& out);
    void clearPending();

    struct OrderResultFields {
        std::string_view ordId_sv;
        std::string_view sCode_sv;
        std::string_view sMsg_sv;
    };

    struct ErrorFields {
        std::string_view code_sv;
        std::string_view msg_sv;
    };

    // ---- msg 分派 ----
    // login/subscribe/error ack 有 "event" 字段
    // order.place / cancel-order 响应有 "id" + "op" + "code" + "data"
    // subscription push 有 "arg" + "data"
    void handleWsApiResponse(WsPending& pending, const OrderResultFields& fields);
    void handleWsApiError(WsPending& pending, const ErrorFields& fields);

    void handleAccountUpdate(simdjson::ondemand::array& arr);
    void handlePositionsUpdate(simdjson::ondemand::array& arr);
    void handleOrdersUpdate(simdjson::ondemand::array& arr);

private:
    // WS session state
    std::atomic<bool> wsLoggedIn_{false};
    std::atomic<int> nextWsId_{100};

    tbb::concurrent_hash_map<int, WsPending> pendingMap_;
    std::atomic<int64_t> pendingLastGcMs_{0};

    // WS 路径
    std::string wsPath_ = "/ws/v5/private";

    // REST 端点
    std::string balanceUrl = "/api/v5/account/balance";
    std::string positionUrl = "/api/v5/account/positions";
    std::string queryOrderUrl = "/api/v5/trade/order";

    int64_t kPendingTtlMs = 30 * 1000;
    size_t  kPendingHardMax = 10000;
    int64_t kGcIntervalMs = 5000;
};