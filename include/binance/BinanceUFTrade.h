#pragma once
//
// Binance USDT-M (FAPI) Trade Unit —— REST + userDataStream WS。
//   REST : boost::beast (net::RestClient) — 全异步
//   WS   : boost::beast (net::WsClient)   — 自带 auto_reconnect
//   Parse: simdjson ondemand
//
// FAPI 特点:
//   1) 需要先 REST POST /fapi/v1/listenKey 拿 listenKey, ws url = wsBase/<listenKey>
//   2) 每 30min 需要 REST PUT /fapi/v1/listenKey 续期
//   3) WS 连上后自动 push 账户/持仓/订单流, 不需要额外 subscribe
//   4) exchange 主动 PING → beast 自动 PONG (cfg.ping_mode = ServerOnly)
//
// 无 WS 下单 (order.place ws-api 版本, Phase 后续再加)。
//
#include "base/BaseTrade.h"
#include <simdjson.h>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>


class BinanceUFTradeUnit : public BaseTradeUnit {

public:
    BinanceUFTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~BinanceUFTradeUnit();

    // subWebsocekt: 先拿 listenKey, 再建 REST + WS, 起 renew 线程。
    virtual void subWebsocekt() override;

    // BeastWsClient message callback
    virtual void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t recv_ns) override;

    // FAPI 不需要 subscribe, 保持 base 的实现即可 (isConnected + log)。
    // override 不写。

    virtual void query_account(const pubsub::TCommand& tcmd) override;
    virtual void query_balance(const pubsub::TCommand& tcmd) override;
    virtual void query_position(const pubsub::TCommand& tcmd) override;
    virtual void add_new_order(const pubsub::TCommand& tcmd) override;
    virtual void cancel_order(const pubsub::TCommand& tcmd) override;
    virtual void query_order(const pubsub::TCommand& tcmd) override;

private:
    // ---- URL / 签名 ----
    std::string buildSignedPath(std::string_view basePath, const std::vector<std::pair<std::string, std::string>>& kvs) const;

    // ---- listenKey ----
    // sync 生成: 内部起 promise/future, 阻塞不超过 15s (启动路径, 非 hot path)。
    // 成功返回 true 并把 listenKey 写入 listenKey_。
    bool generateListenKeySync();

    // async 续期: fire-and-forget, callback 里判 code!=0 就打 log。
    void renewListenKeyAsync();

    // 后台线程: 每 30min 调 renewListenKeyAsync。
    void listenKeyRenewLoop();

    // ---- WS msg 分派 ----
    // "e":"ACCOUNT_UPDATE" → 余额 + 持仓 push
    void handleAccountUpdate(simdjson::ondemand::object& root);
    // "e":"ORDER_TRADE_UPDATE" → 订单回报
    void handleOrderUpdate(simdjson::ondemand::object& root);


private:
    // REST 相对路径
    std::string newOrderUrl = "/fapi/v1/order";
    std::string cancelOrderUrl = "/fapi/v1/order";
    std::string queryOrderUrl = "/fapi/v1/order";
    std::string balanceUrl = "/fapi/v3/account";
    std::string positionUrl = "/fapi/v3/positionRisk";
    std::string listenKeyUrl = "/fapi/v1/listenKey";
    std::string wsSubPath = "/ws/";   // Binance FAPI live: wsBase + "/ws/" + listenKey

    std::string listenKey_;                // ws 连接开始后不再改动

    // ---- listenKey 续期后台线程 ----
    std::thread renewThread_;
    std::atomic<bool> renewStop_{false};
    std::mutex renewMtx_;
    std::condition_variable renewCv_;

    int kListenKeyRenewSec = 30 * 60;  // 30min
};