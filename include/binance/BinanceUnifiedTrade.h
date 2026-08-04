#pragma once
//
// Binance PortfolioMargin (papi) Trade Unit —— REST + userDataStream WS。
//   REST : boost::beast (net::RestClient) — 全异步
//   WS   : boost::beast (net::WsClient)   — 自带 auto_reconnect
//   Parse: simdjson ondemand
//
// PAPI 特点:
//   1) 需要先 REST POST /papi/v1/listenKey 拿 listenKey, ws url = wsBase/ws/<listenKey>
//   2) 每 30min 需要 REST PUT /papi/v1/listenKey 续期
//   3) 下单/撤单/查单 URL 按品种分流: margin / um / cm
//   4) balance 端点是数组, account 端点单独查 totalEquity, adl 也是单独端点
//   5) WS 事件通过 "fs" 字段区分 UM / CM (仅 pos/order update 场景)
//
#include "base/BaseTrade.h"

#include <simdjson.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>


class BinanceUnifiedTradeUnit : public BaseTradeUnit {

public:
    BinanceUnifiedTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~BinanceUnifiedTradeUnit();

    virtual void subWebsocekt() override;
    virtual void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t recv_ns) override;

    virtual void query_account (const pubsub::TCommand& tcmd) override;
    virtual void query_balance (const pubsub::TCommand& tcmd) override;
    virtual void query_position(const pubsub::TCommand& tcmd) override;
    virtual void add_new_order (const pubsub::TCommand& tcmd) override;
    virtual void cancel_order  (const pubsub::TCommand& tcmd) override;
    virtual void query_order   (const pubsub::TCommand& tcmd) override;

private:
    std::string buildSignedPath(std::string_view basePath,
                                const std::vector<std::pair<std::string, std::string>>& kvs) const;

    // ---- listenKey (与 UF 相同的模式) ----
    bool generateListenKeySync();
    void renewListenKeyAsync();
    void listenKeyRenewLoop();

    // ---- WS msg 分派 ----
    // "e":"ACCOUNT_UPDATE"    → 持仓 (papi 的余额不来自这里)
    void handleAccountUpdate(simdjson::ondemand::object& root);
    // "e":"ORDER_TRADE_UPDATE" → 期货订单 (需 "fs" 区分 UM/CM)
    void handleOrderUpdate  (simdjson::ondemand::object& root);
    // "e":"executionReport"    → 现货 / 保证金订单
    void handleExecutionReport(simdjson::ondemand::object& root);

    // ---- 品种 → URL 路径映射 ----
    static const char* newOrderPath(InstType t);
    static const char* cancelOrderPath(InstType t);
    static const char* queryOrderPath(InstType t);
    static const char* positionPathFor(InstType t);

    // query_position 需要先跑一次 adl (回调里再启动 positionRisk 请求)
    void queryPositionWithAdl(InstType instType);


private:
    // 固定端点
    std::string accountUrl     = "/papi/v1/account";
    std::string balanceUrl     = "/papi/v1/balance";
    std::string listenKeyUrl   = "/papi/v1/listenKey";
    std::string wsSubPath      = "/ws/";
    // adl: um / cm 分开
    // std::string adlUrl 由 positionPathFor 相关的辅助拼出

    std::string listenKey_;
    std::thread renewThread_;
    std::atomic<bool> renewStop_{false};
    std::mutex renewMtx_;
    std::condition_variable renewCv_;
};