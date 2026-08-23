#include "bybit/BybitWsTrade.h"

#include <cmath>
#include <cstdlib>

#include <fmt/format.h>
#include <simdjson.h>


namespace {

    constexpr int64_t kPendingTtlMs   = 30 * 1000;
    constexpr size_t  kPendingHardMax = 10000;
    constexpr int64_t kGcIntervalMs   = 5000;

    inline int parse_int_id(std::string_view sv) {
        if (sv.empty()) return 0;
        return std::atoi(std::string(sv).c_str());
    }
}


BybitWsTradeUnit::BybitWsTradeUnit(AccountCfg& a, sm::SecurityManager* s)
    : BaseTradeUnit(a, s) {}
BybitWsTradeUnit::~BybitWsTradeUnit() = default;


// ============================================================================
// category / lookup
// ============================================================================
const char* BybitWsTradeUnit::categoryOf(InstType t) {
    if (t == SPOT)                                    return "spot";
    if (t == USDT_SWAP || t == USDT_FUTURES)          return "linear";
    if (t == C_SWAP    || t == C_FUTURES)             return "inverse";
    return nullptr;
}

bool BybitWsTradeUnit::lookupInstrument(const std::string& originInstId, const std::string& category,
                                         md::InstrumentInfo& info, InstType& out) const {
    if (category == "spot") {
        if (smc->get_instrument_info(BYBIT, SPOT, originInstId.c_str(), info)) { out = SPOT; return true; }
    } else if (category == "linear") {
        if (smc->get_instrument_info(BYBIT, USDT_SWAP,    originInstId.c_str(), info)) { out = USDT_SWAP;    return true; }
        if (smc->get_instrument_info(BYBIT, USDT_FUTURES, originInstId.c_str(), info)) { out = USDT_FUTURES; return true; }
    } else if (category == "inverse") {
        if (smc->get_instrument_info(BYBIT, C_SWAP,    originInstId.c_str(), info)) { out = C_SWAP;    return true; }
        if (smc->get_instrument_info(BYBIT, C_FUTURES, originInstId.c_str(), info)) { out = C_FUTURES; return true; }
    }
    return false;
}


// ============================================================================
// 签名
// ============================================================================
std::string BybitWsTradeUnit::bybitWsAuthSign(long expires_ms) const {
    // sign_payload = "GET/realtime" + expires_ms
    return crypto::HmacEncodeBybit(acc.secretKey, "GET/realtime" + std::to_string(expires_ms));
}

std::string BybitWsTradeUnit::bybitRestSign(const std::string& timestamp,
                                              const std::string& payload) const {
    // REST sign: hex(HMAC-SHA256(secret, ts + apiKey + recvWindow + payload))
    return crypto::HmacEncodeBybit(acc.secretKey,
                                   timestamp + acc.apiKey + recvWindow_ + payload);
}

std::vector<std::pair<std::string, std::string>>
BybitWsTradeUnit::bybitRestHeaders(const std::string& payload) const {
    std::string ts   = std::to_string(crypto::getCurrentTimeMilli());
    std::string sign = bybitRestSign(ts, payload);
    return {
        {"X-BAPI-API-KEY",     acc.apiKey},
        {"X-BAPI-TIMESTAMP",   ts},
        {"X-BAPI-RECV-WINDOW", recvWindow_},
        {"X-BAPI-SIGN",        sign},
    };
}


// ============================================================================
// WS JSON builders
// ============================================================================
std::string BybitWsTradeUnit::buildTradeAuthJson(long expires_ms) const {
    std::string sig = bybitWsAuthSign(expires_ms);
    return fmt::format(
        R"({{"reqId":"{}","op":"auth","args":["{}",{},"{}"]}})",
        kTradeAuthId, escape_json(acc.apiKey), expires_ms, sig);
}

std::string BybitWsTradeUnit::buildPrivateAuthJson(long expires_ms) const {
    std::string sig = bybitWsAuthSign(expires_ms);
    return fmt::format(
        R"({{"reqId":"{}","op":"auth","args":["{}",{},"{}"]}})",
        kUserAuthId, escape_json(acc.apiKey), expires_ms, sig);
}

std::string BybitWsTradeUnit::buildPrivateSubscribeJson() const {
    return fmt::format(
        R"({{"reqId":"{}","op":"subscribe","args":["order","wallet","position"]}})",
        kUserSubId);
}

std::string BybitWsTradeUnit::buildOrderPlaceJson(int reqId,
                                                    const pubsub::TCommand& tcmd,
                                                    const md::InstrumentInfo& info,
                                                    const char* category,
                                                    const char* side, const char* orderType,
                                                    const char* tif,
                                                    const std::string& qty,
                                                    const std::string& price) const
{
    std::string ts_ms = std::to_string(crypto::getCurrentTimeMilli());
    std::string j;
    j.reserve(500);
    j.append(R"({"reqId":")"); j.append(std::to_string(reqId));                     j.push_back('"');
    j.append(R"(,"header":{"X-BAPI-TIMESTAMP":")");  j.append(ts_ms);              j.push_back('"');
    j.append(R"(,"X-BAPI-RECV-WINDOW":")");          j.append(recvWindow_);        j.push_back('"');
    j.append(R"(},"op":"order.create","args":[{)");
    j.append(R"("category":")");     j.append(category);                          j.push_back('"');
    j.append(R"(,"symbol":")");      j.append(info.originInstId);                 j.push_back('"');
    j.append(R"(,"side":")");        j.append(side);                              j.push_back('"');
    j.append(R"(,"orderType":")");   j.append(orderType);                         j.push_back('"');
    j.append(R"(,"qty":")");         j.append(qty);                               j.push_back('"');
    if (orderType[0] != 'M') {   // MARKET 不带 price
        j.append(R"(,"price":")"); j.append(price);                               j.push_back('"');
    }
    if (tif) { j.append(R"(,"timeInForce":")"); j.append(tif);                     j.push_back('"'); }
    j.append(R"(,"orderLinkId":")"); j.append(escape_json(tcmd.body.newOrder.orderSysId));  j.push_back('"');
    j.append(R"(,"reduceOnly":)");   j.append(tcmd.body.newOrder.reduceOnly ? "true" : "false");
    j.append("}]}");
    return j;
}

std::string BybitWsTradeUnit::buildOrderCancelJson(int reqId,
                                                     const pubsub::TCommand& tcmd,
                                                     const md::InstrumentInfo& info,
                                                     const char* category) const
{
    std::string ts_ms = std::to_string(crypto::getCurrentTimeMilli());
    std::string j;
    j.reserve(300);
    j.append(R"({"reqId":")"); j.append(std::to_string(reqId));                    j.push_back('"');
    j.append(R"(,"header":{"X-BAPI-TIMESTAMP":")"); j.append(ts_ms);               j.push_back('"');
    j.append(R"(,"X-BAPI-RECV-WINDOW":")");         j.append(recvWindow_);         j.push_back('"');
    j.append(R"(},"op":"order.cancel","args":[{)");
    j.append(R"("category":")");    j.append(category);                            j.push_back('"');
    j.append(R"(,"symbol":")");     j.append(info.originInstId);                   j.push_back('"');
    if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
        j.append(R"(,"orderId":")"); j.append(tcmd.body.cancelOrder.orderId);      j.push_back('"');
    } else {
        j.append(R"(,"orderLinkId":")"); j.append(escape_json(tcmd.body.cancelOrder.orderSysId));  j.push_back('"');
    }
    j.append("}]}");
    return j;
}


// ============================================================================
// pending map
// ============================================================================
void BybitWsTradeUnit::recordPending(int id, WsReqType type,
                                       const pubsub::RCommand& rcmd,
                                       const md::InstrumentInfo& info) {
    const int64_t now_ms  = crypto::getCurrentTimeMilli();
    const int64_t last_gc = pendingLastGcMs_.load(std::memory_order_relaxed);
    const bool need_gc    = (now_ms - last_gc > kGcIntervalMs);
    if (need_gc) pendingLastGcMs_.store(now_ms, std::memory_order_relaxed);

    std::lock_guard<std::mutex> lk(pendingMtx_);
    if (need_gc) gcPendingLocked(now_ms);
    pendingMap_[id] = WsPending{rcmd, type, now_ms, info};
}

bool BybitWsTradeUnit::takePending(int id, WsPending& out) {
    std::lock_guard<std::mutex> lk(pendingMtx_);
    auto it = pendingMap_.find(id);
    if (it == pendingMap_.end()) return false;
    out = std::move(it->second);
    pendingMap_.erase(it);
    return true;
}

void BybitWsTradeUnit::clearPending() {
    std::lock_guard<std::mutex> lk(pendingMtx_);
    for (auto& kv : pendingMap_) {
        pubsub::RCommand& rc = kv.second.rcmd;
        if (kv.second.type == WsReqType::NEW_ORDER) {
            rc.body.orderResponse.orderStatus = OS_REJECTED;
            rc.body.orderResponse.errorId     = TBDisconnectError;
        } else {
            rc.body.orderResponse.orderStatus = OS_FAILED;
            rc.body.orderResponse.errorId     = TBDisconnectError;
        }
        rc.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rc)
    }
    pendingMap_.clear();
}

void BybitWsTradeUnit::gcPendingLocked(int64_t now_ms) {
    if (pendingMap_.size() > kPendingHardMax) {
        LOG_ERROR("TB {} Bybit pending over hard cap ({}), clearing.",
                  acc.accountId, pendingMap_.size());
        pendingMap_.clear();
        return;
    }
    for (auto it = pendingMap_.begin(); it != pendingMap_.end();) {
        if (now_ms - it->second.ts_ms > kPendingTtlMs) {
            pubsub::RCommand& rc = it->second.rcmd;
            if (it->second.type == WsReqType::NEW_ORDER) {
                rc.body.orderResponse.orderStatus = OS_REJECTED;
                rc.body.orderResponse.errorId     = NetworkError;
            } else {
                rc.body.orderResponse.orderStatus = OS_FAILED;
                rc.body.orderResponse.errorId     = NetworkError;
            }
            rc.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rc)
            it = pendingMap_.erase(it);
        } else {
            ++it;
        }
    }
}


// ============================================================================
// subWebsocekt: 起两条独立 WsClient
// ============================================================================
void BybitWsTradeUnit::subWebsocekt() {
    // REST
    std::string restHost = host_of(acc.restUrl);
    initRestClient(restHost, /*headers=*/{}, /*conns=*/4);

    // 1. Trade WS (base 类的 pWsClient)
    ::net::WsConfig tradeCfg;
    tradeCfg.url                      = acc.wsUrl + tradeWsPath_;
    tradeCfg.ping_mode                = ::net::WsConfig::PingMode::ClientPeriodicText;
    tradeCfg.client_ping_interval_sec = 20;
    tradeCfg.client_ping_text         = R"({"op":"ping"})";
    tradeCfg.auto_reconnect           = true;
    tradeCfg.idle_timeout_sec         = 60;
    LOG_INFO("TB {} Bybit trade ws {}", acc.accountId, tradeCfg.url);
    subWebsocketWithConfig(std::move(tradeCfg));   // 挂到 pWsClient, onWebsocketMsg 回调这里

    // 2. Private WS (自己起, 单独走一个 pPrivateWs_)
    ::net::WsConfig privateCfg;
    privateCfg.url                      = acc.wsUrl + privateWsPath_;
    privateCfg.ping_mode                = ::net::WsConfig::PingMode::ClientPeriodicText;
    privateCfg.client_ping_interval_sec = 20;
    privateCfg.client_ping_text         = R"({"op":"ping"})";
    privateCfg.auto_reconnect           = true;
    privateCfg.idle_timeout_sec         = 60;
    LOG_INFO("TB {} Bybit private ws {}", acc.accountId, privateCfg.url);

    pPrivateWs_ = ::net::WsClient::create(std::move(privateCfg));
    pPrivateWs_->on_open([this]() {
        privateWsAuthed_.store(false);
        if (!pPrivateWs_) return;
        long expires = crypto::getCurrentTimeMilli() + 10000;   // 10s window
        LOG_INFO("TB {} Bybit private ws send auth", acc.accountId);
        pPrivateWs_->send_text(buildPrivateAuthJson(expires));
    });
    pPrivateWs_->on_message([this](const uint8_t* d, size_t n, bool b, int64_t t) {
        onPrivateWsMsg(d, n, b, t);
    });
    pPrivateWs_->on_close([this](int c, const std::string& r) {
        privateWsAuthed_.store(false);
        LOG_ERROR("TB {} Bybit private ws closed code={} reason={}", acc.accountId, c, r);
    });
    pPrivateWs_->on_error([this](const std::string& m) {
        LOG_ERROR("TB {} Bybit private ws error: {}", acc.accountId, m);
    });
    pPrivateWs_->start();
}


// ============================================================================
// Trade WS: onOpen / onWebsocketMsg / onCloseMsg
// ============================================================================
void BybitWsTradeUnit::onOpen() {
    BaseTradeUnit::onOpen();
    tradeWsAuthed_.store(false);
    if (!pWsClient) return;
    long expires = crypto::getCurrentTimeMilli() + 10000;
    LOG_INFO("TB {} Bybit trade ws send auth", acc.accountId);
    pWsClient->send_text(buildTradeAuthJson(expires));
}

void BybitWsTradeUnit::onCloseMsg(int code, const std::string& reason) {
    BaseTradeUnit::onCloseMsg(code, reason);
    tradeWsAuthed_.store(false);
    clearPending();
}

void BybitWsTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len,
                                        bool /*isBinary*/, int64_t /*recv_ns*/) {
    if (len == 0) return;
    try {
        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc_res = g_parser.iterate(padded);
        if (doc_res.error()) return;
        auto& doc = doc_res.value_unsafe();

        handleTradeWsMsg(doc);
    } catch (const std::exception& e) {
        LOG_ERROR("TB {} Bybit trade ws exc: {}", acc.accountId, e.what());
    }
}


// ============================================================================
// Trade WS 消息分派
// ============================================================================
void BybitWsTradeUnit::handleTradeWsMsg(simdjson::ondemand::document& doc) {
    // "op":"pong" 是 ping 响应, 忽略
    // "op":"auth" 是 auth ack
    // "op":"order.create"/"order.cancel"/"order.amend" 是订单响应
    std::string_view op_sv;
    if (doc.find_field_unordered("op").get(op_sv) != simdjson::SUCCESS) return;

    if (op_sv == "pong") return;

    int64_t retCode_val = 0;
    doc.find_field_unordered("retCode").get(retCode_val);
    const int retCode = static_cast<int>(retCode_val);

    std::string_view retMsg_sv;
    doc.find_field_unordered("retMsg").get(retMsg_sv);

    if (op_sv == "auth") {
        if (retCode == 0) {
            tradeWsAuthed_.store(true);
            LOG_INFO("TB {} Bybit trade ws auth OK", acc.accountId);
        } else {
            tradeWsAuthed_.store(false);
            LOG_ERROR("TB {} Bybit trade ws auth FAILED retCode={} retMsg={}",
                      acc.accountId, retCode, retMsg_sv);
        }
        return;
    }

    // 订单响应 (order.create / order.cancel), 有 reqId
    std::string_view req_id_sv;
    if (doc.find_field_unordered("reqId").get(req_id_sv) != simdjson::SUCCESS) {
        // 没 reqId 的响应 (系统级 pong 等), 忽略
        return;
    }
    const int reqId = parse_int_id(req_id_sv);
    WsPending pending;
    if (!takePending(reqId, pending)) {
        LOG_WARN("TB {} Bybit trade ws unknown reqId={} op={}", acc.accountId, reqId, op_sv);
        return;
    }
    if (pending.type == WsReqType::NEW_ORDER) {
        onOrderPlaceResponse(pending, retCode, retMsg_sv, doc);
    } else {
        onOrderCancelResponse(pending, retCode, retMsg_sv, doc);
    }
}


// ---- order.create 响应 (ACK 只有 orderId + orderLinkId) ----
// {"reqId":"...","retCode":0,"retMsg":"OK","op":"order.create",
//   "data":{"orderId":"...","orderLinkId":""},"retExtInfo":{},"header":{...}}
void BybitWsTradeUnit::onOrderPlaceResponse(WsPending& pending, int retCode,
                                              std::string_view retMsg,
                                              simdjson::ondemand::document& doc) {
    pubsub::RCommand& rcmd = pending.rcmd;
    if (rcmd.body.orderResponse.clientOrderId == TESTCLIENTORDERID) return;

    if (retCode == 0) {
        simdjson::ondemand::object data;
        if (doc.find_field_unordered("data").get(data) == simdjson::SUCCESS) {
            std::string_view oid_sv;
            data.find_field_unordered("orderId").get(oid_sv);
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, oid_sv);
        }
        // Bybit ack 只是"接受了", 状态默认 NEW; 成交细节等 private ws 的 order 推送
        rcmd.body.orderResponse.orderStatus = OS_NEW;
        rcmd.body.orderResponse.errorId     = NoError;
    } else {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId     = UnknownError;   // TODO: retCode 映射
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, retMsg);
    }
    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
    PUSH_RCMD(rcmd)
}

void BybitWsTradeUnit::onOrderCancelResponse(WsPending& pending, int retCode,
                                               std::string_view retMsg,
                                               simdjson::ondemand::document& doc) {
    pubsub::RCommand& rcmd = pending.rcmd;

    if (retCode == 0) {
        simdjson::ondemand::object data;
        if (doc.find_field_unordered("data").get(data) == simdjson::SUCCESS) {
            std::string_view oid_sv;
            data.find_field_unordered("orderId").get(oid_sv);
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, oid_sv);
        }
        rcmd.body.orderResponse.orderStatus = OS_CANCELED;
    } else {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = UnknownError;
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, retMsg);
    }
    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
    PUSH_RCMD(rcmd)
}


// ============================================================================
// Private WS: on_message
// ============================================================================
void BybitWsTradeUnit::onPrivateWsMsg(const uint8_t* data, size_t len,
                                        bool /*isBinary*/, int64_t /*recv_ns*/) {
    if (len == 0) return;
    try {
        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc_res = g_parser.iterate(padded);
        if (doc_res.error()) return;

        auto& doc = doc_res.value_unsafe();

        // auth / subscribe ack: {"success":true, "op":"auth", ...} 或 {"retCode":0,"op":"auth",...}
        std::string_view op_sv;
        if (doc.find_field_unordered("op").get(op_sv) == simdjson::SUCCESS) {
            if (op_sv == "auth") {
                handlePrivateAuthAck(doc);
            } else if (op_sv == "subscribe") {
                LOG_INFO("TB {} Bybit private subscribe ack", acc.accountId);
            } else if (op_sv == "pong") {
                // ping 响应
            }
            return;
        }

        // 订阅推送: {"topic":"order","data":[...]} / "wallet" / "position"
        std::string_view topic_sv;
        if (doc.find_field_unordered("topic").get(topic_sv) != simdjson::SUCCESS) return;
        simdjson::ondemand::value data_val;
        if (doc.find_field_unordered("data").get(data_val) != simdjson::SUCCESS) return;

        if      (topic_sv == "order")    handleOrdersUpdate(data_val);
        else if (topic_sv == "wallet")   handleWalletUpdate(data_val);
        else if (topic_sv == "position") handlePositionUpdate(data_val);
    } catch (const std::exception& e) {
        LOG_ERROR("TB {} Bybit private ws exc: {}", acc.accountId, e.what());
    }
}

void BybitWsTradeUnit::handlePrivateAuthAck(simdjson::ondemand::document& doc) {
    bool success = false;
    doc.find_field_unordered("success").get(success);
    // 也可能返回 retCode=0
    int64_t retCode = 0;
    doc.find_field_unordered("retCode").get(retCode);
    const bool ok = success || retCode == 0;

    if (ok) {
        privateWsAuthed_.store(true);
        LOG_INFO("TB {} Bybit private ws auth OK, subscribing", acc.accountId);
        if (pPrivateWs_) pPrivateWs_->send_text(buildPrivateSubscribeJson());
    } else {
        privateWsAuthed_.store(false);
        LOG_ERROR("TB {} Bybit private ws auth FAILED", acc.accountId);
    }
}


// ============================================================================
// Private 订阅推送 handlers (跟 REST 版 BybitTradeUnit 的 handleXxxUpdate 一致)
// ============================================================================
void BybitWsTradeUnit::handleOrdersUpdate(simdjson::ondemand::value& dataArr) {
    simdjson::ondemand::array arr;
    if (dataArr.get_array().get(arr) != simdjson::SUCCESS) return;
    for (auto it : arr) {
        auto o = it.get_object();
        if (o.error()) continue;
        std::string_view cat_sv, sym_sv, ordId_sv, ordLink_sv, side_sv, oType_sv, tif_sv,
                          qty_sv, px_sv, status_sv, cumQty_sv, avg_sv;
        o["category"].get(cat_sv);
        o["symbol"].get(sym_sv);
        o["orderId"].get(ordId_sv);
        o["orderLinkId"].get(ordLink_sv);
        o["side"].get(side_sv);
        o["orderType"].get(oType_sv);
        o["timeInForce"].get(tif_sv);
        o["qty"].get(qty_sv);
        o["price"].get(px_sv);
        o["orderStatus"].get(status_sv);
        o["cumExecQty"].get(cumQty_sv);
        o["avgPrice"].get(avg_sv);

        std::string originInstId(sym_sv);
        std::string category(cat_sv);
        md::InstrumentInfo info;
        InstType instType;
        if (!lookupInstrument(originInstId, category, info, instType)) continue;

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
        rcmd.body.orderResponse.exchangeTypeEnum = BYBIT;
        rcmd.body.orderResponse.instTypeEnum     = instType;
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.instId,     std::string_view(info.instId));
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId,    ordId_sv);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, ordLink_sv);

        rcmd.body.orderResponse.offsetFlag = OF_OPEN;
        if (!side_sv.empty()) rcmd.body.orderResponse.direction = (side_sv[0] == 'B') ? DT_LONG : DT_SHORT;

        if (!oType_sv.empty()) {
            if (oType_sv == "Market") rcmd.body.orderResponse.orderType = OT_MARKET;
            else if (oType_sv == "Limit") {
                if      (tif_sv == "PostOnly") rcmd.body.orderResponse.orderType = OT_POST_ONLY;
                else if (tif_sv == "FOK")      rcmd.body.orderResponse.orderType = OT_FOK;
                else if (tif_sv == "IOC")      rcmd.body.orderResponse.orderType = OT_IOC;
                else                            rcmd.body.orderResponse.orderType = OT_LIMIT;
            }
        }
        if (!qty_sv.empty())    rcmd.body.orderResponse.volumeTotal  = crypto::fast_atod(qty_sv);
        if (!px_sv.empty())     rcmd.body.orderResponse.limitPrice   = crypto::fast_atod(px_sv);
        if (!cumQty_sv.empty()) rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(cumQty_sv);
        if (!avg_sv.empty())    rcmd.body.orderResponse.tradePrice   = crypto::fast_atod(avg_sv);

        if      (status_sv == "New")             rcmd.body.orderResponse.orderStatus = OS_NEW;
        else if (status_sv == "PartiallyFilled") rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
        else if (status_sv == "Filled")          rcmd.body.orderResponse.orderStatus = OS_FILLED;
        else if (status_sv == "Cancelled" ||
                 status_sv == "PartiallyFilledCanceled") rcmd.body.orderResponse.orderStatus = OS_CANCELED;
        else if (status_sv == "Rejected")        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        else                                      rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;

        rcmd.body.orderResponse.updateTime    = crypto::getCurrentTime();
        rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd)
    }
}

void BybitWsTradeUnit::handleWalletUpdate(simdjson::ondemand::value& dataArr) {
    simdjson::ondemand::array outer;
    if (dataArr.get_array().get(outer) != simdjson::SUCCESS) return;
    for (auto entry : outer) {
        auto d = entry.get_object();
        if (d.error()) continue;
        std::string_view totalEq_sv, adjEq_sv, mmr_sv;
        d["totalEquity"].get(totalEq_sv);
        d["totalWalletBalance"].get(adjEq_sv);
        d["totalMaintenanceMargin"].get(mmr_sv);

        simdjson::ondemand::array coins;
        if (d["coin"].get(coins) == simdjson::SUCCESS) {
            std::vector<pubsub::RCommand> pending;
            for (auto it : coins) {
                auto o = it.get_object();
                if (o.error()) continue;
                std::string_view ccy_sv, eq_sv, wal_sv, avail_sv, upl_sv;
                o["coin"].get(ccy_sv);
                o["equity"].get(eq_sv);
                o["walletBalance"].get(wal_sv);
                o["availableToWithdraw"].get(avail_sv);
                o["unrealisedPnl"].get(upl_sv);

                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                rcmd.body.balance.exchangeTypeEnum = BYBIT;
                rcmd.body.balance.instTypeEnum     = SPOT;
                crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.currency,   crypto::to_upper(std::string(ccy_sv)));
                rcmd.body.balance.available     = crypto::fast_atod(avail_sv);
                rcmd.body.balance.total         = crypto::fast_atod(eq_sv.empty() ? wal_sv : eq_sv);
                rcmd.body.balance.unrealizedPnl = crypto::fast_atod(upl_sv);
                rcmd.body.balance.updateTime    = crypto::getCurrentTime();
                rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;
                pending.emplace_back(rcmd);
            }
            for (size_t i = 0; i < pending.size(); ++i) {
                pending[i].body.balance.isLast = (i + 1 == pending.size());
                PUSH_RCMD(pending[i])
            }
        }

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
        rcmd.body.totalAccount.exchangeTypeEnum = BYBIT;
        rcmd.body.totalAccount.instTypeEnum     = SPOT;
        crypto::copy_sv_to_char_array(rcmd.body.totalAccount.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.totalAccount.strategyId, acc.strategyId);
        rcmd.body.totalAccount.totalEquity = crypto::fast_atod(totalEq_sv);
        rcmd.body.totalAccount.adjEquity   = crypto::fast_atod(adjEq_sv);
        rcmd.body.totalAccount.mmr         = crypto::fast_atod(mmr_sv);
        rcmd.body.totalAccount.mgnRatio    = 100.0;
        rcmd.body.totalAccount.updateTime  = crypto::getCurrentTime();
        rcmd.body.totalAccount.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd)
    }
}

void BybitWsTradeUnit::handlePositionUpdate(simdjson::ondemand::value& dataArr) {
    simdjson::ondemand::array arr;
    if (dataArr.get_array().get(arr) != simdjson::SUCCESS) return;
    std::vector<pubsub::RCommand> pending;
    for (auto it : arr) {
        auto o = it.get_object();
        if (o.error()) continue;
        std::string_view cat_sv, sym_sv, side_sv, size_sv, avg_sv, upl_sv, mark_sv, liq_sv;
        o["category"].get(cat_sv);
        o["symbol"].get(sym_sv);
        o["side"].get(side_sv);
        o["size"].get(size_sv);
        o["avgPrice"].get(avg_sv);
        o["unrealisedPnl"].get(upl_sv);
        o["markPrice"].get(mark_sv);
        o["liqPrice"].get(liq_sv);

        std::string originInstId(sym_sv);
        std::string category(cat_sv);
        md::InstrumentInfo info;
        InstType instType;
        if (!lookupInstrument(originInstId, category, info, instType)) continue;

        double sz = crypto::fast_atod(size_sv);
        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
        rcmd.body.position.exchangeTypeEnum = BYBIT;
        rcmd.body.position.instTypeEnum     = instType;
        crypto::copy_sv_to_char_array(rcmd.body.position.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.position.instId,     std::string_view(info.instId));
        rcmd.body.position.direction = (!side_sv.empty() && side_sv[0] == 'B') ? DT_LONG : DT_SHORT;
        rcmd.body.position.volume    = sz;
        rcmd.body.position.avgPrice      = crypto::fast_atod(avg_sv);
        rcmd.body.position.unrealizedPnl = crypto::fast_atod(upl_sv);
        rcmd.body.position.markPrice     = crypto::fast_atod(mark_sv);
        if (!liq_sv.empty()) rcmd.body.position.liquidPrice = crypto::fast_atod(liq_sv);
        rcmd.body.position.updateTime    = crypto::getCurrentTime();
        rcmd.body.position.apiSourceEnum = AS_WEBSOCKET;
        pending.emplace_back(rcmd);
    }
    for (size_t i = 0; i < pending.size(); ++i) {
        pending[i].body.position.isLast = (i + 1 == pending.size());
        PUSH_RCMD(pending[i])
    }
}


// ============================================================================
// query_* : REST (跟 BybitTradeUnit 一致)
// ============================================================================
void BybitWsTradeUnit::query_account(const pubsub::TCommand& tcmd) {
    query_balance(tcmd);
}

void BybitWsTradeUnit::query_balance(const pubsub::TCommand&) {
    if (!pRestClient) return;
    std::string query = "accountType=UNIFIED";
    std::string fullPath = balanceUrl + "?" + query;
    auto headers = bybitRestHeaders(query);
    asyncRequest(boost::beast::http::verb::get, std::move(fullPath), "", "", std::move(headers),
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) { LOG_ERROR("TB {} Bybit query_balance ec: {}", acc.accountId, ec.message()); return; }
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;
                simdjson::ondemand::value list_val;
                if (doc["result"]["list"].get(list_val) != simdjson::SUCCESS) return;
                handleWalletUpdate(list_val);
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} Bybit query_balance cb exc: {}", acc.accountId, e.what());
            }
        });
}

void BybitWsTradeUnit::query_position(const pubsub::TCommand& tcmd) {
    if (!pRestClient) return;
    const char* cat = categoryOf(tcmd.body.queryPosition.instTypeEnum);
    if (!cat) cat = "linear";
    std::string query = fmt::format("category={}&settleCoin=USDT", cat);
    std::string fullPath = positionUrl + "?" + query;
    auto headers = bybitRestHeaders(query);
    asyncRequest(boost::beast::http::verb::get, std::move(fullPath), "", "", std::move(headers),
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) return;
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;
                simdjson::ondemand::value list_val;
                if (doc["result"]["list"].get(list_val) != simdjson::SUCCESS) return;
                handlePositionUpdate(list_val);
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} Bybit query_position cb exc: {}", acc.accountId, e.what());
            }
        });
}

void BybitWsTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);
    if (!pRestClient) return;
    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum,
                                  tcmd.body.queryOrder.instTypeEnum,
                                  tcmd.body.queryOrder.instId, info)) return;
    const char* cat = categoryOf(tcmd.body.queryOrder.instTypeEnum);
    if (!cat) return;
    std::string query = fmt::format("category={}&symbol={}", cat, info.originInstId);
    if (!crypto::str_cmp(tcmd.body.queryOrder.orderId, "")) {
        query += "&orderId=" + std::string(tcmd.body.queryOrder.orderId);
    } else if (!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")) {
        query += "&orderLinkId=" + std::string(tcmd.body.queryOrder.orderSysId);
    } else return;
    std::string fullPath = queryOrderUrl + "?" + query;
    auto headers = bybitRestHeaders(query);
    auto info_captured = info;
    asyncRequest(boost::beast::http::verb::get, std::move(fullPath), "", "", std::move(headers),
        [this, rcmd, info_captured](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
            if (ec) return;
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;
                simdjson::ondemand::array list;
                if (doc["result"]["list"].get_array().get(list) != simdjson::SUCCESS) return;
                for (auto it : list) {
                    auto o = it.get_object();
                    if (o.error()) continue;
                    std::string_view ordId_sv, ordLink_sv, qty_sv, px_sv, cum_sv, avg_sv, status_sv;
                    o["orderId"].get(ordId_sv);
                    o["orderLinkId"].get(ordLink_sv);
                    o["qty"].get(qty_sv);
                    o["price"].get(px_sv);
                    o["cumExecQty"].get(cum_sv);
                    o["avgPrice"].get(avg_sv);
                    o["orderStatus"].get(status_sv);
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, ordId_sv);
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, ordLink_sv);
                    if (!qty_sv.empty())    rcmd.body.orderResponse.volumeTotal  = crypto::fast_atod(qty_sv);
                    if (!px_sv.empty())     rcmd.body.orderResponse.limitPrice   = crypto::fast_atod(px_sv);
                    if (!cum_sv.empty())    rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(cum_sv);
                    if (!avg_sv.empty())    rcmd.body.orderResponse.tradePrice   = crypto::fast_atod(avg_sv);
                    if      (status_sv == "New")             rcmd.body.orderResponse.orderStatus = OS_NEW;
                    else if (status_sv == "PartiallyFilled") rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
                    else if (status_sv == "Filled")          rcmd.body.orderResponse.orderStatus = OS_FILLED;
                    else if (status_sv == "Cancelled")       rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                    else if (status_sv == "Rejected")        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    else                                      rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    break;
                }
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} Bybit query_order cb exc: {}", acc.accountId, e.what());
            }
        });
}


// ============================================================================
// add_new_order (WS trade order.create)
// ============================================================================
void BybitWsTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load() || !tradeWsAuthed_.load() || !pWsClient) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId     = TBDisconnectError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }
    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.newOrder.exchangeTypeEnum,
                                  tcmd.body.newOrder.instTypeEnum,
                                  tcmd.body.newOrder.instId, info)) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId     = SMCInstrumentNotExistError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }
    const char* cat = categoryOf(tcmd.body.newOrder.instTypeEnum);
    if (!cat) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId     = OrderTypeError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const char* side = nullptr;
    if (tcmd.body.newOrder.offsetFlag == OF_OPEN) {
        if      (tcmd.body.newOrder.direction == DT_LONG)  side = "Buy";
        else if (tcmd.body.newOrder.direction == DT_SHORT) side = "Sell";
    } else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
        if      (tcmd.body.newOrder.direction == DT_LONG)  side = "Sell";
        else if (tcmd.body.newOrder.direction == DT_SHORT) side = "Buy";
    }
    if (!side) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId     =
            (tcmd.body.newOrder.offsetFlag == OF_OPEN || tcmd.body.newOrder.offsetFlag == OF_CLOSE)
                ? DirectionError : OffsetFlagError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const char* orderType = nullptr;
    const char* tif       = nullptr;
    switch (tcmd.body.newOrder.orderType) {
        case OT_LIMIT:     orderType = "Limit";  tif = "GTC";      break;
        case OT_MARKET:    orderType = "Market"; tif = "IOC";      break;
        case OT_POST_ONLY: orderType = "Limit";  tif = "PostOnly"; break;
        case OT_FOK:       orderType = "Limit";  tif = "FOK";      break;
        case OT_IOC:       orderType = "Limit";  tif = "IOC";      break;
        default:
            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            rcmd.body.orderResponse.errorId     = OrderTypeError;
            rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
            return;
    }

    double price  = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice  * info.magnifyNumber, info.tickSize);
    double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber,  info.lotSize);
    std::string price_str = fmt::format("{}", price);
    std::string qty_str   = fmt::format("{}", volume);

    const int reqId = nextWsId_.fetch_add(1, std::memory_order_relaxed);
    std::string msg = buildOrderPlaceJson(reqId, tcmd, info, cat, side, orderType, tif, qty_str, price_str);

    recordPending(reqId, WsReqType::NEW_ORDER, rcmd, info);
    LOG_INFO("TB {} Bybit ws order.create reqId={} msg={}", acc.accountId, reqId, msg);
    pWsClient->send_text(std::move(msg));
}


// ============================================================================
// cancel_order (WS trade order.cancel)
// ============================================================================
void BybitWsTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load() || !tradeWsAuthed_.load() || !pWsClient) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = TBDisconnectError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }
    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.cancelOrder.exchangeTypeEnum,
                                  tcmd.body.cancelOrder.instTypeEnum,
                                  tcmd.body.cancelOrder.instId, info)) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = SMCInstrumentNotExistError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }
    const char* cat = categoryOf(tcmd.body.cancelOrder.instTypeEnum);
    if (!cat) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = OrderTypeError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }
    if (crypto::str_cmp(tcmd.body.cancelOrder.orderId, "") &&
        crypto::str_cmp(tcmd.body.cancelOrder.orderSysId, "")) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = OrderIdError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const int reqId = nextWsId_.fetch_add(1, std::memory_order_relaxed);
    std::string msg = buildOrderCancelJson(reqId, tcmd, info, cat);

    recordPending(reqId, WsReqType::CANCEL_ORDER, rcmd, info);
    LOG_INFO("TB {} Bybit ws order.cancel reqId={} msg={}", acc.accountId, reqId, msg);
    pWsClient->send_text(std::move(msg));
}