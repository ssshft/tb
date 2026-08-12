#include "Gateio/GateioSpotWsTrade.h"

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
        // req_id 通常是纯数字字符串, 用 std::atoi 从 std::string 转
        return std::atoi(std::string(sv).c_str());
    }
}


GateioSpotWsTradeUnit::GateioSpotWsTradeUnit(AccountCfg& a, sm::SecurityManager* s)
    : BaseTradeUnit(a, s) {}
GateioSpotWsTradeUnit::~GateioSpotWsTradeUnit() = default;


// ============================================================================
// REST auth headers (HMAC-SHA512, 3 headers)
// ============================================================================
std::vector<std::pair<std::string, std::string>>
GateioSpotWsTradeUnit::gateAuthHeaders(const std::string& method, const std::string& path,
                                         const std::string& query, const std::string& body,
                                         const std::string& time_str) const {
    std::string sign = crypto::getGateioSignatureRest(method, path, time_str, query, body, acc.secretKey);
    return {
        {"KEY",       acc.apiKey},
        {"Timestamp", time_str},
        {"SIGN",      sign},
    };
}


// ============================================================================
// WS JSON builders
// ============================================================================
std::string GateioSpotWsTradeUnit::buildLoginJson(long ts) const {
    // sign payload: channel=spot.login&event=api&time=T
    std::string time_str = std::to_string(ts);
    std::string sign = crypto::getGateioSignatureWs("spot.login", "api", time_str, acc.secretKey);
    // req_id 用固定的 "1" (kLoginId)
    return fmt::format(
        R"({{"time":{},"channel":"spot.login","event":"api","payload":{{)"
        R"("req_id":"{}","req_header":{{}},)"
        R"("api_key":"{}","signature":"{}","timestamp":"{}"}}}})",
        ts, kLoginId, escape_json(acc.apiKey), sign, time_str);
}

std::string GateioSpotWsTradeUnit::buildSubscribeJson(int reqId, const char* channel,
                                                        const char* payload_first,
                                                        const char* payload_second) const {
    // session-authed, payload 里无 auth 字段
    // spot.orders 订阅 payload=["!all"] 表示订阅所有 pair
    // spot.balances 订阅 payload=[] 表示所有 currency
    long ts = crypto::getCurrentTimeSeconds();
    std::string j;
    j.reserve(160);
    j.append(R"({"time":)");    j.append(std::to_string(ts));
    j.append(R"(,"channel":")"); j.append(channel);                                    j.push_back('"');
    j.append(R"(,"event":"subscribe","payload":[)");
    if (payload_first)  { j.push_back('"'); j.append(payload_first);  j.push_back('"'); }
    if (payload_second) { if (payload_first) j.push_back(','); j.push_back('"'); j.append(payload_second); j.push_back('"'); }
    j.append(R"(],"req_id":")"); j.append(std::to_string(reqId));                     j.push_back('"');
    j.push_back('}');
    return j;
}

std::string GateioSpotWsTradeUnit::buildOrderPlaceJson(
    int reqId, const pubsub::TCommand& tcmd, const md::InstrumentInfo& info,
    const std::string& price, const std::string& amount,
    const char* side, const char* tif) const
{
    // session-authed, payload 里无 auth 字段, 只带 req_id + req_param
    long ts = crypto::getCurrentTimeSeconds();
    std::string j;
    j.reserve(400);
    j.append(R"({"time":)"); j.append(std::to_string(ts));
    j.append(R"(,"channel":"spot.order_place","event":"api","payload":{)");
    j.append(R"("req_id":")");                     j.append(std::to_string(reqId));          j.push_back('"');
    j.append(R"(,"req_header":{},"req_param":{)");
    j.append(R"("text":")");                       j.append(escape_json(tcmd.body.newOrder.orderSysId)); j.push_back('"');
    j.append(R"(,"currency_pair":")");             j.append(info.originInstId);                       j.push_back('"');
    j.append(R"(,"type":"limit","account":")");
#ifdef USE_GATEIO_UNIFIED
    j.append("unified");
#else
    j.append(tcmd.body.newOrder.instTypeEnum == MARGIN ? "margin" : "spot");
#endif
    j.push_back('"');
    j.append(R"(,"side":")");                      j.append(side);                                    j.push_back('"');
    j.append(R"(,"amount":")");                    j.append(amount);                                  j.push_back('"');
    j.append(R"(,"price":")");                     j.append(price);                                   j.push_back('"');
    j.append(R"(,"time_in_force":")");             j.append(tif);                                     j.push_back('"');
#ifdef USE_GATEIO_UNIFIED
    j.append(R"(,"auto_borrow":true,"auto_repay":true)");
#endif
    j.append("}}}");
    return j;
}

std::string GateioSpotWsTradeUnit::buildOrderCancelJson(
    int reqId, const pubsub::TCommand& tcmd, const md::InstrumentInfo& info) const
{
    long ts = crypto::getCurrentTimeSeconds();
    std::string j;
    j.reserve(240);
    j.append(R"({"time":)"); j.append(std::to_string(ts));
    j.append(R"(,"channel":"spot.order_cancel","event":"api","payload":{)");
    j.append(R"("req_id":")");                     j.append(std::to_string(reqId));                    j.push_back('"');
    j.append(R"(,"req_header":{},"req_param":{)");
    if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
        j.append(R"("order_id":")");               j.append(tcmd.body.cancelOrder.orderId);            j.push_back('"');
    } else {
        j.append(R"("order_id":")");               j.append(escape_json(tcmd.body.cancelOrder.orderSysId)); j.push_back('"');
    }
    j.append(R"(,"currency_pair":")");             j.append(info.originInstId);                       j.push_back('"');
#ifdef USE_GATEIO_UNIFIED
    j.append(R"(,"account":"unified")");
#endif
    j.append("}}}");
    return j;
}


// ============================================================================
// pending map
// ============================================================================
void GateioSpotWsTradeUnit::recordPending(int id, WsReqType type,
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

bool GateioSpotWsTradeUnit::takePending(int id, WsPending& out) {
    std::lock_guard<std::mutex> lk(pendingMtx_);
    auto it = pendingMap_.find(id);
    if (it == pendingMap_.end()) return false;
    out = std::move(it->second);
    pendingMap_.erase(it);
    return true;
}

void GateioSpotWsTradeUnit::clearPending() {
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

void GateioSpotWsTradeUnit::gcPendingLocked(int64_t now_ms) {
    if (pendingMap_.size() > kPendingHardMax) {
        LOG_ERROR("TB {} Gate spot pending over hard cap ({}), clearing.",
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
// subWebsocekt
// ============================================================================
void GateioSpotWsTradeUnit::subWebsocekt() {
    std::string restHost = host_of(acc.restUrl);
    initRestClient(restHost, /*headers=*/{}, /*conns=*/4);

    ::net::WsConfig cfg;
    cfg.url                      = acc.wsUrl;   // wss://api.gateio.ws/ws/v4/
    cfg.ping_mode                = ::net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text         = R"({"channel":"spot.ping"})";
    cfg.auto_reconnect           = true;
    cfg.idle_timeout_sec         = 60;
    LOG_INFO("TB {} Gate spot ws {} rest {}", acc.accountId, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));
}


// ============================================================================
// onOpen / onCloseMsg
// ============================================================================
void GateioSpotWsTradeUnit::onOpen() {
    BaseTradeUnit::onOpen();
    wsLoggedIn_.store(false);
    if (!pWsClient) return;

    long ts = crypto::getCurrentTimeSeconds();
    std::string logon = buildLoginJson(ts);
    LOG_INFO("TB {} Gate spot ws send spot.login", acc.accountId);
    pWsClient->send_text(std::move(logon));
}

void GateioSpotWsTradeUnit::onCloseMsg(int code, const std::string& reason) {
    BaseTradeUnit::onCloseMsg(code, reason);
    wsLoggedIn_.store(false);
    clearPending();
}


// ============================================================================
// onWebsocketMsg
// ============================================================================
void GateioSpotWsTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len,
                                             bool /*isBinary*/, int64_t /*recv_ns*/) {
    if (len == 0) return;
    try {
        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc_res = g_parser.iterate(padded);
        if (doc_res.error()) return;
        auto& doc = doc_res.value_unsafe();

        // Gate v4 WS 有两类消息:
        //   1) RPC 响应: {"request_id":"...", "ack":..., "header":{...}, "data":{...}}
        //   2) 订阅推送: {"time":..., "channel":"spot.orders", "event":"update", "result":[...]}
        // 用 "request_id" 存在与否区分。
        std::string_view req_id_sv;
        if (doc.find_field_unordered("request_id").get(req_id_sv) == simdjson::SUCCESS) {
            handleRpcResponse(doc);
            return;
        }
        handleSubUpdate(doc);
    } catch (const std::exception& e) {
        LOG_ERROR("TB {} Gate spot ws msg exc: {}", acc.accountId, e.what());
    }
}


// ============================================================================
// RPC 响应分派
// ============================================================================
void GateioSpotWsTradeUnit::handleRpcResponse(simdjson::ondemand::document& doc) {
    std::string_view req_id_sv;
    doc.find_field_unordered("request_id").get(req_id_sv);
    const int reqId = parse_int_id(req_id_sv);

    bool ack = false;
    doc.find_field_unordered("ack").get(ack);

    // header.status 是字符串 "200" / "400"
    int status = 0;
    simdjson::ondemand::object header;
    if (doc.find_field_unordered("header").get(header) == simdjson::SUCCESS) {
        std::string_view status_sv;
        header.find_field_unordered("status").get(status_sv);
        if (!status_sv.empty()) status = std::atoi(std::string(status_sv).c_str());
    }

    if (reqId == kLoginId) {
        onLoginResponse(ack, status, doc);
        return;
    }
    if (reqId == kOrdersSubId || reqId == kBalancesSubId) {
        if (ack && status == 200) {
            LOG_INFO("TB {} Gate spot subscribe reqId={} OK", acc.accountId, reqId);
        } else {
            LOG_ERROR("TB {} Gate spot subscribe reqId={} FAILED status={}",
                      acc.accountId, reqId, status);
        }
        return;
    }

    WsPending pending;
    if (!takePending(reqId, pending)) {
        LOG_WARN("TB {} Gate spot ws unknown req_id={}", acc.accountId, reqId);
        return;
    }
    if (pending.type == WsReqType::NEW_ORDER) {
        onOrderPlaceResponse(pending, ack, status, doc);
    } else {
        onOrderCancelResponse(pending, ack, status, doc);
    }
}

void GateioSpotWsTradeUnit::onLoginResponse(bool ack, int status,
                                              simdjson::ondemand::document& /*doc*/) {
    if (ack && status == 200) {
        wsLoggedIn_.store(true);
        LOG_INFO("TB {} Gate spot spot.login OK, will subscribe channels", acc.accountId);
        if (!pWsClient) return;
        // 订阅 spot.orders (payload=["!all"] 所有 pair) + spot.balances (payload=[] 所有 ccy)
        pWsClient->send_text(buildSubscribeJson(kOrdersSubId,   "spot.orders",   "!all", nullptr));
        pWsClient->send_text(buildSubscribeJson(kBalancesSubId, "spot.balances", nullptr, nullptr));
    } else {
        wsLoggedIn_.store(false);
        LOG_ERROR("TB {} Gate spot spot.login FAILED ack={} status={}", acc.accountId, ack, status);
    }
}


// ---- order.place 响应: {"data":{"result":{id,text,left,avg_deal_price,status,...}}} ----
void GateioSpotWsTradeUnit::onOrderPlaceResponse(WsPending& pending, bool ack, int status,
                                                    simdjson::ondemand::document& doc) {
    pubsub::RCommand& rcmd = pending.rcmd;
    const md::InstrumentInfo& info = pending.info;
    if (rcmd.body.orderResponse.clientOrderId == TESTCLIENTORDERID) return;

    simdjson::ondemand::object data;
    if (doc.find_field_unordered("data").get(data) != simdjson::SUCCESS) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId     = UnknownError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    if (ack && status == 200) {
        simdjson::ondemand::object result;
        if (data.find_field_unordered("result").get(result) == simdjson::SUCCESS) {
            std::string_view id_sv, left_sv, avg_sv, status_sv;
            result.find_field_unordered("id").get(id_sv);
            result.find_field_unordered("left").get(left_sv);
            result.find_field_unordered("avg_deal_price").get(avg_sv);
            result.find_field_unordered("status").get(status_sv);

            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, id_sv);
            if (!avg_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avg_sv);
            double left = std::fabs(crypto::fast_atod(left_sv));
            rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;

            if      (status_sv == "open")      rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.volumeTraded > ZERO_NUM) ? OS_PARTFILLED : OS_NEW;
            else if (status_sv == "closed")    rcmd.body.orderResponse.orderStatus = OS_FILLED;
            else if (status_sv == "cancelled") rcmd.body.orderResponse.orderStatus = OS_CANCELED;
            else                                rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
        } else {
            rcmd.body.orderResponse.orderStatus = OS_NEW;
        }
        rcmd.body.orderResponse.errorId = NoError;
    } else {
        // errs: {"label":"...", "message":"..."}
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        simdjson::ondemand::object errs;
        if (data.find_field_unordered("errs").get(errs) == simdjson::SUCCESS) {
            std::string_view label_sv, msg_sv;
            errs.find_field_unordered("label").get(label_sv);
            errs.find_field_unordered("message").get(msg_sv);
            // get_gateio_errorid 期望整个 body 字符串; 这里 label 直接映射即可,
            // 若映射不到就 UnknownError。
            std::string label(label_sv);
            rcmd.body.orderResponse.errorId = crypto::get_gateio_errorid(label.c_str());
            if (rcmd.body.orderResponse.errorId == 0) {
                rcmd.body.orderResponse.errorId = UnknownError;
            }
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, msg_sv);
        } else {
            rcmd.body.orderResponse.errorId = UnknownError;
        }
    }
    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
    PUSH_RCMD(rcmd)
}


// ---- order.cancel 响应: 结构与 order.place 类似 ----
void GateioSpotWsTradeUnit::onOrderCancelResponse(WsPending& pending, bool ack, int status,
                                                     simdjson::ondemand::document& doc) {
    pubsub::RCommand& rcmd = pending.rcmd;
    const md::InstrumentInfo& info = pending.info;

    simdjson::ondemand::object data;
    if (doc.find_field_unordered("data").get(data) != simdjson::SUCCESS) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = UnknownError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    if (ack && status == 200) {
        simdjson::ondemand::object result;
        if (data.find_field_unordered("result").get(result) == simdjson::SUCCESS) {
            std::string_view id_sv, amount_sv, left_sv, avg_sv;
            result.find_field_unordered("id").get(id_sv);
            result.find_field_unordered("amount").get(amount_sv);
            result.find_field_unordered("left").get(left_sv);
            result.find_field_unordered("avg_deal_price").get(avg_sv);

            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, id_sv);
            if (!avg_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avg_sv);
            double amount = crypto::fast_atod(amount_sv);
            double left   = std::fabs(crypto::fast_atod(left_sv));
            rcmd.body.orderResponse.volumeTraded = amount - left;
        }
        rcmd.body.orderResponse.orderStatus = OS_CANCELED;
    } else {
        simdjson::ondemand::object errs;
        if (data.find_field_unordered("errs").get(errs) == simdjson::SUCCESS) {
            std::string_view label_sv, msg_sv;
            errs.find_field_unordered("label").get(label_sv);
            errs.find_field_unordered("message").get(msg_sv);
            std::string label(label_sv);
            rcmd.body.orderResponse.errorId = crypto::get_gateio_errorid(label.c_str());
            if (rcmd.body.orderResponse.errorId == OrderNotFoundError) {
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            } else {
                rcmd.body.orderResponse.orderStatus = OS_FAILED;
            }
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, msg_sv);
        } else {
            rcmd.body.orderResponse.orderStatus = OS_FAILED;
            rcmd.body.orderResponse.errorId     = UnknownError;
        }
    }
    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
    PUSH_RCMD(rcmd)
}


// ============================================================================
// 订阅推送 (spot.orders / spot.balances update)
// ============================================================================
void GateioSpotWsTradeUnit::handleSubUpdate(simdjson::ondemand::document& doc) {
    std::string_view ch_sv, ev_sv;
    if (doc.find_field_unordered("channel").get(ch_sv) != simdjson::SUCCESS) return;
    if (ch_sv == "spot.pong") return;
    if (doc.find_field_unordered("event").get(ev_sv) != simdjson::SUCCESS || ev_sv != "update") return;

    simdjson::ondemand::value result;
    if (doc.find_field_unordered("result").get(result) != simdjson::SUCCESS) return;

    if (ch_sv == "spot.orders") {
        handleOrdersUpdate(result);
    } else if (ch_sv == "spot.balances" || ch_sv == "spot.cross_balances") {
        handleBalancesUpdate(result);
    }
}

void GateioSpotWsTradeUnit::handleOrdersUpdate(simdjson::ondemand::value& result) {
    simdjson::ondemand::array arr;
    if (result.get_array().get(arr) != simdjson::SUCCESS) return;

    for (auto it : arr) {
        auto o = it.get_object();
        if (o.error()) continue;

        std::string_view pair_sv, id_sv, text_sv, price_sv, amount_sv, side_sv, tif_sv, left_sv, avg_sv, event_sv;
        o["currency_pair"].get(pair_sv);
        o["id"].get(id_sv);
        o["text"].get(text_sv);
        o["price"].get(price_sv);
        o["amount"].get(amount_sv);
        o["side"].get(side_sv);
        o["time_in_force"].get(tif_sv);
        o["left"].get(left_sv);
        o["avg_deal_price"].get(avg_sv);
        o["event"].get(event_sv);

        std::string originInstId(pair_sv);
        md::InstrumentInfo info;
        if (!smc->get_instrument_info(GATEIO, SPOT, originInstId.c_str(), info)) {
            LOG_ERROR("TB {} GATEIO.SPOT smc miss: {}", acc.accountId, originInstId);
            continue;
        }

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
        rcmd.body.orderResponse.exchangeTypeEnum = GATEIO;
        rcmd.body.orderResponse.instTypeEnum     = SPOT;
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.instId,     std::string_view(info.instId));
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId,    id_sv);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, text_sv);

        rcmd.body.orderResponse.limitPrice  = crypto::fast_atod(price_sv)  * info.reduceNumber;
        rcmd.body.orderResponse.volumeTotal = crypto::fast_atod(amount_sv) * info.magnifyNumber;
        rcmd.body.orderResponse.offsetFlag  = OF_OPEN;
        rcmd.body.orderResponse.direction   = (!side_sv.empty() && side_sv[0] == 's') ? DT_SHORT : DT_LONG;

        if (!tif_sv.empty()) {
            switch (tif_sv[0]) {
                case 'g': rcmd.body.orderResponse.orderType = OT_LIMIT;      break;
                case 'i': rcmd.body.orderResponse.orderType = OT_IOC;        break;
                case 'p': rcmd.body.orderResponse.orderType = OT_POST_ONLY;  break;
                case 'f': rcmd.body.orderResponse.orderType = OT_FOK;        break;
                default:  break;
            }
        }
        double left = std::fabs(crypto::fast_atod(left_sv));
        rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
        if (!avg_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avg_sv);

        if (!event_sv.empty()) {
            switch (event_sv[0]) {
                case 'p': rcmd.body.orderResponse.orderStatus = OS_NEW;         break;
                case 'u': rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;  break;
                case 'f':
                    rcmd.body.orderResponse.orderStatus =
                        (rcmd.body.orderResponse.volumeTotal - rcmd.body.orderResponse.volumeTraded < ZERO_NUM)
                            ? OS_FILLED : OS_CANCELED;
                    break;
                default: rcmd.body.orderResponse.orderStatus = OS_UNKNOWN; break;
            }
        }
        rcmd.body.orderResponse.updateTime    = crypto::getCurrentTime();
        rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd)
    }
}

void GateioSpotWsTradeUnit::handleBalancesUpdate(simdjson::ondemand::value& result) {
    simdjson::ondemand::array arr;
    if (result.get_array().get(arr) != simdjson::SUCCESS) return;

    std::vector<pubsub::RCommand> pending;
    for (auto it : arr) {
        auto o = it.get_object();
        if (o.error()) continue;
        std::string_view cur_sv, avail_sv, total_sv;
        o["currency"].get(cur_sv);
        o["available"].get(avail_sv);
        o["total"].get(total_sv);

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
        rcmd.body.balance.exchangeTypeEnum = GATEIO;
        rcmd.body.balance.instTypeEnum     = SPOT;
        crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.balance.currency,   crypto::to_upper(std::string(cur_sv)));
        rcmd.body.balance.available = crypto::fast_atod(avail_sv);
        rcmd.body.balance.total     = crypto::fast_atod(total_sv);
        rcmd.body.balance.updateTime = crypto::getCurrentTime();
        rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;
        pending.emplace_back(rcmd);
    }
    for (size_t i = 0; i < pending.size(); ++i) {
        pending[i].body.balance.isLast = (i + 1 == pending.size());
        PUSH_RCMD(pending[i])
    }
}


// ============================================================================
// query_* : REST (HMAC-SHA512 签名)
// ============================================================================
void GateioSpotWsTradeUnit::query_account (const pubsub::TCommand&) {}   // 走 query_balance
void GateioSpotWsTradeUnit::query_position(const pubsub::TCommand&) {}   // Spot 无

void GateioSpotWsTradeUnit::query_balance(const pubsub::TCommand&) {
    if (!pRestClient) return;
    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    auto headers = gateAuthHeaders("GET", balanceUrl, "", "", time_str);

    asyncRequest(boost::beast::http::verb::get, balanceUrl, "", "", std::move(headers),
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) { LOG_ERROR("TB {} Gate spot query_balance ec: {}", acc.accountId, ec.message()); return; }
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;
                simdjson::ondemand::array arr;
                if (doc.get_array().get(arr) != simdjson::SUCCESS) return;
                std::vector<pubsub::RCommand> pending;
                for (auto it : arr) {
                    auto o = it.get_object();
                    if (o.error()) continue;
                    std::string_view cur_sv, avail_sv, lock_sv;
                    o["currency"].get(cur_sv);
                    o["available"].get(avail_sv);
                    o["locked"].get(lock_sv);
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = GATEIO;
                    rcmd.body.balance.instTypeEnum     = SPOT;
                    crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.currency,   crypto::to_upper(std::string(cur_sv)));
                    rcmd.body.balance.available = crypto::fast_atod(avail_sv);
                    rcmd.body.balance.frozen    = crypto::fast_atod(lock_sv);
                    rcmd.body.balance.total     = rcmd.body.balance.available + rcmd.body.balance.frozen;
                    rcmd.body.balance.updateTime = crypto::getCurrentTime();
                    rcmd.body.balance.apiSourceEnum = AS_REST;
                    pending.emplace_back(rcmd);
                }
                for (size_t i = 0; i < pending.size(); ++i) {
                    pending[i].body.balance.isLast = (i + 1 == pending.size());
                    PUSH_RCMD(pending[i]);
                }
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} Gate spot query_balance cb exc: {}", acc.accountId, e.what());
            }
        });
}

void GateioSpotWsTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);
    if (!pRestClient) return;
    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum,
                                  tcmd.body.queryOrder.instTypeEnum,
                                  tcmd.body.queryOrder.instId, info)) return;

    std::string idSeg;
    if (!crypto::str_cmp(tcmd.body.queryOrder.orderId, "")) {
        idSeg = tcmd.body.queryOrder.orderId;
    } else if (!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")) {
        idSeg = tcmd.body.queryOrder.orderSysId;
    } else return;

    std::string pathBase = queryOrderUrl + "/" + idSeg;
    std::string queryStr = "currency_pair=" + std::string(info.originInstId);
#ifdef USE_GATEIO_UNIFIED
    queryStr += "&account=unified";
#endif
    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    auto headers = gateAuthHeaders("GET", pathBase, queryStr, "", time_str);
    std::string fullPath = pathBase + "?" + queryStr;
    auto info_captured = info;

    asyncRequest(boost::beast::http::verb::get, std::move(fullPath), "", "", std::move(headers),
        [this, rcmd, info_captured](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
            if (ec) return;
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;

                std::string_view status_sv;
                if (doc.find_field_unordered("status").get(status_sv) != simdjson::SUCCESS) {
                    rcmd.body.orderResponse.errorId = crypto::get_gateio_errorid(resp.body.c_str());
                    rcmd.body.orderResponse.orderStatus =
                        (rcmd.body.orderResponse.errorId == OrderNotFoundError) ? OS_REJECTED : OS_UNKNOWN;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;
                }
                std::string_view id_sv, text_sv, amount_sv, price_sv, left_sv, avg_sv, finishAs_sv;
                doc.find_field_unordered("id").get(id_sv);
                doc.find_field_unordered("text").get(text_sv);
                doc.find_field_unordered("amount").get(amount_sv);
                doc.find_field_unordered("price").get(price_sv);
                doc.find_field_unordered("left").get(left_sv);
                doc.find_field_unordered("avg_deal_price").get(avg_sv);
                doc.find_field_unordered("finish_as").get(finishAs_sv);

                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId,    id_sv);
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, text_sv);
                rcmd.body.orderResponse.volumeTotal = crypto::fast_atod(amount_sv);
                rcmd.body.orderResponse.limitPrice  = crypto::fast_atod(price_sv);
                double left = std::fabs(crypto::fast_atod(left_sv));
                rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
                if (!avg_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avg_sv);

                std::string st(status_sv);
                if      (st == "open")      rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.volumeTotal > rcmd.body.orderResponse.volumeTraded && rcmd.body.orderResponse.volumeTraded > ZERO_NUM) ? OS_PARTFILLED : OS_NEW;
                else if (st == "closed")    rcmd.body.orderResponse.orderStatus = OS_FILLED;
                else if (st == "cancelled") rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                else if (finishAs_sv == "filled") rcmd.body.orderResponse.orderStatus = OS_FILLED;
                else                         rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} Gate spot query_order cb exc: {}", acc.accountId, e.what());
            }
        });
}


// ============================================================================
// add_new_order (WS spot.order_place, 无 REST 兜底)
// ============================================================================
void GateioSpotWsTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load() || !wsLoggedIn_.load() || !pWsClient) {
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

    const char* side = nullptr;
    if (tcmd.body.newOrder.offsetFlag == OF_OPEN) {
        if      (tcmd.body.newOrder.direction == DT_LONG)  side = "buy";
        else if (tcmd.body.newOrder.direction == DT_SHORT) side = "sell";
    } else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
        if      (tcmd.body.newOrder.direction == DT_LONG)  side = "sell";
        else if (tcmd.body.newOrder.direction == DT_SHORT) side = "buy";
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

    const char* tif = nullptr;
    bool priceZero  = false;
    switch (tcmd.body.newOrder.orderType) {
        case OT_LIMIT:     tif = "gtc"; break;
        case OT_MARKET:    tif = "ioc"; priceZero = true; break;
        case OT_POST_ONLY: tif = "poc"; break;
        case OT_FOK:       tif = "fok"; break;
        case OT_IOC:       tif = "ioc"; break;
        default:
            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            rcmd.body.orderResponse.errorId     = OrderTypeError;
            rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
            return;
    }

    double price  = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice  * info.magnifyNumber, info.tickSize);
    double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber,  info.lotSize);
    std::string price_str  = priceZero ? "0" : fmt::format("{}", price);
    std::string amount_str = fmt::format("{}", volume);

    const int wsId = nextWsId_.fetch_add(1, std::memory_order_relaxed);
    std::string msg = buildOrderPlaceJson(wsId, tcmd, info, price_str, amount_str, side, tif);

    recordPending(wsId, WsReqType::NEW_ORDER, rcmd, info);
    LOG_INFO("TB {} Gate spot ws order.place id={} msg={}", acc.accountId, wsId, msg);
    pWsClient->send_text(std::move(msg));
}


// ============================================================================
// cancel_order (WS spot.order_cancel)
// ============================================================================
void GateioSpotWsTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load() || !wsLoggedIn_.load() || !pWsClient) {
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
    if (crypto::str_cmp(tcmd.body.cancelOrder.orderId, "") &&
        crypto::str_cmp(tcmd.body.cancelOrder.orderSysId, "")) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = OrderIdError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const int wsId = nextWsId_.fetch_add(1, std::memory_order_relaxed);
    std::string msg = buildOrderCancelJson(wsId, tcmd, info);

    recordPending(wsId, WsReqType::CANCEL_ORDER, rcmd, info);
    LOG_INFO("TB {} Gate spot ws order.cancel id={} msg={}", acc.accountId, wsId, msg);
    pWsClient->send_text(std::move(msg));
}