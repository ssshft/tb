#include "Gateio/GateioUsWsTrade.h"

#include <cmath>
#include <cstdlib>

#include <fmt/format.h>
#include <simdjson.h>


namespace {
    inline std::string host_of(const std::string& url) {
        std::string h = url;
        auto p = h.find("://");
        if (p != std::string::npos) h = h.substr(p + 3);
        auto q = h.find('/');
        if (q != std::string::npos) h = h.substr(0, q);
        return h;
    }


    constexpr int64_t kPendingTtlMs   = 30 * 1000;
    constexpr size_t  kPendingHardMax = 10000;
    constexpr int64_t kGcIntervalMs   = 5000;

    inline std::string escape_json(std::string_view s) {
        std::string out;
        out.reserve(s.size() + 4);
        for (char c : s) {
            switch (c) {
                case '"':  out += "\\\""; break;
                case '\\': out += "\\\\"; break;
                case '\n': out += "\\n";  break;
                case '\r': out += "\\r";  break;
                case '\t': out += "\\t";  break;
                default:
                    if (static_cast<unsigned char>(c) < 0x20) {
                        char buf[8]; std::snprintf(buf, sizeof(buf), "\\u%04x", c); out += buf;
                    } else { out += c; }
            }
        }
        return out;
    }
    inline int parse_int_id(std::string_view sv) {
        if (sv.empty()) return 0;
        return std::atoi(std::string(sv).c_str());
    }
}


GateioUsWsTradeUnit::GateioUsWsTradeUnit(AccountCfg& a, sm::SecurityManager* s)
    : BaseTradeUnit(a, s) {}
GateioUsWsTradeUnit::~GateioUsWsTradeUnit() = default;


int GateioUsWsTradeUnit::mapAdlRanking(int r) {
    if (r >= 5) return 1;
    if (r == 4) return 1;
    if (r == 3) return 3;
    if (r == 2) return 4;
    if (r == 1) return 4;
    return 1;
}


// ============================================================================
// REST auth
// ============================================================================
std::vector<std::pair<std::string, std::string>>
GateioUsWsTradeUnit::gateAuthHeaders(const std::string& method, const std::string& path,
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
std::string GateioUsWsTradeUnit::buildLoginJson(long ts) const {
    std::string time_str = std::to_string(ts);
    std::string sign = crypto::getGateioSignatureWs("futures.login", "api", time_str, acc.secretKey);
    return fmt::format(
        R"({{"time":{},"channel":"futures.login","event":"api","payload":{{)"
        R"("req_id":"{}","req_header":{{}},)"
        R"("api_key":"{}","signature":"{}","timestamp":"{}"}}}})",
        ts, kLoginId, escape_json(acc.apiKey), sign, time_str);
}

std::string GateioUsWsTradeUnit::buildSubscribeJson(int reqId, const char* channel) const {
    long ts = crypto::getCurrentTimeSeconds();
    // futures 订阅 payload=[userId, "!all"] (login 后 userId 可以省, 但 Gate 服务器还是需要一个 payload)
    std::string j;
    j.reserve(180);
    j.append(R"({"time":)");     j.append(std::to_string(ts));
    j.append(R"(,"channel":")"); j.append(channel);                                     j.push_back('"');
    j.append(R"(,"event":"subscribe","payload":[")"); j.append(acc.userId);            j.append(R"(","!all"])");
    j.append(R"(,"req_id":")");  j.append(std::to_string(reqId));                       j.push_back('"');
    j.push_back('}');
    return j;
}

std::string GateioUsWsTradeUnit::buildOrderPlaceJson(
    int reqId, const pubsub::TCommand& tcmd, const md::InstrumentInfo& info,
    const std::string& price, double sizeSigned, const char* tif) const
{
    long ts = crypto::getCurrentTimeSeconds();
    std::string size_str = fmt::format("{}", sizeSigned);
    std::string j;
    j.reserve(400);
    j.append(R"({"time":)"); j.append(std::to_string(ts));
    j.append(R"(,"channel":"futures.order_place","event":"api","payload":{)");
    j.append(R"("req_id":")");                     j.append(std::to_string(reqId));                    j.push_back('"');
    j.append(R"(,"req_header":{},"req_param":{)");
    j.append(R"("text":")");                       j.append(escape_json(tcmd.body.newOrder.orderSysId));  j.push_back('"');
    j.append(R"(,"contract":")");                  j.append(info.originInstId);                       j.push_back('"');
    j.append(R"(,"size":)");                       j.append(size_str);                                // 有符号数, 不加引号
    j.append(R"(,"price":")");                     j.append(price);                                   j.push_back('"');
    j.append(R"(,"tif":")");                       j.append(tif);                                     j.push_back('"');
    j.append(R"(,"reduce_only":)");                j.append(tcmd.body.newOrder.reduceOnly ? "true" : "false");
    j.append("}}}");
    return j;
}

std::string GateioUsWsTradeUnit::buildOrderCancelJson(
    int reqId, const pubsub::TCommand& tcmd, const md::InstrumentInfo& info) const
{
    long ts = crypto::getCurrentTimeSeconds();
    std::string j;
    j.reserve(240);
    j.append(R"({"time":)"); j.append(std::to_string(ts));
    j.append(R"(,"channel":"futures.order_cancel","event":"api","payload":{)");
    j.append(R"("req_id":")");                     j.append(std::to_string(reqId));                    j.push_back('"');
    j.append(R"(,"req_header":{},"req_param":{)");
    if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
        j.append(R"("order_id":")");               j.append(tcmd.body.cancelOrder.orderId);            j.push_back('"');
    } else {
        j.append(R"("order_id":")");               j.append(escape_json(tcmd.body.cancelOrder.orderSysId));  j.push_back('"');
    }
    // futures 的 order_cancel 不需要 contract, 但为了保险还是带上
    j.append(R"(,"contract":")");                  j.append(info.originInstId);                       j.push_back('"');
    j.append("}}}");
    return j;
}


// ============================================================================
// pending map
// ============================================================================
void GateioUsWsTradeUnit::recordPending(int id, WsReqType type,
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

bool GateioUsWsTradeUnit::takePending(int id, WsPending& out) {
    std::lock_guard<std::mutex> lk(pendingMtx_);
    auto it = pendingMap_.find(id);
    if (it == pendingMap_.end()) return false;
    out = std::move(it->second);
    pendingMap_.erase(it);
    return true;
}

void GateioUsWsTradeUnit::clearPending() {
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

void GateioUsWsTradeUnit::gcPendingLocked(int64_t now_ms) {
    if (pendingMap_.size() > kPendingHardMax) {
        LOG_ERROR("TB {} Gate US pending over hard cap ({}), clearing.",
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
// subWebsocekt / onOpen / onCloseMsg
// ============================================================================
void GateioUsWsTradeUnit::subWebsocekt() {
    std::string restHost = host_of(acc.restUrl);
    initRestClient(restHost, /*headers=*/{}, /*conns=*/4);

    ::net::WsConfig cfg;
    cfg.url                      = acc.wsUrl;   // wss://fx-ws.gateio.ws/v4/ws/usdt/
    cfg.ping_mode                = ::net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text         = R"({"channel":"futures.ping"})";
    cfg.auto_reconnect           = true;
    cfg.idle_timeout_sec         = 60;
    LOG_INFO("TB {} Gate US ws {} rest {}", acc.accountId, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));
}

void GateioUsWsTradeUnit::onOpen() {
    BaseTradeUnit::onOpen();
    wsLoggedIn_.store(false);
    if (!pWsClient) return;
    long ts = crypto::getCurrentTimeSeconds();
    LOG_INFO("TB {} Gate US ws send futures.login", acc.accountId);
    pWsClient->send_text(buildLoginJson(ts));
}

void GateioUsWsTradeUnit::onCloseMsg(int code, const std::string& reason) {
    BaseTradeUnit::onCloseMsg(code, reason);
    wsLoggedIn_.store(false);
    clearPending();
}


// ============================================================================
// onWebsocketMsg
// ============================================================================
void GateioUsWsTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len,
                                           bool /*isBinary*/, int64_t /*recv_ns*/) {
    if (len == 0) return;
    try {
        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc = g_parser.iterate(padded);
        if (doc.error()) return;

        std::string_view req_id_sv;
        if (doc.find_field_unordered("request_id").get(req_id_sv) == simdjson::SUCCESS) {
            handleRpcResponse(doc);
            return;
        }
        handleSubUpdate(doc);
    } catch (const std::exception& e) {
        LOG_ERROR("TB {} Gate US ws msg exc: {}", acc.accountId, e.what());
    }
}


// ============================================================================
// RPC 响应分派
// ============================================================================
void GateioUsWsTradeUnit::handleRpcResponse(simdjson::ondemand::document& doc) {
    std::string_view req_id_sv;
    doc.find_field_unordered("request_id").get(req_id_sv);
    const int reqId = parse_int_id(req_id_sv);

    bool ack = false;
    doc.find_field_unordered("ack").get(ack);

    int status = 0;
    simdjson::ondemand::object header;
    if (doc.find_field_unordered("header").get(header) == simdjson::SUCCESS) {
        std::string_view status_sv;
        header.find_field_unordered("status").get(status_sv);
        if (!status_sv.empty()) status = std::atoi(std::string(status_sv).c_str());
    }

    if (reqId == kLoginId) { onLoginResponse(ack, status, doc); return; }
    if (reqId == kOrdersSubId || reqId == kBalancesSubId || reqId == kPositionsSubId) {
        if (ack && status == 200) LOG_INFO("TB {} Gate US subscribe reqId={} OK", acc.accountId, reqId);
        else                       LOG_ERROR("TB {} Gate US subscribe reqId={} FAILED status={}", acc.accountId, reqId, status);
        return;
    }

    WsPending pending;
    if (!takePending(reqId, pending)) {
        LOG_WARN("TB {} Gate US ws unknown req_id={}", acc.accountId, reqId);
        return;
    }
    if (pending.type == WsReqType::NEW_ORDER) onOrderPlaceResponse(pending, ack, status, doc);
    else                                       onOrderCancelResponse(pending, ack, status, doc);
}

void GateioUsWsTradeUnit::onLoginResponse(bool ack, int status,
                                            simdjson::ondemand::document& /*doc*/) {
    if (ack && status == 200) {
        wsLoggedIn_.store(true);
        LOG_INFO("TB {} Gate US futures.login OK", acc.accountId);
        if (!pWsClient) return;
        pWsClient->send_text(buildSubscribeJson(kOrdersSubId,    "futures.orders"));
        pWsClient->send_text(buildSubscribeJson(kBalancesSubId,  "futures.balances"));
        pWsClient->send_text(buildSubscribeJson(kPositionsSubId, "futures.positions"));
    } else {
        wsLoggedIn_.store(false);
        LOG_ERROR("TB {} Gate US futures.login FAILED ack={} status={}", acc.accountId, ack, status);
    }
}


// ---- order.place / cancel 响应 ----
// data.result 里 futures 的字段: id, text, size, price, fill_price, left, status, finish_as ...
void GateioUsWsTradeUnit::onOrderPlaceResponse(WsPending& pending, bool ack, int status,
                                                 simdjson::ondemand::document& doc) {
    pubsub::RCommand& rcmd = pending.rcmd;
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
            std::string_view id_sv, size_sv, left_sv, fill_sv, status_sv, finish_sv;
            result.find_field_unordered("id").get(id_sv);
            result.find_field_unordered("size").get(size_sv);
            result.find_field_unordered("left").get(left_sv);
            result.find_field_unordered("fill_price").get(fill_sv);
            result.find_field_unordered("status").get(status_sv);
            result.find_field_unordered("finish_as").get(finish_sv);

            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, id_sv);
            if (!fill_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(fill_sv);
            double left = std::fabs(crypto::fast_atod(left_sv));
            rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;

            if      (status_sv == "open")           rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.volumeTraded > ZERO_NUM) ? OS_PARTFILLED : OS_NEW;
            else if (finish_sv == "filled")         rcmd.body.orderResponse.orderStatus = OS_FILLED;
            else if (finish_sv == "cancelled" || finish_sv == "ioc" ||
                     finish_sv == "liquidated" || finish_sv == "auto_deleveraged" ||
                     finish_sv == "reduce_only")   rcmd.body.orderResponse.orderStatus = OS_CANCELED;
            else                                    rcmd.body.orderResponse.orderStatus = OS_NEW;
        } else {
            rcmd.body.orderResponse.orderStatus = OS_NEW;
        }
        rcmd.body.orderResponse.errorId = NoError;
    } else {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        simdjson::ondemand::object errs;
        if (data.find_field_unordered("errs").get(errs) == simdjson::SUCCESS) {
            std::string_view label_sv, msg_sv;
            errs.find_field_unordered("label").get(label_sv);
            errs.find_field_unordered("message").get(msg_sv);
            std::string label(label_sv);
            rcmd.body.orderResponse.errorId = crypto::get_gateio_errorid(label.c_str());
            if (rcmd.body.orderResponse.errorId == 0) rcmd.body.orderResponse.errorId = UnknownError;
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, msg_sv);
        } else {
            rcmd.body.orderResponse.errorId = UnknownError;
        }
    }
    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
    PUSH_RCMD(rcmd)
}

void GateioUsWsTradeUnit::onOrderCancelResponse(WsPending& pending, bool ack, int status,
                                                  simdjson::ondemand::document& doc) {
    pubsub::RCommand& rcmd = pending.rcmd;

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
            std::string_view id_sv, size_sv, left_sv, fill_sv;
            result.find_field_unordered("id").get(id_sv);
            result.find_field_unordered("size").get(size_sv);
            result.find_field_unordered("left").get(left_sv);
            result.find_field_unordered("fill_price").get(fill_sv);

            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, id_sv);
            if (!fill_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(fill_sv);
            double sz   = std::fabs(crypto::fast_atod(size_sv));
            double left = std::fabs(crypto::fast_atod(left_sv));
            rcmd.body.orderResponse.volumeTraded = sz - left;
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
// 订阅推送
// ============================================================================
void GateioUsWsTradeUnit::handleSubUpdate(simdjson::ondemand::document& doc) {
    std::string_view ch_sv, ev_sv;
    if (doc.find_field_unordered("channel").get(ch_sv) != simdjson::SUCCESS) return;
    if (ch_sv == "futures.pong") return;
    if (doc.find_field_unordered("event").get(ev_sv) != simdjson::SUCCESS || ev_sv != "update") return;

    simdjson::ondemand::value result;
    if (doc.find_field_unordered("result").get(result) != simdjson::SUCCESS) return;

    if      (ch_sv == "futures.orders")    handleOrdersUpdate(result);
    else if (ch_sv == "futures.balances")  handleBalancesUpdate(result);
    else if (ch_sv == "futures.positions") handlePositionsUpdate(result);
}

void GateioUsWsTradeUnit::handleOrdersUpdate(simdjson::ondemand::value& result) {
    simdjson::ondemand::array arr;
    if (result.get_array().get(arr) != simdjson::SUCCESS) return;

    for (auto it : arr) {
        auto o = it.get_object();
        if (o.error()) continue;

        std::string_view contract_sv, id_sv, text_sv, price_sv, tif_sv, size_sv, left_sv, fill_sv,
                          status_sv, finish_sv;
        bool isClose = false;
        if (o["contract"].get(contract_sv) != simdjson::SUCCESS) continue;
        o["id"].get(id_sv);
        o["text"].get(text_sv);
        o["price"].get(price_sv);
        o["tif"].get(tif_sv);
        o["size"].get(size_sv);
        o["left"].get(left_sv);
        o["fill_price"].get(fill_sv);
        o["status"].get(status_sv);
        o["finish_as"].get(finish_sv);
        bool ic = false;
        if (o["is_close"].get(ic) == simdjson::SUCCESS) isClose = ic;

        std::string originInstId(contract_sv);
        md::InstrumentInfo info;
        if (!smc->get_instrument_info(GATEIO, USDT_SWAP, originInstId.c_str(), info)) continue;

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
        rcmd.body.orderResponse.exchangeTypeEnum = GATEIO;
        rcmd.body.orderResponse.instTypeEnum     = USDT_SWAP;
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.instId,     std::string_view(info.instId));
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId,    id_sv);

        if (!text_sv.empty()) {
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, text_sv);
            if      (text_sv == "auto_deleveraging") rcmd.body.orderResponse.errorId = ADLError;
            else if (text_sv == "liquidation")       rcmd.body.orderResponse.errorId = LiquidationError;
        }
        rcmd.body.orderResponse.offsetFlag = isClose ? OF_CLOSE : OF_OPEN;

        if (!size_sv.empty()) {
            double sz = crypto::fast_atod(size_sv);
            rcmd.body.orderResponse.direction   = sz > 0 ? DT_LONG : DT_SHORT;
            rcmd.body.orderResponse.volumeTotal = std::fabs(sz);
        }
        if (!price_sv.empty()) rcmd.body.orderResponse.limitPrice = crypto::fast_atod(price_sv);
        if (!tif_sv.empty()) {
            switch (tif_sv[0]) {
                case 'g': rcmd.body.orderResponse.orderType = OT_LIMIT;     break;
                case 'i': rcmd.body.orderResponse.orderType = OT_IOC;       break;
                case 'p': rcmd.body.orderResponse.orderType = OT_POST_ONLY; break;
                case 'f': rcmd.body.orderResponse.orderType = OT_FOK;       break;
                default: break;
            }
        }
        if (!left_sv.empty()) {
            double left = std::fabs(crypto::fast_atod(left_sv));
            rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
        }
        if (!fill_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(fill_sv);

        if      (status_sv == "open")           rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.volumeTraded > ZERO_NUM) ? OS_PARTFILLED : OS_NEW;
        else if (finish_sv == "filled")         rcmd.body.orderResponse.orderStatus = OS_FILLED;
        else if (finish_sv == "cancelled" || finish_sv == "liquidated" ||
                 finish_sv == "ioc"       || finish_sv == "auto_deleveraged" ||
                 finish_sv == "reduce_only")   rcmd.body.orderResponse.orderStatus = OS_CANCELED;
        else                                    rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;

        rcmd.body.orderResponse.updateTime    = crypto::getCurrentTime();
        rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd)
    }
}

void GateioUsWsTradeUnit::handleBalancesUpdate(simdjson::ondemand::value& result) {
    simdjson::ondemand::array arr;
    if (result.get_array().get(arr) != simdjson::SUCCESS) return;

    std::vector<pubsub::RCommand> pending;
    for (auto it : arr) {
        auto o = it.get_object();
        if (o.error()) continue;
        std::string_view cur_sv, text_sv, bal_sv;
        o["currency"].get(cur_sv);
        o["text"].get(text_sv);
        o["balance"].get(bal_sv);

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
        rcmd.body.balance.exchangeTypeEnum = GATEIO;
        rcmd.body.balance.instTypeEnum     = SPOT;   // 老 UFTrade 走 SPOT, 保持一致
        crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
        if (!cur_sv.empty()) {
            crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(cur_sv)));
        } else if (crypto::has_str(std::string(text_sv).c_str(), "USDT")) {
            crypto::copy_sv_to_char_array(rcmd.body.balance.currency, std::string("USDT"));
        } else continue;
        if (!bal_sv.empty()) rcmd.body.balance.total = crypto::fast_atod(bal_sv);
        rcmd.body.balance.updateTime    = crypto::getCurrentTime();
        rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;
        pending.emplace_back(rcmd);
    }
    for (size_t i = 0; i < pending.size(); ++i) {
        pending[i].body.balance.isLast = (i + 1 == pending.size());
        PUSH_RCMD(pending[i])
    }
}

void GateioUsWsTradeUnit::handlePositionsUpdate(simdjson::ondemand::value& result) {
    simdjson::ondemand::array arr;
    if (result.get_array().get(arr) != simdjson::SUCCESS) return;

    std::vector<pubsub::RCommand> pending;
    for (auto it : arr) {
        auto o = it.get_object();
        if (o.error()) continue;
        std::string_view contract_sv, size_sv, margin_sv, entry_sv, up_sv, mark_sv, liq_sv, adl_sv;
        if (o["contract"].get(contract_sv) != simdjson::SUCCESS) continue;
        o["size"].get(size_sv);
        o["margin"].get(margin_sv);
        o["entry_price"].get(entry_sv);
        o["unrealised_pnl"].get(up_sv);
        o["mark_price"].get(mark_sv);
        o["liq_price"].get(liq_sv);
        o["adl_ranking"].get(adl_sv);

        std::string originInstId(contract_sv);
        md::InstrumentInfo info;
        if (!smc->get_instrument_info(GATEIO, USDT_SWAP, originInstId.c_str(), info)) continue;

        double sz = crypto::fast_atod(size_sv);
        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
        rcmd.body.position.exchangeTypeEnum = GATEIO;
        rcmd.body.position.instTypeEnum     = USDT_SWAP;
        crypto::copy_sv_to_char_array(rcmd.body.position.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.position.instId,     std::string_view(info.instId));
        rcmd.body.position.direction     = sz >= 0 ? DT_LONG : DT_SHORT;
        rcmd.body.position.volume        = std::fabs(sz) * info.magnifyNumber;
        rcmd.body.position.maintMargin   = crypto::fast_atod(margin_sv);
        rcmd.body.position.avgPrice      = crypto::fast_atod(entry_sv) * info.reduceNumber;
        rcmd.body.position.unrealizedPnl = crypto::fast_atod(up_sv);
        rcmd.body.position.markPrice     = crypto::fast_atod(mark_sv) * info.reduceNumber;
        rcmd.body.position.liquidPrice   = crypto::fast_atod(liq_sv)  * info.reduceNumber;
        rcmd.body.position.adlQuantile   = mapAdlRanking(static_cast<int>(crypto::fast_atod(adl_sv)));
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
// query_* (REST)
// ============================================================================
void GateioUsWsTradeUnit::query_account(const pubsub::TCommand&) {}

void GateioUsWsTradeUnit::query_balance(const pubsub::TCommand&) {
    if (!pRestClient) return;
    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    auto headers = gateAuthHeaders("GET", balanceUrl, "", "", time_str);
    asyncRequest(boost::beast::http::verb::get, balanceUrl, "", "", std::move(headers),
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) { LOG_ERROR("TB {} Gate US query_balance ec: {}", acc.accountId, ec.message()); return; }
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
                    std::string_view cur_sv, avail_sv, om_sv, pm_sv, tot_sv;
                    o["currency"].get(cur_sv);
                    o["available"].get(avail_sv);
                    o["order_margin"].get(om_sv);
                    o["position_margin"].get(pm_sv);
                    o["total"].get(tot_sv);
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = GATEIO;
                    rcmd.body.balance.instTypeEnum     = SPOT;
                    crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.currency,   crypto::to_upper(std::string(cur_sv)));
                    rcmd.body.balance.available = crypto::fast_atod(avail_sv);
                    rcmd.body.balance.frozen    = crypto::fast_atod(om_sv) + crypto::fast_atod(pm_sv);
                    rcmd.body.balance.total     = crypto::fast_atod(tot_sv);
                    rcmd.body.balance.updateTime = crypto::getCurrentTime();
                    rcmd.body.balance.apiSourceEnum = AS_REST;
                    pending.emplace_back(rcmd);
                }
                for (size_t i = 0; i < pending.size(); ++i) {
                    pending[i].body.balance.isLast = (i + 1 == pending.size());
                    PUSH_RCMD(pending[i]);
                }
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} Gate US query_balance cb exc: {}", acc.accountId, e.what());
            }
        });
}

void GateioUsWsTradeUnit::query_position(const pubsub::TCommand&) {
    if (!pRestClient) return;
    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    auto headers = gateAuthHeaders("GET", positionUrl, "", "", time_str);
    asyncRequest(boost::beast::http::verb::get, positionUrl, "", "", std::move(headers),
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) { LOG_ERROR("TB {} Gate US query_position ec: {}", acc.accountId, ec.message()); return; }
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
                    std::string_view contract_sv, size_sv, margin_sv, entry_sv, up_sv, mark_sv, liq_sv, adl_sv;
                    if (o["contract"].get(contract_sv) != simdjson::SUCCESS) continue;
                    o["size"].get(size_sv);
                    o["margin"].get(margin_sv);
                    o["entry_price"].get(entry_sv);
                    o["unrealised_pnl"].get(up_sv);
                    o["mark_price"].get(mark_sv);
                    o["liq_price"].get(liq_sv);
                    o["adl_ranking"].get(adl_sv);

                    std::string originInstId(contract_sv);
                    md::InstrumentInfo info;
                    if (!smc->get_instrument_info(GATEIO, USDT_SWAP, originInstId.c_str(), info)) continue;

                    double sz = crypto::fast_atod(size_sv);
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                    rcmd.body.position.exchangeTypeEnum = GATEIO;
                    rcmd.body.position.instTypeEnum     = USDT_SWAP;
                    crypto::copy_sv_to_char_array(rcmd.body.position.accountId,  acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.instId,     std::string_view(info.instId));
                    rcmd.body.position.direction     = sz >= 0 ? DT_LONG : DT_SHORT;
                    rcmd.body.position.volume        = std::fabs(sz) * info.magnifyNumber;
                    rcmd.body.position.maintMargin   = crypto::fast_atod(margin_sv);
                    rcmd.body.position.avgPrice      = crypto::fast_atod(entry_sv) * info.reduceNumber;
                    rcmd.body.position.unrealizedPnl = crypto::fast_atod(up_sv);
                    rcmd.body.position.markPrice     = crypto::fast_atod(mark_sv) * info.reduceNumber;
                    rcmd.body.position.liquidPrice   = crypto::fast_atod(liq_sv)  * info.reduceNumber;
                    rcmd.body.position.adlQuantile   = mapAdlRanking(static_cast<int>(crypto::fast_atod(adl_sv)));
                    rcmd.body.position.updateTime    = crypto::getCurrentTime();
                    rcmd.body.position.apiSourceEnum = AS_REST;
                    pending.emplace_back(rcmd);
                }
                for (size_t i = 0; i < pending.size(); ++i) {
                    pending[i].body.position.isLast = (i + 1 == pending.size());
                    PUSH_RCMD(pending[i]);
                }
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} Gate US query_position cb exc: {}", acc.accountId, e.what());
            }
        });
}

void GateioUsWsTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);
    if (!pRestClient) return;
    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum,
                                  tcmd.body.queryOrder.instTypeEnum,
                                  tcmd.body.queryOrder.instId, info)) return;
    std::string idSeg;
    if (!crypto::str_cmp(tcmd.body.queryOrder.orderId, ""))       idSeg = tcmd.body.queryOrder.orderId;
    else if (!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")) idSeg = tcmd.body.queryOrder.orderSysId;
    else return;

    std::string pathBase = queryOrderUrl + "/" + idSeg;
    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    auto headers = gateAuthHeaders("GET", pathBase, "", "", time_str);
    auto info_captured = info;

    asyncRequest(boost::beast::http::verb::get, pathBase, "", "", std::move(headers),
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
                std::string_view id_sv, text_sv, size_sv, price_sv, left_sv, fill_sv, finish_sv;
                doc.find_field_unordered("id").get(id_sv);
                doc.find_field_unordered("text").get(text_sv);
                doc.find_field_unordered("size").get(size_sv);
                doc.find_field_unordered("price").get(price_sv);
                doc.find_field_unordered("left").get(left_sv);
                doc.find_field_unordered("fill_price").get(fill_sv);
                doc.find_field_unordered("finish_as").get(finish_sv);

                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId,    id_sv);
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, text_sv);
                rcmd.body.orderResponse.volumeTotal = std::fabs(crypto::fast_atod(size_sv));
                rcmd.body.orderResponse.limitPrice  = crypto::fast_atod(price_sv);
                double left = std::fabs(crypto::fast_atod(left_sv));
                rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
                if (!fill_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(fill_sv);

                if      (status_sv == "open")           rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.volumeTotal > rcmd.body.orderResponse.volumeTraded && rcmd.body.orderResponse.volumeTraded > ZERO_NUM) ? OS_PARTFILLED : OS_NEW;
                else if (finish_sv == "filled")         rcmd.body.orderResponse.orderStatus = OS_FILLED;
                else if (finish_sv == "cancelled" || finish_sv == "liquidated" ||
                         finish_sv == "ioc"       || finish_sv == "auto_deleveraged" ||
                         finish_sv == "reduce_only" || finish_sv == "reduce_out")
                                                        rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                else                                    rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} Gate US query_order cb exc: {}", acc.accountId, e.what());
            }
        });
}


// ============================================================================
// add_new_order (WS futures.order_place)
// ============================================================================
void GateioUsWsTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
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

    double price  = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice  * info.magnifyNumber, info.tickSize);
    double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber,  info.lotSize);

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

    // Gate futures size 带正负号
    double sizeSigned = 0;
    if (tcmd.body.newOrder.offsetFlag == OF_OPEN) {
        if      (tcmd.body.newOrder.direction == DT_LONG)  sizeSigned =  volume;
        else if (tcmd.body.newOrder.direction == DT_SHORT) sizeSigned = -volume;
    } else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
        if      (tcmd.body.newOrder.direction == DT_LONG)  sizeSigned = -volume;
        else if (tcmd.body.newOrder.direction == DT_SHORT) sizeSigned =  volume;
    }
    if (sizeSigned == 0) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId     =
            (tcmd.body.newOrder.offsetFlag == OF_OPEN || tcmd.body.newOrder.offsetFlag == OF_CLOSE)
                ? DirectionError : OffsetFlagError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    std::string price_str = priceZero ? "0" : fmt::format("{}", price);
    const int wsId = nextWsId_.fetch_add(1, std::memory_order_relaxed);
    std::string msg = buildOrderPlaceJson(wsId, tcmd, info, price_str, sizeSigned, tif);

    recordPending(wsId, WsReqType::NEW_ORDER, rcmd, info);
    LOG_INFO("TB {} Gate US ws order.place id={} msg={}", acc.accountId, wsId, msg);
    pWsClient->send_text(std::move(msg));
}


// ============================================================================
// cancel_order (WS futures.order_cancel)
// ============================================================================
void GateioUsWsTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
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
    LOG_INFO("TB {} Gate US ws order.cancel id={} msg={}", acc.accountId, wsId, msg);
    pWsClient->send_text(std::move(msg));
}