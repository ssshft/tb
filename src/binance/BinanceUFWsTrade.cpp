#include "binance/BinanceUFWsTrade.h"

#include <cmath>

#include <fmt/format.h>
#include <simdjson.h>


namespace {
    thread_local simdjson::ondemand::parser g_parser;

    constexpr int64_t kPendingTtlMs   = 30 * 1000;
    constexpr size_t  kPendingHardMax = 10000;
    constexpr int64_t kGcIntervalMs   = 5000;
}


BinanceUFWsTradeUnit::BinanceUFWsTradeUnit(AccountCfg& a, sm::SecurityManager* s)
    : BaseTradeUnit(a, s) {
    if (!signer_.init_from_pem(acc.secretKey)) {
        LOG_ERROR("TB {} UF Ed25519 PEM init FAILED. Orders will be rejected.", acc.accountId);
    } else {
        LOG_INFO("TB {} UF Ed25519 signer ready.", acc.accountId);
    }
}

BinanceUFWsTradeUnit::~BinanceUFWsTradeUnit() = default;


// ============================================================================
// InstType 查找 (WS 事件的 symbol 需要按 USDT_SWAP / USDT_FUTURES 兜底试)
// ============================================================================
bool BinanceUFWsTradeUnit::lookupInstrument(const std::string& originInstId,
                                             md::InstrumentInfo& info, InstType& out) const {
    if (smc->get_instrument_info(BINANCE, USDT_SWAP,    originInstId.c_str(), info)) { out = USDT_SWAP;    return true; }
    if (smc->get_instrument_info(BINANCE, USDT_FUTURES, originInstId.c_str(), info)) { out = USDT_FUTURES; return true; }
    return false;
}


// ============================================================================
// REST signing (Ed25519 → base64 → URL-encode)
// ============================================================================
std::string BinanceUFWsTradeUnit::signPayloadForRest(const std::string& qs) const {
    return crypto::url_encode_component(signer_.sign_base64(qs));
}

std::string BinanceUFWsTradeUnit::buildRestSignedPath(
    std::string_view basePath,
    const std::vector<std::pair<std::string, std::string>>& kvs) const
{
    std::string qs;
    qs.reserve(256);
    for (size_t i = 0; i < kvs.size(); ++i) {
        if (i) qs.push_back('&');
        qs += kvs[i].first;
        qs.push_back('=');
        qs += kvs[i].second;
    }
    std::string sig_enc = signPayloadForRest(qs);

    std::string full;
    full.reserve(basePath.size() + qs.size() + sig_enc.size() + 12);
    full.append(basePath);
    full.push_back('?');
    full.append(qs);
    full.append("&signature=");
    full.append(sig_enc);
    return full;
}


// ============================================================================
// WS JSON builders
// ============================================================================
std::string BinanceUFWsTradeUnit::buildLogonJson() {
    long ts = crypto::getCurrentTimeMilli();
    std::string payload = fmt::format("apiKey={}&timestamp={}", acc.apiKey, ts);
    std::string sig     = signer_.sign_base64(payload);
    return fmt::format(
        R"({{"id":{},"method":"session.logon","params":{{"apiKey":"{}","timestamp":{},"signature":"{}"}}}})",
        kSessionLogonId, escape_json(acc.apiKey), ts, sig);
}

std::string BinanceUFWsTradeUnit::buildUserSubscribeJson() const {
    return fmt::format(R"({{"id":{},"method":"userDataStream.subscribe"}})", kUserStreamSubId);
}

std::string BinanceUFWsTradeUnit::buildOrderPlaceJson(
    int wsId,
    const pubsub::TCommand& tcmd, const md::InstrumentInfo& info,
    const std::string& price, const std::string& amount,
    const char* side, const char* type,
    const char* tif, const char* respType) const
{
    std::string j;
    j.reserve(400);
    j.append(R"({"id":)");
    j.append(std::to_string(wsId));
    j.append(R"(,"method":"order.place","params":{)");
    j.append(R"("symbol":")");     j.append(info.originInstId);                                    j.push_back('"');
    j.append(R"(,"side":")");      j.append(side);                                                 j.push_back('"');
    j.append(R"(,"type":")");      j.append(type);                                                 j.push_back('"');
    if (tif) {
        j.append(R"(,"timeInForce":")"); j.append(tif);                                            j.push_back('"');
    }
    if (type[0] != 'M') {   // MARKET 不带 price
        j.append(R"(,"price":")");     j.append(price);                                            j.push_back('"');
    }
    j.append(R"(,"quantity":")");                  j.append(amount);                               j.push_back('"');
    j.append(R"(,"newClientOrderId":")");          j.append(escape_json(tcmd.body.newOrder.orderSysId));  j.push_back('"');
    j.append(R"(,"newOrderRespType":")");          j.append(respType);                             j.push_back('"');
    j.append(R"(,"reduceOnly":)");                 j.append(tcmd.body.newOrder.reduceOnly ? "true" : "false");
    j.append("}}");
    return j;
}

std::string BinanceUFWsTradeUnit::buildOrderCancelJson(
    int wsId, const pubsub::TCommand& tcmd, const md::InstrumentInfo& info) const
{
    std::string j;
    j.reserve(200);
    j.append(R"({"id":)");
    j.append(std::to_string(wsId));
    j.append(R"(,"method":"order.cancel","params":{)");
    j.append(R"("symbol":")");     j.append(info.originInstId);                                    j.push_back('"');
    if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
        j.append(R"(,"orderId":)");          j.append(tcmd.body.cancelOrder.orderId);
    } else {
        j.append(R"(,"origClientOrderId":")");  j.append(escape_json(tcmd.body.cancelOrder.orderSysId));  j.push_back('"');
    }
    j.append("}}");
    return j;
}


// ============================================================================
// pending map
// ============================================================================
void BinanceUFWsTradeUnit::recordPending(int id, WsReqType type,
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

bool BinanceUFWsTradeUnit::takePending(int id, WsPending& out) {
    std::lock_guard<std::mutex> lk(pendingMtx_);
    auto it = pendingMap_.find(id);
    if (it == pendingMap_.end()) return false;
    out = std::move(it->second);
    pendingMap_.erase(it);
    return true;
}

void BinanceUFWsTradeUnit::clearPending() {
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

void BinanceUFWsTradeUnit::gcPendingLocked(int64_t now_ms) {
    if (pendingMap_.size() > kPendingHardMax) {
        LOG_ERROR("TB {} UF pending map over hard cap ({}), clearing.",
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
void BinanceUFWsTradeUnit::subWebsocekt() {
    std::string restHost = host_of(acc.restUrl);
    initRestClient(restHost,
                   /*default_headers=*/{{"X-MBX-APIKEY", acc.apiKey}},
                   /*max_connections=*/4);

    ::net::WsConfig cfg;
    cfg.url                = acc.wsUrl;   // wss://ws-fapi.binance.com/ws-fapi/v1
    cfg.ping_mode          = ::net::WsConfig::PingMode::ServerOnly;
    cfg.auto_reconnect     = true;
    cfg.idle_timeout_sec   = 60;
    LOG_INFO("TB {} UF ws-fapi {} rest {}", acc.accountId, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));
}


// ============================================================================
// onOpen / onCloseMsg
// ============================================================================
void BinanceUFWsTradeUnit::onOpen() {
    BaseTradeUnit::onOpen();
    wsLoggedIn_.store(false);
    if (!signer_.valid() || !pWsClient) {
        LOG_ERROR("TB {} UF onOpen: signer invalid or pWsClient null", acc.accountId);
        return;
    }
    LOG_INFO("TB {} UF ws send session.logon", acc.accountId);
    pWsClient->send_text(buildLogonJson());
}

void BinanceUFWsTradeUnit::onCloseMsg(int code, const std::string& reason) {
    BaseTradeUnit::onCloseMsg(code, reason);
    wsLoggedIn_.store(false);
    clearPending();
}


// ============================================================================
// onWebsocketMsg
// ============================================================================
void BinanceUFWsTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len,
                                           bool /*isBinary*/, int64_t recv_ns) {
    if (len == 0) return;
    try {
        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc_res = g_parser.iterate(padded);
        if (doc_res.error()) return;
        auto& doc = doc_res.value_unsafe();

        int64_t id_val = 0;
        if (doc.find_field_unordered("id").get(id_val) == simdjson::SUCCESS) {
            handleWsApiResponse(doc, recv_ns);
            return;
        }
        simdjson::ondemand::object ev;
        if (doc.find_field_unordered("event").get(ev) == simdjson::SUCCESS) {
            handleUserDataEvent(ev);
        }
    } catch (const std::exception& e) {
        LOG_ERROR("TB {} UF ws msg exc: {}", acc.accountId, e.what());
    }
}


// ============================================================================
// ws-api 响应分派
// ============================================================================
void BinanceUFWsTradeUnit::handleWsApiResponse(simdjson::ondemand::document& doc,
                                                 int64_t recv_ns) {
    int64_t id_val = 0;
    doc.find_field_unordered("id").get(id_val);
    const int id = static_cast<int>(id_val);

    int64_t status_val = 0;
    doc.find_field_unordered("status").get(status_val);
    const int status = static_cast<int>(status_val);

    if (id == kSessionLogonId) { onLogonResponse(status, doc); return; }
    if (id == kUserStreamSubId) {
        if (status == 200) LOG_INFO("TB {} UF userDataStream.subscribe OK", acc.accountId);
        else               LOG_ERROR("TB {} UF userDataStream.subscribe FAILED status={}", acc.accountId, status);
        return;
    }

    WsPending pending;
    if (!takePending(id, pending)) {
        LOG_WARN("TB {} UF ws-api unknown response id={}", acc.accountId, id);
        return;
    }
    if (pending.type == WsReqType::NEW_ORDER) onOrderPlaceResponse(pending, status, doc, recv_ns);
    else                                        onOrderCancelResponse(pending, status, doc, recv_ns);
}

void BinanceUFWsTradeUnit::onLogonResponse(int status,
                                            simdjson::ondemand::document& doc) {
    if (status == 200) {
        wsLoggedIn_.store(true);
        LOG_INFO("TB {} UF session.logon OK, will subscribe userDataStream", acc.accountId);
        if (pWsClient) pWsClient->send_text(buildUserSubscribeJson());
    } else {
        wsLoggedIn_.store(false);
        std::string_view msg_sv;
        simdjson::ondemand::object err;
        if (doc.find_field_unordered("error").get(err) == simdjson::SUCCESS) {
            err.find_field_unordered("msg").get(msg_sv);
        }
        LOG_ERROR("TB {} UF session.logon FAILED status={} msg={}", acc.accountId, status, msg_sv);
    }
}

void BinanceUFWsTradeUnit::onOrderPlaceResponse(WsPending& pending, int status,
                                                  simdjson::ondemand::document& doc,
                                                  int64_t /*recv_ns*/) {
    pubsub::RCommand& rcmd = pending.rcmd;
    const md::InstrumentInfo& info = pending.info;
    if (rcmd.body.orderResponse.clientOrderId == TESTCLIENTORDERID) return;

    if (status == 200) {
        simdjson::ondemand::object result;
        if (doc.find_field_unordered("result").get(result) == simdjson::SUCCESS) {
            int64_t oid = 0;
            if (result.find_field_unordered("orderId").get(oid) == simdjson::SUCCESS) {
                fmt::format_to(rcmd.body.orderResponse.orderId, "{}", oid);
            }
            // UF 用 avgPrice + executedQty; cumQuote 是名义金额
            std::string_view exec_sv, avg_sv, status_sv;
            result.find_field_unordered("executedQty").get(exec_sv);
            result.find_field_unordered("avgPrice").get(avg_sv);
            result.find_field_unordered("status").get(status_sv);
            if (!exec_sv.empty()) rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(exec_sv) * info.magnifyNumber;
            if (!avg_sv.empty())  rcmd.body.orderResponse.tradePrice   = crypto::fast_atod(avg_sv)  * info.reduceNumber;
            if (!status_sv.empty()) {
                std::string st(status_sv);
                rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(st);
            } else {
                rcmd.body.orderResponse.orderStatus = OS_NEW;
            }
        } else {
            rcmd.body.orderResponse.orderStatus = OS_NEW;
        }
    } else {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        simdjson::ondemand::object err;
        if (doc.find_field_unordered("error").get(err) == simdjson::SUCCESS) {
            int64_t code = 0;
            err.find_field_unordered("code").get(code);
            std::string_view msg_sv;
            err.find_field_unordered("msg").get(msg_sv);
            rcmd.body.orderResponse.errorId = crypto::get_binance_errorid(static_cast<int>(code));
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, msg_sv);
        } else {
            rcmd.body.orderResponse.errorId = UnknownError;
        }
    }
    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
    PUSH_RCMD(rcmd)
}

void BinanceUFWsTradeUnit::onOrderCancelResponse(WsPending& pending, int status,
                                                   simdjson::ondemand::document& doc,
                                                   int64_t /*recv_ns*/) {
    pubsub::RCommand& rcmd = pending.rcmd;
    const md::InstrumentInfo& info = pending.info;

    if (status == 200) {
        simdjson::ondemand::object result;
        if (doc.find_field_unordered("result").get(result) == simdjson::SUCCESS) {
            int64_t oid = 0;
            if (result.find_field_unordered("orderId").get(oid) == simdjson::SUCCESS) {
                fmt::format_to(rcmd.body.orderResponse.orderId, "{}", oid);
            }
            std::string_view exec_sv, avg_sv;
            result.find_field_unordered("executedQty").get(exec_sv);
            result.find_field_unordered("avgPrice").get(avg_sv);
            if (!exec_sv.empty()) rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(exec_sv) * info.magnifyNumber;
            if (!avg_sv.empty())  rcmd.body.orderResponse.tradePrice   = crypto::fast_atod(avg_sv)  * info.reduceNumber;
        }
        rcmd.body.orderResponse.orderStatus = OS_CANCELED;
    } else {
        simdjson::ondemand::object err;
        if (doc.find_field_unordered("error").get(err) == simdjson::SUCCESS) {
            int64_t code = 0;
            err.find_field_unordered("code").get(code);
            std::string_view msg_sv;
            err.find_field_unordered("msg").get(msg_sv);
            rcmd.body.orderResponse.errorId = crypto::get_binance_errorid(static_cast<int>(code));
            rcmd.body.orderResponse.orderStatus =
                (rcmd.body.orderResponse.errorId == OrderNotFoundError) ? OS_REJECTED : OS_FAILED;
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
// userDataStream event handlers
// ============================================================================
void BinanceUFWsTradeUnit::handleUserDataEvent(simdjson::ondemand::object& event) {
    std::string_view e_sv;
    if (event.find_field_unordered("e").get(e_sv) != simdjson::SUCCESS) return;
    if      (e_sv == "ACCOUNT_UPDATE")     handleAccountUpdate(event);
    else if (e_sv == "ORDER_TRADE_UPDATE") handleOrderUpdate(event);
}

// ACCOUNT_UPDATE: {"e":"ACCOUNT_UPDATE","T":ts,"a":{"B":[{a,cw,bc,wb}],"P":[{s,pa,ps,iw,ep,up}]}}
void BinanceUFWsTradeUnit::handleAccountUpdate(simdjson::ondemand::object& event) {
    simdjson::ondemand::object a_obj;
    if (event.find_field_unordered("a").get(a_obj) != simdjson::SUCCESS) return;

    // Balances
    simdjson::ondemand::array B_arr;
    if (a_obj["B"].get(B_arr) == simdjson::SUCCESS) {
        std::vector<pubsub::RCommand> pending;
        for (auto b_val : B_arr) {
            auto b = b_val.get_object();
            if (b.error()) continue;
            std::string_view a_sv, cw_sv, bc_sv, wb_sv;
            b["a"].get(a_sv);
            b["cw"].get(cw_sv);
            b["bc"].get(bc_sv);
            b["wb"].get(wb_sv);

            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
            rcmd.body.balance.exchangeTypeEnum = BINANCE;
            rcmd.body.balance.instTypeEnum     = USDT_SWAP;
            crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
            crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
            crypto::copy_sv_to_char_array(rcmd.body.balance.currency,   crypto::to_upper(std::string(a_sv)));
            rcmd.body.balance.available = crypto::fast_atod(cw_sv);
            rcmd.body.balance.frozen    = crypto::fast_atod(bc_sv);
            rcmd.body.balance.total     = crypto::fast_atod(wb_sv);
            rcmd.body.balance.updateTime    = crypto::getCurrentTime();
            rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;
            pending.emplace_back(rcmd);
        }
        for (size_t i = 0; i < pending.size(); ++i) {
            pending[i].body.balance.isLast = (i + 1 == pending.size());
            PUSH_RCMD(pending[i])
        }
    }

    // Positions
    simdjson::ondemand::array P_arr;
    if (a_obj["P"].get(P_arr) == simdjson::SUCCESS) {
        std::vector<pubsub::RCommand> pending;
        for (auto p_val : P_arr) {
            auto p = p_val.get_object();
            if (p.error()) continue;
            std::string_view s_sv, ps_sv, pa_sv, iw_sv, ep_sv, up_sv;
            p["s"].get(s_sv);
            p["pa"].get(pa_sv);
            p["ps"].get(ps_sv);
            p["iw"].get(iw_sv);
            p["ep"].get(ep_sv);
            p["up"].get(up_sv);
            // ps: BOTH / LONG / SHORT; 老 UFTrade 只取 BOTH
            if (ps_sv.empty() || ps_sv[0] != 'B') continue;

            std::string originInstId(s_sv);
            md::InstrumentInfo info;
            InstType inst;
            if (!lookupInstrument(originInstId, info, inst)) continue;

            double positionAmt = crypto::fast_atod(pa_sv);
            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
            rcmd.body.position.exchangeTypeEnum = BINANCE;
            rcmd.body.position.instTypeEnum     = inst;
            crypto::copy_sv_to_char_array(rcmd.body.position.accountId,  acc.accountId);
            crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
            crypto::copy_sv_to_char_array(rcmd.body.position.instId,     std::string_view(info.instId));
            rcmd.body.position.direction     = positionAmt >= 0 ? DT_LONG : DT_SHORT;
            rcmd.body.position.volume        = std::fabs(positionAmt) * info.magnifyNumber;
            rcmd.body.position.maintMargin   = crypto::fast_atod(iw_sv);
            rcmd.body.position.avgPrice      = crypto::fast_atod(ep_sv) * info.reduceNumber;
            rcmd.body.position.unrealizedPnl = crypto::fast_atod(up_sv);
            rcmd.body.position.updateTime    = crypto::getCurrentTime();
            rcmd.body.position.apiSourceEnum = AS_WEBSOCKET;
            pending.emplace_back(rcmd);
        }
        for (size_t i = 0; i < pending.size(); ++i) {
            pending[i].body.position.isLast = (i + 1 == pending.size());
            PUSH_RCMD(pending[i])
        }
    }
}

// ORDER_TRADE_UPDATE: {"e":"ORDER_TRADE_UPDATE","T":ts,"o":{s,c,S,f,o,q,p,X,z,ap,l,L}}
void BinanceUFWsTradeUnit::handleOrderUpdate(simdjson::ondemand::object& event) {
    simdjson::ondemand::object o;
    if (event.find_field_unordered("o").get(o) != simdjson::SUCCESS) return;

    std::string_view s_sv, c_sv, S_sv, f_sv, ot_sv, q_sv, p_sv, X_sv, z_sv, ap_sv, l_sv, L_sv;
    o["s"].get(s_sv);
    o["c"].get(c_sv);
    o["S"].get(S_sv);
    o["f"].get(f_sv);
    o["o"].get(ot_sv);
    o["q"].get(q_sv);
    o["p"].get(p_sv);
    o["X"].get(X_sv);
    o["z"].get(z_sv);
    o["ap"].get(ap_sv);
    o["l"].get(l_sv);
    o["L"].get(L_sv);

    std::string originInstId(s_sv);
    md::InstrumentInfo info;
    InstType inst;
    if (!lookupInstrument(originInstId, info, inst)) {
        LOG_ERROR("TB {} UF order upd smc miss: {}", acc.accountId, originInstId);
        return;
    }

    pubsub::RCommand rcmd;
    memset(&rcmd, 0, sizeof(pubsub::RCommand));
    rcmd.cmdTypeEnum = pubsub::CMD_RPT_NEW_ORDER;
    rcmd.body.orderResponse.exchangeTypeEnum = BINANCE;
    rcmd.body.orderResponse.instTypeEnum     = inst;
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountId,  acc.accountId);
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.strategyId, acc.strategyId);
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.instId,     std::string_view(info.instId));
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, c_sv);

    rcmd.body.orderResponse.offsetFlag = OF_OPEN;
    if (!S_sv.empty()) rcmd.body.orderResponse.direction = (S_sv[0] == 'B') ? DT_LONG : DT_SHORT;

    std::string tif(f_sv), ot(ot_sv);
    rcmd.body.orderResponse.orderType = crypto::get_binance_ordertype(tif.c_str(), ot.c_str());

    if (!q_sv.empty())  rcmd.body.orderResponse.volumeTotal  = crypto::fast_atod(q_sv)  * info.magnifyNumber;
    if (!p_sv.empty())  rcmd.body.orderResponse.limitPrice   = crypto::fast_atod(p_sv)  * info.reduceNumber;
    if (!z_sv.empty())  rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(z_sv)  * info.magnifyNumber;
    if (!ap_sv.empty()) rcmd.body.orderResponse.tradePrice   = crypto::fast_atod(ap_sv) * info.reduceNumber;
    if (!l_sv.empty())  rcmd.body.orderResponse.tradeDiff    = crypto::fast_atod(l_sv)  * info.magnifyNumber;
    if (!L_sv.empty())  rcmd.body.orderResponse.fillPrice    = crypto::fast_atod(L_sv)  * info.reduceNumber;

    if (!X_sv.empty()) {
        std::string X_str(X_sv);
        rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(X_str);
    }

    rcmd.body.orderResponse.updateTime    = crypto::getCurrentTime();
    rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
    PUSH_RCMD(rcmd)
}


// ============================================================================
// query_* (REST, Ed25519)
// ============================================================================
void BinanceUFWsTradeUnit::query_account(const pubsub::TCommand&) { /* 走 balance */ }

void BinanceUFWsTradeUnit::query_balance(const pubsub::TCommand&) {
    if (!pRestClient || !signer_.valid()) return;
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"recvWindow", "5000"},
        {"timestamp",  std::to_string(crypto::getCurrentTimeMilli())},
    };
    std::string path = buildRestSignedPath(balanceUrl, kvs);
    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "",
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) { LOG_ERROR("TB {} UF query_balance ec: {}", acc.accountId, ec.message()); return; }
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;
                simdjson::ondemand::array arr;
                if (doc["assets"].get(arr) != simdjson::SUCCESS) return;
                std::vector<pubsub::RCommand> pending;
                for (auto it : arr) {
                    auto o = it.get_object();
                    if (o.error()) continue;
                    std::string_view asset_sv, avail_sv, margin_sv, wallet_sv;
                    o["asset"].get(asset_sv);
                    o["availableBalance"].get(avail_sv);
                    o["marginBalance"].get(margin_sv);
                    o["walletBalance"].get(wallet_sv);

                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = BINANCE;
                    rcmd.body.balance.instTypeEnum     = USDT_SWAP;
                    crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.currency,   crypto::to_upper(std::string(asset_sv)));
                    rcmd.body.balance.available = crypto::fast_atod(avail_sv);
                    rcmd.body.balance.frozen    = crypto::fast_atod(margin_sv);
                    rcmd.body.balance.total     = crypto::fast_atod(wallet_sv);
                    rcmd.body.balance.updateTime = crypto::getCurrentTime();
                    rcmd.body.balance.apiSourceEnum = AS_REST;
                    pending.emplace_back(rcmd);
                }
                for (size_t i = 0; i < pending.size(); ++i) {
                    pending[i].body.balance.isLast = (i + 1 == pending.size());
                    PUSH_RCMD(pending[i]);
                }
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} UF query_balance cb exc: {}", acc.accountId, e.what());
            }
        });
}

void BinanceUFWsTradeUnit::query_position(const pubsub::TCommand&) {
    if (!pRestClient || !signer_.valid()) return;
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"recvWindow", "5000"},
        {"timestamp",  std::to_string(crypto::getCurrentTimeMilli())},
    };
    std::string path = buildRestSignedPath(positionUrl, kvs);
    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "",
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) { LOG_ERROR("TB {} UF query_position ec: {}", acc.accountId, ec.message()); return; }
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
                    std::string_view sym_sv, pa_sv, ps_sv, mm_sv, ep_sv, up_sv, mp_sv, lp_sv;
                    o["symbol"].get(sym_sv);
                    o["positionAmt"].get(pa_sv);
                    o["positionSide"].get(ps_sv);
                    o["maintMargin"].get(mm_sv);
                    o["entryPrice"].get(ep_sv);
                    o["unRealizedProfit"].get(up_sv);
                    o["markPrice"].get(mp_sv);
                    o["liquidationPrice"].get(lp_sv);
                    int64_t adl = 0;
                    o["adl"].get(adl);

                    if (ps_sv.empty() || ps_sv[0] != 'B') continue;
                    std::string originInstId(sym_sv);
                    md::InstrumentInfo info;
                    InstType inst;
                    if (!lookupInstrument(originInstId, info, inst)) continue;

                    double positionAmt = crypto::fast_atod(pa_sv);
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                    rcmd.body.position.exchangeTypeEnum = BINANCE;
                    rcmd.body.position.instTypeEnum     = inst;
                    crypto::copy_sv_to_char_array(rcmd.body.position.accountId,  acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.instId,     std::string_view(info.instId));
                    rcmd.body.position.direction     = positionAmt >= 0 ? DT_LONG : DT_SHORT;
                    rcmd.body.position.volume        = std::fabs(positionAmt) * info.magnifyNumber;
                    rcmd.body.position.maintMargin   = crypto::fast_atod(mm_sv);
                    rcmd.body.position.avgPrice      = crypto::fast_atod(ep_sv) * info.reduceNumber;
                    rcmd.body.position.unrealizedPnl = crypto::fast_atod(up_sv);
                    rcmd.body.position.markPrice     = crypto::fast_atod(mp_sv) * info.reduceNumber;
                    rcmd.body.position.liquidPrice   = crypto::fast_atod(lp_sv) * info.reduceNumber;
                    rcmd.body.position.adlQuantile   = static_cast<int>(adl) + 1;
                    rcmd.body.position.updateTime    = crypto::getCurrentTime();
                    rcmd.body.position.apiSourceEnum = AS_REST;
                    pending.emplace_back(rcmd);
                }
                for (size_t i = 0; i < pending.size(); ++i) {
                    pending[i].body.position.isLast = (i + 1 == pending.size());
                    PUSH_RCMD(pending[i]);
                }
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} UF query_position cb exc: {}", acc.accountId, e.what());
            }
        });
}

void BinanceUFWsTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);
    if (!pRestClient || !signer_.valid()) return;
    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum,
                                  tcmd.body.queryOrder.instTypeEnum,
                                  tcmd.body.queryOrder.instId, info)) return;
    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(5);
    kvs.emplace_back("recvWindow", "5000");
    kvs.emplace_back("symbol",     info.originInstId);
    kvs.emplace_back("timestamp",  std::to_string(crypto::getCurrentTimeMilli()));
    if (!crypto::str_cmp(tcmd.body.queryOrder.orderId, "")) {
        kvs.emplace_back("orderId", tcmd.body.queryOrder.orderId);
    } else if (!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")) {
        kvs.emplace_back("origClientOrderId", tcmd.body.queryOrder.orderSysId);
    } else {
        return;
    }
    std::string path = buildRestSignedPath(queryOrderUrl, kvs);
    auto info_captured = info;
    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "",
        [this, rcmd, info_captured](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
            if (ec) return;
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;

                int64_t code = 0;
                if (doc.find_field_unordered("code").get(code) == simdjson::SUCCESS) {
                    long now = crypto::getCurrentTime();
                    if (rcmd.body.orderResponse.clientOrderId > 0 &&
                        now - rcmd.body.orderResponse.clientOrderId > ORDER_REJECTED_TIME_OUT) {
                        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    } else {
                        rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                    }
                    rcmd.body.orderResponse.errorId    = crypto::get_binance_errorid(static_cast<int>(code));
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd);
                    return;
                }
                int64_t oid = 0;
                if (doc.find_field_unordered("orderId").get(oid) == simdjson::SUCCESS) {
                    fmt::format_to(rcmd.body.orderResponse.orderId, "{}", oid);
                }
                std::string_view origQ_sv, price_sv, execQ_sv, avg_sv, status_sv;
                doc.find_field_unordered("origQty").get(origQ_sv);
                doc.find_field_unordered("price").get(price_sv);
                doc.find_field_unordered("executedQty").get(execQ_sv);
                doc.find_field_unordered("avgPrice").get(avg_sv);
                doc.find_field_unordered("status").get(status_sv);
                if (!origQ_sv.empty()) rcmd.body.orderResponse.volumeTotal  = crypto::fast_atod(origQ_sv) * info_captured.magnifyNumber;
                if (!price_sv.empty()) rcmd.body.orderResponse.limitPrice   = crypto::fast_atod(price_sv) * info_captured.reduceNumber;
                if (!execQ_sv.empty()) rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(execQ_sv) * info_captured.magnifyNumber;
                if (!avg_sv.empty())   rcmd.body.orderResponse.tradePrice   = crypto::fast_atod(avg_sv)   * info_captured.reduceNumber;
                if (!status_sv.empty()) {
                    std::string st(status_sv);
                    rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(st);
                }
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd);
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} UF query_order cb exc: {}", acc.accountId, e.what());
            }
        });
}


// ============================================================================
// add_new_order (WS order.place, 无 REST 兜底)
// ============================================================================
void BinanceUFWsTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load() || !wsLoggedIn_.load() || !signer_.valid() || !pWsClient) {
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
        if      (tcmd.body.newOrder.direction == DT_LONG)  side = "BUY";
        else if (tcmd.body.newOrder.direction == DT_SHORT) side = "SELL";
    } else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
        if      (tcmd.body.newOrder.direction == DT_LONG)  side = "SELL";
        else if (tcmd.body.newOrder.direction == DT_SHORT) side = "BUY";
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

    const char* type    = nullptr;
    const char* tif     = nullptr;
    const char* respTyp = "ACK";
    switch (tcmd.body.newOrder.orderType) {
        // UF 的 POST_ONLY 用 GTX (LIMIT+GTX), 不是 LIMIT_MAKER
        case OT_LIMIT:     type = "LIMIT";  tif = "GTC"; respTyp = "ACK";    break;
        case OT_MARKET:    type = "MARKET"; tif = nullptr; respTyp = "RESULT"; break;
        case OT_POST_ONLY: type = "LIMIT";  tif = "GTX"; respTyp = "ACK";    break;
        case OT_FOK:       type = "LIMIT";  tif = "FOK"; respTyp = "RESULT"; break;
        case OT_IOC:       type = "LIMIT";  tif = "IOC"; respTyp = "RESULT"; break;
        default:
            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            rcmd.body.orderResponse.errorId     = OrderTypeError;
            rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
            return;
    }

    double price  = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice  * info.magnifyNumber, info.tickSize);
    double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber,  info.lotSize);
    std::string price_str  = fmt::format("{}", price);
    std::string amount_str = fmt::format("{}", volume);

    const int wsId = nextWsId_.fetch_add(1, std::memory_order_relaxed);
    std::string msg = buildOrderPlaceJson(wsId, tcmd, info, price_str, amount_str,
                                          side, type, tif, respTyp);

    recordPending(wsId, WsReqType::NEW_ORDER, rcmd, info);
    LOG_INFO("TB {} UF ws order.place id={} msg={}", acc.accountId, wsId, msg);
    pWsClient->send_text(std::move(msg));
}


// ============================================================================
// cancel_order (WS order.cancel)
// ============================================================================
void BinanceUFWsTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
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
    LOG_INFO("TB {} UF ws order.cancel id={} msg={}", acc.accountId, wsId, msg);
    pWsClient->send_text(std::move(msg));
}