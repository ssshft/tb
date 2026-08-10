#include "binance/BinanceSpotWsTrade.h"
#include <algorithm>
#include <fmt/format.h>
#include <simdjson.h>


BinanceSpotWsTradeUnit::BinanceSpotWsTradeUnit(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {
    if (!signer_.init_from_pem(acc.secretKey)) {
        LOG_ERROR("TB {} Ed25519 PEM init FAILED. All orders will be rejected.", acc.accountId);
    } else {
        LOG_INFO("TB {} Ed25519 signer ready.", acc.accountId);
    }
}

BinanceSpotWsTradeUnit::~BinanceSpotWsTradeUnit() = default;


// ============================================================================
// REST signing (Ed25519 → base64 → URL-encode for query)
// ============================================================================
std::string BinanceSpotWsTradeUnit::signPayloadForRest(const std::string& qs) const {
    return crypto::url_encode_component(signer_.sign_base64(qs));
}

std::string BinanceSpotWsTradeUnit::buildRestSignedPath(std::string_view basePath, const std::vector<std::pair<std::string, std::string>>& kvs) const {
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
std::string BinanceSpotWsTradeUnit::buildLogonJson() {
    long ts = crypto::getCurrentTimeMilli();
    // Ed25519 payload: 字母序 → "apiKey=X&timestamp=T"
    std::string payload = fmt::format("apiKey={}&timestamp={}", acc.apiKey, ts);
    std::string sig = signer_.sign_base64(payload);   // 放 JSON body, 不 URL-encode
    return fmt::format(R"({{"id":{},"method":"session.logon","params":{{"apiKey":"{}","timestamp":{},"signature":"{}"}}}})", kSessionLogonId, escape_json(acc.apiKey), ts, sig);
}

std::string BinanceSpotWsTradeUnit::buildUserSubscribeJson() const {
    // session 已认证, 无需 signature 参数
    return fmt::format(R"({{"id":{},"method":"userDataStream.subscribe"}})", kUserStreamSubId);
}

std::string BinanceSpotWsTradeUnit::buildOrderPlaceJson(
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
        j.append(R"(,"timeInForce":")");   j.append(tif);                                          j.push_back('"');
    }
    if (type[0] != 'M') {   // MARKET 无价
        j.append(R"(,"price":")");         j.append(price);                                        j.push_back('"');
    }
    j.append(R"(,"quantity":")");                  j.append(amount);                               j.push_back('"');
    j.append(R"(,"newClientOrderId":")");          j.append(escape_json(tcmd.body.newOrder.orderSysId));  j.push_back('"');
    j.append(R"(,"newOrderRespType":")");          j.append(respType);                             j.push_back('"');
    j.append("}}");
    return j;
}

std::string BinanceSpotWsTradeUnit::buildOrderCancelJson(
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
        j.append(R"(,"origClientOrderId":")");   j.append(escape_json(tcmd.body.cancelOrder.orderSysId));  j.push_back('"');
    }
    j.append("}}");
    return j;
}

// ============================================================================
// pending map
// ============================================================================
void BinanceSpotWsTradeUnit::recordPending(int id, pubsub::CommandType type, const pubsub::RCommand& rcmd) {
    const int64_t now_ms = crypto::getCurrentTimeMilli();
    const int64_t last_gc = pendingLastGcMs_.load(std::memory_order_relaxed);
    const bool need_gc = (now_ms - last_gc > kGcIntervalMs);
    if (need_gc) {
        pendingLastGcMs_.store(now_ms, std::memory_order_relaxed);

        clearPending();
    }

    tbb::concurrent_hash_map<int, WsPending>::accessor acc;
    pendingMap_.insert(acc, id);
    acc->second.type = type;
    acc->second.rcmd = rcmd;
    acc->second.ts_ms = now_ms;
}

bool BinanceSpotWsTradeUnit::takePending(int id, WsPending& out) {
    tbb::concurrent_hash_map<int, WsPending>::accessor acc;
    if (!pendingMap_.find(acc, id)) {
        return false;
    }

    out = std::move(acc->second);
    pendingMap_.erase(acc);
    return true;
}

void BinanceSpotWsTradeUnit::clearPending() {
    const int64_t now_ms = crypto::getCurrentTimeMilli();
    if (pendingMap_.size() > kPendingHardMax) {
        pendingMap_.clear();
        return;
    }

    for (auto it = pendingMap_.begin(); it != pendingMap_.end(); ++it) {
        if (now_ms - it->second.ts_ms > kPendingTtlMs) {
            pendingMap_.erase(it->first);
        }

    }
}

// ============================================================================
// subWebsocekt: REST (query 用) + WS ws-api
// ============================================================================
void BinanceSpotWsTradeUnit::subWebsocekt() {
    std::string restHost = host_of(acc.restUrl);
    initRestClient(restHost, {{"X-MBX-APIKEY", acc.apiKey}}, 4);

    net::WsConfig cfg;
    cfg.url = acc.wsUrl;   // wss://ws-api.binance.com/ws-api/v3
    cfg.ping_mode = ::net::WsConfig::PingMode::ServerOnly;
    cfg.auto_reconnect = true;
    cfg.idle_timeout_sec   = 60;
    LOG_INFO("TB {} spot ws-api {} rest {}", acc.accountId, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));
}


// ============================================================================
// onOpen: 发 session.logon
// ============================================================================
void BinanceSpotWsTradeUnit::onOpen() {
    BaseTradeUnit::onOpen();
    wsLoggedIn_.store(false);

    if (!signer_.valid() || !pWsClient) {
        LOG_ERROR("TB {} onOpen: signer invalid or pWsClient null, cannot logon", acc.accountId);
        return;
    }
    std::string logon = buildLogonJson();
    LOG_INFO("TB {} spot ws send session.logon", acc.accountId);
    pWsClient->send_text(std::move(logon));
}


// ============================================================================
// onCloseMsg: 清空 pending + 重置 loggedIn
// ============================================================================
void BinanceSpotWsTradeUnit::onCloseMsg(int code, const std::string& reason) {
    BaseTradeUnit::onCloseMsg(code, reason);
    wsLoggedIn_.store(false);
    clearPending();
}


// ============================================================================
// onWebsocketMsg
// ============================================================================
void BinanceSpotWsTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool /*isBinary*/, int64_t recv_ns) {
    try {
        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc_res = g_parser.iterate(padded);
        if (doc_res.error()) {
            return;
        }
        auto& doc = doc_res.value_unsafe();

        // ws-api 响应有 "id" 字段, userDataStream 推送外层是 {"event":{...}}
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
        LOG_ERROR("TB {} ws msg exc: {}", acc.accountId, e.what());
    }
}


// ============================================================================
// ws-api 响应分派
// ============================================================================
void BinanceSpotWsTradeUnit::handleWsApiResponse(simdjson::ondemand::document& doc, int64_t recv_ns) {
    int64_t id_val = 0;
    doc.find_field_unordered("id").get(id_val);
    const int id = static_cast<int>(id_val);

    int64_t status_val = 0;
    doc.find_field_unordered("status").get(status_val);
    const int status = static_cast<int>(status_val);

    if (id == kSessionLogonId) {
        onLogonResponse(status, doc);
        return;
    }
    if (id == kUserStreamSubId) {
        if (status == 200) {
            LOG_INFO("TB {} spot userDataStream.subscribe OK", acc.accountId);
        } else {
            LOG_ERROR("TB {} spot userDataStream.subscribe FAILED status={}", acc.accountId, status);
        }
        return;
    }

    WsPending pending;
    if (!takePending(id, pending)) {
        LOG_WARN("TB {} spot ws-api unknown response id={}", acc.accountId, id);
        return;
    }
    if (pending.type == pubsub::CMD_NEW_ORDER {
        onOrderPlaceResponse(pending, status, doc, recv_ns);
    } else {
        onOrderCancelResponse(pending, status, doc, recv_ns);
    }
}

void BinanceSpotWsTradeUnit::onLogonResponse(int status, simdjson::ondemand::document& doc) {
    if (status == 200) {
        wsLoggedIn_.store(true);
        LOG_INFO("TB {} spot session.logon OK, will subscribe userDataStream", acc.accountId);
        if (pWsClient) {
            pWsClient->send_text(buildUserSubscribeJson());
        }
    } else {
        wsLoggedIn_.store(false);
        std::string_view msg_sv;
        simdjson::ondemand::object err;
        if (doc.find_field_unordered("error").get(err) == simdjson::SUCCESS) {
            err.find_field_unordered("msg").get(msg_sv);
        }
        LOG_ERROR("TB {} spot session.logon FAILED status={} msg={}", acc.accountId, status, msg_sv);
    }
}

void BinanceSpotWsTradeUnit::onOrderPlaceResponse(WsPending& pending, int status, simdjson::ondemand::document& doc, int64_t /*recv_ns*/) {
    pubsub::RCommand& rcmd = pending.rcmd;
 
    md::InstrumentInfo info;
    if (!smc->get_instrument_info(rcmd.body.orderResponse.exchangeTypeEnum, rcmd.body.orderResponse.instTypeEnum, rcmd.body.orderResponse.instId, info)) {
        LOG_ERROR("TB {} exec report smc miss: {}", acc.accountId, rcmd.body.orderResponse.instId);
        return;
    }

    // 测试单不上报
    if (rcmd.body.orderResponse.clientOrderId == TESTCLIENTORDERID) {
        return;
    }

    if (status == 200) {
        simdjson::ondemand::object result;
        if (doc.find_field_unordered("result").get(result) == simdjson::SUCCESS) {
            int64_t oid = 0;
            if (result.find_field_unordered("orderId").get(oid) == simdjson::SUCCESS) {
                fmt::format_to(rcmd.body.orderResponse.orderId, "{}", oid);
            }
            std::string_view exec_sv, cumQ_sv, status_sv;
            result.find_field_unordered("executedQty").get(exec_sv);
            result.find_field_unordered("cummulativeQuoteQty").get(cumQ_sv);
            result.find_field_unordered("status").get(status_sv);

            if (!exec_sv.empty()) {
                rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(exec_sv) * info.magnifyNumber;
            }
            if (rcmd.body.orderResponse.volumeTraded > 0 && !cumQ_sv.empty()) {
                rcmd.body.orderResponse.tradePrice = crypto::fast_atod(cumQ_sv) / rcmd.body.orderResponse.volumeTraded * info.reduceNumber;
            }
            if (!status_sv.empty()) {
                std::string st(status_sv);
                rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(st);
            } else {
                // ACK 响应没有 status 字段, 默认 NEW
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

void BinanceSpotWsTradeUnit::onOrderCancelResponse(WsPending& pending, int status, simdjson::ondemand::document& doc, int64_t /*recv_ns*/) {
    pubsub::RCommand& rcmd = pending.rcmd;
 
    md::InstrumentInfo info;
    if (!smc->get_instrument_info(rcmd.body.orderResponse.exchangeTypeEnum, rcmd.body.orderResponse.instTypeEnum, rcmd.body.orderResponse.instId, info)) {
        LOG_ERROR("TB {} exec report smc miss: {}", acc.accountId, rcmd.body.orderResponse.instId);
        return;
    }

    if (status == 200) {
        simdjson::ondemand::object result;
        if (doc.find_field_unordered("result").get(result) == simdjson::SUCCESS) {
            int64_t oid = 0;
            if (result.find_field_unordered("orderId").get(oid) == simdjson::SUCCESS) {
                fmt::format_to(rcmd.body.orderResponse.orderId, "{}", oid);
            }
            std::string_view exec_sv, cumQ_sv;
            result.find_field_unordered("executedQty").get(exec_sv);
            result.find_field_unordered("cummulativeQuoteQty").get(cumQ_sv);
            if (!exec_sv.empty()) {
                rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(exec_sv) * info.magnifyNumber;
            }
            if (rcmd.body.orderResponse.volumeTraded > 0 && !cumQ_sv.empty()) {
                rcmd.body.orderResponse.tradePrice = crypto::fast_atod(cumQ_sv) / rcmd.body.orderResponse.volumeTraded * info.reduceNumber;
            }
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
            rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.errorId == OrderNotFoundError) ? OS_REJECTED : OS_FAILED;
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, msg_sv);
        } else {
            rcmd.body.orderResponse.orderStatus = OS_FAILED;
            rcmd.body.orderResponse.errorId = UnknownError;
        }
    }
    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
    PUSH_RCMD(rcmd)
}


// ============================================================================
// userDataStream 事件推送 (跟 BinanceSpotTrade 一致)
// ============================================================================
void BinanceSpotWsTradeUnit::handleUserDataEvent(simdjson::ondemand::object& event) {
    std::string_view e_sv;
    if (event.find_field_unordered("e").get(e_sv) != simdjson::SUCCESS) {
        return;
    }

    if (e_sv == "outboundAccountPosition") {
        handleAccountPosition(event);
    }
    else if (e_sv == "executionReport") {
        handleExecutionReport(event);
    }         
}

void BinanceSpotWsTradeUnit::handleAccountPosition(simdjson::ondemand::object& ev) {
    simdjson::ondemand::array balances;
    if (ev.find_field_unordered("B").get(balances) != simdjson::SUCCESS) {
        return;
    }

    std::vector<pubsub::RCommand> pending;
    pending.reserve(8);
    for (auto b_val : balances) {
        auto b = b_val.get_object();
        if (b.error()) {
            continue;
        }
        std::string_view a_sv, f_sv, l_sv;
        b["a"].get(a_sv);
        b["f"].get(f_sv);
        b["l"].get(l_sv);

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
        rcmd.body.balance.exchangeTypeEnum = BINANCE;
        rcmd.body.balance.instTypeEnum = SPOT;
        crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(a_sv)));
        rcmd.body.balance.available = crypto::fast_atod(f_sv);
        rcmd.body.balance.frozen = crypto::fast_atod(l_sv);
        rcmd.body.balance.total = rcmd.body.balance.available + rcmd.body.balance.frozen;
        rcmd.body.balance.updateTime = crypto::getCurrentTime();
        rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;
        pending.emplace_back(rcmd);
    }
}

void BinanceSpotWsTradeUnit::handleExecutionReport(simdjson::ondemand::object& ev) {
    std::string_view s_sv;
    if (ev.find_field_unordered("s").get(s_sv) != simdjson::SUCCESS) {
        return;
    }

    md::InstrumentInfo info;
    std::string originInstId(s_sv);
    if (!smc->get_instrument_info(BINANCE, SPOT, originInstId.c_str(), info)) {
        LOG_ERROR("TB {} exec report smc miss: {}", acc.accountId, originInstId);
        return;
    }

    pubsub::RCommand rcmd;
    memset(&rcmd, 0, sizeof(pubsub::RCommand));
    rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
    rcmd.body.orderResponse.exchangeTypeEnum = BINANCE;
    rcmd.body.orderResponse.instTypeEnum = SPOT;
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountId,  acc.accountId);
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.strategyId, acc.strategyId);
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.instId, std::string_view(info.instId));

    int64_t i_val = 0;
    if (ev.find_field_unordered("i").get(i_val) == simdjson::SUCCESS) {
        fmt::format_to(rcmd.body.orderResponse.orderId, "{}", i_val);
    }

    std::string_view C_sv, c_sv;
    bool used_C = false;
    if (ev.find_field_unordered("C").get(C_sv) == simdjson::SUCCESS && !C_sv.empty()) {
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, C_sv);
        used_C = true;
    }
    if (!used_C && ev.find_field_unordered("c").get(c_sv) == simdjson::SUCCESS) {
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, c_sv);
    }

    rcmd.body.orderResponse.offsetFlag = OF_OPEN;
    std::string_view S_sv;
    if (ev.find_field_unordered("S").get(S_sv) == simdjson::SUCCESS && !S_sv.empty()) {
        rcmd.body.orderResponse.direction = (S_sv[0] == 'B') ? DT_LONG : DT_SHORT;
    }

    std::string_view f_sv, o_sv;
    ev.find_field_unordered("f").get(f_sv);
    ev.find_field_unordered("o").get(o_sv);
    std::string tif(f_sv), oty(o_sv);
    rcmd.body.orderResponse.orderType = crypto::get_binance_ordertype(tif.c_str(), oty.c_str());

    std::string_view X_sv;
    if (ev.find_field_unordered("X").get(X_sv) == simdjson::SUCCESS) {
        std::string X_str(X_sv);
        rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(X_str);
    }

    std::string_view l_sv, L_sv, z_sv, Z_sv, q_sv, p_sv;
    ev.find_field_unordered("l").get(l_sv);
    ev.find_field_unordered("L").get(L_sv);
    ev.find_field_unordered("z").get(z_sv);
    ev.find_field_unordered("Z").get(Z_sv);
    ev.find_field_unordered("q").get(q_sv);
    ev.find_field_unordered("p").get(p_sv);

    if (!l_sv.empty()) {
        rcmd.body.orderResponse.tradeDiff = crypto::fast_atod(l_sv) * info.magnifyNumber;
    }

    if (!L_sv.empty()) {
        rcmd.body.orderResponse.fillPrice = crypto::fast_atod(L_sv) * info.reduceNumber;
    }

    if (!z_sv.empty()) {
        rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(z_sv) * info.magnifyNumber;
    }

    if (rcmd.body.orderResponse.volumeTraded > 0 && !Z_sv.empty()) {
        rcmd.body.orderResponse.tradePrice = crypto::fast_atod(Z_sv) / rcmd.body.orderResponse.volumeTraded * info.reduceNumber;
    }

    if (!q_sv.empty()) {
        rcmd.body.orderResponse.volumeTotal = crypto::fast_atod(q_sv) * info.magnifyNumber;
    }

    if (!p_sv.empty()) {
        rcmd.body.orderResponse.limitPrice = crypto::fast_atod(p_sv) * info.reduceNumber;
    }

    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
    rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
    PUSH_RCMD(rcmd)
}


// ============================================================================
// query_* : REST (Ed25519 签名)
// ============================================================================
void BinanceSpotWsTradeUnit::query_account(const pubsub::TCommand&)  {  // 走 query_balance

}   
void BinanceSpotWsTradeUnit::query_position(const pubsub::TCommand&) {  // Spot 无

}  

void BinanceSpotWsTradeUnit::query_balance(const pubsub::TCommand&) {
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"recvWindow", "5000"},
        {"timestamp",  std::to_string(crypto::getCurrentTimeMilli())},
    };
    std::string path = buildRestSignedPath(balanceUrl, kvs);
    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "",
        [this](boost::system::error_code ec, net::HttpResponse resp) {
            if (ec) { LOG_ERROR("TB {} query_balance ec: {}", acc.accountId, ec.message()); return; }
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;
                simdjson::ondemand::array balances;
                if (doc.find_field_unordered("balances").get(balances) != simdjson::SUCCESS) {
                    LOG_ERROR("TB {} query_balance no 'balances': {}", acc.accountId, resp.body);
                    return;
                }
                std::vector<pubsub::RCommand> pending;
                pending.reserve(32);
                for (auto b_val : balances) {
                    auto b = b_val.get_object();
                    if (b.error()) continue;
                    std::string_view asset_sv, free_sv, locked_sv;
                    b["asset"].get(asset_sv);
                    b["free"].get(free_sv);
                    b["locked"].get(locked_sv);

                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = BINANCE;
                    rcmd.body.balance.instTypeEnum     = SPOT;
                    crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(asset_sv)));
                    rcmd.body.balance.available  = crypto::fast_atod(free_sv);
                    rcmd.body.balance.frozen = crypto::fast_atod(locked_sv);
                    rcmd.body.balance.total = rcmd.body.balance.available + rcmd.body.balance.frozen;
                    rcmd.body.balance.updateTime = crypto::getCurrentTime();
                    rcmd.body.balance.apiSourceEnum = AS_REST;
                    pending.emplace_back(rcmd);
                }
                for (size_t i = 0; i < pending.size(); ++i) {
                    pending[i].body.balance.isLast = (i + 1 == pending.size());
                    PUSH_RCMD(pending[i]);
                }
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} query_balance cb exc: {}", acc.accountId, e.what());
            }
        });
}

void BinanceSpotWsTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);
    if (!pRestClient || !signer_.valid()) return;
    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum, tcmd.body.queryOrder.instTypeEnum, tcmd.body.queryOrder.instId, info)) {
        LOG_INFO("TB {} query_order smc miss: {}", acc.accountId, tcmd.body.queryOrder.instId);
        return;
    }
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
    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "",
        [this, rcmd, info](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
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
                        rcmd.body.orderResponse.errorId = crypto::get_binance_errorid(static_cast<int>(code));
                        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                        PUSH_RCMD(rcmd);
                    }
                    return;
                }
                int64_t oid = 0;
                if (doc.find_field_unordered("orderId").get(oid) == simdjson::SUCCESS) {
                    fmt::format_to(rcmd.body.orderResponse.orderId, "{}", oid);
                }
                std::string_view origQ_sv, price_sv, execQ_sv, cumQ_sv, status_sv;
                doc.find_field_unordered("origQty").get(origQ_sv);
                doc.find_field_unordered("price").get(price_sv);
                doc.find_field_unordered("executedQty").get(execQ_sv);
                doc.find_field_unordered("cummulativeQuoteQty").get(cumQ_sv);
                doc.find_field_unordered("status").get(status_sv);

                if (!origQ_sv.empty()) {
                    rcmd.body.orderResponse.volumeTotal = crypto::fast_atod(origQ_sv) * info.magnifyNumber;
                }

                if (!price_sv.empty()) {
                    rcmd.body.orderResponse.limitPrice = crypto::fast_atod(price_sv) * info.reduceNumber;
                }

                if (!execQ_sv.empty()) {
                    rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(execQ_sv) * info.magnifyNumber;
                }

                if (rcmd.body.orderResponse.volumeTraded > 0 && !cumQ_sv.empty()) {
                    rcmd.body.orderResponse.tradePrice = crypto::fast_atod(cumQ_sv) / rcmd.body.orderResponse.volumeTraded * info.reduceNumber;
                }
                if (!status_sv.empty()) {
                    std::string st(status_sv);
                    rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(st);
                }
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd);
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} query_order cb exc: {}", acc.accountId, e.what());
            }
        });
}


// ============================================================================
// add_new_order (WS ws-api order.place, 无 REST 兜底)
// ============================================================================
void BinanceSpotWsTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load() || !wsLoggedIn_.load() || !signer_.valid() || !pWsClient) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId     = TBDisconnectError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.newOrder.exchangeTypeEnum, tcmd.body.newOrder.instTypeEnum, tcmd.body.newOrder.instId, info)) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId = SMCInstrumentNotExistError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const char* side = nullptr;
    if (tcmd.body.newOrder.offsetFlag == OF_OPEN) {
        if (tcmd.body.newOrder.direction == DT_LONG) {
            side = "BUY";
        }
        else if (tcmd.body.newOrder.direction == DT_SHORT) {
            side = "SELL";
        }
    } else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
        if (tcmd.body.newOrder.direction == DT_LONG) {
            side = "SELL";
        }
        else if (tcmd.body.newOrder.direction == DT_SHORT) {
            side = "BUY";
        }
    }
    if (!side) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId = (tcmd.body.newOrder.offsetFlag == OF_OPEN || tcmd.body.newOrder.offsetFlag == OF_CLOSE) ? DirectionError : OffsetFlagError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const char* type = nullptr;
    const char* tif = nullptr;
    const char* respTyp = "ACK";
    switch (tcmd.body.newOrder.orderType) {
        case OT_LIMIT:     
            type = "LIMIT";       
            tif = "GTC";    
            respTyp = "ACK";    
            break;
        case OT_MARKET:    
            type = "MARKET";      
            tif = nullptr;  
            respTyp = "RESULT"; 
            break;
        case OT_POST_ONLY: 
            type = "LIMIT_MAKER"; 
            tif = nullptr;  
            respTyp = "ACK";    
            break;
        case OT_FOK:       
            type = "LIMIT";       
            tif = "FOK";    
            respTyp = "RESULT"; 
            break;
        case OT_IOC:       
            type = "LIMIT";       
            tif = "IOC";    
            respTyp = "RESULT"; 
            break;
        default:
            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            rcmd.body.orderResponse.errorId = OrderTypeError;
            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
            return;
    }

    double price = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice * info.magnifyNumber, info.tickSize);
    double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber,  info.lotSize);
    std::string price_str = fmt::format("{}", price);
    std::string amount_str = fmt::format("{}", volume);

    const int wsId = nextWsId_.fetch_add(1, std::memory_order_relaxed);
    std::string msg = buildOrderPlaceJson(wsId, tcmd, info, price_str, amount_str, side, type, tif, respTyp);

    // 先记 pending, 再发 msg —— 反过来的话回执可能先到达 lookup miss
    recordPending(wsId, pubsub::CMD_NEW_ORDER, rcmd);
    LOG_INFO("TB {} spot ws order.place id={} msg={}", acc.accountId, wsId, msg);
    pWsClient->send_text(std::move(msg));
}


// ============================================================================
// cancel_order (WS ws-api order.cancel)
// ============================================================================
void BinanceSpotWsTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load() || !wsLoggedIn_.load() || !pWsClient) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = TBDisconnectError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.cancelOrder.exchangeTypeEnum, tcmd.body.cancelOrder.instTypeEnum, tcmd.body.cancelOrder.instId, info)) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = SMCInstrumentNotExistError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    if (crypto::str_cmp(tcmd.body.cancelOrder.orderId, "") && crypto::str_cmp(tcmd.body.cancelOrder.orderSysId, "")) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = OrderIdError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const int wsId = nextWsId_.fetch_add(1, std::memory_order_relaxed);
    std::string msg = buildOrderCancelJson(wsId, tcmd, info);

    recordPending(wsId, pubsub::CMD_CANCEL_ORDER, rcmd);
    LOG_INFO("TB {} spot ws order.cancel id={} msg={}", acc.accountId, wsId, msg);
    pWsClient->send_text(std::move(msg));
}