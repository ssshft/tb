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
    j.append(R"("symbol":")");
    j.append(info.originInstId);                                    
    j.push_back('"');
    j.append(R"(,"side":")");      
    j.append(side);                                                 
    j.push_back('"');
    j.append(R"(,"timestamp":")");      
    j.append(std::to_string(crypto::getCurrentTimeMilli()));                                                 
    j.push_back('"');
    j.append(R"(,"type":")");      
    j.append(type);                                                 
    j.push_back('"');
    if (tif) {
        j.append(R"(,"timeInForce":")");   
        j.append(tif);                                          
        j.push_back('"');
    }
    if (type[0] != 'M') {   // MARKET 无价
        j.append(R"(,"price":")");         
        j.append(price);                                        
        j.push_back('"');
    }
    j.append(R"(,"quantity":")");                  
    j.append(amount);                               
    j.push_back('"');
    j.append(R"(,"newClientOrderId":")");          
    j.append(escape_json(tcmd.body.newOrder.orderSysId));  
    j.push_back('"');
    j.append(R"(,"newOrderRespType":")");          
    j.append(respType);                             
    j.push_back('"');
    j.append("}}");
    return j;
}

std::string BinanceSpotWsTradeUnit::buildOrderCancelJson(int wsId, const pubsub::TCommand& tcmd, const md::InstrumentInfo& info) const {
    std::string j;
    j.reserve(200);
    j.append(R"({"id":)");
    j.append(std::to_string(wsId));
    j.append(R"(,"method":"order.cancel","params":{)");
    j.append(R"("symbol":")");     
    j.append(info.originInstId);                                    
    j.push_back('"');
    j.append(R"(,"timestamp":")");      
    j.append(std::to_string(crypto::getCurrentTimeMilli()));                                                 
    j.push_back('"');
    if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
        j.append(R"(,"orderId":)");          
        j.append(tcmd.body.cancelOrder.orderId);
    } else {
        j.append(R"(,"origClientOrderId":")");   
        j.append(escape_json(tcmd.body.cancelOrder.orderSysId));  
        j.push_back('"');
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
    cfg.ping_mode = net::WsConfig::PingMode::ServerOnly;
    cfg.auto_reconnect = true;
    cfg.idle_timeout_sec = 60;
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
        std::string msg(reinterpret_cast<const char*>(data), len);
        std::cout << "onWebsocketMsg: " << msg << std::endl;

        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc = g_parser.iterate(padded);
        if (doc.error()) {
            return;
        }

        auto doc_value = doc.get_object().value_unsafe();
    
        // ws-api 响应有 "id" 字段, userDataStream 推送外层是 {"event":{...}}
        int64_t id = 0;
        int status = 0;
        simdjson::ondemand::object ev;
        simdjson::ondemand::object result;
        simdjson::ondemand::object error;
        bool has_id = false;
        bool has_result = false;
        bool has_event = false;
        bool has_error = false;

        for (auto field : doc_value) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "id") {
                has_id = field.value().get(id) == simdjson::SUCCESS;
            }
            else if (k == "status") {
                field.value().get(status);
            }
            else if (k == "result") {
                has_result = field.value().get(result) == simdjson::SUCCESS;
            }
            else if (k == "event") {
                has_event = field.value().get(ev) == simdjson::SUCCESS;
            }
            else if (k == "error") {
                has_error = field.value().get(error) == simdjson::SUCCESS;
            }
        }

        if (has_id) {
            if (id == kSessionLogonId) {
                if (status == 200) {
                    wsLoggedIn_.store(true);
                    LOG_INFO("TB {} spot session.logon OK, will subscribe userDataStream", acc.accountId);
                    if (pWsClient) {
                        pWsClient->send_text(buildUserSubscribeJson());
                    }
                } else {
                    wsLoggedIn_.store(false);
                    if (has_error) {
                        std::string_view msg_sv;
                        error["msg"].get(msg_sv);
                        LOG_ERROR("TB {} spot session.logon FAILED status={} msg={}", acc.accountId, status, msg_sv);
                    }
                }
            }
            else if (id == kUserStreamSubId) {
                if (status == 200) {
                    LOG_INFO("TB {} spot userDataStream.subscribe OK", acc.accountId);
                } else {
                    LOG_ERROR("TB {} spot userDataStream.subscribe FAILED status={}", acc.accountId, status);
                }
            }
            else {
                WsPending pending;
                if (takePending(id, pending)) {
                    if (status == 200 && has_result) {
                        handleWsApiResponse(pending, result);
                    }
                    else {
                        if (has_error) {
                            handleWsApiError(pending, error);
                        }
                    }     
                }
                else {
                    LOG_WARN("TB {} spot ws-api unknown response id={}", acc.accountId, id);
                }
            }
        }

        if (has_event) {
            handleUserDataEvent(ev);
        }
    } catch (const std::exception& e) {
        LOG_ERROR("TB {} ws msg exc: {}", acc.accountId, e.what());
    }
}


// ============================================================================
// ws-api 响应分派
// ============================================================================
void BinanceSpotWsTradeUnit::handleWsApiResponse(WsPending& pending, simdjson::ondemand::object& result) {
    pubsub::RCommand& rcmd = pending.rcmd;

    // 测试单不上报
    if (rcmd.body.orderResponse.clientOrderId == TESTCLIENTORDERID) {
        return;
    }
 
    md::InstrumentInfo info;
    if (!smc->get_instrument_info(rcmd.body.orderResponse.exchangeTypeEnum, rcmd.body.orderResponse.instTypeEnum, rcmd.body.orderResponse.instId, info)) {
        LOG_ERROR("TB {} exec report smc miss: {}", acc.accountId, rcmd.body.orderResponse.instId);
        return;
    }

    if (pending.type == pubsub::CMD_NEW_ORDER) {
        int64_t orderId = 0;
        for (auto field : result) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "orderId") {
                field.value().get(orderId);
            }
        }

        fmt::format_to(rcmd.body.orderResponse.orderId, "{}", orderId);
        rcmd.body.orderResponse.orderStatus = OS_NEW;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
    } else if (pending.type == pubsub::CMD_CANCEL_ORDER) {
        int64_t orderId = 0;
        std::string_view execQ_sv;
        std::string_view cumQ_sv;

        for (auto field : ev) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "orderId") {
                field.value().get(orderId);
            }
            else if (k == "executedQty") {
                field.value().get(execQ_sv);
            }
            else if (k == "cummulativeQuoteQty") {
                field.value().get(cumQ_sv);
            }
        }

        fmt::format_to(rcmd.body.orderResponse.orderId, "{}", orderId);
        if (!execQ_sv.empty()) {
            rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(execQ_sv) * info.magnifyNumber;
        }
        if (rcmd.body.orderResponse.volumeTraded > 0 && !cumQ_sv.empty()) {
            double cumQ = crypto::fast_atod(cumQ_sv);
            rcmd.body.orderResponse.tradePrice = cumQ / rcmd.body.orderResponse.volumeTraded * info.reduceNumber;
        }

        rcmd.body.orderResponse.orderStatus = OS_CANCELED;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
    }
}

void BinanceSpotWsTradeUnit::handleWsApiError(WsPending& pending, simdjson::ondemand::object& error) {
    pubsub::RCommand& rcmd = pending.rcmd;

    // 测试单不上报
    if (rcmd.body.orderResponse.clientOrderId == TESTCLIENTORDERID) {
        return;
    }
 
    int code = 0;
    std::string_view msg_sv;

    for (auto field : error) {
        std::string_view k = field.unescaped_key().value_unsafe();
        if (k == "code") {
            field.value().get(code);
        }
        else if (k == "msg") {
            field.value().get(msg_sv);
        }
    }

    rcmd.body.orderResponse.errorId = crypto::get_binance_errorid(static_cast<int>(code));

    if (pending.type == pubsub::CMD_NEW_ORDER) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;    
    } else if (pending.type == pubsub::CMD_CANCEL_ORDER) {
        rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.errorId == OrderNotFoundError) ? OS_REJECTED : OS_FAILED;
    }

    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, msg_sv);
    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
    PUSH_RCMD(rcmd)
}

void BinanceSpotWsTradeUnit::handleUserDataEvent(simdjson::ondemand::object& ev) {
    std::string_view e_sv; // event

    // execution report
    std::string_view s_sv;
    std::string_view c_sv;
    std::string_view C_sv;
    std::string_view S_sv;
    std::string_view f_sv;
    std::string_view o_sv;
    std::string_view X_sv;
    std::string_view l_sv;
    std::string_view L_sv;
    std::string_view z_sv;
    std::string_view Z_sv;
    std::string_view q_sv;
    std::string_view p_sv;
    int64_t i_val = 0;
    std::string_view i_sv;
    bool has_i = false;

    // balances
    simdjson::ondemand::array balances;

    for (auto field : ev) {
        std::string_view k = field.unescaped_key().value_unsafe();
        switch (k[0]) {
            case 'e':
                field.value().get(e_sv);
                break;
            case 'B':
                field.value().get(balances);
                break;      
            case 's':
                field.value().get(s_sv);
                break;
            case 'c':
                field.value().get(c_sv);
                break;
            case 'C':
                field.value().get(C_sv);
                break;
            case 'S':
                field.value().get(S_sv);
                break;
            case 'f':
                field.value().get(f_sv);
                break;
            case 'o':
                field.value().get(o_sv);
                break;
            case 'X':
                field.value().get(X_sv);
                break;
            case 'i':
                has_i = field.value().get(i_sv) == simdjson::SUCCESS;
                if (!has_i) {
                    field.value().get(i_sv);
                }
                break;
            case 'l':
                field.value().get(l_sv);
                break;
            case 'L':
                field.value().get(L_sv);
                break;
            case 'z':
                field.value().get(z_sv);
                break;
            case 'Z':
                field.value().get(Z_sv);
                break;
            case 'q':
                field.value().get(q_sv);
                break;
            case 'p':
                field.value().get(p_sv);
                break;
            default:
                break;
        }
    }  

    if (e_sv == "outboundAccountPosition") {
        // 先收集再回填 isLast (Binance 数组无长度头, ondemand 无 size())
        std::vector<pubsub::RCommand> pending;
        
        for (auto b_val : balances) {
            auto b_res = b_val.get_object();
            if (b_res.error()) {
                continue;
            }
            auto& b = b_res.value_unsafe();

            std::string_view a_sv;
            std::string_view f_sv;
            std::string_view l_sv;
            for (auto field : b) {
                std::string_view k = field.unescaped_key().value_unsafe();
                switch (k[0]) {
                    case 'a':
                        field.value().get(a_sv);
                        break;
                    case 'f':
                        field.value().get(f_sv);
                        break;
                    case 'l':
                        field.value().get(l_sv);
                        break;
                    default:
                        break;
                }
            }

            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
            rcmd.body.balance.exchangeTypeEnum = BINANCE;
            rcmd.body.balance.instTypeEnum = SPOT;
            crypto::copy_sv_to_char_array(rcmd.body.balance.accountId, acc.accountId);
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
    else if (e_sv == "executionReport") {
        if (s_sv.empty()) {
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
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountId, acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.instId, std::string_view(info.instId));

        if (has_i) {
            fmt::format_to(rcmd.body.orderResponse.orderId, "{}", i_val);
        }
        else {
            if (!i_sv.empty()) {
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, i_sv);
            }
        }

        // orderSysId: 优先 C (origClientOrderId, 非空), 否则 c (clientOrderId)
        if (!C_sv.empty()) {
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, C_sv);
        }
        if (!c_sv.empty()) {
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, c_sv);
        }

        rcmd.body.orderResponse.offsetFlag = OF_OPEN;
        if (!S_sv.empty()) {
            rcmd.body.orderResponse.direction = (S_sv[0] == 'B') ? DT_LONG : DT_SHORT;
        }

        std::string tif(f_sv), oty(o_sv);
        rcmd.body.orderResponse.orderType = crypto::get_binance_ordertype(tif.c_str(), oty.c_str());

        if (!X_sv.empty()) {
            std::string X_str(X_sv);
            rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(X_str);
        }
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
            rcmd.body.orderResponse.limitPrice  = crypto::fast_atod(p_sv) * info.reduceNumber;
        }

        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd)
    }
}

// ============================================================================
// query_* : REST (Ed25519 签名)
// ============================================================================
void BinanceSpotWsTradeUnit::query_account(const pubsub::TCommand& tcmd) {  // 走 query_balance
    query_balance(tcmd);
}   
void BinanceSpotWsTradeUnit::query_position(const pubsub::TCommand&) {  // Spot 无

}  

void BinanceSpotWsTradeUnit::query_balance(const pubsub::TCommand& tcmd) {
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"recvWindow", "5000"},
        {"timestamp",  std::to_string(crypto::getCurrentTimeMilli())},
    };
    std::string path = buildRestSignedPath(balanceUrl, kvs);
    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "", [this](boost::system::error_code ec, net::HttpResponse resp) {
        if (ec) {
            LOG_ERROR("TB {} query_balance ec: {}", acc.accountId, ec.message());
            return;
        }
        try {
            std::cout << "query balance: " << resp.body << std::endl;
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                LOG_ERROR("TB {} query_balance parse err: {}", acc.accountId, simdjson::error_message(doc.error()));
                return;
            }

            simdjson::ondemand::array balances;
            if (doc["balances"].get(balances) != simdjson::SUCCESS) {
                LOG_ERROR("TB {} query_balance no 'balances': {}", acc.accountId, resp.body);
                return;
            }

            std::vector<pubsub::RCommand> pending;
            pending.reserve(32);

            for (auto b_val : balances) {
                auto b_res = b_val.get_object();
                if (b_res.error()) {
                    continue;
                }
                auto& b = b_res.value_unsafe();

                std::string_view a_sv;
                std::string_view f_sv;
                std::string_view l_sv;
                for (auto field : b) {
                    std::string_view k = field.unescaped_key().value_unsafe();

                    switch (k[0]) {
                        case 'a':
                            field.value().get(a_sv);
                            break;
                        case 'f':
                            field.value().get(f_sv);
                            break;
                        case 'l':
                            field.value().get(l_sv);
                            break;
                        default:
                            break;
                    }
                }

                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                rcmd.body.balance.exchangeTypeEnum = BINANCE;
                rcmd.body.balance.instTypeEnum = SPOT;
                crypto::copy_sv_to_char_array(rcmd.body.balance.accountId, acc.accountId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(a_sv)));
                rcmd.body.balance.available = crypto::fast_atod(f_sv);
                rcmd.body.balance.frozen = crypto::fast_atod(l_sv);
                rcmd.body.balance.total = rcmd.body.balance.available + rcmd.body.balance.frozen;
                rcmd.body.balance.updateTime = crypto::getCurrentTime();
                rcmd.body.balance.apiSourceEnum = AS_REST;
                pending.emplace_back(rcmd);
            }

            if (pending.empty()) {
                LOG_INFO("TB {} no balance, push USDT=0", acc.accountId);
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                rcmd.body.balance.exchangeTypeEnum = BINANCE;
                rcmd.body.balance.instTypeEnum = SPOT;
                crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.currency,   std::string("USDT"));
                rcmd.body.balance.updateTime = crypto::getCurrentTime();
                rcmd.body.balance.apiSourceEnum = AS_REST;
                rcmd.body.balance.isLast = true;
                PUSH_RCMD(rcmd);
                return;
            }

            for (size_t i = 0; i < pending.size(); ++i) {
                pending[i].body.balance.isLast = (i + 1 == pending.size());
                PUSH_RCMD(pending[i]);
            }
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} query_balance exception: {}", acc.accountId, e.what());
        }
    });
}

void BinanceSpotWsTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum, tcmd.body.queryOrder.instTypeEnum, tcmd.body.queryOrder.instId, info)) {
        LOG_INFO("TB {} query_order smc miss: {}", acc.accountId, tcmd.body.queryOrder.instId);
        return;
    }
    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(5);
    kvs.emplace_back("recvWindow", "5000");
    kvs.emplace_back("symbol", info.originInstId);
    kvs.emplace_back("timestamp", std::to_string(crypto::getCurrentTimeMilli()));
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
            if (ec) {
                LOG_ERROR("TB {} query_order ec: {}", acc.accountId, ec.message());
                return;
            }
            try {
                std::cout << "query order: " << resp.body << std::endl;
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) {
                    LOG_ERROR("TB {} query_order parse err: {}", acc.accountId, resp.body);
                    return;
                }

                auto doc_value = doc.get_object().value_unsafe();

                int64_t code = 0;
                int64_t orderId = 0;
                std::string_view origQ_sv;
                std::string_view price_sv;
                std::string_view execQ_sv;
                std::string_view cumQ_sv;
                std::string_view status_sv;

                bool has_code = false;
                bool has_orderId = false;

                for (auto field : doc_value) {
                    std::string_view k = field.unescaped_key().value_unsafe();
                    if (k == "code") {
                        has_code = field.value().get(code) == simdjson::SUCCESS;
                    }
                    else if (k == "orderId") {
                        has_orderId = field.value().get(orderId) == simdjson::SUCCESS;
                    }
                    else if (k == "origQty") {
                        field.value().get(origQ_sv);
                    }
                    else if (k == "price") {
                        field.value().get(price_sv);
                    }
                    else if (k == "executedQty") {
                        field.value().get(execQ_sv);
                    }
                    else if (k == "cummulativeQuoteQty") {
                        field.value().get(cumQ_sv);
                    }
                    else if (k == "status") {
                        field.value().get(status_sv);
                    }

                }

                if (has_code) {
                    long now = crypto::getCurrentTime();
                    if (rcmd.body.orderResponse.clientOrderId > 0 && now - rcmd.body.orderResponse.clientOrderId > ORDER_REJECTED_TIME_OUT) {
                        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                        rcmd.body.orderResponse.errorId = crypto::get_binance_errorid(static_cast<int>(code));
                        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                        PUSH_RCMD(rcmd);
                    }
                    return;   
                }

                if (has_orderId) {
                    fmt::format_to(rcmd.body.orderResponse.orderId, "{}", orderId);
                
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
                        double cumQ = crypto::fast_atod(cumQ_sv);
                        rcmd.body.orderResponse.tradePrice = cumQ / rcmd.body.orderResponse.volumeTraded * info.reduceNumber;
                    }
                    if (!status_sv.empty()) {
                        std::string st(status_sv);
                        rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(st);
                    }
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd);
                }
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} query_order cb exception: {}", acc.accountId, e.what());
            }
        });
}


// ============================================================================
// add_new_order (WS ws-api order.place, 无 REST 兜底)
// ============================================================================
void BinanceSpotWsTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load() || !wsLoggedIn_.load()) {
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

    if (!isConnected.load() || !wsLoggedIn_.load()) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = TBDisconnectError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
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