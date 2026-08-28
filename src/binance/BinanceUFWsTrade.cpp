#include "binance/BinanceUFWsTrade.h"
#include <cmath>
#include <fmt/format.h>
#include <simdjson.h>


BinanceUFWsTradeUnit::BinanceUFWsTradeUnit(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {
    if (!signer_.init_from_pem(acc.secretKey)) {
        LOG_ERROR("TB {} UF Ed25519 PEM init FAILED. Orders will be rejected.", acc.accountName);
    } else {
        LOG_INFO("TB {} UF Ed25519 signer ready.", acc.accountName);
    }
}

BinanceUFWsTradeUnit::~BinanceUFWsTradeUnit() {
    renewStop_.store(true);
    renewCv_.notify_all();
    if (renewThread_.joinable()) {
        renewThread_.join();
    }
}


// ============================================================================
// REST signing (Ed25519 → base64 → URL-encode)
// ============================================================================
std::string BinanceUFWsTradeUnit::signPayloadForRest(const std::string& qs) const {
    return crypto::url_encode_component(signer_.sign_base64(qs));
}

std::string BinanceUFWsTradeUnit::buildRestSignedPath(std::string_view basePath, const std::vector<std::pair<std::string, std::string>>& kvs) const {
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
    int64_t ts = crypto::getCurrentTimeMilli();
    std::string payload = fmt::format("apiKey={}&timestamp={}", acc.apiKey, ts);
    std::string sig = signer_.sign_base64(payload);
    return fmt::format(R"({{"id":{},"method":"session.logon","params":{{"apiKey":"{}","timestamp":{},"signature":"{}"}}}})", kSessionLogonId, escape_json(acc.apiKey), ts, sig);
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
    if (type[0] != 'M') {   // MARKET 不带 price
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
    j.append(R"(,"reduceOnly":)");                 
    j.append(tcmd.body.newOrder.reduceOnly ? "true" : "false");
    j.append("}}");
    return j;
}

std::string BinanceUFWsTradeUnit::buildOrderCancelJson(int wsId, const pubsub::TCommand& tcmd, const md::InstrumentInfo& info) const
{
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
void BinanceUFWsTradeUnit::recordPending(int id, pubsub::CommandType type, const pubsub::RCommand& rcmd) {
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

bool BinanceUFWsTradeUnit::takePending(int id, WsPending& out) {
    tbb::concurrent_hash_map<int, WsPending>::accessor acc;
    if (!pendingMap_.find(acc, id)) {
        return false;
    }

    out = std::move(acc->second);
    pendingMap_.erase(acc);
    return true;
}

void BinanceUFWsTradeUnit::clearPending() {
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
// listenKey
// ============================================================================
bool BinanceUFWsTradeUnit::generateListenKeySync() {
    // POST /fapi/v1/listenKey (需要 X-MBX-APIKEY header, 不需要 signature)
    std::promise<std::string> prom;
    auto fut = prom.get_future();
    bool responded = false;

    asyncRequest(boost::beast::http::verb::post, listenKeyUrl, /*body=*/"", /*ct=*/"",
        [this, &prom, &responded](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (responded) {
                return;   // 双回调保护
            }
            responded = true;
            if (ec) {
                LOG_ERROR("TB {} listenKey req ec: {}", acc.accountName, ec.message());
                prom.set_value("");
                return;
            }
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) {
                    prom.set_value(""); 
                    return; 
                }
                std::string_view lk;
                if (doc["listenKey"].get(lk) == simdjson::SUCCESS) {
                    prom.set_value(std::string(lk));
                } else {
                    LOG_ERROR("TB {} listenKey resp missing: {}", acc.accountName, resp.body);
                    prom.set_value("");
                }
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} listenKey parse exc: {}", acc.accountName, e.what());
                prom.set_value("");
            }
        });

    if (fut.wait_for(std::chrono::seconds(15)) != std::future_status::ready) {
        LOG_ERROR("TB {} listenKey req timeout", acc.accountName);
        return false;
    }
    listenKey_ = fut.get();
    if (listenKey_.empty()) {
        return false;
    }
    LOG_INFO("TB {} listenKey={}", acc.accountName, listenKey_);
    return true;
}

void BinanceUFWsTradeUnit::renewListenKeyAsync() {
    if (listenKey_.empty()) {
        return;
    }

    // PUT /fapi/v1/listenKey (仅需 X-MBX-APIKEY header)
    asyncRequest(boost::beast::http::verb::put, listenKeyUrl, /*body=*/"", /*ct=*/"",
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) {
                LOG_ERROR("TB {} listenKey renew ec: {}", acc.accountName, ec.message());
                return;
            }
            if (resp.status_code != 200) {
                LOG_ERROR("TB {} listenKey renew status={} body={}", acc.accountName, resp.status_code, resp.body);
                return;
            }
            LOG_DEBUG("TB {} listenKey renewed", acc.accountName);
        });
}

void BinanceUFWsTradeUnit::listenKeyRenewLoop() {
    while (!renewStop_.load()) {
        std::unique_lock<std::mutex> lk(renewMtx_);
        if (renewCv_.wait_for(lk, std::chrono::seconds(kListenKeyRenewSec), [this]{ 
            return renewStop_.load(); 
        })) {
            return;
        }
        renewListenKeyAsync();
    }
}

// ============================================================================
// subWebsocekt
// ============================================================================
void BinanceUFWsTradeUnit::subWebsocekt() {
    std::string restHost = host_of(acc.restUrl);
    initRestClient(restHost, {{"X-MBX-APIKEY", acc.apiKey}}, 4);

    // 2. listenKey (启动路径, 允许短暂 block ≤15s)
    if (!generateListenKeySync()) {
        LOG_ERROR("TB {} listenKey gen failed, ws NOT started", acc.accountName);
        return;
    }
    
    // ws userdata connect
    net::WsConfig cfg;
    cfg.url = acc.wsUrl + wsSubPath + listenKey_;
    cfg.ping_mode = net::WsConfig::PingMode::ServerOnly;
    cfg.auto_reconnect = true;
    cfg.idle_timeout_sec = 60;
    LOG_INFO("TB {} UF ws userdata {} rest {}", acc.accountName, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));

    // ws trade connect
    net::WsConfig tradeCfg;
    tradeCfg.url = acc.wsTradeUrl;   // wss://ws-fapi.binance.com/ws-fapi/v1
    tradeCfg.ping_mode = net::WsConfig::PingMode::ServerOnly;
    tradeCfg.auto_reconnect = true;
    tradeCfg.idle_timeout_sec = 60;
    LOG_INFO("TB {} UF user data stream {}", acc.accountName, tradeCfg.url);

    pWsTradeClient = net::WsClient::create(std::move(tradeCfg));

    pWsTradeClient->on_open([this]() {
        LOG_INFO("TB {} UF ws trade connected", acc.accountName);
        isTradeConnected.store(true);
        wsLoggedIn_.store(false);
        if (!signer_.valid() || !pWsTradeClient) {
            LOG_ERROR("TB {} UF onOpen: signer invalid or pWsTradeClient null", acc.accountName);
            return;
        }
        LOG_INFO("TB {} UF ws send session.logon", acc.accountName);
        pWsTradeClient->send_text(buildLogonJson());
    });

    pWsTradeClient->on_message([this](const uint8_t* d, size_t n, bool b, int64_t t) {
        this->onWsTradeMsg(d, n, b, t);
    });

    pWsTradeClient->on_close([this](int c, const::std::string& r) {
        LOG_WARN("TB {} UF user data stream closed code={} reason={} (auto connect)", acc.accountName, c, r);
        isTradeConnected.store(false);
        wsLoggedIn_.store(false);
        clearPending();
    });

    pWsTradeClient->on_error([this](const std::string& m) {
        LOG_ERROR("TB {} UF user data stream error: {}", acc.accountName, m);
        isTradeConnected.store(false);
        
    });

    pWsTradeClient->start();

    renewThread_ = std::thread([this] {
        listenKeyRenewLoop();
    });
}

// ============================================================================
void BinanceUFWsTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool, int64_t) {
    try {
        std::string msg(reinterpret_cast<const char*>(data), len);
        std::cout << "onWebsocketMsg: " << msg << std::endl;

        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc = g_parser.iterate(padded);
        if (doc.error()) {
            return;
        }

        auto doc_value = doc.get_object().value_unsafe();

        std::string_view e_sv;
        simdjson::ondemand::object o_obj;
        simdjson::ondemand::object a_obj;
        bool has_order_trade_update = false;
        bool has_account_update = false;

        for (auto field : doc_value) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "e") {
                field.value().get(e_sv);
                if (e_sv == "ORDER_TRADE_UPDATE") {
                   has_order_trade_update = true; 
                }
                else if (e_sv == "ACCOUNT_UPDATE") {
                    has_account_update = true;
                }
                if (e_sv == "listenKeyExpired") {
                    LOG_WARN("TB {} listenKey expired, will regen", acc.accountName);
                    std::thread([this]{
                        if (generateListenKeySync()) {
                            LOG_INFO("TB {} listenKey regenerated, will reconnect", acc.accountName);
                            net::WsConfig cfg;
                            cfg.url = acc.wsUrl + wsSubPath + listenKey_;
                            cfg.ping_mode = net::WsConfig::PingMode::ServerOnly;
                            cfg.auto_reconnect = true;
                            cfg.idle_timeout_sec = 60;
                            LOG_INFO("TB {} UF ws userdata reconnect", acc.accountName, cfg.url);
                            subWebsocketWithConfig(std::move(cfg));
                        }
                    }).detach();
                }
            }
            else if (k == "o") {
                if (has_order_trade_update && field.value().get(o_obj) == simdjson::SUCCESS) {
                    handleOrderUpdate(o_obj);
                }
            }
            else if (k == "a") {
                if (has_account_update && field.value().get(a_obj) == simdjson::SUCCESS) {
                    handleAccountUpdate(a_obj);
                }
            }
        }
    }
    catch (const std::exception& e) {
        LOG_ERROR("TB {} UF ws msg exc: {}", acc.accountName, e.what());
    }
}


// ============================================================================
// onWebsocketMsg
// ============================================================================
void BinanceUFWsTradeUnit::onWsTradeMsg(const uint8_t* data, size_t len, bool /*isBinary*/, int64_t recv_ns) {
    try {
        std::string msg(reinterpret_cast<const char*>(data), len);
        std::cout << "onWsTradeMsg: " << msg << std::endl;

        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc = g_parser.iterate(padded);
        if (doc.error()) {
            return;
        }

        auto doc_value = doc.get_object().value_unsafe();
    
        // ws-api 响应有 "id" 字段, userDataStream 推送外层是 {"event":{...}}
        int64_t id = 0;
        int status = 0;
        simdjson::ondemand::object result;
        simdjson::ondemand::object error;
        bool has_id = false;
        bool has_result = false;
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
                break;
            }
            else if (k == "error") {
                has_error = field.value().get(error) == simdjson::SUCCESS;
                break;
            }
        }

        if (has_id) {
            if (id == kSessionLogonId) {
                if (status == 200) {
                    wsLoggedIn_.store(true);
                    LOG_INFO("TB {} spot session.logon OK", acc.accountName);
                } else {
                    wsLoggedIn_.store(false);
                    if (has_error) {
                        std::string_view msg_sv;
                        error["msg"].get(msg_sv);
                        LOG_ERROR("TB {} spot session.logon FAILED status={} msg={}", acc.accountName, status, msg_sv);
                    }
                }
            }
            else {
                WsPending pending;
                if (status == 200 && has_result) {
                    if (takePending(id, pending)) {
                        handleWsApiResponse(pending, result);
                    }
                }
                else if (has_error) {       
                    if (takePending(id, pending)) {
                        handleWsApiError(pending, error);
                    }
                }     
            }
        }
    } catch (const std::exception& e) {
        LOG_ERROR("TB {} ws msg exc: {}", acc.accountName, e.what());
    }
}

// ============================================================================
// ws-api 响应分派
// ============================================================================
void BinanceUFWsTradeUnit::handleWsApiResponse(WsPending& pending, simdjson::ondemand::object& result) {
    pubsub::RCommand& rcmd = pending.rcmd;

    // 测试单不上报
    if (rcmd.body.orderResponse.clientOrderId == TESTCLIENTORDERID) {
        return;
    }
 
    md::InstrumentInfo info;
    if (!smc->get_instrument_info(rcmd.body.orderResponse.exchangeTypeEnum, rcmd.body.orderResponse.instTypeEnum, rcmd.body.orderResponse.instId, info)) {
        LOG_ERROR("TB {} exec report smc miss: {}", acc.accountName, rcmd.body.orderResponse.instId);
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
        std::string_view avgP_sv;

        for (auto field : result) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "orderId") {
                field.value().get(orderId);
            }
            else if (k == "executedQty") {
                field.value().get(execQ_sv);
            }
            else if (k == "avgPrice") {
                field.value().get(avgP_sv);
            }
        }

        fmt::format_to(rcmd.body.orderResponse.orderId, "{}", orderId);
        if (!execQ_sv.empty()) {
            rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(execQ_sv) * info.magnifyNumber;
        }
        if (!avgP_sv.empty()) {
            rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avgP_sv) * info.reduceNumber;
        }

        rcmd.body.orderResponse.orderStatus = OS_CANCELED;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
    }
}

void BinanceUFWsTradeUnit::handleWsApiError(WsPending& pending, simdjson::ondemand::object& error) {
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

// ---- ACCOUNT_UPDATE ----
//   { "e":"ACCOUNT_UPDATE", "a":{ "B":[{a,cw,bc,wb}], "P":[{s,pa,ps,iw,ep,up}] } }
void BinanceUFWsTradeUnit::handleAccountUpdate(simdjson::ondemand::object& a) {
    simdjson::ondemand::array balances;
    simdjson::ondemand::array positions;
    for (auto field : a) {
        std::string_view k = field.unescaped_key().value_unsafe();
        if (k == "B") {
            if (field.value().get(balances) == simdjson::SUCCESS) {      
                for (auto b_val : balances) {
                    auto b_res = b_val.get_object();
                    if (b_res.error()) {
                        continue;
                    }
                    auto& b = b_res.value_unsafe();

                    std::string_view a_sv;
                    std::string_view wb_sv;
                    std::string_view cw_sv;
                    std::string_view bc_sv;
                    for (auto field : b) {
                        std::string_view k = field.unescaped_key().value_unsafe();
                        if (k == "a") {
                            field.value().get(a_sv);
                        }
                        else if (k == "wb") {
                            field.value().get(wb_sv);
                        }
                        else if (k == "cw") {
                            field.value().get(cw_sv);
                        }
                        else if (k == "bc") {
                            field.value().get(bc_sv);
                        }
                    }

                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = BINANCE;
                    rcmd.body.balance.instTypeEnum = USDT_SWAP;
                    crypto::copy_sv_to_char_array(rcmd.body.balance.accountName, acc.accountName);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(a_sv)));
                    rcmd.body.balance.total = crypto::fast_atod(wb_sv);
                    rcmd.body.balance.available = crypto::fast_atod(cw_sv);
                    rcmd.body.balance.frozen = crypto::fast_atod(bc_sv);
                    rcmd.body.balance.updateTime = crypto::getCurrentTime();
                    rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;
                    PUSH_RCMD(rcmd)
                }
            }
        }
        else if (k == "P") {
            if (field.value().get(positions) == simdjson::SUCCESS) {      
                for (auto b_val : positions) {
                    auto b_res = b_val.get_object();
                    if (b_res.error()) {
                        continue;
                    }
                    auto& b = b_res.value_unsafe();

                    std::string_view s_sv;
                    std::string_view pa_sv;
                    std::string_view ep_sv;
                    std::string_view up_sv;
                    std::string_view iw_sv;
                    std::string_view ps_sv;   
                    for (auto field : b) {
                        std::string_view k = field.unescaped_key().value_unsafe();
                        if (k == "s") {
                            field.value().get(s_sv);
                        }
                        else if (k == "pa") {
                            field.value().get(pa_sv);
                        }
                        else if (k == "ep") {
                            field.value().get(ep_sv);
                        }
                        else if (k == "up") {
                            field.value().get(up_sv);
                        }
                        else if (k == "iw") {
                            field.value().get(iw_sv);
                        }
                        else if (k == "ps") {
                            field.value().get(ps_sv);
                        }
                    }

                    if (ps_sv.empty() || ps_sv[0] != 'B') {
                        continue;   // 只要 BOTH 单向持仓
                    }

                    std::string originInstId(s_sv);
                    md::InstrumentInfo info;
                    InstType inst = USDT_SWAP;
                    if (smc->get_instrument_info(BINANCE, USDT_SWAP, originInstId.c_str(), info)) {
                        inst = USDT_SWAP;
                    } else if (smc->get_instrument_info(BINANCE, USDT_FUTURES, originInstId.c_str(), info)) {
                        inst = USDT_FUTURES;
                    } else {
                        continue;
                    }

                    double positionAmt = crypto::fast_atod(pa_sv);
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                    rcmd.body.position.exchangeTypeEnum = BINANCE;
                    rcmd.body.position.instTypeEnum = inst;
                    crypto::copy_sv_to_char_array(rcmd.body.position.accountName, acc.accountName);
                    crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view(info.instId));
                    rcmd.body.position.direction = positionAmt >= 0 ? DT_LONG : DT_SHORT;
                    rcmd.body.position.volume = std::fabs(positionAmt) * info.magnifyNumber;
                    rcmd.body.position.maintMargin = crypto::fast_atod(iw_sv);
                    rcmd.body.position.avgPrice = crypto::fast_atod(ep_sv) * info.reduceNumber;
                    rcmd.body.position.unrealizedPnl = crypto::fast_atod(up_sv);
                    rcmd.body.position.updateTime = crypto::getCurrentTime();
                    rcmd.body.position.apiSourceEnum = AS_WEBSOCKET;
                    PUSH_RCMD(rcmd);
                }
            }
        }
    }
}
                                
void BinanceUFWsTradeUnit::handleOrderUpdate(simdjson::ondemand::object& o) {
    std::string_view s_sv;
    std::string_view c_sv;
    std::string_view S_sv;
    std::string_view o_sv;
    std::string_view f_sv;
    std::string_view q_sv;
    std::string_view p_sv;
    std::string_view ap_sv;
    std::string_view X_sv;
    int64_t i_val = 0;
    std::string_view l_sv;
    std::string_view z_sv;
    std::string_view L_sv;
    for (auto field : o) {
        std::string_view k = field.unescaped_key().value_unsafe();
        if (k == "s") {
            field.value().get(s_sv);
        }
        else if (k == "c") {
            field.value().get(c_sv);
        }
        else if (k == "S") {
            field.value().get(S_sv);
        }
        else if (k == "o") {
            field.value().get(o_sv);
        }
        else if (k == "f") {
            field.value().get(f_sv);
        }
        else if (k == "q") {
            field.value().get(q_sv);
        }
        else if (k == "p") {
            field.value().get(p_sv);
        }
        else if (k == "ap") {
            field.value().get(ap_sv);
        }
        else if (k == "X") {
            field.value().get(X_sv);
        }
        else if (k == "i") {
            field.value().get(i_val);
        }
        else if (k == "l") {
            field.value().get(l_sv);
        }
        else if (k == "z") {
            field.value().get(z_sv);
        }
        else if (k == "L") {
            field.value().get(L_sv);
        }
    }

    std::string originInstId(s_sv);
    md::InstrumentInfo info;
    InstType inst = USDT_SWAP;
    if (smc->get_instrument_info(BINANCE, USDT_SWAP, originInstId.c_str(), info)) {
        inst = USDT_SWAP;
    } else if (smc->get_instrument_info(BINANCE, USDT_FUTURES, originInstId.c_str(), info)) {
        inst = USDT_FUTURES;
    } else {
        LOG_ERROR("TB {} UF order upd smc miss: {}", acc.accountName, originInstId);
        return;
    }

    pubsub::RCommand rcmd;
    memset(&rcmd, 0, sizeof(pubsub::RCommand));
    rcmd.cmdTypeEnum = pubsub::CMD_RPT_NEW_ORDER;
    rcmd.body.orderResponse.exchangeTypeEnum = BINANCE;
    rcmd.body.orderResponse.instTypeEnum = inst;
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountName, acc.accountName);
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.strategyId, acc.strategyId);
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.instId, std::string_view(info.instId));
    fmt::format_to(rcmd.body.orderResponse.orderId, "{}", i_val);
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, c_sv);

    rcmd.body.orderResponse.offsetFlag = OF_OPEN;
    if (!S_sv.empty()) {
        rcmd.body.orderResponse.direction = (S_sv[0] == 'B') ? DT_LONG : DT_SHORT;
    }

    std::string tif(f_sv);
    std::string ot(o_sv);
    rcmd.body.orderResponse.orderType = crypto::get_binance_ordertype(tif.c_str(), ot.c_str());
    rcmd.body.orderResponse.volumeTotal = crypto::fast_atod(q_sv) * info.magnifyNumber;
    rcmd.body.orderResponse.limitPrice = crypto::fast_atod(p_sv) * info.reduceNumber;
    rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(z_sv) * info.magnifyNumber;
    rcmd.body.orderResponse.tradePrice = crypto::fast_atod(ap_sv) * info.reduceNumber;
    rcmd.body.orderResponse.tradeDiff = crypto::fast_atod(l_sv) * info.magnifyNumber;
    rcmd.body.orderResponse.fillPrice = crypto::fast_atod(L_sv) * info.reduceNumber;

    std::string X_str(X_sv);
    rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(X_str);
    
    if (c_sv[0] == 'a' && c_sv[2] == 't') {
        rcmd.body.orderResponse.errorId = LiquidationError;
    }
    else if (c_sv[0] == 'a' && c_sv[2] == 'l') {
        rcmd.body.orderResponse.errorId = ADLError;
    }

    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
    rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
    PUSH_RCMD(rcmd)
}

// ============================================================================
// query_* (REST, Ed25519)
// ============================================================================
void BinanceUFWsTradeUnit::query_account(const pubsub::TCommand& tcmd) { 
    query_balance(tcmd);
}

void BinanceUFWsTradeUnit::query_balance(const pubsub::TCommand& tcmd) {
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"recvWindow", "5000"},
        {"timestamp",  std::to_string(crypto::getCurrentTimeMilli())},
    };
    std::string path = buildRestSignedPath(balanceUrl, kvs);

    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "", [this](boost::system::error_code ec, ::net::HttpResponse resp) {
        if (ec) { 
            LOG_ERROR("TB {} UF query_balance ec: {}", acc.accountName, ec.message()); 
            return; 
        }
        
        try {
            std::cout << "query balance: " << resp.body << std::endl;
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                return;
            }

            simdjson::ondemand::array assets;
            if (doc["assets"].get(assets) != simdjson::SUCCESS) {
                LOG_ERROR("TB {} UF query_balance no 'assets': {}", acc.accountName, resp.body);
                return;
            }

            std::vector<pubsub::RCommand> pendingBalances;
            pendingBalances.reserve(8);

            for (auto b_val : assets) {
                auto b_res = b_val.get_object();
                if (b_res.error()) {
                    continue;
                }
                auto& b = b_res.value_unsafe();

                std::string_view asset_sv;
                std::string_view walletBalance_sv;
                std::string_view marginBalance_sv;
                std::string_view availableBalance_sv;
                for (auto field : b) {
                    std::string_view k = field.unescaped_key().value_unsafe();
                    if (k == "asset") {
                        field.value().get(asset_sv);
                    }
                    else if (k == "walletBalance") {
                        field.value().get(walletBalance_sv);
                    }
                    else if (k == "marginBalance") {
                        field.value().get(marginBalance_sv);
                    }
                    else if (k == "availableBalance") {
                        field.value().get(availableBalance_sv);
                    }
                }

                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                rcmd.body.balance.exchangeTypeEnum = BINANCE;
                rcmd.body.balance.instTypeEnum = USDT_SWAP;
                crypto::copy_sv_to_char_array(rcmd.body.balance.accountName, acc.accountName);
                crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(asset_sv)));
                rcmd.body.balance.available = crypto::fast_atod(availableBalance_sv);
                rcmd.body.balance.frozen = crypto::fast_atod(marginBalance_sv);
                rcmd.body.balance.total = crypto::fast_atod(walletBalance_sv);
                rcmd.body.balance.updateTime = crypto::getCurrentTime();
                rcmd.body.balance.apiSourceEnum = AS_REST;
                pendingBalances.emplace_back(rcmd);
            }

            if (pendingBalances.empty()) {
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                rcmd.body.balance.exchangeTypeEnum = BINANCE;
                rcmd.body.balance.instTypeEnum = USDT_SWAP;
                crypto::copy_sv_to_char_array(rcmd.body.balance.accountName, acc.accountName);
                crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.currency, std::string("USDT"));
                rcmd.body.balance.updateTime = crypto::getCurrentTime();
                rcmd.body.balance.apiSourceEnum = AS_REST;
                rcmd.body.balance.isLast = true;
                PUSH_RCMD(rcmd);
                return;
            }
            for (size_t i = 0; i < pendingBalances.size(); ++i) {
                pendingBalances[i].body.balance.isLast = (i + 1 == pendingBalances.size());
                PUSH_RCMD(pendingBalances[i]);
            }


            simdjson::ondemand::array positions;
            if (doc["positions"].get(positions) != simdjson::SUCCESS) {
                LOG_ERROR("TB {} UF query_position not array: {}", acc.accountName, resp.body);
                return;
            }

            std::vector<pubsub::RCommand> pendingPositions;
            pendingPositions.reserve(8);
            
            for (auto b_val : positions) {
                auto b_res = b_val.get_object();
                if (b_res.error()) {
                    continue;
                }
                auto& b = b_res.value_unsafe();

                std::string_view symbol_sv;
                std::string_view positionSide_sv;
                std::string_view positionAmt_sv;
                std::string_view entryPrice_sv;
                std::string_view markPrice_sv;
                std::string_view unRealizedProfit_sv;
                std::string_view liquidationPrice_sv;
                std::string_view maintMargin_sv;
                int64_t adl = 0;
                for (auto field : b) {
                    std::string_view k = field.unescaped_key().value_unsafe();
                    if (k == "symbol") {
                        field.value().get(symbol_sv);
                    }
                    else if (k == "positionSide") {
                        field.value().get(positionSide_sv);
                    }
                    else if (k == "positionAmt") {
                        field.value().get(positionAmt_sv);
                    }
                    else if (k == "entryPrice") {
                        field.value().get(entryPrice_sv);
                    }
                    else if (k == "markPrice") {
                        field.value().get(markPrice_sv);
                    }
                    else if (k == "unRealizedProfit") {
                        field.value().get(unRealizedProfit_sv);
                    }
                    else if (k == "liquidationPrice") {
                        field.value().get(liquidationPrice_sv);
                    }
                    else if (k == "maintMargin") {
                        field.value().get(maintMargin_sv);
                    }
                    else if (k == "adl") {
                        field.value().get(adl);
                    }
                }

                if (positionSide_sv.empty() || positionSide_sv[0] != 'B') {
                    continue;
                }

                std::string originInstId(symbol_sv);
                md::InstrumentInfo info;
                InstType inst = USDT_SWAP;
                if (smc->get_instrument_info(BINANCE, USDT_SWAP, originInstId.c_str(), info)) {
                    inst = USDT_SWAP;
                }
                else if (smc->get_instrument_info(BINANCE, USDT_FUTURES, originInstId.c_str(), info)) {
                    inst = USDT_FUTURES;
                } 
                else {
                    continue;
                }

                double positionAmt = crypto::fast_atod(positionAmt_sv);
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                rcmd.body.position.exchangeTypeEnum = BINANCE;
                rcmd.body.position.instTypeEnum = inst;
                crypto::copy_sv_to_char_array(rcmd.body.position.accountName, acc.accountName);
                crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view(info.instId));
                rcmd.body.position.direction = positionAmt >= 0 ? DT_LONG : DT_SHORT;
                rcmd.body.position.volume = std::abs(positionAmt) * info.magnifyNumber;
                rcmd.body.position.maintMargin = crypto::fast_atod(maintMargin_sv);
                rcmd.body.position.avgPrice = crypto::fast_atod(entryPrice_sv) * info.reduceNumber;
                rcmd.body.position.unrealizedPnl = crypto::fast_atod(unRealizedProfit_sv);
                rcmd.body.position.markPrice = crypto::fast_atod(markPrice_sv) * info.reduceNumber;
                rcmd.body.position.liquidPrice = crypto::fast_atod(liquidationPrice_sv) * info.reduceNumber;
                rcmd.body.position.adlQuantile = static_cast<int>(adl) + 1;
                rcmd.body.position.updateTime = crypto::getCurrentTime();
                rcmd.body.position.apiSourceEnum = AS_REST;
                pendingPositions.emplace_back(rcmd);
            }

            if (pendingPositions.empty()) {
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                rcmd.body.position.exchangeTypeEnum = BINANCE;
                rcmd.body.position.instTypeEnum = USDT_SWAP;
                crypto::copy_sv_to_char_array(rcmd.body.position.accountName, acc.accountName);
                crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view("BTC-USDT"));
                rcmd.body.position.updateTime = crypto::getCurrentTime();
                rcmd.body.position.apiSourceEnum = AS_REST;
                rcmd.body.position.isLast = true;
                PUSH_RCMD(rcmd);
                return;
            }
            for (size_t i = 0; i < pendingPositions.size(); ++i) {
                pendingPositions[i].body.position.isLast = (i + 1 == pendingPositions.size());
                PUSH_RCMD(pendingPositions[i]);
            }
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} UF query_balance cb exc: {}", acc.accountName, e.what());
        }
    });
}

void BinanceUFWsTradeUnit::query_position(const pubsub::TCommand&) {
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"recvWindow", "5000"},
        {"timestamp",  std::to_string(crypto::getCurrentTimeMilli())},
    };
    std::string path = buildRestSignedPath(positionUrl, kvs);

    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "", [this](boost::system::error_code ec, ::net::HttpResponse resp) {
        if (ec) { 
            LOG_ERROR("TB {} UF query_position ec: {}", acc.accountName, ec.message()); 
            return; 
        }
        try {
            std::cout << "query position: " << resp.body << std::endl;
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                return; 
            }

            simdjson::ondemand::array positions;
            if (doc.get_array().get(positions) != simdjson::SUCCESS) {
                LOG_ERROR("TB {} UF query_position not array: {}", acc.accountName, resp.body);
                return;
            }

            std::vector<pubsub::RCommand> pending;
            for (auto b_val : positions) {
                auto b_res = b_val.get_object();
                if (b_res.error()) {
                    continue;
                }
                auto& b = b_res.value_unsafe();

                std::string_view symbol_sv;
                std::string_view positionSide_sv;
                std::string_view positionAmt_sv;
                std::string_view entryPrice_sv;
                std::string_view markPrice_sv;
                std::string_view unRealizedProfit_sv;
                std::string_view liquidationPrice_sv;
                std::string_view maintMargin_sv;
                int64_t adl = 0;
                for (auto field : b) {
                    std::string_view k = field.unescaped_key().value_unsafe();
                    if (k == "symbol") {
                        field.value().get(symbol_sv);
                    }
                    else if (k == "positionSide") {
                        field.value().get(positionSide_sv);
                    }
                    else if (k == "positionAmt") {
                        field.value().get(positionAmt_sv);
                    }
                    else if (k == "entryPrice") {
                        field.value().get(entryPrice_sv);
                    }
                    else if (k == "markPrice") {
                        field.value().get(markPrice_sv);
                    }
                    else if (k == "unRealizedProfit") {
                        field.value().get(unRealizedProfit_sv);
                    }
                    else if (k == "liquidationPrice") {
                        field.value().get(liquidationPrice_sv);
                    }
                    else if (k == "maintMargin") {
                        field.value().get(maintMargin_sv);
                    }
                    else if (k == "adl") {
                        field.value().get(adl);
                    }
                }

                if (positionSide_sv.empty() || positionSide_sv[0] != 'B') {
                    continue;
                }

                std::string originInstId(symbol_sv);
                md::InstrumentInfo info;
                InstType inst = USDT_SWAP;
                if (smc->get_instrument_info(BINANCE, USDT_SWAP, originInstId.c_str(), info)) {
                    inst = USDT_SWAP;
                }
                else if (smc->get_instrument_info(BINANCE, USDT_FUTURES, originInstId.c_str(), info)) {
                    inst = USDT_FUTURES;
                } 
                else {
                    continue;
                }

                double positionAmt = crypto::fast_atod(positionAmt_sv);
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                rcmd.body.position.exchangeTypeEnum = BINANCE;
                rcmd.body.position.instTypeEnum = inst;
                crypto::copy_sv_to_char_array(rcmd.body.position.accountName, acc.accountName);
                crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view(info.instId));
                rcmd.body.position.direction = positionAmt >= 0 ? DT_LONG : DT_SHORT;
                rcmd.body.position.volume = std::abs(positionAmt) * info.magnifyNumber;
                rcmd.body.position.maintMargin = crypto::fast_atod(maintMargin_sv);
                rcmd.body.position.avgPrice = crypto::fast_atod(entryPrice_sv) * info.reduceNumber;
                rcmd.body.position.unrealizedPnl = crypto::fast_atod(unRealizedProfit_sv);
                rcmd.body.position.markPrice = crypto::fast_atod(markPrice_sv) * info.reduceNumber;
                rcmd.body.position.liquidPrice = crypto::fast_atod(liquidationPrice_sv) * info.reduceNumber;
                rcmd.body.position.adlQuantile = static_cast<int>(adl) + 1;
                rcmd.body.position.updateTime = crypto::getCurrentTime();
                rcmd.body.position.apiSourceEnum = AS_REST;
                pending.emplace_back(rcmd);
            }

            if (pending.empty()) {
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                rcmd.body.position.exchangeTypeEnum = BINANCE;
                rcmd.body.position.instTypeEnum = USDT_SWAP;
                crypto::copy_sv_to_char_array(rcmd.body.position.accountName, acc.accountName);
                crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view("BTC-USDT"));
                rcmd.body.position.updateTime = crypto::getCurrentTime();
                rcmd.body.position.apiSourceEnum = AS_REST;
                rcmd.body.position.isLast = true;
                PUSH_RCMD(rcmd);
                return;
            }
            for (size_t i = 0; i < pending.size(); ++i) {
                pending[i].body.position.isLast = (i + 1 == pending.size());
                PUSH_RCMD(pending[i]);
            }
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} UF query_position cb exc: {}", acc.accountName, e.what());
        }
    });
}

void BinanceUFWsTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum, tcmd.body.queryOrder.instTypeEnum, tcmd.body.queryOrder.instId, info)) {
        LOG_INFO("TB {} UF query_order smc miss: {}", acc.accountName, tcmd.body.queryOrder.instId);
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
    LOG_INFO("TB {} UF query_order: {}", acc.accountName, path);

    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "", [this, rcmd, info](boost::system::error_code ec, net::HttpResponse resp) mutable {
        if (ec) {
            LOG_ERROR("TB {} query_order ec: {}", acc.accountName, ec.message());
            return;
        }
        try {
            std::cout << "query order: " << resp.body << std::endl;
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                LOG_ERROR("TB {} query_order parse err: {}", acc.accountName, resp.body);
                return;
            }

            auto doc_value = doc.get_object().value_unsafe();

            int64_t code = 0;
            int64_t orderId = 0;
            std::string_view origQ_sv;
            std::string_view price_sv;
            std::string_view execQ_sv;
            std::string_view avgP_sv;
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
                else if (k == "avgPrice") {
                    field.value().get(avgP_sv);
                }
                else if (k == "status") {
                    field.value().get(status_sv);
                }
            }

            if (has_code) {
                int64_t now = crypto::getCurrentTime();
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

                if (!avgP_sv.empty()) {
                    rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avgP_sv) * info.reduceNumber;;
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
            LOG_ERROR("TB {} query_order cb exception: {}", acc.accountName, e.what());
        }
    });
}


// ============================================================================
// add_new_order (WS order.place, 无 REST 兜底)
// ============================================================================
void BinanceUFWsTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (!isTradeConnected.load() || !wsLoggedIn_.load()) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId = TBDisconnectError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
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
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const char* type = nullptr;
    const char* tif = nullptr;
    const char* respTyp = "ACK";
    switch (tcmd.body.newOrder.orderType) {
        // UF 的 POST_ONLY 用 GTX (LIMIT+GTX), 不是 LIMIT_MAKER
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
            type = "LIMIT";  
            tif = "GTX"; 
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

    recordPending(wsId, pubsub::CMD_NEW_ORDER, rcmd);
    LOG_INFO("TB {} UF ws order.place id={} msg={}", acc.accountName, wsId, msg);
    pWsTradeClient->send_text(std::move(msg));
}

// ============================================================================
// cancel_order (WS order.cancel)
// ============================================================================
void BinanceUFWsTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    if (!isTradeConnected.load() || !wsLoggedIn_.load()) {
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
    LOG_INFO("TB {} UF ws order.cancel id={} msg={}", acc.accountName, wsId, msg);
    pWsTradeClient->send_text(std::move(msg));
}