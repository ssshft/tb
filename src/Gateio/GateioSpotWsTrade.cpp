#include "Gateio/GateioSpotWsTrade.h"
#include <cmath>
#include <cstdlib>
#include <fmt/format.h>
#include <simdjson.h>


GateioSpotWsTradeUnit::GateioSpotWsTradeUnit(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {

}

GateioSpotWsTradeUnit::~GateioSpotWsTradeUnit() = default;


// ============================================================================
// WS JSON builders
// ============================================================================
std::string GateioSpotWsTradeUnit::buildLoginJson(int64_t ts) const {
    // sign payload: channel=spot.login&event=api&time=T
    std::string time_str = std::to_string(ts);
    std::string sign = crypto::getGateioSignatureWsApi("spot.login", "api", time_str, "", acc.secretKey);
    // req_id 用固定的 "1" (kLoginId)
    return fmt::format(
        R"({{"time":{},"channel":"spot.login","event":"api","payload":{{)"
        R"("req_id":"{}","req_header":{{}},)"
        R"("api_key":"{}","signature":"{}","timestamp":"{}"}}}})",
        ts, kLoginId, acc.apiKey, sign, time_str);
}

std::string GateioSpotWsTradeUnit::buildSubscribeJson(int reqId, const char* channel, const char* payload_first, const char* payload_second) const {
    // session-authed, payload 里无 auth 字段
    // spot.orders 订阅 payload=["!all"] 表示订阅所有 pair
    // spot.balances 订阅 payload=[] 表示所有 currency
    int64_t ts = crypto::getCurrentTimeSeconds();
    std::string j;
    j.reserve(160);
    j.append(R"({"time":)");
    j.append(std::to_string(ts));
    j.append(R"(,"channel":")"); 
    j.append(channel);                                    
    j.push_back('"');
    j.append(R"(,"event":"subscribe","payload":[)");

    if (payload_first) { 
        j.push_back('"'); 
        j.append(payload_first);  
        j.push_back('"'); 
    }

    if (payload_second) { 
        if (payload_first) {
            j.push_back(',');
        } 
        j.push_back('"'); 
        j.append(payload_second); 
        j.push_back('"'); 
    }

    j.append(R"(],"req_id":")"); 
    j.append(std::to_string(reqId));                     
    j.push_back('"');
    j.push_back('}');
    return j;
}

std::string GateioSpotWsTradeUnit::buildOrderPlaceJson( int reqId, const pubsub::TCommand& tcmd, const md::InstrumentInfo& info,
    const std::string& price, const std::string& amount,
    const char* side, const char* tif) const {
    // session-authed, payload 里无 auth 字段, 只带 req_id + req_param
    int64_t ts = crypto::getCurrentTimeSeconds();
    std::string j;
    j.reserve(400);
    j.append(R"({"time":)"); 
    j.append(std::to_string(ts));
    j.append(R"(,"channel":"spot.order_place","event":"api","payload":{)");
    j.append(R"("req_id":")");                     
    j.append(std::to_string(reqId));          
    j.push_back('"');
    j.append(R"(,"req_header":{},"req_param":{)");
    j.append(R"("text":")");                       
    j.append(tcmd.body.newOrder.orderSysId); 
    j.push_back('"');
    j.append(R"(,"currency_pair":")");             
    j.append(info.originInstId);                       
    j.push_back('"');
    j.append(R"(,"type":"limit","account":")");
#ifdef USE_GATEIO_UNIFIED
    j.append("unified");
#else
    j.append(tcmd.body.newOrder.instTypeEnum == MARGIN ? "margin" : "spot");
#endif
    j.push_back('"');
    j.append(R"(,"side":")");                      
    j.append(side);                                    
    j.push_back('"');
    j.append(R"(,"amount":")");                    
    j.append(amount);                                  
    j.push_back('"');
    j.append(R"(,"price":")");                     
    j.append(price);                                   
    j.push_back('"');
    j.append(R"(,"time_in_force":")");             
    j.append(tif);                                     
    j.push_back('"');
#ifdef USE_GATEIO_UNIFIED
    j.append(R"(,"auto_borrow":true,"auto_repay":true)");
#endif
    j.append("}}}");
    return j;
}

std::string GateioSpotWsTradeUnit::buildOrderCancelJson(int reqId, const pubsub::TCommand& tcmd, const md::InstrumentInfo& info) const {
    int64_t ts = crypto::getCurrentTimeSeconds();
    std::string j;
    j.reserve(240);
    j.append(R"({"time":)"); 
    j.append(std::to_string(ts));
    j.append(R"(,"channel":"spot.order_cancel","event":"api","payload":{)");
    j.append(R"("req_id":")");                     
    j.append(std::to_string(reqId));                    
    j.push_back('"');
    j.append(R"(,"req_header":{},"req_param":{)");
    if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
        j.append(R"("order_id":")");               
        j.append(tcmd.body.cancelOrder.orderId);            
        j.push_back('"');
    } else {
        j.append(R"("order_id":")");               
        j.append(tcmd.body.cancelOrder.orderSysId); 
        j.push_back('"');
    }
    j.append(R"(,"currency_pair":")");             
    j.append(info.originInstId);                       
    j.push_back('"');
#ifdef USE_GATEIO_UNIFIED
    j.append(R"(,"account":"unified")");
#endif
    j.append("}}}");
    return j;
}

// ============================================================================
// pending map
// ============================================================================
void GateioSpotWsTradeUnit::recordPending(int id, pubsub::CommandType type, const pubsub::RCommand& rcmd) {
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

bool GateioSpotWsTradeUnit::takePending(int id, WsPending& out) {
    tbb::concurrent_hash_map<int, WsPending>::accessor acc;
    if (!pendingMap_.find(acc, id)) {
        return false;
    }

    out = std::move(acc->second);
    pendingMap_.erase(acc);
    return true;
}

void GateioSpotWsTradeUnit::clearPending() {
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
// subWebsocekt
// ============================================================================
void GateioSpotWsTradeUnit::subWebsocekt() {
    std::string restHost = crypto::host_of(acc.restUrl);
    initRestClient(restHost, {}, 4);

    net::WsConfig cfg;
    cfg.url = acc.wsUrl;   // wss://api.gateio.ws/ws/v4/
    cfg.ping_mode = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text = R"({"channel":"spot.ping"})";
    cfg.auto_reconnect = true;
    cfg.idle_timeout_sec = 60;
    LOG_INFO("TB {} Gate spot ws {} rest {}", acc.accountName, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));
}

// ============================================================================
// onOpen / onCloseMsg
// ============================================================================
void GateioSpotWsTradeUnit::onOpen() {
    BaseTradeUnit::onOpen();
    wsLoggedIn_.store(false);
 
    int64_t ts = crypto::getCurrentTimeSeconds();
    std::string logon = buildLoginJson(ts);
    LOG_INFO("TB {} Gate spot ws send spot.login", acc.accountName);
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
void GateioSpotWsTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool /*isBinary*/, int64_t /*recv_ns*/) {
    try {
        std::string msg(reinterpret_cast<const char*>(data), len);
        std::cout << "onWebsocketMsg: " << msg << std::endl;

        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc = g_parser.iterate(padded);
        if (doc.error()) {
            return;
        }

        auto doc_value = doc.get_object().value_unsafe();
    
        // Gate v4 WS 有两类消息:
        //   1) RPC 响应: {"request_id":"...", "ack":..., "header":{...}, "data":{...}}
        //   2) 订阅推送: {"time":..., "channel":"spot.orders", "event":"update", "result":[...]}
        // 用 "request_id" 存在与否区分。

        std::string_view req_id_sv;
        std::string_view status_sv;
        simdjson::ondemand::object header;
        simdjson::ondemand::object data;
        bool has_request_id = false;
        bool has_data_result = false;
        bool has_data_error = false;

        OrderResultFields orf;
        ErrorFields ef;

        std::string_view channel_sv;
        std::string_view event_sv;
        simdjson::ondemand::array result_obj;

        bool has_balances = false;
        bool has_orders = false;
        bool has_event = false;

        for (auto field : doc_value) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "request_id") {
                has_request_id = field.value().get(req_id_sv) == simdjson::SUCCESS;
            }
            else if (k == "header") {
                if (field.value().get(header) == simdjson::SUCCESS) {
                    header["status"].get(status_sv);
                }
            }
            else if (k == "channel") {
                field.value().get(channel_sv);
                if (channel_sv == "spot.balances") {
                    has_balances = true; 
                }
                else if (channel_sv == "spot.orders") {
                    has_orders = true;
                }
            }
            else if (k == "event") {
               if (field.value().get(event_sv) == simdjson::SUCCESS) {
                    if (event_sv == "update") {
                       has_event = true; 
                    }
                }
            }
            else if (k == "data") {
                if (field.value().get(data) == simdjson::SUCCESS) {
                    for (auto d : data) {
                        std::string_view dk = d.unescaped_key().value_unsafe();
                        if (dk == "result") {
                            simdjson::ondemand::object res;
                            if (d.value().get(res) == simdjson::SUCCESS) {
                                has_data_result = true;
                                for (auto r : res) {
                                    std::string_view rk = r.unescaped_key().value_unsafe(); 
                                    if (rk == "id") {
                                        r.value().get(orf.id_sv);
                                    }
                                    else if (rk == "amount") {
                                        r.value().get(orf.amount_sv);
                                    }
                                    else if (rk == "left") {
                                        r.value().get(orf.left_sv);
                                    }
                                    else if (rk == "avg_deal_price") {
                                        r.value().get(orf.avg_sv);
                                    }
                                    else if (rk == "status") {
                                        r.value().get(orf.status_sv);
                                    } else if (rk == "req_id") { // 有req_id的回报不推送
                                        has_data_result = false;
                                    }
                                }
                            }
                        }
                        else if (dk == "errs") {
                            simdjson::ondemand::object err;
                            if (d.value().get(err) == simdjson::SUCCESS) {
                                has_data_error = true;
                                for (auto r : err) {
                                   std::string_view rk = r.unescaped_key().value_unsafe(); 
                                    if (rk == "label") {
                                        r.value().get(ef.label_sv);
                                    }
                                    else if (rk == "message") {
                                        r.value().get(ef.message_sv);
                                    }
                                }
                            }    
                        }
                    }
                }
            }
            else if (k == "result") {
                field.value().get(result_obj);
                break;
            }
        }

        auto id = crypto::fast_atol(req_id_sv);
        auto status = crypto::fast_atol(status_sv);
        if (has_request_id) {
            if (id == kLoginId) {
                if (status == 200) {
                    wsLoggedIn_.store(true);
                    LOG_INFO("TB {} spot session.logon OK, will subscribe userDataStream", acc.accountName);
                    if (pWsClient) {
                        pWsClient->send_text(buildSubscribeJson(kOrdersSubId, "spot.orders", "!all", nullptr));
                        pWsClient->send_text(buildSubscribeJson(kBalancesSubId, "spot.balances", nullptr, nullptr));
                    }
                } else {
                    wsLoggedIn_.store(false);
                    LOG_ERROR("TB {} Gate spot spot.login FAILED status={}", acc.accountName, status);
                }
            }
            else if (id == kOrdersSubId || id == kBalancesSubId) {
                if (status == 200) {
                    LOG_INFO("TB {} Gate spot subscribe reqId={} OK", acc.accountName, id);
                } else {
                    LOG_ERROR("TB {} Gate spot subscribe reqId={} FAILED status={}", acc.accountName, id, status);
                }
            }
            else {
                WsPending pending;
                if (status == 200 && has_data_result) {
                    if (takePending(id, pending)) {
                        handleWsApiResponse(pending, orf);
                    }
                }
                else if (has_data_error){ 
                    if (takePending(id, pending)) {
                        handleWsApiError(pending, ef);
                    }
                }     
            }
        }

        if (has_event) {
            if (has_balances) {
                handleBalancesUpdate(result_obj);
            }
            else if (has_orders) {
                handleOrdersUpdate(result_obj);
            }
        }
    } catch (const std::exception& e) {
        LOG_ERROR("TB {} Gate spot ws msg exc: {}", acc.accountName, e.what());
    }
}

void GateioSpotWsTradeUnit::handleWsApiResponse(WsPending& pending, const OrderResultFields& fields) {
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
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, fields.id_sv);
        if (!fields.avg_sv.empty()) {
            rcmd.body.orderResponse.tradePrice = crypto::fast_atod(fields.avg_sv);
        }

        double left = std::fabs(crypto::fast_atod(fields.left_sv));
        rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;

        if (fields.status_sv == "open") {
            rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.volumeTraded > ZERO_NUM) ? OS_PARTFILLED : OS_NEW;
        }
        else if (fields.status_sv == "closed") {
            rcmd.body.orderResponse.orderStatus = OS_FILLED;
        }
        else if (fields.status_sv == "cancelled") {
            rcmd.body.orderResponse.orderStatus = OS_CANCELED;
        }
        else {
            rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
        }        

        rcmd.body.orderResponse.errorId = NoError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();

        PUSH_RCMD(rcmd)
    } else if (pending.type == pubsub::CMD_CANCEL_ORDER) {
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, fields.id_sv);
        if (!fields.avg_sv.empty()) {
            rcmd.body.orderResponse.tradePrice = crypto::fast_atod(fields.avg_sv);
        }
        double amount = crypto::fast_atod(fields.amount_sv);
        double left = std::fabs(crypto::fast_atod(fields.left_sv));
        rcmd.body.orderResponse.volumeTraded = amount - left;

        rcmd.body.orderResponse.orderStatus = OS_CANCELED;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
    }
}

void GateioSpotWsTradeUnit::handleWsApiError(WsPending& pending, const ErrorFields& fields) {
    pubsub::RCommand& rcmd = pending.rcmd;

    // 测试单不上报
    if (rcmd.body.orderResponse.clientOrderId == TESTCLIENTORDERID) {
        return;
    }
 
    rcmd.body.orderResponse.errorId = crypto::get_gateio_errorid(std::string(fields.label_sv).c_str());

    if (pending.type == pubsub::CMD_NEW_ORDER) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;    
    } else if (pending.type == pubsub::CMD_CANCEL_ORDER) {
        rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.errorId == OrderNotFoundError) ? OS_REJECTED : OS_FAILED;
    }

    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, fields.message_sv);
    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
    PUSH_RCMD(rcmd)
}

// ---- spot.balances / spot.cross_balances update ----
void GateioSpotWsTradeUnit::handleBalancesUpdate(simdjson::ondemand::array& arr) {
    for (auto b_val : arr) {
        auto b_res = b_val.get_object();
        if (b_res.error()) {
            continue;
        }
        auto& b = b_res.value_unsafe();

        std::string_view cur_sv;
        std::string_view avail_sv;
        std::string_view total_sv;
        for (auto field : b) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "currency") {
                field.value().get(cur_sv);
            }
            else if (k == "available") {
                field.value().get(avail_sv);
            }
            else if (k == "total") {
                field.value().get(total_sv);
            }
        }

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
        rcmd.body.balance.exchangeTypeEnum = GATEIO;
        rcmd.body.balance.instTypeEnum = SPOT;
        crypto::copy_sv_to_char_array(rcmd.body.balance.accountName, acc.accountName);
        crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(cur_sv)));
        rcmd.body.balance.available = crypto::fast_atod(avail_sv);
        rcmd.body.balance.total = crypto::fast_atod(total_sv);
        rcmd.body.balance.updateTime = crypto::getCurrentTime();
        rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd)
    }
}

// ---- spot.orders update ----
void GateioSpotWsTradeUnit::handleOrdersUpdate(simdjson::ondemand::array& arr) {
    for (auto b_val : arr) {
        auto b_res = b_val.get_object();
        if (b_res.error()) {
            continue;
        }
        auto& b = b_res.value_unsafe();

        std::string_view pair_sv; 
        std::string_view id_sv;
        std::string_view text_sv;
        std::string_view price_sv;
        std::string_view amount_sv;
        std::string_view side_sv;
        std::string_view tif_sv;
        std::string_view left_sv;
        std::string_view avg_sv;
        std::string_view event_sv;
        for (auto field : b) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "currency_pair") {
                field.value().get(pair_sv);
            }
            else if (k == "id") {
                field.value().get(id_sv);
            }
            else if (k == "text") {
                field.value().get(text_sv);
            }
            else if (k == "price") {
                field.value().get(price_sv);
            }
            else if (k == "amount") {
                field.value().get(amount_sv);
            }
            else if (k == "side") {
                field.value().get(side_sv);
            }
            else if (k == "time_in_force") {
                field.value().get(tif_sv);
            }
            else if (k == "left") {
                field.value().get(left_sv);
            }
            else if (k == "avg_deal_price") {
                field.value().get(avg_sv);
            }
            else if (k == "event") {
                field.value().get(event_sv);
            }
        }

        std::string originInstId(pair_sv);
        md::InstrumentInfo info;
        if (!smc->get_instrument_info(GATEIO, SPOT, originInstId.c_str(), info)) {
            LOG_ERROR("TB {} not found GATEIO.SPOT.{} smc info", acc.accountName, originInstId);
            continue;
        }

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
        rcmd.body.orderResponse.exchangeTypeEnum = GATEIO;
        rcmd.body.orderResponse.instTypeEnum = SPOT;
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountName, acc.accountName);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.instId, std::string_view(info.instId));
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, id_sv);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, text_sv);

        rcmd.body.orderResponse.limitPrice = crypto::fast_atod(price_sv) * info.reduceNumber;
        rcmd.body.orderResponse.volumeTotal = crypto::fast_atod(amount_sv) * info.magnifyNumber;
        rcmd.body.orderResponse.offsetFlag = OF_OPEN;
        rcmd.body.orderResponse.direction = (!side_sv.empty() && side_sv[0] == 's') ? DT_SHORT : DT_LONG;

        if (!tif_sv.empty()) {
            switch (tif_sv[0]) {
                case 'g': 
                    rcmd.body.orderResponse.orderType = OT_LIMIT;      
                    break;
                case 'i': 
                    rcmd.body.orderResponse.orderType = OT_IOC;        
                    break;
                case 'p': 
                    rcmd.body.orderResponse.orderType = OT_POST_ONLY;  
                    break;
                case 'f': 
                    rcmd.body.orderResponse.orderType = OT_FOK;        
                    break;
                default:  break;
            }
        }

        double left = std::fabs(crypto::fast_atod(left_sv));
        rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;

        if (!avg_sv.empty()) {
            rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avg_sv);
        }

        // 如果接入成交推送，报单有成交的可以考虑不推送
        if (!event_sv.empty()) {
            switch (event_sv[0]) {
                case 'p': 
                    rcmd.body.orderResponse.orderStatus = OS_NEW;         
                    break;
                case 'u': 
                    rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;  
                    break;
                case 'f':
                    rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.volumeTotal - rcmd.body.orderResponse.volumeTraded < ZERO_NUM) ? OS_FILLED : OS_CANCELED;
                    break;
                default:  
                    rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;     
                    break;
            }
        }
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd)
    }
}

// ============================================================================
// query_* : REST (HMAC-SHA512 签名)
// ============================================================================
void GateioSpotWsTradeUnit::query_account (const pubsub::TCommand& tcmd) {
    query_balance(tcmd);
}

void GateioSpotWsTradeUnit::query_position(const pubsub::TCommand& tcmd) {

}

void GateioSpotWsTradeUnit::query_balance(const pubsub::TCommand& tcmd) {
    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign = crypto::getGateioSignatureRest("GET", balanceUrl, time_str, "", "", acc.secretKey);
    std::vector<std::pair<std::string, std::string>> headers = {{"KEY", acc.apiKey}, {"Timestamp", time_str}, {"SIGN", sign}};

    asyncRequest(boost::beast::http::verb::get, balanceUrl, "", "application/json", std::move(headers), [this](boost::system::error_code ec, ::net::HttpResponse resp) {
        if (ec) { 
            LOG_ERROR("TB {} Gate query_balance ec: {}", acc.accountName, ec.message()); 
            return; 
        }
        try {
            std::cout << "query_balance: " << resp.body << std::endl;
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                return;
            }

            simdjson::ondemand::array arr;
            if (doc.get_array().get(arr) != simdjson::SUCCESS) {
                return;
            }

            std::vector<pubsub::RCommand> pending;
            for (auto b_val : arr) {
                auto b_res = b_val.get_object();
                if (b_res.error()) {
                    continue;
                }
                auto& b = b_res.value_unsafe();

                std::string_view cur_sv;
                std::string_view avail_sv;
                std::string_view lock_sv;
                for (auto field : b) {
                    std::string_view k = field.unescaped_key().value_unsafe();
                    if (k == "currency") {
                        field.value().get(cur_sv);
                    }
                    else if (k == "available") {
                        field.value().get(avail_sv);
                    }
                    else if (k == "locked") {
                        field.value().get(lock_sv);
                    }
                }

                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                rcmd.body.balance.exchangeTypeEnum = GATEIO;
                rcmd.body.balance.instTypeEnum = SPOT;
                crypto::copy_sv_to_char_array(rcmd.body.balance.accountName, acc.accountName);
                crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(cur_sv)));
                rcmd.body.balance.available = crypto::fast_atod(avail_sv);
                rcmd.body.balance.frozen = crypto::fast_atod(lock_sv);
                rcmd.body.balance.total = rcmd.body.balance.available + rcmd.body.balance.frozen;
                rcmd.body.balance.updateTime = crypto::getCurrentTime();
                rcmd.body.balance.apiSourceEnum = AS_REST;
                pending.emplace_back(rcmd);
            }
            if (pending.empty()) {
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                rcmd.body.balance.exchangeTypeEnum = GATEIO;
                rcmd.body.balance.instTypeEnum = SPOT;
                crypto::copy_sv_to_char_array(rcmd.body.balance.accountName, acc.accountName);
                crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.currency, std::string("USDT"));
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
            LOG_ERROR("TB {} Gate query_balance cb exc: {}", acc.accountName, e.what());
        }
    });
}

void GateioSpotWsTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum, tcmd.body.queryOrder.instTypeEnum, tcmd.body.queryOrder.instId, info)) {
        LOG_INFO("TB {} Gate spot query_order smc miss: {}", acc.accountName, tcmd.body.queryOrder.instId);
        return;
    }

    std::string idSeg;
    if (!crypto::str_cmp(tcmd.body.queryOrder.orderId, "")) {
        idSeg = tcmd.body.queryOrder.orderId;
    } else if (!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")) {
        idSeg = tcmd.body.queryOrder.orderSysId;
    } else {
        return;
    }

    std::string pathBase = queryOrderUrl + "/" + idSeg;
    std::string queryStr = "currency_pair=" + std::string(info.originInstId);
#ifdef USE_GATEIO_UNIFIED
    queryStr += "&account=unified";
#endif

    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign = crypto::getGateioSignatureRest("GET", pathBase, time_str, queryStr, "", acc.secretKey);
    std::vector<std::pair<std::string, std::string>> headers = {{"KEY", acc.apiKey}, {"Timestamp", time_str}, {"SIGN", sign}};

    std::string fullPath = pathBase + "?" + queryStr;
    LOG_INFO("TB {} Gate spot query_order: {}", acc.accountName, fullPath);

    asyncRequest(boost::beast::http::verb::get, std::move(fullPath), "", "application/json", std::move(headers), [this, rcmd, info](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
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

            std::string_view status_sv;
            std::string_view id_sv;
            std::string_view text_sv;
            std::string_view amount_sv;
            std::string_view price_sv;
            std::string_view left_sv;
            std::string_view avg_sv;
            std::string_view finishAs_sv;

            for (auto field : doc_value) {
                std::string_view k = field.unescaped_key().value_unsafe();
                if (k == "status") {
                    field.value().get(status_sv);
                }
                else if (k == "id") {
                    field.value().get(id_sv);
                }
                else if (k == "text") {
                    field.value().get(text_sv);
                }
                else if (k == "amount") {
                    field.value().get(amount_sv);
                }
                else if (k == "price") {
                    field.value().get(price_sv);
                }
                else if (k == "left") {
                    field.value().get(left_sv);
                }
                else if (k == "avg_deal_price") {
                    field.value().get(avg_sv);
                }
                else if (k == "finish_as") {
                    field.value().get(finishAs_sv);
                }
            }

            if (!status_sv.empty()) {
                rcmd.body.orderResponse.errorId = crypto::get_gateio_errorid(resp.body.c_str());
                rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.errorId == OrderNotFoundError) ? OS_REJECTED : OS_UNKNOWN;
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;

            }

            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, id_sv);
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, text_sv);
            rcmd.body.orderResponse.volumeTotal = crypto::fast_atod(amount_sv);
            rcmd.body.orderResponse.limitPrice = crypto::fast_atod(price_sv);
            double left = std::fabs(crypto::fast_atod(left_sv));
            left = left > 0 ? left : -left;
            rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
            if (!avg_sv.empty()) {
                rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avg_sv);
            }

            if (status_sv == "open") {
                rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.volumeTotal > rcmd.body.orderResponse.volumeTraded && rcmd.body.orderResponse.volumeTraded > ZERO_NUM) ? OS_PARTFILLED : OS_NEW;
            } else if (status_sv == "closed") {
                rcmd.body.orderResponse.orderStatus = OS_FILLED;
            } else if (status_sv == "cancelled") {
                rcmd.body.orderResponse.orderStatus = OS_CANCELED;
            } else if (finishAs_sv == "filled") {
                rcmd.body.orderResponse.orderStatus = OS_FILLED;
            } else {
                rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
            }
            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} Gate spot query_order cb exc: {}", acc.accountName, e.what());
        }
    });
}


// ============================================================================
// add_new_order (WS spot.order_place, 无 REST 兜底)
// ============================================================================
void GateioSpotWsTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load() || !wsLoggedIn_.load()) {
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
            side = "buy";
        }
        else if (tcmd.body.newOrder.direction == DT_SHORT) {
            side = "sell";
        }
    } else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
        if (tcmd.body.newOrder.direction == DT_LONG) {
            side = "sell";
        }
        else if (tcmd.body.newOrder.direction == DT_SHORT) {
            side = "buy";
        }
    }

    if (!side) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId = (tcmd.body.newOrder.offsetFlag == OF_OPEN || tcmd.body.newOrder.offsetFlag == OF_CLOSE) ? DirectionError : OffsetFlagError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const char* tif = nullptr;
    bool priceZero = false;
    switch (tcmd.body.newOrder.orderType) {
        case OT_LIMIT:     
            tif = "gtc"; 
            break;
        case OT_MARKET:    
            tif = "ioc"; 
            priceZero = true; 
            break;
        case OT_POST_ONLY: 
            tif = "poc"; 
            break;
        case OT_FOK:       
            tif = "fok"; 
            break;
        case OT_IOC:       
            tif = "ioc"; 
            break;
        default:
            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            rcmd.body.orderResponse.errorId = OrderTypeError;
            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
            return;
    }

    double price = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice * info.magnifyNumber, info.tickSize);
    double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber, info.lotSize);
    std::string price_str = priceZero ? "0" : fmt::format("{}", price);
    std::string amount_str = fmt::format("{}", volume);

    const int wsId = nextWsId_.fetch_add(1, std::memory_order_relaxed);
    std::string msg = buildOrderPlaceJson(wsId, tcmd, info, price_str, amount_str, side, tif);

    recordPending(wsId, pubsub::CMD_NEW_ORDER, rcmd);
    LOG_INFO("TB {} Gate spot ws order.place id={} msg={}", acc.accountName, wsId, msg);
    pWsClient->send_text(std::move(msg));
}


// ============================================================================
// cancel_order (WS spot.order_cancel)
// ============================================================================
void GateioSpotWsTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
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
    LOG_INFO("TB {} Gate spot ws order.cancel id={} msg={}", acc.accountName, wsId, msg);
    pWsClient->send_text(std::move(msg));
}