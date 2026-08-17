#include "Gateio/GateioSpotTrade.h"

#include <cmath>

#include <fmt/format.h>
#include <simdjson.h>


GateioSpotTradeUnit::GateioSpotTradeUnit(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {

}
GateioSpotTradeUnit::~GateioSpotTradeUnit() {

}


// ============================================================================
// subWebsocekt: 建 REST + WS
// ============================================================================
void GateioSpotTradeUnit::subWebsocekt() {
    // REST
    std::string restHost = host_of(acc.restUrl);
    initRestClient(restHost, {}, 4);

    // WS
    net::WsConfig cfg;
    cfg.url = acc.wsUrl;
    cfg.ping_mode = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    // Gate ping 允许无 time 字段; 若加 time 会被固化在 cfg 里, 老 timestamp 服务器一般也接受。
    cfg.client_ping_text = R"({"channel":"spot.ping"})";
    cfg.auto_reconnect = true;
    cfg.idle_timeout_sec = 60;

    LOG_INFO("TB {} Gate spot ws {} rest {}", acc.accountId, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));
}

// ============================================================================
// onOpen: 现场签发 subscribe (auth signature TTL 短, 不能复用)
// ============================================================================
void GateioSpotTradeUnit::onOpen() {
    BaseTradeUnit::onOpen();
 
    std::string orders = buildOrdersSubscribeJson();
    std::string balances = buildBalancesSubscribeJson();

    LOG_INFO("TB {} Gate spot subscribe orders={} balances={}", acc.accountId, orders, balances);
    pWsClient->send_text(std::move(orders));
    pWsClient->send_text(std::move(balances));
}

// ---- subscribe JSON builders ----
std::string GateioSpotTradeUnit::buildOrdersSubscribeJson() const {
    long ts = crypto::getCurrentTimeSeconds();
    std::string time_str = std::to_string(ts);
    std::string channel = "spot.orders";
    std::string sign = crypto::getGateioSignatureWs(channel, "subscribe", time_str, acc.secretKey);
    return fmt::format(
        R"({{"time":{},"channel":"{}","event":"subscribe",)"
        R"("payload":["!all"],)"
        R"("auth":{{"method":"api_key","KEY":"{}","SIGN":"{}"}}}})",
        ts, channel, acc.apiKey, sign);
}

std::string GateioSpotTradeUnit::buildBalancesSubscribeJson() const {
    long ts = crypto::getCurrentTimeSeconds();
    std::string time_str = std::to_string(ts);
    std::string channel = "spot.balances";
    std::string sign = crypto::getGateioSignatureWs(channel, "subscribe", time_str, acc.secretKey);
    return fmt::format(
        R"({{"time":{},"channel":"{}","event":"subscribe",)"
        R"("auth":{{"method":"api_key","KEY":"{}","SIGN":"{}"}}}})",
        ts, channel, acc.apiKey, sign);
}

// ============================================================================
// onWebsocketMsg
// ============================================================================
void GateioSpotTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool, int64_t) {
    try {
        std::string msg(reinterpret_cast<const char*>(data), len);
        std::cout << "onWebsocketMsg: " << msg << std::endl;

        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc = g_parser.iterate(padded);
        if (doc.error()) {
            return;
        }

        auto doc_value = doc.get_object().value_unsafe();

        std::string_view channel_sv;
        std::string_view event_sv;
        simdjson::ondemand::array result_obj;
        bool has_balances = false;
        bool has_orders = false;
        bool has_event = false;

        for (auto field : doc_value) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "channel") {
                field.value().get(channel_sv);
                if (channel_sv == "spot.balances") {
                   has_balances = true; 
                }
                else if (channel_sv == "spot.orders") {
                    has_orders = true;
                }
            }
            else if (k == "event") {
                if (field.value().get(event_sv) == simdjson::SUCCE) {
                    if (event_sv == "update") {
                       has_event = true; 
                    }
                }
            }
            else if (k == "result") {
                if (field.value().get(result_obj) == simdjson::SUCCES) {
                    if (has_event && has_balances) {
                        handleBalancesUpdate(result_obj);
                    }
                    else if (has_event && has_orders) {
                        handleOrdersUpdate(result_obj);
                    }
                }
            }
        }
    }
    catch (const std::exception& e) {
        LOG_ERROR("TB {} Gate spot ws exc: {}", acc.accountId, e.what());
    }
}

// ---- spot.balances / spot.cross_balances update ----
void GateioSpotTradeUnit::handleBalancesUpdate(simdjson::ondemand::array& arr) {
    for (auto b_res : arr) {
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
        crypto::copy_sv_to_char_array(rcmd.body.balance.accountId, acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(cur_sv)));
        rcmd.body.balance.available = crypto::fast_atod(avail_sv);
        rcmd.body.balance.total = crypto::fast_atod(total_sv);
        rcmd.body.balance.updateTime = crypto::getCurrentTime();
        rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;
        pending.emplace_back(rcmd);
        PUSH_RCMD(rcmd)
    }
}

// ---- spot.orders update ----
void GateioSpotTradeUnit::handleOrdersUpdate(simdjson::ondemand::array& arr) {
    for (auto b_res : arr) {
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
            LOG_ERROR("TB {} not found GATEIO.SPOT.{} smc info", acc.accountId, originInstId);
            continue;
        }

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
        rcmd.body.orderResponse.exchangeTypeEnum = GATEIO;
        rcmd.body.orderResponse.instTypeEnum = SPOT;
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountId, acc.accountId);
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
// query_account —— 只在 unified 账户下才有意义
// ============================================================================
void GateioSpotTradeUnit::query_account(const pubsub::TCommand&) {
#ifndef USE_GATEIO_UNIFIED
    return;   // 非统一账户模式不查
#endif

    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());    
    std::string sign = crypto::getGateioSignatureRest("GET", unifiedUrl, time_str, "", "", acc.secretKey);
    std::vector<std::pair<std::string, std::string>> headers = {{"KEY", acc.apiKey}, {"Timestamp", time_str}, {"SIGN", sign}};

    asyncRequest(boost::beast::http::verb::get, unifiedUrl, "", "", std::move(headers), [this](boost::system::error_code ec, ::net::HttpResponse resp) {
        if (ec) { 
            LOG_ERROR("TB {} UF query_account ec: {}", acc.accountId, ec.message()); 
            return; 
        }

        try {
            std::cout << "query_account: " << resp.body << std::endl;
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                LOG_ERROR("TB {} query_account parse err: {}", acc.accountId, resp.body);
                return;
            }

            auto doc_value = doc.get_object().value_unsafe();

            simdjson::ondemand::object balances;
            std::string_view teq_sv;
            std::string_view tmb_sv;
            std::string_view tmm_sv;
            std::string_view tmr_sv;

            std::vector<pubsub::RCommand> pending;
           
            for (auto field : doc_value) {
                std::string_view k = field.unescaped_key().value_unsafe();
                if (k == "balances") {
                    field.value().get(balances);
                    for (auto field : balances) {
                        std::string_view asset = field.unescaped_key().value_unsafe();
                        auto b = field.value().get_object();

                        std::string_view av_sv;
                        std::string_view fr_sv;
                        std::string_view eq_sv;
                        for (ass : b) {
                            std::string_view v = ass.unescaped_key().value_unsafe();
                            if (v == "available") {
                                ass.value().get(av_sv);
                            }
                            else if (v == "freeze") {
                                ass.value().get(fr_sv);
                            }
                            else if (v == "equity") {
                                ass.value().get(eq_sv);
                            }
                        }

                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                        rcmd.body.balance.exchangeTypeEnum = GATEIO;
                        rcmd.body.balance.instTypeEnum = SPOT;
                        crypto::copy_sv_to_char_array(rcmd.body.balance.accountId, acc.accountId);
                        crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                        crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(asset)));
                        rcmd.body.balance.available = crypto::fast_atod(av_sv);
                        rcmd.body.balance.frozen = crypto::fast_atod(fr_sv);
                        rcmd.body.balance.total = crypto::fast_atod(eq_sv);
                        rcmd.body.balance.updateTime = crypto::getCurrentTime();
                        rcmd.body.balance.apiSourceEnum = AS_REST;
                        pending.emplace_back(rcmd);  
                    }                  }
                }
                else if (k == "unified_account_total_equity") {
                    field.value().get(teq_sv);
                }
                else if (k == "total_margin_balance") {
                    field.value().get(tmb_sv);
                }
                else if (k == "total_maintenance_margin") {
                    field.value().get(tmm_sv);
                }
                else if (k == "total_maintenance_margin_rate") {
                    field.value().get(tmr_sv);
                }
            }

            for (size_t i = 0; i < pending.size(); ++i) {
                pending[i].body.balance.isLast = (i + 1 == pending.size());
                PUSH_RCMD(pending[i]);
            }

            // totalAccount
            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
            rcmd.body.totalAccount.exchangeTypeEnum = GATEIO;
            rcmd.body.totalAccount.instTypeEnum = SPOT;
            crypto::copy_sv_to_char_array(rcmd.body.totalAccount.accountId, acc.accountId);
            crypto::copy_sv_to_char_array(rcmd.body.totalAccount.strategyId, acc.strategyId);
            rcmd.body.totalAccount.totalEquity = crypto::fast_atod(teq_sv);
            rcmd.body.totalAccount.adjEquity = crypto::fast_atod(tmb_sv);
            rcmd.body.totalAccount.mmr = crypto::fast_atod(tmm_sv);
            rcmd.body.totalAccount.mgnRatio = tmr_sv.empty() ? 9999.0 : crypto::fast_atod(tmr_sv);
            rcmd.body.totalAccount.updateTime = crypto::getCurrentTime();
            rcmd.body.totalAccount.apiSourceEnum = AS_REST;
            PUSH_RCMD(rcmd)
        }      
        catch (const std::exception& e) {
            LOG_ERROR("TB {} Gate query_account cb exc: {}", acc.accountId, e.what());
        }
    });
}


// ============================================================================
// query_balance —— GET /api/v4/spot/accounts (array)
// ============================================================================
void GateioSpotTradeUnit::query_balance(const pubsub::TCommand&) {
    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign = crypto::getGateioSignatureRest("GET", balanceUrl, time_str, "", "", acc.secretKey);
    std::vector<std::pair<std::string, std::string>> headers = {{"KEY", acc.apiKey}, {"Timestamp", time_str}, {"SIGN", sign}};

    asyncRequest(boost::beast::http::verb::get, balanceUrl, "", "", std::move(headers), [this](boost::system::error_code ec, ::net::HttpResponse resp) {
        if (ec) { 
            LOG_ERROR("TB {} Gate query_balance ec: {}", acc.accountId, ec.message()); 
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
                crypto::copy_sv_to_char_array(rcmd.body.balance.accountId, acc.accountId);
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
                crypto::copy_sv_to_char_array(rcmd.body.balance.accountId, acc.accountId);
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
            LOG_ERROR("TB {} Gate query_balance cb exc: {}", acc.accountId, e.what());
        }
    });
}


void GateioSpotTradeUnit::query_position(const pubsub::TCommand&) {
    // Spot 无持仓
}


// ============================================================================
// add_new_order —— POST /api/v4/spot/orders (body 是 JSON)
// ============================================================================
void GateioSpotTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load()) {
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

    double price = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice * info.magnifyNumber, info.tickSize);
    double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber, info.lotSize);

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

    const char* account = nullptr;
#ifdef USE_GATEIO_UNIFIED
    account = "unified";
#else
    if (tcmd.body.newOrder.instTypeEnum == SPOT) {
        account = "spot";
    }
    else if (tcmd.body.newOrder.instTypeEnum == MARGIN) {
        account = "margin";
    }
    else {
        rcmd.body.orderResponse.errorId = InstTypeError;
        PUSH_RCMD(rcmd)
        return;
    }
#endif

    std::string price_str = priceZero ? "0" : fmt::format("{}", price);
    std::string amount_str = fmt::format("{}", volume);

#ifdef USE_GATEIO_UNIFIED
    std::string body = fmt::format(
        R"({{"text":"{}","currency_pair":"{}","price":"{}","amount":"{}","side":"{}","time_in_force":"{}","account":"unified","auto_borrow":true,"auto_repay":true}})",
        tcmd.body.newOrder.orderSysId, info.originInstId, price_str, amount_str, side, tif);
#else
    std::string body = fmt::format(
        R"({{"text":"{}","currency_pair":"{}","price":"{}","amount":"{}","side":"{}","time_in_force":"{}","account":"{}"}})",
        tcmd.body.newOrder.orderSysId, info.originInstId, price_str, amount_str, side, tif, account);
#endif

    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign = crypto::getGateioSignatureRest("POST", newOrderUrl, time_str, "", body, acc.secretKey);
    std::vector<std::pair<std::string, std::string>> headers = {{"KEY", acc.apiKey}, {"Timestamp", time_str}, {"SIGN", sign}};


    LOG_INFO("TB {} Gate spot add_new_order body={}", acc.accountId, body);

    asyncRequest(boost::beast::http::verb::post, newOrderUrl, std::move(body), "", std::move(headers), [this, rcmd](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
        if (ec) {
            if (ec == boost::system::errc::no_stream_resources || ec == boost::system::errc::no_buffer_space || ec == boost::system::errc::not_connected) {
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            }
            else {
                rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
            }

            rcmd.body.orderResponse.errorId = NetworkError;
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, ec.message());
            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
            return;
        }

        if (rcmd.body.orderResponse.clientOrderId == TESTCLIENTORDERID) {
            return;
        }

        try {
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                rcmd.body.orderResponse.errorId = UnknownError;
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;
            }

            auto doc_value = doc.get_object().value_unsafe();

            std::string_view status_sv;
            std::string_view finishAs_sv;
            std::string_view label_sv;
            std::string_view text_sv;
            std::string_view avg_sv;
            std::string_view left_sv;
            std::string_view id_sv;
            bool has_status = false;
            bool has_label = false;

            for (auto field : doc_value) {
                std::string_view k = field.unescaped_key().value_unsafe();
                if (k == "status") {
                    has_status = field.value().get(status_sv) == simdjson::SUCCESS;
                }
                else if (k == "finish_as") {
                    field.value().get(finishAs_sv);
                }
                else if (k == "label") {
                    has_label = field.value().get(label_sv) == simdjson::SUCCESS;
                }
                else if (k == "text") {
                    field.value().get(text_sv);
                }
                else if (k == "avg_deal_price") {
                    field.value().get(avg_sv);
                }
                else if (k == "left") {
                    field.value().get(left_sv);
                }
                else if (k == "id") {
                    field.value().get(id_sv);
                }
            }

            if (has_status) {
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, id_sv);
                rcmd.body.orderResponse.errorId = ERROR_NoError;
                nano_strcpy(rcmd.body.orderResponse.orderSysId, text_sv);
                if (!avg_sv.empty()) {
                    rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avg_sv);
                }

                double left = std::fabs(crypto::fast_atod(left_sv));
                left = left > 0 ? left : -left;
                rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
                
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

                PUSH_RCMD(rcmd)
            }
            else if (has_label) {
                rcmd.body.orderResponse.errorId = crypto::get_gateio_errorid(resp.body.c_str());
                if (rcmd.body.orderResponse.errorId == 0) {
                    rcmd.body.orderResponse.errorId = UnknownError;
                }
                
                crypto::replace_string(resp.body, ":", "");
                crypto::replace_string(resp.body, "\"", "");
                nano_strcpy(rcmd.body.orderResponse.originMsg, originMsg.c_str(), ORIGINMSG_SIZE);
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                LOG_WARN("{}",resp.body.c_str());
                PUSH_RCMD(rcmd)
            }
            else {
                rcmd.body.orderResponse.orderStatus = OrderStatus_UNKNOWN;
                rcmd.body.orderResponse.errorId = ERROR_UnknownError; 
                crypto::replace_string(resp.body, ":", "");
                crypto::replace_string(resp.body, "\"", "");
                nano_strcpy(rcmd.body.orderResponse.originMsg, originMsg.c_str(), ORIGINMSG_SIZE);
                PUSH_RCMD(rcmd)  
            }
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} Gate spot add_new_order cb exc: {}", acc.accountId, e.what());
            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            rcmd.body.orderResponse.errorId  = NetworkError;
            rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
        }
    });
}


// ============================================================================
// cancel_order —— DELETE /api/v4/spot/orders/{id}?currency_pair=X
// ============================================================================
void GateioSpotTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.cancelOrder.exchangeTypeEnum, tcmd.body.cancelOrder.instTypeEnum, tcmd.body.cancelOrder.instId, info)) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = SMCInstrumentNotExistError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    std::string idSeg = "";
    if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
        idSeg = tcmd.body.cancelOrder.orderId;
    } else if (!crypto::str_cmp(tcmd.body.cancelOrder.orderSysId, "")) {
        idSeg = tcmd.body.cancelOrder.orderSysId;
    } else {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = OrderIdError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    std::string pathBase = cancelOrderUrl + "/" + idSeg;
    std::string queryStr = "currency_pair=" + std::string(info.originInstId);
#ifdef USE_GATEIO_UNIFIED
    queryStr += "&account=unified";
#endif

    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign = crypto::getGateioSignatureRest("DELETE", pathBase, time_str, queryStr, "", acc.secretKey);
    std::vector<std::pair<std::string, std::string>> headers = {{"KEY", acc.apiKey}, {"Timestamp", time_str}, {"SIGN", sign}};

    std::string fullPath = pathBase + "?" + queryStr;
    LOG_INFO("TB {} Gate spot cancel_order: {}", acc.accountId, fullPath);

    asyncRequest(boost::beast::http::verb::delete_, std::move(fullPath), "", "", std::move(headers), [this, rcmd](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
        if (ec) {
            LOG_ERROR("TB {} cancel_order ec: {}", acc.accountId, ec.message());
            rcmd.body.orderResponse.orderStatus = OS_FAILED;
            rcmd.body.orderResponse.errorId = NetworkError;
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, ec.message());
            rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
            return;
        }
        try {
            std::cout << "cancel_order : " << resp.body.c_str() << std::endl;
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                rcmd.body.orderResponse.orderStatus = OS_FAILED;
                rcmd.body.orderResponse.errorId = UnknownError;
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;
            }

            auto doc_value = doc.get_object().value_unsafe();

            std::string_view status_sv;
            std::string_view label_sv;
            std::string_view avg_sv;
            std::string_view left_sv;
            std::string_view id_sv;
            bool has_status = false;
            bool has_label = false;

            for (auto field : doc_value) {
                std::string_view k = field.unescaped_key().value_unsafe();
                if (k == "status") {
                    has_status = field.value().get(status_sv) == simdjson::SUCCESS;
                }
                else if (k == "label") {
                    has_label = field.value().get(label_sv) == simdjson::SUCCESS;
                }
                else if (k == "avg_deal_price") {
                    field.value().get(avg_sv);
                }
                else if (k == "left") {
                    field.value().get(left_sv);
                }
                else if (k == "id") {
                    field.value().get(id_sv);
                }
            }

            if (has_status) {
                crypto::copy_sv_to_char_array(rcmd.body.newOrder.orderId, id_sv);
                if (!avg_sv.empty()) {
                    rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avg_sv);
                }
                double left = std::fabs(crypto::fast_atod(left_sv));
                left = left > 0 ? left : -left;
                rcmd.body.newOrder.volumeTraded = rcmd.body.newOrder.volumeTotal - left;
                rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                PUSH_RCMD(rcmd)
            }
            else if (has_label) {
                rcmd.body.orderResponse.orderStatus = OS_FAILED;
                rcmd.body.orderResponse.errorId = crypto::get_gateio_errorid(resp.body.c_str());
                if (rcmd.body.orderResponse.errorId == OrderNotFoundError) {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                }
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, label_sv);
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)ss
            }
            else {
                rcmd.body.newOrder.orderStatus = OS_FAILED;
                rcmd.body.newOrder.ErrorID = ERROR_UnknownError; 
                crypto::replace_string(resp.body, ":", "");
                crypto::replace_string(resp.body, "\"", "");
                nano_strcpy(rcmd.body.newOrder.originMsg, originMsg.c_str(), ORIGINMSG_SIZE);
                PUSH_RCMD(rcmd)  
            }
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} Gate spot cancel_order cb exc: {}", acc.accountId, e.what());
        }
    });
}


// ============================================================================
// query_order —— GET /api/v4/spot/orders/{id}?currency_pair=X
// ============================================================================
void GateioSpotTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum, tcmd.body.queryOrder.instTypeEnum, tcmd.body.queryOrder.instId, info)) {
        LOG_INFO("TB {} Gate spot query_order smc miss: {}", acc.accountId, tcmd.body.queryOrder.instId);
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
    LOG_INFO("TB {} Gate spot query_order: {}", acc.accountId, fullPath);

    asyncRequest(boost::beast::http::verb::get, std::move(fullPath), "", "", std::move(headers), [this, rcmd, info](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
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
                LOG_ERROR("TB {} Gate spot query_order cb exc: {}", acc.accountId, e.what());
            }
        });
}
