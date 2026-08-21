#include "Gateio/GateioUSTrade.h"

#include <cmath>

#include <fmt/format.h>
#include <simdjson.h>

GateioUSTradeUnit::GateioUSTradeUnit(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {

}

GateioUSTradeUnit::~GateioUSTradeUnit() {

}

// ============================================================================
// subWebsocekt
// ============================================================================
void GateioUSTradeUnit::subWebsocekt() {
    std::string restHost = host_of(acc.restUrl);
    initRestClient(restHost, {}, 4);

    net::WsConfig cfg;
    cfg.url = acc.wsUrl;
    cfg.ping_mode = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text = R"({"channel":"futures.ping"})";
    cfg.auto_reconnect = true;
    cfg.idle_timeout_sec = 60;

    LOG_INFO("TB {} Gate US ws {} rest {}", acc.accountId, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));
}


// ============================================================================
// onOpen: 现场签发 subscribe
// ============================================================================
void GateioUSTradeUnit::onOpen() {
    BaseTradeUnit::onOpen();

    pWsClient->send_text(buildOrdersSubscribeJson());
    pWsClient->send_text(buildBalancesSubscribeJson());
    pWsClient->send_text(buildPositionsSubscribeJson());
}


// ---- subscribe JSON builders ----
std::string GateioUSTradeUnit::buildOrdersSubscribeJson() const {
    long ts = crypto::getCurrentTimeSeconds();
    std::string time_str = std::to_string(ts);
    std::string channel = "futures.orders";
    std::string sign = crypto::getGateioSignatureWs(channel, "subscribe", time_str, acc.secretKey);
    return fmt::format(
        R"({{"time":{},"channel":"{}","event":"subscribe",)"
        R"("payload":["{}","!all"],)"
        R"("auth":{{"method":"api_key","KEY":"{}","SIGN":"{}"}}}})",
        ts, channel, acc.userId, acc.apiKey, sign);
}

std::string GateioUSTradeUnit::buildBalancesSubscribeJson() const {
    long ts = crypto::getCurrentTimeSeconds();
    std::string time_str = std::to_string(ts);
    std::string channel = "futures.balances";
    std::string sign = crypto::getGateioSignatureWs(channel, "subscribe", time_str, acc.secretKey);
    return fmt::format(
        R"({{"time":{},"channel":"{}","event":"subscribe",)"
        R"("payload":["{}"],)"
        R"("auth":{{"method":"api_key","KEY":"{}","SIGN":"{}"}}}})",
        ts, channel, acc.userId, acc.apiKey, sign);
}

std::string GateioUSTradeUnit::buildPositionsSubscribeJson() const {
    long ts = crypto::getCurrentTimeSeconds();
    std::string time_str = std::to_string(ts);
    std::string channel = "futures.positions";
    std::string sign = crypto::getGateioSignatureWs(channel, "subscribe", time_str, acc.secretKey);
    return fmt::format(
        R"({{"time":{},"channel":"{}","event":"subscribe",)"
        R"("payload":["{}","!all"],)"
        R"("auth":{{"method":"api_key","KEY":"{}","SIGN":"{}"}}}})",
        ts, channel, acc.userId, acc.apiKey, sign);
}

// ============================================================================
// onWebsocketMsg
// ============================================================================
void GateioUSTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool, int64_t) {
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
        bool has_positions = false;
        bool has_orders = false;
        bool has_event = false;

        for (auto field : doc_value) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "channel") {
                field.value().get(channel_sv);
                if (channel_sv == "futures.balances") {
                   has_balances = true; 
                }
                else if (channel_sv == "futures.positions") {
                    has_positions = true;
                }
                else if (channel_sv == "futures.orders") {
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
            else if (k == "result") {
                if (field.value().get(result_obj) == simdjson::SUCCESS) {
                    if (has_event && has_balances) {
                        handleBalancesUpdate(result_obj);
                    }
                    else if (has_event && has_positions) {
                        handlePositionsUpdate(result_obj);
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

// ---- futures.orders update ----
void GateioUSTradeUnit::handleOrdersUpdate(simdjson::ondemand::array& arr) {
    for (auto b_val : arr) {
        auto b_res = b_val.get_object();
        if (b_res.error()) {
            continue;
        }
        auto& b = b_res.value_unsafe();

        std::string_view contract_sv;
        int64_t id = 0;
        std::string_view text_sv;
        double price = 0.0;
        std::string_view tif_sv;
        double size = 0.0;
        double left = 0.0;
        double fill = 0.0;
        std::string_view status_sv;
        std::string_view finish_sv;
        bool isClose = false;

        for (auto field : b) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "contract") {
                field.value().get(contract_sv);
            }
            else if (k == "id") {
                field.value().get(id);
            }
            else if (k == "text") {
                field.value().get(text_sv);
            }
            else if (k == "price") {
                field.value().get(price);
            }
            else if (k == "tif") {
                field.value().get(tif_sv);
            }
            else if (k == "size") {
                field.value().get(size);
            }
            else if (k == "left") {
                field.value().get(left);
            }
            else if (k == "fill_price") {
                field.value().get(fill);
            }
            else if (k == "status") {
                field.value().get(status_sv);
            }
            else if (k == "finish_as") {
                field.value().get(finish_sv);
            }
            else if (k == "is_close") {
                field.value().get(isClose);
            }
        }

        std::string originInstId(contract_sv);
        md::InstrumentInfo info;
        if (!smc->get_instrument_info(GATEIO, USDT_SWAP, originInstId.c_str(), info)) { 
            LOG_ERROR("TB {} not found GATEIO.USDT_SWAP.{} smc info", acc.accountId, originInstId);
            continue;
        }

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
        rcmd.body.orderResponse.exchangeTypeEnum = GATEIO;
        rcmd.body.orderResponse.instTypeEnum = USDT_SWAP;
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountId, acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.instId, std::string_view(info.instId));
        fmt::format_to(rcmd.body.orderResponse.orderId, "{}", id);

        if (!text_sv.empty()) {
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, text_sv);
            if (text_sv == "auto_deleveraging") {
                rcmd.body.orderResponse.errorId = ADLError;
            }
            else if (text_sv == "liquidation") {
                rcmd.body.orderResponse.errorId = LiquidationError;
            }
        }

        rcmd.body.orderResponse.offsetFlag = isClose ? OF_CLOSE : OF_OPEN;

        rcmd.body.orderResponse.direction = size > 0 ? DT_LONG : DT_SHORT;
        rcmd.body.orderResponse.volumeTotal = std::fabs(size);
        
        rcmd.body.orderResponse.limitPrice = price;
        

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
                default: 
                    rcmd.body.orderResponse.orderType = OT_LIMIT;
                    break;
            }
        }

        left = std::fabs(left);
        rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
        
        rcmd.body.orderResponse.tradePrice = fill;

        if (status_sv == "open") {
            rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.volumeTotal > rcmd.body.orderResponse.volumeTraded && rcmd.body.orderResponse.volumeTraded > ZERO_NUM) ? OS_PARTFILLED : OS_NEW;
        } else {
            if (finish_sv == "filled") {
                rcmd.body.orderResponse.orderStatus = OS_FILLED;
            }
            else if (finish_sv == "cancelled" || finish_sv == "liquidated" || finish_sv == "ioc" || finish_sv == "auto_deleveraged" || finish_sv == "reduce_only") {
                rcmd.body.orderResponse.orderStatus = OS_CANCELED;
            }
            else {
                rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
            }                                    
        }

        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd)
    }
}

// ---- futures.balances update ----
void GateioUSTradeUnit::handleBalancesUpdate(simdjson::ondemand::array& arr) {
    for (auto b_val : arr) {
        auto b_res = b_val.get_object();
        if (b_res.error()) {
            continue;
        }
        auto& b = b_res.value_unsafe();

        std::string_view cur_sv;
        double bal = 0;
        for (auto field : b) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "currency") {
                field.value().get(cur_sv);
            }
            else if (k == "balance") {
                field.value().get(bal);
            }
        }

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
        rcmd.body.balance.exchangeTypeEnum = GATEIO;
        rcmd.body.balance.instTypeEnum = USDT_SWAP;
        crypto::copy_sv_to_char_array(rcmd.body.balance.accountId, acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(cur_sv)));
        rcmd.body.balance.total = bal;
        rcmd.body.balance.available = rcmd.body.balance.total;
        rcmd.body.balance.updateTime = crypto::getCurrentTime();
        rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd)
    }
}


// ---- futures.positions update ----
void GateioUSTradeUnit::handlePositionsUpdate(simdjson::ondemand::array& arr) {
    for (auto b_val : arr) {
        auto b_res = b_val.get_object();
        if (b_res.error()) {
            continue;
        }
        auto& b = b_res.value_unsafe();

        std::string_view contract_sv;
        int size = 0;
        double margin = 0;
        double entry = 0;
        double up = 0;
        double mark = 0;
        double liq = 0;
        double adl = 1;
        for (auto field : b) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "contract") {
                field.value().get(contract_sv);
            }
            else if (k == "size") {
                field.value().get(size);
            }
            else if (k == "margin") {
                field.value().get(margin);
            }
            else if (k == "entry_price") {
                field.value().get(entry);
            }
            else if (k == "unrealised_pnl") {
                field.value().get(up);
            }
            else if (k == "mark_price") {
                field.value().get(mark);
            }
            else if (k == "liq_price") {
                field.value().get(liq);
            }
            else if (k == "adl_ranking") {
                field.value().get(adl);
            }
        }

        std::string originInstId(contract_sv);
        md::InstrumentInfo info;
        if (!smc->get_instrument_info(GATEIO, USDT_SWAP, originInstId.c_str(), info)) {
            continue;
        }

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
        rcmd.body.position.exchangeTypeEnum = GATEIO;
        rcmd.body.position.instTypeEnum = USDT_SWAP;
        crypto::copy_sv_to_char_array(rcmd.body.position.accountId, acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view(info.instId));
        rcmd.body.position.direction = size >= 0 ? DT_LONG : DT_SHORT;
        rcmd.body.position.volume = std::fabs(size) * info.magnifyNumber;
        rcmd.body.position.maintMargin = margin;
        rcmd.body.position.avgPrice = entry * info.reduceNumber;
        rcmd.body.position.unrealizedPnl = up;
        rcmd.body.position.markPrice = mark * info.reduceNumber;
        rcmd.body.position.liquidPrice = liq * info.reduceNumber;

        double adl_s = static_cast<int>(adl);
        if (adl_s >= 5) {
            adl = 1;
        }
        else if (adl_s == 4) {
            adl = 1;
        }
        else if (adl_s == 3) {
            adl = 3;
        }
        else if (adl_s == 2) {
            adl = 4;
        }
        else if (adl_s == 1) {
            adl = 4;
        }

        rcmd.body.position.adlQuantile = adl;
        rcmd.body.position.updateTime = crypto::getCurrentTime();
        rcmd.body.position.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd);
    }
}

// ============================================================================
// query_account —— 无实现 (老代码也是空)
// ============================================================================
void GateioUSTradeUnit::query_account(const pubsub::TCommand&) {

}


// ============================================================================
// query_balance —— GET /api/v4/futures/usdt/accounts (array)
// ============================================================================
void GateioUSTradeUnit::query_balance(const pubsub::TCommand&) {
    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign = crypto::getGateioSignatureRest("GET", balanceUrl, time_str, "", "", acc.secretKey);
    std::vector<std::pair<std::string, std::string>> headers = {{"KEY", acc.apiKey}, {"Timestamp", time_str}, {"SIGN", sign}};

    asyncRequest(boost::beast::http::verb::get, balanceUrl, "", "application/json", std::move(headers), [this](boost::system::error_code ec, ::net::HttpResponse resp) {
        if (ec) { 
            LOG_ERROR("TB {} Gate US query_balance ec: {}", acc.accountId, ec.message()); 
            return; 
        }

        try {
           simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                LOG_ERROR("TB {} query_order parse err: {}", acc.accountId, resp.body);
                return;
            }

            auto doc_value = doc.get_object().value_unsafe();

            std::vector<pubsub::RCommand> pending;
            std::string_view cur_sv;
            std::string_view avail_sv;
            std::string_view om_sv;
            std::string_view pm_sv;
            std::string_view tot_sv;
            for (auto field : doc_value) {
                std::string_view k = field.unescaped_key().value_unsafe();
                if (k == "currency") {
                    field.value().get(cur_sv);
                }
                else if (k == "available") {
                    field.value().get(avail_sv);
                }
                else if (k == "order_margin") {
                    field.value().get(om_sv);
                }
                else if (k == "position_margin") {
                    field.value().get(pm_sv);
                }
                else if (k == "total") {
                    field.value().get(tot_sv);
                }
            }

            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
            rcmd.body.balance.exchangeTypeEnum = GATEIO;
            rcmd.body.balance.instTypeEnum = USDT_SWAP;
            crypto::copy_sv_to_char_array(rcmd.body.balance.accountId, acc.accountId);
            crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
            crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(cur_sv)));
            rcmd.body.balance.available = crypto::fast_atod(avail_sv);
            rcmd.body.balance.frozen = crypto::fast_atod(om_sv) + crypto::fast_atod(pm_sv);
            rcmd.body.balance.total = crypto::fast_atod(tot_sv);
            rcmd.body.balance.updateTime = crypto::getCurrentTime();
            rcmd.body.balance.apiSourceEnum = AS_REST;
            pending.emplace_back(rcmd);
            
            if (pending.empty()) {
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                rcmd.body.balance.exchangeTypeEnum = GATEIO;
                rcmd.body.balance.instTypeEnum = USDT_SWAP;
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
            LOG_ERROR("TB {} Gate US query_balance cb exc: {}", acc.accountId, e.what());
        }
    });
}


// ============================================================================
// query_position —— GET /api/v4/futures/usdt/positions
// ============================================================================
void GateioUSTradeUnit::query_position(const pubsub::TCommand&) {
    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign = crypto::getGateioSignatureRest("GET", positionUrl, time_str, "", "", acc.secretKey);
    std::vector<std::pair<std::string, std::string>> headers = {{"KEY", acc.apiKey}, {"Timestamp", time_str}, {"SIGN", sign}};

    asyncRequest(boost::beast::http::verb::get, positionUrl, "", "application/json", std::move(headers), [this](boost::system::error_code ec, ::net::HttpResponse resp) {
        if (ec) { 
            LOG_ERROR("TB {} Gate US query_position ec: {}", acc.accountId, ec.message()); 
            return; 
        }

        try {
            std::cout << "query_position: " << resp.body << std::endl;
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

                std::string_view contract_sv;
                int size = 0;
                std::string_view margin_sv;
                std::string_view entry_sv;
                std::string_view up_sv;
                std::string_view mark_sv;
                std::string_view liq_sv;
                std::string_view adl_sv;
                for (auto field : b) {
                    std::string_view k = field.unescaped_key().value_unsafe();
                    if (k == "contract") {
                        field.value().get(contract_sv);
                    }
                    else if (k == "size") {
                        field.value().get(size);
                    }
                    else if (k == "margin") {
                        field.value().get(margin_sv);
                    }
                    else if (k == "entry_price") {
                        field.value().get(entry_sv);
                    }
                    else if (k == "unrealised_pnl") {
                        field.value().get(up_sv);
                    }
                    else if (k == "mark_price") {
                        field.value().get(mark_sv);
                    }
                    else if (k == "liq_price") {
                        field.value().get(liq_sv);
                    }
                    else if (k == "adl_ranking") {
                        field.value().get(adl_sv);
                    }
                }

                std::string originInstId(contract_sv);
                md::InstrumentInfo info;
                if (!smc->get_instrument_info(GATEIO, USDT_SWAP, originInstId.c_str(), info)) {
                    continue;
                }

                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                rcmd.body.position.exchangeTypeEnum = GATEIO;
                rcmd.body.position.instTypeEnum = USDT_SWAP;
                crypto::copy_sv_to_char_array(rcmd.body.position.accountId, acc.accountId);
                crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view(info.instId));
                rcmd.body.position.direction = size >= 0 ? DT_LONG : DT_SHORT;
                rcmd.body.position.volume = std::fabs(size) * info.magnifyNumber;
                rcmd.body.position.maintMargin = crypto::fast_atod(margin_sv);
                rcmd.body.position.avgPrice = crypto::fast_atod(entry_sv) * info.reduceNumber;
                rcmd.body.position.unrealizedPnl = crypto::fast_atod(up_sv);
                rcmd.body.position.markPrice = crypto::fast_atod(mark_sv) * info.reduceNumber;
                rcmd.body.position.liquidPrice = crypto::fast_atod(liq_sv) * info.reduceNumber;

                double adl = 1;
                double adl_s = static_cast<int>(crypto::fast_atod(adl_sv));
                if (adl_s >= 5) {
                    adl = 1;
                }
                else if (adl_s == 4) {
                    adl = 1;
                }
                else if (adl_s == 3) {
                    adl = 3;
                }
                else if (adl_s == 2) {
                    adl = 4;
                }
                else if (adl_s == 1) {
                    adl = 4;
                }
        
                rcmd.body.position.adlQuantile = adl;
                rcmd.body.position.updateTime = crypto::getCurrentTime();
                rcmd.body.position.apiSourceEnum = AS_REST;
                pending.emplace_back(rcmd);
            }
            if (pending.empty()) {
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                rcmd.body.position.exchangeTypeEnum = GATEIO;
                rcmd.body.position.instTypeEnum = USDT_SWAP;
                crypto::copy_sv_to_char_array(rcmd.body.position.accountId, acc.accountId);
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
            LOG_ERROR("TB {} Gate US query_position cb exc: {}", acc.accountId, e.what());
        }
    });
}


// ============================================================================
// add_new_order —— POST /api/v4/futures/usdt/orders
// ============================================================================
void GateioUSTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
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

    double price = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice * info.magnifyNumber, info.tickSize);
    double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber, info.lotSize);

    // Gate futures 用带正负号的 size (正=多, 负=空), tif 走 gtc/ioc/poc/fok
    const char* tif = nullptr;
    bool priceZero  = false;
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

    // size 正负规则:
    //   OPEN + LONG   → +vol   (买多)
    //   OPEN + SHORT  → -vol   (卖空)
    //   CLOSE + LONG  → -vol   (卖平多)
    //   CLOSE + SHORT → +vol   (买平空)
    double sizeSigned = 0;
    if (tcmd.body.newOrder.offsetFlag == OF_OPEN) {
        if (tcmd.body.newOrder.direction == DT_LONG) {
            sizeSigned = volume;
        }
        else if (tcmd.body.newOrder.direction == DT_SHORT) {
            sizeSigned = -volume;
        }
    } else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
        if (tcmd.body.newOrder.direction == DT_LONG) {
            sizeSigned = -volume;
        }
        else if (tcmd.body.newOrder.direction == DT_SHORT) {
            sizeSigned = volume;
        }
    }

    if (sizeSigned == 0) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId = (tcmd.body.newOrder.offsetFlag == OF_OPEN || tcmd.body.newOrder.offsetFlag == OF_CLOSE) ? DirectionError : OffsetFlagError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    int pricePrecision = static_cast<int>(std::llround(-std::log10(info.tickSize)));
    int sizePrecision = static_cast<int>(std::llround(-std::log10(info.lotSize)));
 
    std::string price_str = priceZero ? "0" : fmt::format("{:.{}f}", price, pricePrecision);
    std::string size_str = fmt::format("{:.{}f}", sizeSigned, sizePrecision);

    std::string body = fmt::format(
        R"({{"text":"{}","contract":"{}","price":"{}","size":{},"tif":"{}","reduce_only":{}}})",
        escape_json(tcmd.body.newOrder.orderSysId), info.originInstId, price_str, size_str, tif,
        tcmd.body.newOrder.reduceOnly ? "true" : "false");

    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign = crypto::getGateioSignatureRest("POST", newOrderUrl, time_str, "", body, acc.secretKey);
    std::vector<std::pair<std::string, std::string>> headers = {{"KEY", acc.apiKey}, {"Timestamp", time_str}, {"SIGN", sign}};

    LOG_INFO("TB {} Gate US add_new_order body={}", acc.accountId, body);

    asyncRequest(boost::beast::http::verb::post, newOrderUrl, std::move(body), "application/json", std::move(headers), [this, rcmd, info](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
        std::cout << "add new order: " << resp.body << std::endl;
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
                rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                rcmd.body.orderResponse.errorId = UnknownError;
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;
            }

            auto doc_value = doc.get_object().value_unsafe();

            int64_t id = 0;
            std::string_view label_sv;
            std::string_view fill_sv;
            std::string_view left_sv;
            std::string_view status_sv;
            std::string_view finishAs_sv;

            bool has_id = false;
            bool has_label = false;

            for (auto field : doc_value) {
                std::string_view k = field.unescaped_key().value_unsafe();
                if (k == "id") {
                    has_id = field.value().get(id) == simdjson::SUCCESS;
                }
                else if (k == "label") {
                    has_label = field.value().get(label_sv) == simdjson::SUCCESS;
                }
                else if (k == "fill_price") {
                    field.value().get(fill_sv);
                }
                else if (k == "left") {
                    field.value().get(left_sv);
                }
                else if (k == "status") {
                    field.value().get(status_sv);
                }
                else if (k == "finish_as") {
                    field.value().get(finishAs_sv);
                }
            }

            if (has_id) {
                fmt::format_to(rcmd.body.orderResponse.orderId, "{}", id);
                rcmd.body.orderResponse.errorId = NoError;
                if (!fill_sv.empty()) {
                    rcmd.body.orderResponse.tradePrice = crypto::fast_atod(fill_sv);
                }
                double left = std::fabs(crypto::fast_atod(left_sv));
                rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;

                // 用 status/finish_as 判 orderStatus, 逻辑与 query_order 一致
                if (status_sv == "open") {
                    rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.volumeTraded > ZERO_NUM) ? OS_PARTFILLED : OS_NEW;
                } else if (finishAs_sv == "filled") {
                    rcmd.body.orderResponse.orderStatus = OS_FILLED;
                } else if (finishAs_sv == "cancelled" || finishAs_sv == "ioc" || finishAs_sv == "liquidated" || finishAs_sv == "auto_deleveraged" || finishAs_sv == "reduce_only" || finishAs_sv == "reduce_out") {
                    rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                } else if (rcmd.body.orderResponse.orderType == OT_IOC) {
                    rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.volumeTraded < rcmd.body.orderResponse.volumeTotal) ? OS_CANCELED : OS_FILLED;
                } else {
                    rcmd.body.orderResponse.orderStatus = OS_NEW;
                }
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            } 
            else if (has_label) {
                rcmd.body.orderResponse.orderStatus = crypto::get_gateio_orderstatus(std::string(label_sv).c_str());
                rcmd.body.orderResponse.errorId = crypto::get_gateio_errorid(resp.body.c_str());
                if (rcmd.body.orderResponse.errorId == 0) {
                    rcmd.body.orderResponse.errorId = UnknownError;
                }
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, label_sv);
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            } else {
                rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                rcmd.body.orderResponse.errorId = UnknownError;
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            }
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} Gate US add_new_order cb exc: {}", acc.accountId, e.what());
            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            rcmd.body.orderResponse.errorId = NetworkError;
            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
        }
    });
}


// ============================================================================
// cancel_order —— DELETE /api/v4/futures/usdt/orders/{id}
// ============================================================================
void GateioUSTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load()) {
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

    std::string idSeg;
    if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
        idSeg = tcmd.body.cancelOrder.orderId;
    } else if (!crypto::str_cmp(tcmd.body.cancelOrder.orderSysId, "")) {
        idSeg = tcmd.body.cancelOrder.orderSysId;
    } else {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = OrderIdError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    std::string pathBase = cancelOrderUrl + "/" + idSeg;
    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign = crypto::getGateioSignatureRest("DELETE", pathBase, time_str, "", "", acc.secretKey);
    std::vector<std::pair<std::string, std::string>> headers = {{"KEY", acc.apiKey}, {"Timestamp", time_str}, {"SIGN", sign}};

    LOG_INFO("TB {} Gate US cancel_order: {}", acc.accountId, pathBase);

    asyncRequest(boost::beast::http::verb::delete_, pathBase, "", "application/json", std::move(headers), [this, rcmd, info](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
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
            int64_t id = 0;
            std::string_view fill_sv;
            std::string_view left_sv;
            
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
                else if (k == "id") {
                    field.value().get(id);
                }
                else if (k == "fill_price") {
                    field.value().get(fill_sv);
                }
                else if (k == "left") {
                    field.value().get(left_sv);
                }
            }

            if (has_status) {
                fmt::format_to(rcmd.body.orderResponse.orderId, "{}", id);
                if (!fill_sv.empty()) {
                    rcmd.body.orderResponse.tradePrice = crypto::fast_atod(fill_sv);
                }

                double left = std::fabs(crypto::fast_atod(left_sv));
                rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
                rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
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
                PUSH_RCMD(rcmd)
            }
            else {
                rcmd.body.orderResponse.orderStatus = OS_FAILED;
                rcmd.body.orderResponse.errorId = UnknownError; 
                crypto::replace_string(resp.body, ":", "");
                crypto::replace_string(resp.body, "\"", "");
                strncpy(rcmd.body.orderResponse.originMsg, resp.body.c_str(), ORIGINMSG_SIZE);
                PUSH_RCMD(rcmd)  
            }
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} Gate US cancel_order cb exc: {}", acc.accountId, e.what());
        }
    });
}


// ============================================================================
// query_order —— GET /api/v4/futures/usdt/orders/{id}
// ============================================================================
void GateioUSTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum, tcmd.body.queryOrder.instTypeEnum, tcmd.body.queryOrder.instId, info)) {
        LOG_INFO("TB {} Gate US query_order smc miss: {}", acc.accountId, tcmd.body.queryOrder.instId);
        return;
    }

    std::string idSeg = "";
    if (!crypto::str_cmp(tcmd.body.queryOrder.orderId, "")) {
        idSeg = tcmd.body.queryOrder.orderId;
    } else if (!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")) {
        idSeg = tcmd.body.queryOrder.orderSysId;
    } else {
        return;
    }

    std::string pathBase = queryOrderUrl + "/" + idSeg;
    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign = crypto::getGateioSignatureRest("GET", pathBase, time_str, "", "", acc.secretKey);
    std::vector<std::pair<std::string, std::string>> headers = {{"KEY", acc.apiKey}, {"Timestamp", time_str}, {"SIGN", sign}};

    LOG_INFO("TB {} Gate US query_order: {}", acc.accountId, pathBase);

    asyncRequest(boost::beast::http::verb::get, pathBase, "", "application/json", std::move(headers), [this, rcmd, info](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
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
            int64_t id = 0;
            std::string_view text_sv;
            std::string_view size_sv;
            std::string_view price_sv;
            std::string_view left_sv;
            std::string_view fill_sv;
            std::string_view finishAs_sv;

            bool has_status = false;

            for (auto field : doc_value) {
                std::string_view k = field.unescaped_key().value_unsafe();
                if (k == "status") {
                    has_status = field.value().get(status_sv) == simdjson::SUCCESS;
                }
                else if (k == "id") {
                    field.value().get(id);
                }
                else if (k == "text") {
                    field.value().get(text_sv);
                }
                else if (k == "size") {
                    field.value().get(size_sv);
                }
                else if (k == "price") {
                    field.value().get(price_sv);
                }
                else if (k == "left") {
                    field.value().get(left_sv);
                }
                else if (k == "fill_price") {
                    field.value().get(fill_sv);
                }
                else if (k == "finish_as") {
                    field.value().get(finishAs_sv);
                }
            }

            if (has_status) {
                fmt::format_to(rcmd.body.orderResponse.orderId, "{}", id);
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, text_sv);
                rcmd.body.orderResponse.volumeTotal = std::fabs(crypto::fast_atod(size_sv));
                rcmd.body.orderResponse.limitPrice = crypto::fast_atod(price_sv);
                double left = std::fabs(crypto::fast_atod(left_sv));
                rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;

                if (!fill_sv.empty()) {
                    rcmd.body.orderResponse.tradePrice = crypto::fast_atod(fill_sv);
                }

                if (status_sv == "open") {
                    rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.volumeTotal > rcmd.body.orderResponse.volumeTraded && rcmd.body.orderResponse.volumeTraded > ZERO_NUM) ? OS_PARTFILLED : OS_NEW;
                } else {
                    if (finishAs_sv == "filled") {
                        rcmd.body.orderResponse.orderStatus = OS_FILLED;
                    }                           
                    else if (finishAs_sv == "cancelled" || finishAs_sv == "liquidated" || finishAs_sv == "ioc" || finishAs_sv == "auto_deleveraged" || finishAs_sv == "reduce_only" || finishAs_sv == "reduce_out") {
                        rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                    }                                                       
                    else {
                        rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                    }                                                      
                }
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)      
            }
            else {
                rcmd.body.orderResponse.errorId = crypto::get_gateio_errorid(resp.body.c_str());
                rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.errorId == OrderNotFoundError) ? OS_REJECTED : OS_UNKNOWN;
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            }
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} Gate US query_order cb exc: {}", acc.accountId, e.what());
        }
    });
}
