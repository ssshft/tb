#include <cmath>
#include <fmt/format.h>
#include <simdjson.h>
#include "bybit/BybitTrade.h"

BybitTradeUnit::BybitTradeUnit(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {

}

BybitTradeUnit::~BybitTradeUnit() {

}

// ============================================================================
// WS auth / subscribe
// ============================================================================
std::string BybitTradeUnit::buildAuthJson() const {
    std::string ts = std::to_string(crypto::getCurrentTimeMilli() + 3000);
    std::string sign = crypto::getBybitSignatureWsAuth(acc.secretKey, ts, "GET/realtime");

    return fmt::format(R"({{"op":"auth","args":["{}",{},"{}"]}})", acc.apiKey, ts, sign);
}

std::string BybitTradeUnit::buildSubscribeJson() const {
    // 订阅账户流: order / wallet / position (三个 topic 一起)
    return R"({"op":"subscribe","args":["order","wallet","position"]})";
}


// ============================================================================
// subWebsocekt
// ============================================================================
void BybitTradeUnit::subWebsocekt() {
    std::string restHost = host_of(acc.restUrl);
    initRestClient(restHost, {}, 4);

    net::WsConfig cfg;
    cfg.url = acc.wsUrl;
    cfg.ping_mode = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text = R"({"op":"ping"})";
    cfg.auto_reconnect = true;
    cfg.idle_timeout_sec = 60;
    LOG_INFO("TB {} Bybit ws {} rest {}", acc.accountId, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));
}


// ============================================================================
// onOpen: auth + subscribe
// ============================================================================
void BybitTradeUnit::onOpen() {
    BaseTradeUnit::onOpen();
    pWsClient->send_text(buildAuthJson());
    pWsClient->send_text(buildSubscribeJson());
}

// ============================================================================
// onWebsocketMsg
// ============================================================================
void BybitTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool, int64_t) {
    try {
        std::string msg(reinterpret_cast<const char*>(data), len);
        std::cout << "onWebsocketMsg: " << msg << std::endl;

        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc = g_parser.iterate(padded);
        if (doc.error()) {
            return;
        }

        auto doc_value = doc.get_object().value_unsafe();

        std::string_view topic_sv;
        simdjson::ondemand::array data_arr;

        for (auto field : doc_value) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "topic") {
                field.value().get(topic_sv);
            }
            else if (k == "data") {
                field.value().get(data_arr);
                if (topic_sv == "wallet") {
                    handleAccountUpdate(data_arr);
                }
                else if (topic_sv == "position") {
                    handlePositionsUpdate(data_arr);
                }
                else if (topic_sv == "order") {
                    handleOrdersUpdate(data_arr);
                }
            }
        }
    }
    catch (const std::exception& e) {
        LOG_ERROR("TB {} Bybit ws exc: {}", acc.accountId, e.what());
    }
}

void BybitTradeUnit::handleWalletUpdate(simdjson::ondemand::array& dataArr) {
    for (auto b_val : dataArr) {
        auto b_res = b_val.get_object();
        if (b_res.error()) {
            continue;
        }
        auto& b = b_res.value_unsafe();

        std::string_view totalEq_sv;
        std::string_view adjEq_sv;
        std::string_view imr_sv;
        std::string_view mmr_sv;
        simdjson::ondemand::array coin_arr;

        for (auto field : b) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "totalEquity") {
                field.value().get(totalEq_sv);
            }
            else if (k == "totalWalletBalance") {
                field.value().get(adjEq_sv);
            }
            else if (k == "totalInitialMargin") {
                field.value().get(imr_sv);
            }
            else if (k == "totalMaintenanceMargin") {
                field.value().get(mmr_sv);
            }
            else if (k == "coin") {
                field.value().get(coin_arr);
                for (auto c_val : coin_arr) {
                    auto b_res = c_val.get_object();
                    if (c_res.error()) {
                        continue;
                    }
                    auto& c = c_res.value_unsafe();

                    std::string_view ccy_sv;
                    std::string_view eq_sv;
                    std::string_view wal_sv;
                    std::string_view avail_sv;
                    std::string_view upl_sv;
                    for (auto f : c) {
                        std::string_view ck = f.unescaped_key().value_unsafe();
                        if (ck == "coin") {
                            f.value().get(ccy_sv);
                        }
                        else if (ck == "equity") {
                            f.value().get(eq_sv);
                        }
                        else if (ck == "walletBalance") {
                            f.value().get(wal_sv);
                        }
                        else if (ck == "availableToWithdraw") {
                            f.value().get(avail_sv);
                        }
                        else if (ck == "unrealisedPnl") {
                            f.value().get(upl_sv);
                        }
                    }

                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = BYBIT;
                    rcmd.body.balance.instTypeEnum = SPOT;
                    crypto::copy_sv_to_char_array(rcmd.body.balance.accountId, acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(ccy_sv)));
                    rcmd.body.balance.available = crypto::fast_atod(avail_sv);
                    rcmd.body.balance.total = crypto::fast_atod(eq_sv.empty() ? wal_sv : eq_sv);
                    rcmd.body.balance.unrealizedPnl = crypto::fast_atod(upl_sv);
                    rcmd.body.balance.updateTime = crypto::getCurrentTime();
                    rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;
                    PUSH_RCMD(rcmd);
                }
            }
        }

        // totalAccount
        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
        rcmd.body.totalAccount.exchangeTypeEnum = BYBIT;
        rcmd.body.totalAccount.instTypeEnum = SPOT;
        crypto::copy_sv_to_char_array(rcmd.body.totalAccount.accountId, acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.totalAccount.strategyId, acc.strategyId);
        rcmd.body.totalAccount.totalEquity = crypto::fast_atod(totalEq_sv);
        rcmd.body.totalAccount.adjEquity = crypto::fast_atod(adjEq_sv);
        rcmd.body.totalAccount.mmr = crypto::fast_atod(mmr_sv);
        rcmd.body.totalAccount.mgnRatio = 100.0;
        rcmd.body.totalAccount.updateTime = crypto::getCurrentTime();
        rcmd.body.totalAccount.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd)
    
    }
}

// ---- position update ----
// data = [{category, symbol, side, size, avgPrice, positionValue, unrealisedPnl, markPrice, liqPrice, ...}]
void BybitTradeUnit::handlePositionUpdate(simdjson::ondemand::array& dataArr) {
    for (auto b_val : dataArr) {
        auto b_res = b_val.get_object();
        if (b_res.error()) {
            continue;
        }
        auto& b = b_res.value_unsafe();

        std::string_view cat_sv;
        std::string_view sym_sv;
        std::string_view side_sv;
        std::string_view size_sv;
        std::string_view avg_sv;
        std::string_view upl_sv;
        std::string_view mark_sv;
        std::string_view liq_sv;
        int adl;

        for (auto field : b) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "category") {
                field.value().get(cat_sv);
            }
            else if (k == "symbol") {
                field.value().get(sym_sv);
            }
            else if (k == "side") {
                field.value().get(side_sv);
            }
            else if (k == "size") {
                field.value().get(size_sv);
            }
            else if (k == "avgPrice") {
                field.value().get(avg_sv);
            }
            else if (k == "unrealisedPnl") {
                field.value().get(upl_sv);
            }
            else if (k == "markPrice") {
                field.value().get(mark_sv);
            }
            else if (k == "liqPrice") {
                field.value().get(liq_sv);
            }
            else if (k == "adlRankIndicator") {
                field.value().get(adl);
            }
        }

        std::string originInstId(sym_sv);
        std::string category(cat_sv);
        md::InstrumentInfo info;
        InstType instType;

        if (category == "linear") {
            if (smc->get_instrument_info(BYBIT, USDT_SWAP, originInstId.c_str(), info)) { 
                instType = USDT_SWAP;
            }
            else if (smc->get_instrument_info(BYBIT, USDT_FUTURES, originInstId.c_str(), info)) { 
                instType = USDT_FUTURES; 
            }
        } else if (category == "inverse") {
            if (smc->get_instrument_info(BYBIT, C_SWAP, originInstId.c_str(), info)) { 
                instType = C_SWAP; 
            }
            else if (smc->get_instrument_info(BYBIT, C_FUTURES, originInstId.c_str(), info)) { 
                instType = C_FUTURES; 
            }
        }

        double sz = crypto::fast_atod(size_sv);
        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
        rcmd.body.position.exchangeTypeEnum = BYBIT;
        rcmd.body.position.instTypeEnum = instType;
        crypto::copy_sv_to_char_array(rcmd.body.position.accountId, acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view(info.instId));
        rcmd.body.position.direction = (!side_sv.empty() && side_sv[0] == 'B') ? DT_LONG : DT_SHORT;
        rcmd.body.position.volume = sz;
        rcmd.body.position.avgPrice = crypto::fast_atod(avg_sv);
        rcmd.body.position.unrealizedPnl = crypto::fast_atod(upl_sv);
        rcmd.body.position.markPrice = crypto::fast_atod(mark_sv);
        rcmd.body.position.liquidPrice = crypto::fast_atod(liq_sv);
        rcmd.body.position.adlQuantile = adl >= 2 ? adl - 1 : adl;
        rcmd.body.position.updateTime = crypto::getCurrentTime();
        rcmd.body.position.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd);
    }
}

// ---- order update ----
// data = [{category, symbol, orderId, orderLinkId, side, orderType, timeInForce,
//          qty, price, orderStatus, cumExecQty, avgPrice, cumExecValue, ...}]
void BybitTradeUnit::handleOrdersUpdate(simdjson::ondemand::array& dataArr) {
    for (auto b_val : dataArr) {
        auto b_res = b_val.get_object();
        if (b_res.error()) {
            continue;
        }
        auto& b = b_res.value_unsafe();

        std::string_view cat_sv;
        std::string_view symbol_sv;
        std::string_view ordId_sv;
        std::string_view ordLink_sv;
        std::string_view qty_sv;
        std::string_view price_sv;
        std::string_view cum_sv;
        std::string_view avg_sv;
        std::string_vie side_sv;
        std::string_vie oType_sv;
        std::string_vie tif_sv;
        std::string_view status_sv;
        std::string_view createTy_sv;

        for (auto field : b) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "category") {
                field.value().get(cat_sv);
            }
            else if (k == "symbol") {
                field.value().get(symbol_sv);
            }
            else if (k == "orderId") {
                field.value().get(ordId_sv);
            }
            else if (k == "orderLinkId") {
                field.value().get(ordLink_sv);
            }
            else if (k == "qty") {
                field.value().get(qty_sv);
            }
            else if (k == "price") {
                field.value().get(price_sv);
            }
            else if (k == "cumExecQty") {
                field.value().get(cum_sv);
            }
            else if (k == "avgPrice") {
                field.value().get(avg_sv);
            }
            else if (k == "side") {
                field.value().get(side_sv);
            }
            else if (k == "orderType") {
                field.value().get(oType_sv);
            }
            else if (k == "timeInForce") {
                field.value().get(tif_sv);
            }
            else if (k == "orderStatus") {
                field.value().get(status_sv);
            }
            else if (k == "createType") {
                field.value().get(createTy_sv);
            }
        }

        std::string originInstId(symbol_sv);
        std::string category(cat_sv);
        md::InstrumentInfo info;
        InstType instType;

        if (category == "linear") {
            if (smc->get_instrument_info(BYBIT, USDT_SWAP, originInstId.c_str(), info)) { 
                instType = USDT_SWAP;
            }
            else if (smc->get_instrument_info(BYBIT, USDT_FUTURES, originInstId.c_str(), info)) { 
                instType = USDT_FUTURES; 
            }
        } else if (category == "inverse") {
            if (smc->get_instrument_info(BYBIT, C_SWAP, originInstId.c_str(), info)) { 
                instType = C_SWAP; 
            }
            else if (smc->get_instrument_info(BYBIT, C_FUTURES, originInstId.c_str(), info)) { 
                instType = C_FUTURES; 
            }
        }

        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, ordId_sv);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, ordLink_sv);
        crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view(info.instId));

        if (!qty_sv.empty()) {
            rcmd.body.orderResponse.volumeTotal = crypto::fast_atod(qty_sv);
        }

        if (!price_sv.empty()) {
            rcmd.body.orderResponse.limitPrice = crypto::fast_atod(price_sv);
        }

        if (!cum_sv.empty()) {
            rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(cum_sv);
        }

        if (!avg_sv.empty()) {
            rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avg_sv);
        }

        rcmd.body.orderResponse.offsetFlag = OF_OPEN;
        if (!side_sv.empty()) {
            rcmd.body.orderResponse.direction = (side_sv[0] == 'B') ? DT_LONG : DT_SHORT;
        }

        if (!oType_sv.empty()) {
            // Bybit: "Limit" / "Market". Post-only 靠 timeInForce=PostOnly。
            if (oType_sv == "Market") {
                rcmd.body.orderResponse.orderType = OT_MARKET;
            } else if (oType_sv == "Limit") {
                if (tif_sv == "PostOnly") {
                    rcmd.body.orderResponse.orderType = OT_POST_ONLY;
                }
                else if (tif_sv == "FOK") {
                    rcmd.body.orderResponse.orderType = OT_FOK;
                }
                else if (tif_sv == "IOC") {
                    rcmd.body.orderResponse.orderType = OT_IOC;
                }
                else {
                    rcmd.body.orderResponse.orderType = OT_LIMIT;
                }                        
            }
        }

        if (status_sv[0] == 'N' && status_sv[2] == 'e' || status_sv[0] == 'C') {
            rcmd.body.orderResponse.orderStatus = OS_NEW;
        }
        else if (status_sv[0] == 'P') {
            if (status_sv.size() > 15) {
                rcmd.body.orderResponse.orderStatus = OS_CANCELED;
            }
            else {
                rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
            }
        }
        else if (status_sv[0] == 'F') {
            rcmd.body.orderResponse.orderStatus = OS_FILLED;
        }
        else if (status_sv[0] == 'C' && status_sv[0] == 'n') {
            rcmd.body.orderResponse.orderStatus = OS_CANCELED;
        }
        else {
            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        }

        if (createTy_sv == "CreateByAdl_PassThrough") {
            rcmd.body.orderResponse.errorId = ADLError;
        }
        else if (createTy_sv == "CreateByLiq") {
            rcmd.body.orderResponse.errorId = LiquidationError;
        }

        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
    }













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
        if (!lookupInstrument(originInstId, category, info, instType)) {
            LOG_ERROR("TB {} Bybit order not found in smc: {} ({})", acc.accountId, originInstId, category);
            continue;
        }

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
            // Bybit: "Limit" / "Market". Post-only 靠 timeInForce=PostOnly。
            if (oType_sv == "Market") {
                rcmd.body.orderResponse.orderType = OT_MARKET;
            } else if (oType_sv == "Limit") {
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

        // Bybit orderStatus: New / PartiallyFilled / Filled / Cancelled / Rejected / ...
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

// ============================================================================
// query_account —— 走 balance (Bybit 无独立 account 端点)
// ============================================================================
void BybitTradeUnit::query_account(const pubsub::TCommand& tcmd) {
    query_balance(tcmd);
}


// ============================================================================
// query_balance —— GET /v5/account/wallet-balance?accountType=UNIFIED
// ============================================================================
void BybitTradeUnit::query_balance(const pubsub::TCommand&) {
    std::string query = "accountType=UNIFIED";
    std::string fullPath = balanceUrl + "?" + query;

    std::string ts = std::to_string(crypto::getCurrentTimeMilli());
    std::string sign = crypto::getBybitSignatureRest(acc.secretKey, ts, acc.apiKey, "5000", query);
    std::vector<std::pair<std::string, std::string>> headers = {{"X-BAPI-API-KEY", acc.apiKey}, {"X-BAPI-TIMESTAMP", ts}, {"X-BAPI-RECV-WINDOW", "5000"}, {"X-BAPI-SIGN", sign}};

    asyncRequest(boost::beast::http::verb::get, std::move(fullPath), "", "", std::move(headers), [this](boost::system::error_code ec, ::net::HttpResponse resp) {
        if (ec) { 
            LOG_ERROR("TB {} Bybit query_balance ec: {}", acc.accountId, ec.message()); 
            return; 
        }
        
        try {
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
              return;  
            }

            std::vector<pubsub::RCommand> pending;
            simdjson::ondemand::array list_arr;
            if (doc["result"]["list"].get(list_arr) == simdjson::SUCCESS) {
                for (auto b_val : list_arr) {
                    auto b_res = b_val.get_object();
                    if (b_res.error()) {
                        continue;
                    }
                    auto& b = b_res.value_unsafe();

                    std::string_view totalEq_sv;
                    std::string_view adjEq_sv;
                    std::string_view imr_sv;
                    std::string_view mmr_sv;
                    simdjson::ondemand::array coin_arr;

                    for (auto field : b) {
                        std::string_view k = field.unescaped_key().value_unsafe();
                        if (k == "totalEquity") {
                            field.value().get(totalEq_sv);
                        }
                        else if (k == "totalWalletBalance") {
                            field.value().get(adjEq_sv);
                        }
                        else if (k == "totalInitialMargin") {
                            field.value().get(imr_sv);
                        }
                        else if (k == "totalMaintenanceMargin") {
                            field.value().get(mmr_sv);
                        }
                        else if (k == "coin") {
                            field.value().get(coin_arr);
                            for (auto c_val : coin_arr) {
                                auto b_res = c_val.get_object();
                                if (c_res.error()) {
                                    continue;
                                }
                                auto& c = c_res.value_unsafe();

                                std::string_view ccy_sv;
                                std::string_view eq_sv;
                                std::string_view wal_sv;
                                std::string_view avail_sv;
                                std::string_view upl_sv;
                                for (auto f : c) {
                                    std::string_view ck = f.unescaped_key().value_unsafe();
                                    if (ck == "coin") {
                                        f.value().get(ccy_sv);
                                    }
                                    else if (ck == "equity") {
                                        f.value().get(eq_sv);
                                    }
                                    else if (ck == "walletBalance") {
                                        f.value().get(wal_sv);
                                    }
                                    else if (ck == "availableToWithdraw") {
                                        f.value().get(avail_sv);
                                    }
                                    else if (ck == "unrealisedPnl") {
                                        f.value().get(upl_sv);
                                    }
                                }

                                pubsub::RCommand rcmd;
                                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                                rcmd.body.balance.exchangeTypeEnum = BYBIT;
                                rcmd.body.balance.instTypeEnum = SPOT;
                                crypto::copy_sv_to_char_array(rcmd.body.balance.accountId, acc.accountId);
                                crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                                crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(ccy_sv)));
                                rcmd.body.balance.available = crypto::fast_atod(avail_sv);
                                rcmd.body.balance.total = crypto::fast_atod(eq_sv.empty() ? wal_sv : eq_sv);
                                rcmd.body.balance.unrealizedPnl = crypto::fast_atod(upl_sv);
                                rcmd.body.balance.updateTime = crypto::getCurrentTime();
                                rcmd.body.balance.apiSourceEnum = AS_REST;
                                pending.emplace_back(rcmd);
                            }
                        }
                    }

                    // totalAccount
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
                    rcmd.body.totalAccount.exchangeTypeEnum = BYBIT;
                    rcmd.body.totalAccount.instTypeEnum = SPOT;
                    crypto::copy_sv_to_char_array(rcmd.body.totalAccount.accountId, acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.totalAccount.strategyId, acc.strategyId);
                    rcmd.body.totalAccount.totalEquity = crypto::fast_atod(totalEq_sv);
                    rcmd.body.totalAccount.adjEquity = crypto::fast_atod(adjEq_sv);
                    rcmd.body.totalAccount.mmr = crypto::fast_atod(mmr_sv);
                    rcmd.body.totalAccount.mgnRatio = 100.0;
                    rcmd.body.totalAccount.updateTime = crypto::getCurrentTime();
                    rcmd.body.totalAccount.apiSourceEnum = AS_REST;
                    PUSH_RCMD(rcmd)
                
                }
            }

            if (pending.empty()) {
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                rcmd.body.balance.exchangeTypeEnum = BYBIT;
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
        } catch (const std::exception& e) {
            LOG_ERROR("TB {} Bybit query_balance cb exc: {}", acc.accountId, e.what());
        }
    });
}


// ============================================================================
// query_position —— GET /v5/position/list?category=X&settleCoin=USDT
// ============================================================================
void BybitTradeUnit::query_position(const pubsub::TCommand& tcmd) {
    std::string category = "";
    if (tcmd.body.queryPosition.instTypeEnum == USDT_SWAP || tcmd.body.queryPosition.instTypeEnum == USDT_FUTURES) {
        category = "linear";
    }
    else if (tcmd.body.queryPosition.instTypeEnum == C_SWAP || tcmd.body.queryPosition.instTypeEnum == C_FUTURES) {
        category = "inverse";
    }

    std::string query = fmt::format("category={}&settleCoin=USDT", category);
    std::string fullPath = positionUrl + "?" + query;

    std::string ts = std::to_string(crypto::getCurrentTimeMilli());
    std::string sign = crypto::getBybitSignatureRest(acc.secretKey, ts, acc.apiKey, "5000", query);
    std::vector<std::pair<std::string, std::string>> headers = {{"X-BAPI-API-KEY", acc.apiKey}, {"X-BAPI-TIMESTAMP", ts}, {"X-BAPI-RECV-WINDOW", "5000"}, {"X-BAPI-SIGN", sign}};

    asyncRequest(boost::beast::http::verb::get, std::move(fullPath), "", "", std::move(headers), [this](boost::system::error_code ec, ::net::HttpResponse resp) {
        if (ec) { 
            LOG_ERROR("TB {} Bybit query_position ec: {}", acc.accountId, ec.message()); 
            return; 
        }

        try {
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
              return;  
            }

            std::vector<pubsub::RCommand> pending;
            simdjson::ondemand::array list_arr;
            if (doc["result"]["list"].get(list_arr) == simdjson::SUCCESS) {
                for (auto b_val : list_arr) {
                    auto b_res = b_val.get_object();
                    if (b_res.error()) {
                        continue;
                    }
                    auto& b = b_res.value_unsafe();

                    std::string_view cat_sv;
                    std::string_view sym_sv;
                    std::string_view side_sv;
                    std::string_view size_sv;
                    std::string_view avg_sv;
                    std::string_view upl_sv;
                    std::string_view mark_sv;
                    std::string_view liq_sv;
                    int adl;

                    for (auto field : b) {
                        std::string_view k = field.unescaped_key().value_unsafe();
                        if (k == "category") {
                            field.value().get(cat_sv);
                        }
                        else if (k == "symbol") {
                            field.value().get(sym_sv);
                        }
                        else if (k == "side") {
                            field.value().get(side_sv);
                        }
                        else if (k == "size") {
                            field.value().get(size_sv);
                        }
                        else if (k == "avgPrice") {
                            field.value().get(avg_sv);
                        }
                        else if (k == "unrealisedPnl") {
                            field.value().get(upl_sv);
                        }
                        else if (k == "markPrice") {
                            field.value().get(mark_sv);
                        }
                        else if (k == "liqPrice") {
                            field.value().get(liq_sv);
                        }
                        else if (k == "adlRankIndicator") {
                            field.value().get(adl);
                        }
                    }

                    std::string originInstId(sym_sv);
                    std::string category(cat_sv);
                    md::InstrumentInfo info;
                    InstType instType;
        
                    if (category == "linear") {
                        if (smc->get_instrument_info(BYBIT, USDT_SWAP, originInstId.c_str(), info)) { 
                            instType = USDT_SWAP;
                        }
                        else if (smc->get_instrument_info(BYBIT, USDT_FUTURES, originInstId.c_str(), info)) { 
                            instType = USDT_FUTURES; 
                        }
                    } else if (category == "inverse") {
                        if (smc->get_instrument_info(BYBIT, C_SWAP, originInstId.c_str(), info)) { 
                            instType = C_SWAP; 
                        }
                        else if (smc->get_instrument_info(BYBIT, C_FUTURES, originInstId.c_str(), info)) { 
                            instType = C_FUTURES; 
                        }
                    }

                    double sz = crypto::fast_atod(size_sv);
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                    rcmd.body.position.exchangeTypeEnum = BYBIT;
                    rcmd.body.position.instTypeEnum = instType;
                    crypto::copy_sv_to_char_array(rcmd.body.position.accountId, acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view(info.instId));
                    rcmd.body.position.direction = (!side_sv.empty() && side_sv[0] == 'B') ? DT_LONG : DT_SHORT;
                    rcmd.body.position.volume = sz;
                    rcmd.body.position.avgPrice = crypto::fast_atod(avg_sv);
                    rcmd.body.position.unrealizedPnl = crypto::fast_atod(upl_sv);
                    rcmd.body.position.markPrice = crypto::fast_atod(mark_sv);
                    rcmd.body.position.liquidPrice = crypto::fast_atod(liq_sv);
                    rcmd.body.position.adlQuantile = adl >= 2 ? adl - 1 : adl;
                    rcmd.body.position.updateTime = crypto::getCurrentTime();
                    rcmd.body.position.apiSourceEnum = AS_REST;
                    pending.emplace_back(rcmd);
                }
            }

            if (pending.empty()) {
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                rcmd.body.position.exchangeTypeEnum = BYBIT;
                rcmd.body.position.instTypeEnum = USDT_SWAP;
                crypto::copy_sv_to_char_array(rcmd.body.position.accountId, acc.accountId);
                crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view("BTC-USDT"));
                rcmd.body.position.updateTime = crypto::getCurrentTime();
                rcmd.body.position.apiSourceEnum = AS_REST;
                rcmd.body.position.isLast = true;
                PUSH_RCMD(rcmd);
            }

            for (size_t i = 0; i < pending.size(); ++i) {
                pending[i].body.position.isLast = (i + 1 == pending.size());
                PUSH_RCMD(pending[i])
            }
        } catch (const std::exception& e) {
            LOG_ERROR("TB {} Bybit query_position cb exc: {}", acc.accountId, e.what());
        }
    });
}


// ============================================================================
// add_new_order —— POST /v5/order/create (body JSON)
// ============================================================================
void BybitTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
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

    std::string category = "";
    if (tcmd.body.newOrder.instTypeEnum == USDT_SWAP) {
        category = "spot";
    }
    else if (tcmd.body.newOrder.instTypeEnum == USDT_SWAP || tcmd.body.newOrder.instTypeEnum == USDT_FUTURES) {
        category = "linear";
    }
    else if (tcmd.body.newOrder.instTypeEnum == C_SWAP || tcmd.body.newOrder.instTypeEnum == C_FUTURES) {
        category = "inverse";
    }

    if (category == "") {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId = OrderTypeError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const char* side = nullptr;
    if (tcmd.body.newOrder.offsetFlag == OF_OPEN) {
        if (tcmd.body.newOrder.direction == DT_LONG) {
            side = "Buy";
        }
        else if (tcmd.body.newOrder.direction == DT_SHORT) {
            side = "Sell";
        }
    } else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
        if (tcmd.body.newOrder.direction == DT_LONG) {
            side = "Sell";
        }
        else if (tcmd.body.newOrder.direction == DT_SHORT) {
            side = "Buy";
        }
    }
    if (!side) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId = (tcmd.body.newOrder.offsetFlag == OF_OPEN || tcmd.body.newOrder.offsetFlag == OF_CLOSE) ? DirectionError : OffsetFlagError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const char* ordType = nullptr;
    const char* tif = nullptr;
    switch (tcmd.body.newOrder.orderType) {
        case OT_LIMIT:     
            ordType = "Limit";  
            tif = "GTC";      
            break;
        case OT_MARKET:    
            ordType = "Market"; 
            tif = "IOC";      
            break;
        case OT_POST_ONLY: 
            ordType = "Limit";  
            tif = "PostOnly"; 
            break;
        case OT_FOK:       
            ordType = "Limit";  
            tif = "FOK";      
            break;
        case OT_IOC:       
            ordType = "Limit";  
            tif = "IOC";      
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
    std::string price_str = fmt::format("{}", price);
    std::string qty_str = fmt::format("{}", volume);

    // Bybit body: {category, symbol, side, orderType, qty, price(可选,Market跳过), timeInForce, orderLinkId, reduceOnly}
    std::string body;
    if (tcmd.body.newOrder.orderType == OT_MARKET) {
        body = fmt::format(
            R"({{"category":"{}","symbol":"{}","side":"{}","orderType":"{}","qty":"{}","timeInForce":"{}","orderLinkId":"{}","reduceOnly":{}}})",
            cat, info.originInstId, side, ordType, qty_str, tif,
            escape_json(tcmd.body.newOrder.orderSysId),
            tcmd.body.newOrder.reduceOnly ? "true" : "false");
    } else {
        body = fmt::format(
            R"({{"category":"{}","symbol":"{}","side":"{}","orderType":"{}","qty":"{}","price":"{}","timeInForce":"{}","orderLinkId":"{}","reduceOnly":{}}})",
            cat, info.originInstId, side, ordType, qty_str, price_str, tif,
            escape_json(tcmd.body.newOrder.orderSysId),
            tcmd.body.newOrder.reduceOnly ? "true" : "false");
    }

    LOG_INFO("TB {} Bybit add_new_order body={}", acc.accountId, body);

    std::string ts = std::to_string(crypto::getCurrentTimeMilli());
    std::string sign = crypto::getBybitSignatureRest(acc.secretKey, ts, acc.apiKey, "5000", body);
    std::vector<std::pair<std::string, std::string>> headers = {{"X-BAPI-API-KEY", acc.apiKey}, {"X-BAPI-TIMESTAMP", ts}, {"X-BAPI-RECV-WINDOW", "5000"}, {"X-BAPI-SIGN", sign}};

    asyncRequest(boost::beast::http::verb::post, orderUrl, std::move(body), "application/json", std::move(headers),
        [this, rcmd, info](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
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

                int64_t retCode = 0;
                std::string_view retMsg_sv;
                simdjson::ondemand::object result;
                std::string_view orderId_sv;

                for (auto field : doc_value) {
                    std::string_view k = field.unescaped_key().value_unsafe();
                    if (k == "retCode") {
                        field.value().get(retCode);
                    }
                    else if (k == "retMsg") {
                        field.value().get(retMsg_sv);
                    }
                    else if (k == "result") {
                        field.value().get(result);
                        result["orderId"].get(orderId_sv);
                    }
                }

                if (retCode == 0 && !orderId_sv.empty()) {
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, orderId_sv);
                    rcmd.body.orderResponse.orderStatus = OS_NEW;
                }
                else {
                    rcmd.body.orderResponse.errorId = crypto::get_bybit_errorid(retCode);
                    if (rcmd.body.orderResponse.errorId == 10016) {
                        rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                    }
                    else {
                        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    }

                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, retMsg_sv);
                }

                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} Bybit add_new_order cb exc: {}", acc.accountId, e.what());
            }
        });
}


// ============================================================================
// cancel_order —— POST /v5/order/cancel
// ============================================================================
void BybitTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.cancelOrder.exchangeTypeEnum, tcmd.body.cancelOrder.instTypeEnum, tcmd.body.cancelOrder.instId, info)) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = SMCInstrumentNotExistError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    std::string category = "";
    if (tcmd.body.cancelOrder.instTypeEnum == USDT_SWAP) {
        category = "spot";
    }
    else if (tcmd.body.cancelOrder.instTypeEnum == USDT_SWAP || tcmd.body.cancelOrder.instTypeEnum == USDT_FUTURES) {
        category = "linear";
    }
    else if (tcmd.body.cancelOrder.instTypeEnum == C_SWAP || tcmd.body.cancelOrder.instTypeEnum == C_FUTURES) {
        category = "inverse";
    }

    if (category == "") {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = OrderTypeError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    std::string body;
    if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
        body = fmt::format(R"({{"category":"{}","symbol":"{}","orderId":"{}"}})", cat, info.originInstId, tcmd.body.cancelOrder.orderId);
    } else if (!crypto::str_cmp(tcmd.body.cancelOrder.orderSysId, "")) {
        body = fmt::format(R"({{"category":"{}","symbol":"{}","orderLinkId":"{}"}})", cat, info.originInstId, escape_json(tcmd.body.cancelOrder.orderSysId));
    } else {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = OrderIdError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    LOG_INFO("TB {} Bybit cancel_order body={}", acc.accountId, body);

    std::string ts = std::to_string(crypto::getCurrentTimeMilli());
    std::string sign = crypto::getBybitSignatureRest(acc.secretKey, ts, acc.apiKey, "5000", body);
    std::vector<std::pair<std::string, std::string>> headers = {{"X-BAPI-API-KEY", acc.apiKey}, {"X-BAPI-TIMESTAMP", ts}, {"X-BAPI-RECV-WINDOW", "5000"}, {"X-BAPI-SIGN", sign}};

    asyncRequest(boost::beast::http::verb::post, cancelOrderUrl, std::move(body), "application/json", std::move(headers), [this, rcmd](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
            if (ec) {
                rcmd.body.orderResponse.orderStatus = OS_FAILED;
                rcmd.body.orderResponse.errorId = NetworkError;
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, ec.message());
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;
            }

            try {
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

                int64_t retCode = 0;
                std::string_view retMsg_sv;

                for (auto field : doc_value) {
                    std::string_view k = field.unescaped_key().value_unsafe();
                    if (k == "retCode") {
                        field.value().get(retCode);
                    }
                    else if (k == "retMsg") {
                        field.value().get(retMsg_sv);
                    }
                }

                if (retCode != 0) {
                    if (retCode == 170213) {
                        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    }
                    else {
                        rcmd.body.orderResponse.orderStatus = OS_FAILED;
                    }

                    rcmd.body.orderResponse.errorId = crypto::get_bybit_errorid(retCode);
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, retMsg_sv);

                    rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                }
                else { // 正常的返回不推送，报单实际状态在ws推送中

                }
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} Bybit cancel_order cb exc: {}", acc.accountId, e.what());
            }
        });
}


// ============================================================================
// query_order —— GET /v5/order/realtime?category=X&orderId=Y (或 orderLinkId)
// ============================================================================
void BybitTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum, tcmd.body.queryOrder.instTypeEnum, tcmd.body.queryOrder.instId, info)) {
        return;
    }

    std::string category = "";
    if (tcmd.body.cancelOrder.instTypeEnum == USDT_SWAP) {
        category = "spot";
    }
    else if (tcmd.body.cancelOrder.instTypeEnum == USDT_SWAP || tcmd.body.cancelOrder.instTypeEnum == USDT_FUTURES) {
        category = "linear";
    }
    else if (tcmd.body.cancelOrder.instTypeEnum == C_SWAP || tcmd.body.cancelOrder.instTypeEnum == C_FUTURES) {
        category = "inverse";
    }

    std::string query = fmt::format("category={}&symbol={}", category, info.originInstId);
    if (!crypto::str_cmp(tcmd.body.queryOrder.orderId, "")) {
        query += "&orderId=" + std::string(tcmd.body.queryOrder.orderId);
    } else if (!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")) {
        query += "&orderLinkId=" + std::string(tcmd.body.queryOrder.orderSysId);
    } else {
        return;
    }
    std::string fullPath = queryOrderUrl + "?" + query;

    LOG_INFO("TB {} Bybit query_order: {}", acc.accountId, fullPath);

    std::string ts = std::to_string(crypto::getCurrentTimeMilli());
    std::string sign = crypto::getBybitSignatureRest(acc.secretKey, ts, acc.apiKey, "5000", query);
    std::vector<std::pair<std::string, std::string>> headers = {{"X-BAPI-API-KEY", acc.apiKey}, {"X-BAPI-TIMESTAMP", ts}, {"X-BAPI-RECV-WINDOW", "5000"}, {"X-BAPI-SIGN", sign}};

    asyncRequest(boost::beast::http::verb::get, std::move(fullPath), "", "", std::move(headers), [this, rcmd, info](boost::system::error_code ec, net::HttpResponse resp) mutable {
        if (ec) {
            LOG_ERROR("TB {} query_order ec: {}", acc.accountId, ec.message());
            return;
        }

        try {
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                LOG_ERROR("TB {} query_order parse err: {}", acc.accountId, resp.body);
              return;  
            }

            std::vector<pubsub::RCommand> pending;
            simdjson::ondemand::array list_arr;
            if (doc["result"]["list"].get(list_arr) == simdjson::SUCCESS) {
                for (auto b_val : list_arr) {
                    auto b_res = b_val.get_object();
                    if (b_res.error()) {
                        continue;
                    }
                    auto& b = b_res.value_unsafe();

                    std::string_view symbol_sv;
                    std::string_view ordId_sv;
                    std::string_view ordLink_sv;
                    std::string_view qty_sv;
                    std::string_view price_sv;
                    std::string_view cum_sv;
                    std::string_view avg_sv;
                    std::string_view status_sv;

                    for (auto field : b) {
                        std::string_view k = field.unescaped_key().value_unsafe();
                        if (k == "symbol") {
                            field.value().get(symbol_sv);
                        }
                        else if (k == "orderId") {
                            field.value().get(ordId_sv);
                        }
                        else if (k == "orderLinkId") {
                            field.value().get(ordLink_sv);
                        }
                        else if (k == "qty") {
                            field.value().get(qty_sv);
                        }
                        else if (k == "price") {
                            field.value().get(price_sv);
                        }
                        else if (k == "cumExecQty") {
                            field.value().get(cum_sv);
                        }
                        else if (k == "avgPrice") {
                            field.value().get(avg_sv);
                        }
                        else if (k == "orderStatus") {
                            field.value().get(status_sv);
                        }
                    }

                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, ordId_sv);
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, ordLink_sv);

                    if (!qty_sv.empty()) {
                        rcmd.body.orderResponse.volumeTotal = crypto::fast_atod(qty_sv);
                    }

                    if (!price_sv.empty()) {
                        rcmd.body.orderResponse.limitPrice = crypto::fast_atod(price_sv);
                    }

                    if (!cum_sv.empty()) {
                        rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(cum_sv);
                    }

                    if (!avg_sv.empty()) {
                        rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avg_sv);
                    }

                    if (status_sv[0] == 'N' && status_sv[2] == 'e' || status_sv[0] == 'C') {
                        rcmd.body.orderResponse.orderStatus = OS_NEW;
                    }
                    else if (status_sv[0] == 'P') {
                        if (status_sv.size() > 15) {
                            rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                        }
                        else {
                            rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
                        }
                    }
                    else if (status_sv[0] == 'F') {
                        rcmd.body.orderResponse.orderStatus = OS_FILLED;
                    }
                    else if (status_sv[0] == 'C' && status_sv[0] == 'n') {
                        rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                    }
                    else {
                        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    }

                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                }
            }
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} Bybit query_order cb exc: {}", acc.accountId, e.what());
        }
    });
}