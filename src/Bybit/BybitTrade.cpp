#include "Bybit/BybitTrade.h"

#include <cmath>

#include <fmt/format.h>
#include <simdjson.h>



BybitTradeUnit::BybitTradeUnit(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {}
BybitTradeUnit::~BybitTradeUnit() {}


// ============================================================================
// category 映射
// ============================================================================
const char* BybitTradeUnit::categoryOf(InstType t) {
    if (t == SPOT)                                    return "spot";
    if (t == USDT_SWAP || t == USDT_FUTURES)          return "linear";
    if (t == C_SWAP    || t == C_FUTURES)             return "inverse";
    return nullptr;
}


// ============================================================================
// signature / headers
// ============================================================================
std::string BybitTradeUnit::bybitSign(const std::string& timestamp,
                                      const std::string& payload) const {
    // sign_content = timestamp + apiKey + recvWindow + payload
    return crypto::HmacEncodeBybit(acc.secretKey,
                                   timestamp + acc.apiKey + recvWindow_ + payload);
}

std::vector<std::pair<std::string, std::string>>
BybitTradeUnit::bybitAuthHeaders(const std::string& payload) const {
    std::string ts   = std::to_string(crypto::getCurrentTimeMilli());
    std::string sign = bybitSign(ts, payload);
    return {
        {"X-BAPI-API-KEY",     acc.apiKey},
        {"X-BAPI-TIMESTAMP",   ts},
        {"X-BAPI-RECV-WINDOW", recvWindow_},
        {"X-BAPI-SIGN",        sign},
    };
}


// ============================================================================
// WS auth / subscribe
// ============================================================================
std::string BybitTradeUnit::buildAuthJson() const {
    // Bybit WS auth: expires (ms) = now + 5000; sign = hex(HMAC-SHA256(secret, "GET/realtime" + expires))
    long expires = crypto::getCurrentTimeMilli() + 5000;
    std::string payload = "GET/realtime" + std::to_string(expires);
    std::string sign    = crypto::HmacEncodeBybit(acc.secretKey, payload);
    return fmt::format(
        R"({{"op":"auth","args":["{}",{},"{}"]}})",
        escape_json(acc.apiKey), expires, sign);
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
    initRestClient(restHost, /*headers=*/{}, /*conns=*/4);

    ::net::WsConfig cfg;
    cfg.url                      = acc.wsUrl + wsPath;
    cfg.ping_mode                = ::net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text         = R"({"op":"ping"})";
    cfg.auto_reconnect           = true;
    cfg.idle_timeout_sec         = 60;
    LOG_INFO("TB {} Bybit ws {} rest {}", acc.accountId, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));
}


// ============================================================================
// onOpen: auth + subscribe
// ============================================================================
void BybitTradeUnit::onOpen() {
    BaseTradeUnit::onOpen();
    if (!pWsClient) return;
    pWsClient->send_text(buildAuthJson());
    pWsClient->send_text(buildSubscribeJson());
}


// ============================================================================
// InstType lookup
// ============================================================================
bool BybitTradeUnit::lookupInstrument(const std::string& originInstId, const std::string& category,
                                      md::InstrumentInfo& info, InstType& out) const {
    // category 已明确, 直接查
    if (category == "spot") {
        if (smc->get_instrument_info(BYBIT, SPOT, originInstId.c_str(), info)) { out = SPOT; return true; }
    } else if (category == "linear") {
        if (smc->get_instrument_info(BYBIT, USDT_SWAP,    originInstId.c_str(), info)) { out = USDT_SWAP;    return true; }
        if (smc->get_instrument_info(BYBIT, USDT_FUTURES, originInstId.c_str(), info)) { out = USDT_FUTURES; return true; }
    } else if (category == "inverse") {
        if (smc->get_instrument_info(BYBIT, C_SWAP,    originInstId.c_str(), info)) { out = C_SWAP;    return true; }
        if (smc->get_instrument_info(BYBIT, C_FUTURES, originInstId.c_str(), info)) { out = C_FUTURES; return true; }
    } else {
        // 未知 category, 遍历
        for (InstType t : {SPOT, USDT_SWAP, USDT_FUTURES, C_SWAP, C_FUTURES}) {
            if (smc->get_instrument_info(BYBIT, t, originInstId.c_str(), info)) { out = t; return true; }
        }
    }
    return false;
}


// ============================================================================
// onWebsocketMsg
// ============================================================================
void BybitTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool, int64_t) {
    if (len == 0) return;
    try {
        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc = g_parser.iterate(padded);
        if (doc.error()) return;

        // auth/subscribe ack: {"success":true,"op":"auth","conn_id":"..."}
        std::string_view op_sv;
        if (doc["op"].get(op_sv) == simdjson::SUCCESS) {
            bool success = false;
            doc["success"].get(success);
            LOG_INFO("TB {} Bybit ws op={} success={}", acc.accountId, op_sv, success);
            return;
        }

        // channel message: {"topic":"order|wallet|position","data":[...]}
        std::string_view topic_sv;
        if (doc["topic"].get(topic_sv) != simdjson::SUCCESS) return;
        simdjson::ondemand::value data_val;
        if (doc["data"].get(data_val) != simdjson::SUCCESS) return;

        if      (topic_sv == "order")    handleOrdersUpdate(data_val);
        else if (topic_sv == "wallet")   handleWalletUpdate(data_val);
        else if (topic_sv == "position") handlePositionUpdate(data_val);
    }
    catch (const std::exception& e) {
        LOG_ERROR("TB {} Bybit ws exc: {}", acc.accountId, e.what());
    }
}


// ---- order update ----
// data = [{category, symbol, orderId, orderLinkId, side, orderType, timeInForce,
//          qty, price, orderStatus, cumExecQty, avgPrice, cumExecValue, ...}]
void BybitTradeUnit::handleOrdersUpdate(simdjson::ondemand::value& dataArr) {
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


// ---- wallet update ----
// data = [{accountType, totalEquity, totalWalletBalance, coin:[{coin, equity, walletBalance,
//                                                                availableToWithdraw, unrealisedPnl, ...}]}]
void BybitTradeUnit::handleWalletUpdate(simdjson::ondemand::value& dataArr) {
    simdjson::ondemand::array outer;
    if (dataArr.get_array().get(outer) != simdjson::SUCCESS) return;

    for (auto entry : outer) {
        auto d = entry.get_object();
        if (d.error()) continue;

        std::string_view totalEq_sv, adjEq_sv, imr_sv, mmr_sv;
        d["totalEquity"].get(totalEq_sv);
        d["totalWalletBalance"].get(adjEq_sv);
        d["totalInitialMargin"].get(imr_sv);
        d["totalMaintenanceMargin"].get(mmr_sv);

        simdjson::ondemand::array coins;
        if (d["coin"].get(coins) == simdjson::SUCCESS) {
            std::vector<pubsub::RCommand> pending;
            for (auto it : coins) {
                auto o = it.get_object();
                if (o.error()) continue;
                std::string_view ccy_sv, eq_sv, wal_sv, avail_sv, upl_sv;
                o["coin"].get(ccy_sv);
                o["equity"].get(eq_sv);
                o["walletBalance"].get(wal_sv);
                o["availableToWithdraw"].get(avail_sv);
                o["unrealisedPnl"].get(upl_sv);

                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                rcmd.body.balance.exchangeTypeEnum = BYBIT;
                rcmd.body.balance.instTypeEnum     = SPOT;
                crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.currency,   crypto::to_upper(std::string(ccy_sv)));
                rcmd.body.balance.available    = crypto::fast_atod(avail_sv);
                rcmd.body.balance.total        = crypto::fast_atod(eq_sv.empty() ? wal_sv : eq_sv);
                rcmd.body.balance.unrealizedPnl = crypto::fast_atod(upl_sv);
                rcmd.body.balance.updateTime   = crypto::getCurrentTime();
                rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;
                pending.emplace_back(rcmd);
            }
            for (size_t i = 0; i < pending.size(); ++i) {
                pending[i].body.balance.isLast = (i + 1 == pending.size());
                PUSH_RCMD(pending[i])
            }
        }

        // totalAccount
        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
        rcmd.body.totalAccount.exchangeTypeEnum = BYBIT;
        rcmd.body.totalAccount.instTypeEnum     = SPOT;
        crypto::copy_sv_to_char_array(rcmd.body.totalAccount.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.totalAccount.strategyId, acc.strategyId);
        rcmd.body.totalAccount.totalEquity = crypto::fast_atod(totalEq_sv);
        rcmd.body.totalAccount.adjEquity   = crypto::fast_atod(adjEq_sv);
        rcmd.body.totalAccount.mmr         = crypto::fast_atod(mmr_sv);
        rcmd.body.totalAccount.mgnRatio    = 100.0;   // Bybit wallet ws 无直接 mgnRatio, 由 REST 侧算
        rcmd.body.totalAccount.updateTime  = crypto::getCurrentTime();
        rcmd.body.totalAccount.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd)
    }
}


// ---- position update ----
// data = [{category, symbol, side, size, avgPrice, positionValue, unrealisedPnl, markPrice, liqPrice, ...}]
void BybitTradeUnit::handlePositionUpdate(simdjson::ondemand::value& dataArr) {
    simdjson::ondemand::array arr;
    if (dataArr.get_array().get(arr) != simdjson::SUCCESS) return;

    std::vector<pubsub::RCommand> pending;
    for (auto it : arr) {
        auto o = it.get_object();
        if (o.error()) continue;
        std::string_view cat_sv, sym_sv, side_sv, size_sv, avg_sv, upl_sv, mark_sv, liq_sv;
        o["category"].get(cat_sv);
        o["symbol"].get(sym_sv);
        o["side"].get(side_sv);
        o["size"].get(size_sv);
        o["avgPrice"].get(avg_sv);
        o["unrealisedPnl"].get(upl_sv);
        o["markPrice"].get(mark_sv);
        o["liqPrice"].get(liq_sv);

        std::string originInstId(sym_sv);
        std::string category(cat_sv);
        md::InstrumentInfo info;
        InstType instType;
        if (!lookupInstrument(originInstId, category, info, instType)) continue;

        double sz = crypto::fast_atod(size_sv);
        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
        rcmd.body.position.exchangeTypeEnum = BYBIT;
        rcmd.body.position.instTypeEnum     = instType;
        crypto::copy_sv_to_char_array(rcmd.body.position.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.position.instId,     std::string_view(info.instId));
        // side: "Buy" / "Sell" / "None"
        rcmd.body.position.direction = (!side_sv.empty() && side_sv[0] == 'B') ? DT_LONG : DT_SHORT;
        rcmd.body.position.volume    = sz;
        rcmd.body.position.avgPrice      = crypto::fast_atod(avg_sv);
        rcmd.body.position.unrealizedPnl = crypto::fast_atod(upl_sv);
        rcmd.body.position.markPrice     = crypto::fast_atod(mark_sv);
        if (!liq_sv.empty()) rcmd.body.position.liquidPrice = crypto::fast_atod(liq_sv);
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
// query_account —— 走 balance (Bybit 无独立 account 端点)
// ============================================================================
void BybitTradeUnit::query_account(const pubsub::TCommand& tcmd) {
    query_balance(tcmd);
}


// ============================================================================
// query_balance —— GET /v5/account/wallet-balance?accountType=UNIFIED
// ============================================================================
void BybitTradeUnit::query_balance(const pubsub::TCommand&) {
    if (!pRestClient) return;
    std::string query = "accountType=UNIFIED";
    std::string fullPath = balanceUrl + "?" + query;
    auto headers = bybitAuthHeaders(query);

    asyncRequest(boost::beast::http::verb::get, std::move(fullPath), "", "", std::move(headers),
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) { LOG_ERROR("TB {} Bybit query_balance ec: {}", acc.accountId, ec.message()); return; }
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;
                simdjson::ondemand::value list_val;
                if (doc["result"]["list"].get(list_val) != simdjson::SUCCESS) return;
                handleWalletUpdate(list_val);   // 结构与 ws 一致
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} Bybit query_balance cb exc: {}", acc.accountId, e.what());
            }
        });
}


// ============================================================================
// query_position —— GET /v5/position/list?category=X&settleCoin=USDT
// ============================================================================
void BybitTradeUnit::query_position(const pubsub::TCommand& tcmd) {
    if (!pRestClient) return;
    // Bybit position 必须按 category 查, 默认查 linear + settleCoin=USDT
    const char* cat = categoryOf(tcmd.body.queryPosition.instTypeEnum);
    if (!cat) cat = "linear";

    std::string query = fmt::format("category={}&settleCoin=USDT", cat);
    std::string fullPath = positionUrl + "?" + query;
    auto headers = bybitAuthHeaders(query);

    asyncRequest(boost::beast::http::verb::get, std::move(fullPath), "", "", std::move(headers),
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) return;
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;
                simdjson::ondemand::value list_val;
                if (doc["result"]["list"].get(list_val) != simdjson::SUCCESS) return;
                handlePositionUpdate(list_val);
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
    const char* cat = categoryOf(tcmd.body.newOrder.instTypeEnum);
    if (!cat) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId     = OrderTypeError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const char* side = nullptr;
    if (tcmd.body.newOrder.offsetFlag == OF_OPEN) {
        if      (tcmd.body.newOrder.direction == DT_LONG)  side = "Buy";
        else if (tcmd.body.newOrder.direction == DT_SHORT) side = "Sell";
    } else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
        if      (tcmd.body.newOrder.direction == DT_LONG)  side = "Sell";
        else if (tcmd.body.newOrder.direction == DT_SHORT) side = "Buy";
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

    const char* ordType = nullptr;
    const char* tif     = nullptr;
    switch (tcmd.body.newOrder.orderType) {
        case OT_LIMIT:     ordType = "Limit";  tif = "GTC";      break;
        case OT_MARKET:    ordType = "Market"; tif = "IOC";      break;
        case OT_POST_ONLY: ordType = "Limit";  tif = "PostOnly"; break;
        case OT_FOK:       ordType = "Limit";  tif = "FOK";      break;
        case OT_IOC:       ordType = "Limit";  tif = "IOC";      break;
        default:
            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            rcmd.body.orderResponse.errorId     = OrderTypeError;
            rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
            return;
    }

    double price  = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice  * info.magnifyNumber, info.tickSize);
    double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber,  info.lotSize);
    std::string price_str = fmt::format("{}", price);
    std::string qty_str   = fmt::format("{}", volume);

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

    auto headers = bybitAuthHeaders(body);
    LOG_INFO("TB {} Bybit add_new_order body={}", acc.accountId, body);

    auto info_captured = info;

    asyncRequest(boost::beast::http::verb::post, orderUrl, std::move(body), "application/json",
                 std::move(headers),
        [this, rcmd, info_captured](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
            if (ec) {
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                rcmd.body.orderResponse.errorId     = NetworkError;
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, ec.message());
                rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;
            }
            if (rcmd.body.orderResponse.clientOrderId == TESTCLIENTORDERID) return;

            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    rcmd.body.orderResponse.errorId     = UnknownError;
                    rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;
                }
                // Bybit: {"retCode":0,"retMsg":"OK","result":{"orderId":"...","orderLinkId":"..."},...}
                int64_t retCode = 0;
                doc["retCode"].get(retCode);
                std::string_view retMsg_sv;
                doc["retMsg"].get(retMsg_sv);
                if (retCode != 0) {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    rcmd.body.orderResponse.errorId     = UnknownError;
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, retMsg_sv);
                    rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;
                }
                std::string_view oid_sv;
                if (doc["result"]["orderId"].get(oid_sv) == simdjson::SUCCESS) {
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, oid_sv);
                    rcmd.body.orderResponse.orderStatus = OS_NEW;
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
    if (!smc->get_instrument_info(tcmd.body.cancelOrder.exchangeTypeEnum,
                                  tcmd.body.cancelOrder.instTypeEnum,
                                  tcmd.body.cancelOrder.instId, info)) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = SMCInstrumentNotExistError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }
    const char* cat = categoryOf(tcmd.body.cancelOrder.instTypeEnum);
    if (!cat) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = OrderTypeError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    std::string body;
    if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
        body = fmt::format(R"({{"category":"{}","symbol":"{}","orderId":"{}"}})",
                           cat, info.originInstId, tcmd.body.cancelOrder.orderId);
    } else if (!crypto::str_cmp(tcmd.body.cancelOrder.orderSysId, "")) {
        body = fmt::format(R"({{"category":"{}","symbol":"{}","orderLinkId":"{}"}})",
                           cat, info.originInstId, escape_json(tcmd.body.cancelOrder.orderSysId));
    } else {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = OrderIdError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    auto headers = bybitAuthHeaders(body);
    LOG_INFO("TB {} Bybit cancel_order body={}", acc.accountId, body);

    asyncRequest(boost::beast::http::verb::post, cancelOrderUrl, std::move(body), "application/json",
                 std::move(headers),
        [this, rcmd](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
            if (ec) {
                rcmd.body.orderResponse.orderStatus = OS_FAILED;
                rcmd.body.orderResponse.errorId     = NetworkError;
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, ec.message());
                rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;
            }
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;

                int64_t retCode = 0;
                doc["retCode"].get(retCode);
                std::string_view retMsg_sv;
                doc["retMsg"].get(retMsg_sv);
                if (retCode != 0) {
                    rcmd.body.orderResponse.orderStatus = OS_FAILED;
                    rcmd.body.orderResponse.errorId     = UnknownError;
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, retMsg_sv);
                    rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;
                }
                std::string_view oid_sv;
                if (doc["result"]["orderId"].get(oid_sv) == simdjson::SUCCESS) {
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, oid_sv);
                }
                rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
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
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum,
                                  tcmd.body.queryOrder.instTypeEnum,
                                  tcmd.body.queryOrder.instId, info)) {
        return;
    }
    const char* cat = categoryOf(tcmd.body.queryOrder.instTypeEnum);
    if (!cat) return;

    std::string query = fmt::format("category={}&symbol={}", cat, info.originInstId);
    if (!crypto::str_cmp(tcmd.body.queryOrder.orderId, "")) {
        query += "&orderId=" + std::string(tcmd.body.queryOrder.orderId);
    } else if (!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")) {
        query += "&orderLinkId=" + std::string(tcmd.body.queryOrder.orderSysId);
    } else {
        return;
    }
    std::string fullPath = queryOrderUrl + "?" + query;
    auto headers = bybitAuthHeaders(query);
    LOG_INFO("TB {} Bybit query_order: {}", acc.accountId, fullPath);

    auto info_captured = info;

    asyncRequest(boost::beast::http::verb::get, std::move(fullPath), "", "", std::move(headers),
        [this, rcmd, info_captured](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
            if (ec) return;
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;

                simdjson::ondemand::array list;
                if (doc["result"]["list"].get_array().get(list) != simdjson::SUCCESS) return;
                for (auto it : list) {
                    auto o = it.get_object();
                    if (o.error()) continue;
                    std::string_view ordId_sv, ordLink_sv, qty_sv, px_sv, cum_sv, avg_sv, status_sv;
                    o["orderId"].get(ordId_sv);
                    o["orderLinkId"].get(ordLink_sv);
                    o["qty"].get(qty_sv);
                    o["price"].get(px_sv);
                    o["cumExecQty"].get(cum_sv);
                    o["avgPrice"].get(avg_sv);
                    o["orderStatus"].get(status_sv);

                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, ordId_sv);
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, ordLink_sv);
                    if (!qty_sv.empty())    rcmd.body.orderResponse.volumeTotal  = crypto::fast_atod(qty_sv);
                    if (!px_sv.empty())     rcmd.body.orderResponse.limitPrice   = crypto::fast_atod(px_sv);
                    if (!cum_sv.empty())    rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(cum_sv);
                    if (!avg_sv.empty())    rcmd.body.orderResponse.tradePrice   = crypto::fast_atod(avg_sv);

                    if      (status_sv == "New")             rcmd.body.orderResponse.orderStatus = OS_NEW;
                    else if (status_sv == "PartiallyFilled") rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
                    else if (status_sv == "Filled")          rcmd.body.orderResponse.orderStatus = OS_FILLED;
                    else if (status_sv == "Cancelled")       rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                    else if (status_sv == "Rejected")        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    else                                      rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    break;
                }
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} Bybit query_order cb exc: {}", acc.accountId, e.what());
            }
        });
}