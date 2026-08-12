#include "gateio/GateioSpotTrade.h"

#include <cmath>

#include <fmt/format.h>
#include <simdjson.h>


namespace {
    // 同 string_view
    inline std::string escape_json_sv(std::string_view sv) {
        return escape_json(std::string(sv));
    }

    // 已用 sha256 的 body hash payload; Gate signature 支持空 body
    inline std::string gate_sign_get(const std::string& path, const std::string& query,
                                     const std::string& time, const std::string& secret) {
        return crypto::getGateioSignatureRest("GET", path, time, query, "", secret);
    }

    // 构造 Gate 每次都要变的 3 个 header
    inline std::vector<std::pair<std::string, std::string>>
    gate_auth_headers(const std::string& key, const std::string& time, const std::string& sign) {
        return {{"KEY", key}, {"Timestamp", time}, {"SIGN", sign}};
    }
}


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
    initRestClient(restHost, /*headers=*/{}, /*conns=*/4);

    // WS
    ::net::WsConfig cfg;
    cfg.url                      = acc.wsUrl;
    cfg.ping_mode                = ::net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    // Gate ping 允许无 time 字段; 若加 time 会被固化在 cfg 里, 老 timestamp 服务器一般也接受。
    cfg.client_ping_text         = R"({"channel":"spot.ping"})";
    cfg.auto_reconnect           = true;
    cfg.idle_timeout_sec         = 60;

    LOG_INFO("TB {} Gate spot ws {} rest {}", acc.accountId, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));
}


// ============================================================================
// onOpen: 现场签发 subscribe (auth signature TTL 短, 不能复用)
// ============================================================================
void GateioSpotTradeUnit::onOpen() {
    BaseTradeUnit::onOpen();
    if (!pWsClient) return;

    std::string orders   = buildOrdersSubscribeJson();
    std::string balances = buildBalancesSubscribeJson();

    LOG_INFO("TB {} Gate spot subscribe orders={} balances={}", acc.accountId, orders, balances);
    pWsClient->send_text(std::move(orders));
    pWsClient->send_text(std::move(balances));
}


// ---- subscribe JSON builders ----
std::string GateioSpotTradeUnit::buildOrdersSubscribeJson() const {
    long ts   = crypto::getCurrentTimeSeconds();
    std::string time_str = std::to_string(ts);
    std::string channel  = "spot.orders";
    std::string sign     = crypto::getGateioSignatureWs(channel, "subscribe", time_str, acc.secretKey);
    return fmt::format(
        R"({{"time":{},"channel":"{}","event":"subscribe",)"
        R"("payload":["!all"],)"
        R"("auth":{{"method":"api_key","KEY":"{}","SIGN":"{}"}}}})",
        ts, channel, escape_json(acc.apiKey), sign);
}

std::string GateioSpotTradeUnit::buildBalancesSubscribeJson() const {
    long ts   = crypto::getCurrentTimeSeconds();
    std::string time_str = std::to_string(ts);
    std::string channel  = "spot.balances";
    std::string sign     = crypto::getGateioSignatureWs(channel, "subscribe", time_str, acc.secretKey);
    return fmt::format(
        R"({{"time":{},"channel":"{}","event":"subscribe",)"
        R"("auth":{{"method":"api_key","KEY":"{}","SIGN":"{}"}}}})",
        ts, channel, escape_json(acc.apiKey), sign);
}


// ============================================================================
// onWebsocketMsg
// ============================================================================
void GateioSpotTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool, int64_t) {
    if (len == 0) return;
    try {
        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc = g_parser.iterate(padded);
        if (doc.error()) return;

        std::string_view ch_sv, ev_sv;
        if (doc["channel"].get(ch_sv) != simdjson::SUCCESS) return;
        if (ch_sv == "spot.pong") return;
        if (doc["event"].get(ev_sv) != simdjson::SUCCESS || ev_sv != "update") return;

        simdjson::ondemand::value result;
        if (doc["result"].get(result) != simdjson::SUCCESS) return;

        if (ch_sv == "spot.orders") {
            handleOrdersUpdate(result);
        } else if (ch_sv == "spot.balances" || ch_sv == "spot.cross_balances") {
            handleBalancesUpdate(result);
        }
    }
    catch (const std::exception& e) {
        LOG_ERROR("TB {} Gate spot ws exc: {}", acc.accountId, e.what());
    }
}


// ---- spot.orders update ----
void GateioSpotTradeUnit::handleOrdersUpdate(simdjson::ondemand::value& result) {
    simdjson::ondemand::array arr;
    if (result.get_array().get(arr) != simdjson::SUCCESS) return;

    for (auto it : arr) {
        auto o = it.get_object();
        if (o.error()) continue;

        std::string_view pair_sv, id_sv, text_sv, price_sv, amount_sv, side_sv, tif_sv, left_sv, avg_sv, event_sv;
        o["currency_pair"].get(pair_sv);
        o["id"].get(id_sv);
        o["text"].get(text_sv);
        o["price"].get(price_sv);
        o["amount"].get(amount_sv);
        o["side"].get(side_sv);
        o["time_in_force"].get(tif_sv);
        o["left"].get(left_sv);
        o["avg_deal_price"].get(avg_sv);
        o["event"].get(event_sv);

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
        rcmd.body.orderResponse.instTypeEnum     = SPOT;
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.instId,     std::string_view(info.instId));
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId,    id_sv);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, text_sv);

        rcmd.body.orderResponse.limitPrice  = crypto::fast_atod(price_sv)  * info.reduceNumber;
        rcmd.body.orderResponse.volumeTotal = crypto::fast_atod(amount_sv) * info.magnifyNumber;
        rcmd.body.orderResponse.offsetFlag  = OF_OPEN;
        rcmd.body.orderResponse.direction   = (!side_sv.empty() && side_sv[0] == 's') ? DT_SHORT : DT_LONG;

        if (!tif_sv.empty()) {
            switch (tif_sv[0]) {
                case 'g': rcmd.body.orderResponse.orderType = OT_LIMIT;      break;
                case 'i': rcmd.body.orderResponse.orderType = OT_IOC;        break;
                case 'p': rcmd.body.orderResponse.orderType = OT_POST_ONLY;  break;
                case 'f': rcmd.body.orderResponse.orderType = OT_FOK;        break;
                default:  break;
            }
        }

        double left = std::fabs(crypto::fast_atod(left_sv));
        rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;

        if (!avg_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avg_sv);

        if (!event_sv.empty()) {
            switch (event_sv[0]) {
                case 'p': rcmd.body.orderResponse.orderStatus = OS_NEW;         break;
                case 'u': rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;  break;
                case 'f':
                    rcmd.body.orderResponse.orderStatus =
                        (rcmd.body.orderResponse.volumeTotal - rcmd.body.orderResponse.volumeTraded < ZERO_NUM)
                            ? OS_FILLED : OS_CANCELED;
                    break;
                default:  rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;     break;
            }
        }
        rcmd.body.orderResponse.updateTime    = crypto::getCurrentTime();
        rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd)
    }
}


// ---- spot.balances / spot.cross_balances update ----
void GateioSpotTradeUnit::handleBalancesUpdate(simdjson::ondemand::value& result) {
    simdjson::ondemand::array arr;
    if (result.get_array().get(arr) != simdjson::SUCCESS) return;

    std::vector<pubsub::RCommand> pending;
    for (auto it : arr) {
        auto o = it.get_object();
        if (o.error()) continue;
        std::string_view cur_sv, avail_sv, total_sv;
        o["currency"].get(cur_sv);
        o["available"].get(avail_sv);
        o["total"].get(total_sv);

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
        rcmd.body.balance.exchangeTypeEnum = GATEIO;
        rcmd.body.balance.instTypeEnum     = SPOT;
        crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.balance.currency,   crypto::to_upper(std::string(cur_sv)));
        rcmd.body.balance.available  = crypto::fast_atod(avail_sv);
        rcmd.body.balance.total      = crypto::fast_atod(total_sv);
        rcmd.body.balance.updateTime = crypto::getCurrentTime();
        rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;
        pending.emplace_back(rcmd);
    }
    for (size_t i = 0; i < pending.size(); ++i) {
        pending[i].body.balance.isLast = (i + 1 == pending.size());
        PUSH_RCMD(pending[i])
    }
}


// ============================================================================
// query_account —— 只在 unified 账户下才有意义
// ============================================================================
void GateioSpotTradeUnit::query_account(const pubsub::TCommand&) {
#ifndef USE_GATEIO_UNIFIED
    return;   // 非统一账户模式不查
#endif
    if (!pRestClient) return;

    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign     = gate_sign_get(unifiedUrl, "", time_str, acc.secretKey);
    auto headers = gate_auth_headers(acc.apiKey, time_str, sign);

    asyncRequest(boost::beast::http::verb::get, unifiedUrl, "", "", std::move(headers),
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) return;
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;

                // balances 是对象 (currency -> {...})
                simdjson::ondemand::object balances;
                if (doc["balances"].get(balances) == simdjson::SUCCESS) {
                    std::vector<pubsub::RCommand> pending;
                    for (auto field : balances) {
                        auto ku = field.unescaped_key();
                        if (ku.error()) continue;
                        auto b = field.value().get_object();
                        if (b.error()) continue;
                        std::string_view av_sv, fr_sv, eq_sv;
                        b["available"].get(av_sv);
                        b["freeze"].get(fr_sv);
                        b["equity"].get(eq_sv);

                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                        rcmd.body.balance.exchangeTypeEnum = GATEIO;
                        rcmd.body.balance.instTypeEnum     = SPOT;
                        crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
                        crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                        crypto::copy_sv_to_char_array(rcmd.body.balance.currency,   crypto::to_upper(std::string(ku.value_unsafe())));
                        rcmd.body.balance.available  = crypto::fast_atod(av_sv);
                        rcmd.body.balance.frozen     = crypto::fast_atod(fr_sv);
                        rcmd.body.balance.total      = crypto::fast_atod(eq_sv);
                        rcmd.body.balance.updateTime = crypto::getCurrentTime();
                        rcmd.body.balance.apiSourceEnum = AS_REST;
                        pending.emplace_back(rcmd);
                    }
                    for (size_t i = 0; i < pending.size(); ++i) {
                        pending[i].body.balance.isLast = (i + 1 == pending.size());
                        PUSH_RCMD(pending[i]);
                    }
                }

                // totalAccount
                std::string_view teq_sv, tmb_sv, tmm_sv, tmr_sv;
                doc["unified_account_total_equity"].get(teq_sv);
                doc["total_margin_balance"].get(tmb_sv);
                doc["total_maintenance_margin"].get(tmm_sv);
                doc["total_maintenance_margin_rate"].get(tmr_sv);

                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
                rcmd.body.totalAccount.exchangeTypeEnum = GATEIO;
                rcmd.body.totalAccount.instTypeEnum     = SPOT;
                crypto::copy_sv_to_char_array(rcmd.body.totalAccount.accountId,  acc.accountId);
                crypto::copy_sv_to_char_array(rcmd.body.totalAccount.strategyId, acc.strategyId);
                rcmd.body.totalAccount.totalEquity = crypto::fast_atod(teq_sv);
                rcmd.body.totalAccount.adjEquity   = crypto::fast_atod(tmb_sv);
                rcmd.body.totalAccount.mmr         = crypto::fast_atod(tmm_sv);
                rcmd.body.totalAccount.mgnRatio    = tmr_sv.empty() ? 9999.0 : crypto::fast_atod(tmr_sv);
                rcmd.body.totalAccount.updateTime  = crypto::getCurrentTime();
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
    if (!pRestClient) return;

    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign     = gate_sign_get(balanceUrl, "", time_str, acc.secretKey);
    auto headers = gate_auth_headers(acc.apiKey, time_str, sign);

    asyncRequest(boost::beast::http::verb::get, balanceUrl, "", "", std::move(headers),
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) { LOG_ERROR("TB {} Gate query_balance ec: {}", acc.accountId, ec.message()); return; }
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
                    std::string_view cur_sv, avail_sv, lock_sv;
                    o["currency"].get(cur_sv);
                    o["available"].get(avail_sv);
                    o["locked"].get(lock_sv);

                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = GATEIO;
                    rcmd.body.balance.instTypeEnum     = SPOT;
                    crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.currency,   crypto::to_upper(std::string(cur_sv)));
                    rcmd.body.balance.available = crypto::fast_atod(avail_sv);
                    rcmd.body.balance.frozen    = crypto::fast_atod(lock_sv);
                    rcmd.body.balance.total     = rcmd.body.balance.available + rcmd.body.balance.frozen;
                    rcmd.body.balance.updateTime = crypto::getCurrentTime();
                    rcmd.body.balance.apiSourceEnum = AS_REST;
                    pending.emplace_back(rcmd);
                }
                if (pending.empty()) {
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = GATEIO;
                    rcmd.body.balance.instTypeEnum     = SPOT;
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
        if      (tcmd.body.newOrder.direction == DT_LONG)  side = "buy";
        else if (tcmd.body.newOrder.direction == DT_SHORT) side = "sell";
    } else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
        if      (tcmd.body.newOrder.direction == DT_LONG)  side = "sell";
        else if (tcmd.body.newOrder.direction == DT_SHORT) side = "buy";
    }
    if (!side) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId     =
            (tcmd.body.newOrder.offsetFlag == OF_OPEN || tcmd.body.newOrder.offsetFlag == OF_CLOSE)
                ? DirectionError : OffsetFlagError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
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

    const char* account = nullptr;
#ifdef USE_GATEIO_UNIFIED
    account = "unified";
#else
    if      (tcmd.body.newOrder.instTypeEnum == SPOT)   account = "spot";
    else if (tcmd.body.newOrder.instTypeEnum == MARGIN) account = "margin";
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
        escape_json(tcmd.body.newOrder.orderSysId), info.originInstId, price_str, amount_str, side, tif);
#else
    std::string body = fmt::format(
        R"({{"text":"{}","currency_pair":"{}","price":"{}","amount":"{}","side":"{}","time_in_force":"{}","account":"{}"}})",
        escape_json(tcmd.body.newOrder.orderSysId), info.originInstId, price_str, amount_str, side, tif, account);
#endif

    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign     = crypto::getGateioSignatureRest("POST", newOrderUrl, time_str, "", body, acc.secretKey);
    auto headers = gate_auth_headers(acc.apiKey, time_str, sign);

    LOG_INFO("TB {} Gate spot add_new_order body={}", acc.accountId, body);

    auto info_captured = info;

    asyncRequest(boost::beast::http::verb::post, newOrderUrl, std::move(body), "application/json",
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

                std::string_view id_sv, status_sv, left_sv, avg_sv, label_sv;
                bool has_status = (doc.find_field_unordered("status").get(status_sv) == simdjson::SUCCESS);
                bool has_label  = (doc.find_field_unordered("label").get(label_sv)  == simdjson::SUCCESS);

                if (has_status) {
                    doc.find_field_unordered("id").get(id_sv);
                    doc.find_field_unordered("left").get(left_sv);
                    doc.find_field_unordered("avg_deal_price").get(avg_sv);

                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, id_sv);
                    rcmd.body.orderResponse.errorId = NoError;
                    if (!avg_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avg_sv);

                    double left = std::fabs(crypto::fast_atod(left_sv));
                    rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;

                    std::string st(status_sv);
                    // Gate spot status: open / closed / cancelled
                    if      (st == "open")      rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.volumeTraded > ZERO_NUM) ? OS_PARTFILLED : OS_NEW;
                    else if (st == "closed")    rcmd.body.orderResponse.orderStatus = OS_FILLED;
                    else if (st == "cancelled") rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                    else                        rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;

                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                } else if (has_label) {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    rcmd.body.orderResponse.errorId     = crypto::get_gateio_errorid(resp.body.c_str());
                    if (rcmd.body.orderResponse.errorId == 0) rcmd.body.orderResponse.errorId = UnknownError;
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, label_sv);
                    rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                } else {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    rcmd.body.orderResponse.errorId     = UnknownError;
                    rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                }
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} Gate spot add_new_order cb exc: {}", acc.accountId, e.what());
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                rcmd.body.orderResponse.errorId     = NetworkError;
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
    if (!smc->get_instrument_info(tcmd.body.cancelOrder.exchangeTypeEnum,
                                  tcmd.body.cancelOrder.instTypeEnum,
                                  tcmd.body.cancelOrder.instId, info)) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = SMCInstrumentNotExistError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
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
        rcmd.body.orderResponse.errorId     = OrderIdError;
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
    std::string sign     = crypto::getGateioSignatureRest("DELETE", pathBase, time_str, queryStr, "", acc.secretKey);
    auto headers = gate_auth_headers(acc.apiKey, time_str, sign);

    std::string fullPath = pathBase + "?" + queryStr;
    LOG_INFO("TB {} Gate spot cancel_order: {}", acc.accountId, fullPath);

    auto info_captured = info;

    asyncRequest(boost::beast::http::verb::delete_, std::move(fullPath), "", "", std::move(headers),
        [this, rcmd, info_captured](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
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

                std::string_view status_sv, label_sv, id_sv, avg_sv, amount_sv, left_sv;
                bool has_status = (doc.find_field_unordered("status").get(status_sv) == simdjson::SUCCESS);
                bool has_label  = (doc.find_field_unordered("label").get(label_sv)  == simdjson::SUCCESS);

                if (has_status) {
                    doc.find_field_unordered("id").get(id_sv);
                    doc.find_field_unordered("avg_deal_price").get(avg_sv);
                    doc.find_field_unordered("amount").get(amount_sv);
                    doc.find_field_unordered("left").get(left_sv);

                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, id_sv);
                    if (!avg_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avg_sv);
                    double amount = crypto::fast_atod(amount_sv);
                    double left   = std::fabs(crypto::fast_atod(left_sv));
                    rcmd.body.orderResponse.volumeTraded = amount - left;
                    rcmd.body.orderResponse.orderStatus  = OS_CANCELED;
                    rcmd.body.orderResponse.updateTime   = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                } else if (has_label) {
                    rcmd.body.orderResponse.orderStatus = OS_FAILED;
                    rcmd.body.orderResponse.errorId     = crypto::get_gateio_errorid(resp.body.c_str());
                    if (rcmd.body.orderResponse.errorId == OrderNotFoundError) rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, label_sv);
                    rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
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
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum,
                                  tcmd.body.queryOrder.instTypeEnum,
                                  tcmd.body.queryOrder.instId, info)) {
        LOG_INFO("TB {} Gate spot query_order smc miss: {}", acc.accountId, tcmd.body.queryOrder.instId);
        return;
    }

    std::string idSeg;
    // Fix: 老代码把逻辑写反了 (少了 !), 这里改对: id 非空才 append。
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
    std::string sign     = crypto::getGateioSignatureRest("GET", pathBase, time_str, queryStr, "", acc.secretKey);
    auto headers = gate_auth_headers(acc.apiKey, time_str, sign);

    std::string fullPath = pathBase + "?" + queryStr;
    LOG_INFO("TB {} Gate spot query_order: {}", acc.accountId, fullPath);

    auto info_captured = info;

    asyncRequest(boost::beast::http::verb::get, std::move(fullPath), "", "", std::move(headers),
        [this, rcmd, info_captured](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
            if (ec) return;
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;

                std::string_view status_sv;
                if (doc.find_field_unordered("status").get(status_sv) != simdjson::SUCCESS) {
                    // error
                    rcmd.body.orderResponse.errorId = crypto::get_gateio_errorid(resp.body.c_str());
                    rcmd.body.orderResponse.orderStatus =
                        (rcmd.body.orderResponse.errorId == OrderNotFoundError) ? OS_REJECTED : OS_UNKNOWN;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;
                }

                std::string_view id_sv, text_sv, amount_sv, price_sv, left_sv, avg_sv, finishAs_sv;
                doc.find_field_unordered("id").get(id_sv);
                doc.find_field_unordered("text").get(text_sv);
                doc.find_field_unordered("amount").get(amount_sv);
                doc.find_field_unordered("price").get(price_sv);
                doc.find_field_unordered("left").get(left_sv);
                doc.find_field_unordered("avg_deal_price").get(avg_sv);
                doc.find_field_unordered("finish_as").get(finishAs_sv);

                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId,    id_sv);
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, text_sv);
                rcmd.body.orderResponse.volumeTotal = crypto::fast_atod(amount_sv);
                rcmd.body.orderResponse.limitPrice  = crypto::fast_atod(price_sv);
                double left = std::fabs(crypto::fast_atod(left_sv));
                rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
                if (!avg_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avg_sv);

                std::string st(status_sv);
                if (st == "open") {
                    rcmd.body.orderResponse.orderStatus =
                        (rcmd.body.orderResponse.volumeTotal > rcmd.body.orderResponse.volumeTraded && rcmd.body.orderResponse.volumeTraded > ZERO_NUM)
                            ? OS_PARTFILLED : OS_NEW;
                } else if (st == "closed") {
                    rcmd.body.orderResponse.orderStatus = OS_FILLED;
                } else if (st == "cancelled") {
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