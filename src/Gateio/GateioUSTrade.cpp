#include "gateio/GateioUSTrade.h"

#include <cmath>

#include <fmt/format.h>
#include <simdjson.h>


namespace {
    thread_local simdjson::ondemand::parser g_parser;

    // 构造 Gate 每次都要变的 3 个 header
    inline std::vector<std::pair<std::string, std::string>>
    gate_auth_headers(const std::string& key, const std::string& time, const std::string& sign) {
        return {{"KEY", key}, {"Timestamp", time}, {"SIGN", sign}};
    }
}


GateioUSTradeUnit::GateioUSTradeUnit(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {}
GateioUSTradeUnit::~GateioUSTradeUnit() {}


// Gate adl_ranking (1-5) → 我们内部 adlQuantile 的自定义映射 (老代码里的逻辑)
int GateioUSTradeUnit::mapAdlRanking(int r) {
    // 老逻辑归纳:
    //   r >= 5 → 1
    //   r == 4 → 1  (5-4)
    //   r == 3 → 3
    //   r == 2 → 4
    //   r == 1 → 4  (5-1)
    //   其他   → 1
    if (r >= 5) return 1;
    if (r == 4) return 1;
    if (r == 3) return 3;
    if (r == 2) return 4;
    if (r == 1) return 4;
    return 1;
}


// ============================================================================
// subWebsocekt
// ============================================================================
void GateioUSTradeUnit::subWebsocekt() {
    std::string restHost = host_of(acc.restUrl);
    initRestClient(restHost, /*headers=*/{}, /*conns=*/4);

    ::net::WsConfig cfg;
    cfg.url                      = acc.wsUrl;
    cfg.ping_mode                = ::net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text         = R"({"channel":"futures.ping"})";
    cfg.auto_reconnect           = true;
    cfg.idle_timeout_sec         = 60;

    LOG_INFO("TB {} Gate US ws {} rest {}", acc.accountId, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));
}


// ============================================================================
// onOpen: 现场签发 subscribe
// ============================================================================
void GateioUSTradeUnit::onOpen() {
    BaseTradeUnit::onOpen();
    if (!pWsClient) return;

    pWsClient->send_text(buildOrdersSubscribeJson());
    pWsClient->send_text(buildBalancesSubscribeJson());
    pWsClient->send_text(buildPositionsSubscribeJson());
}


// ---- subscribe JSON builders ----
std::string GateioUSTradeUnit::buildOrdersSubscribeJson() const {
    long ts = crypto::getCurrentTimeSeconds();
    std::string time_str = std::to_string(ts);
    std::string channel  = "futures.orders";
    std::string sign     = crypto::getGateioSignatureWs(channel, "subscribe", time_str, acc.secretKey);
    return fmt::format(
        R"({{"time":{},"channel":"{}","event":"subscribe",)"
        R"("payload":["{}","!all"],)"
        R"("auth":{{"method":"api_key","KEY":"{}","SIGN":"{}"}}}})",
        ts, channel, escape_json(acc.userId), escape_json(acc.apiKey), sign);
}

std::string GateioUSTradeUnit::buildBalancesSubscribeJson() const {
    long ts = crypto::getCurrentTimeSeconds();
    std::string time_str = std::to_string(ts);
    std::string channel  = "futures.balances";
    std::string sign     = crypto::getGateioSignatureWs(channel, "subscribe", time_str, acc.secretKey);
    return fmt::format(
        R"({{"time":{},"channel":"{}","event":"subscribe",)"
        R"("payload":["{}"],)"
        R"("auth":{{"method":"api_key","KEY":"{}","SIGN":"{}"}}}})",
        ts, channel, escape_json(acc.userId), escape_json(acc.apiKey), sign);
}

std::string GateioUSTradeUnit::buildPositionsSubscribeJson() const {
    long ts = crypto::getCurrentTimeSeconds();
    std::string time_str = std::to_string(ts);
    std::string channel  = "futures.positions";
    std::string sign     = crypto::getGateioSignatureWs(channel, "subscribe", time_str, acc.secretKey);
    return fmt::format(
        R"({{"time":{},"channel":"{}","event":"subscribe",)"
        R"("payload":["{}","!all"],)"
        R"("auth":{{"method":"api_key","KEY":"{}","SIGN":"{}"}}}})",
        ts, channel, escape_json(acc.userId), escape_json(acc.apiKey), sign);
}


// ============================================================================
// onWebsocketMsg
// ============================================================================
void GateioUSTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool, int64_t) {
    if (len == 0) return;
    try {
        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc = g_parser.iterate(padded);
        if (doc.error()) return;

        std::string_view ch_sv, ev_sv;
        if (doc["channel"].get(ch_sv) != simdjson::SUCCESS) return;
        if (ch_sv == "futures.pong") return;
        if (doc["event"].get(ev_sv) != simdjson::SUCCESS || ev_sv != "update") return;

        simdjson::ondemand::value result;
        if (doc["result"].get(result) != simdjson::SUCCESS) return;

        if      (ch_sv == "futures.orders")    handleOrdersUpdate(result);
        else if (ch_sv == "futures.balances")  handleBalancesUpdate(result);
        else if (ch_sv == "futures.positions") handlePositionsUpdate(result);
    }
    catch (const std::exception& e) {
        LOG_ERROR("TB {} Gate US ws exc: {}", acc.accountId, e.what());
    }
}


// ---- futures.orders update ----
void GateioUSTradeUnit::handleOrdersUpdate(simdjson::ondemand::value& result) {
    simdjson::ondemand::array arr;
    if (result.get_array().get(arr) != simdjson::SUCCESS) return;

    for (auto it : arr) {
        auto o = it.get_object();
        if (o.error()) continue;

        std::string_view contract_sv, id_sv, text_sv, price_sv, tif_sv, size_sv, left_sv, fill_sv,
                          status_sv, finish_sv;
        bool isClose = false;
        if (o["contract"].get(contract_sv) != simdjson::SUCCESS) continue;
        o["id"].get(id_sv);
        o["text"].get(text_sv);
        o["price"].get(price_sv);
        o["tif"].get(tif_sv);
        o["size"].get(size_sv);
        o["left"].get(left_sv);
        o["fill_price"].get(fill_sv);
        o["status"].get(status_sv);
        o["finish_as"].get(finish_sv);
        // is_close 是 bool
        bool ic = false;
        if (o["is_close"].get(ic) == simdjson::SUCCESS) isClose = ic;

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
        rcmd.body.orderResponse.instTypeEnum     = USDT_SWAP;
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.instId,     std::string_view(info.instId));
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId,    id_sv);

        if (!text_sv.empty()) {
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, text_sv);
            if      (text_sv == "auto_deleveraging") rcmd.body.orderResponse.errorId = ADLError;
            else if (text_sv == "liquidation")       rcmd.body.orderResponse.errorId = LiquidationError;
        }

        rcmd.body.orderResponse.offsetFlag = isClose ? OF_CLOSE : OF_OPEN;

        if (!size_sv.empty()) {
            double sz = crypto::fast_atod(size_sv);
            rcmd.body.orderResponse.direction   = sz > 0 ? DT_LONG : DT_SHORT;
            rcmd.body.orderResponse.volumeTotal = std::fabs(sz);
        }
        if (!price_sv.empty()) rcmd.body.orderResponse.limitPrice = crypto::fast_atod(price_sv);
        if (!tif_sv.empty()) {
            switch (tif_sv[0]) {
                case 'g': rcmd.body.orderResponse.orderType = OT_LIMIT;     break;
                case 'i': rcmd.body.orderResponse.orderType = OT_IOC;       break;
                case 'p': rcmd.body.orderResponse.orderType = OT_POST_ONLY; break;
                case 'f': rcmd.body.orderResponse.orderType = OT_FOK;       break;
                default: break;
            }
        }
        if (!left_sv.empty()) {
            double left = std::fabs(crypto::fast_atod(left_sv));
            rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
        }
        if (!fill_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(fill_sv);

        if (status_sv == "open") {
            rcmd.body.orderResponse.orderStatus =
                (rcmd.body.orderResponse.volumeTotal > rcmd.body.orderResponse.volumeTraded && rcmd.body.orderResponse.volumeTraded > ZERO_NUM)
                    ? OS_PARTFILLED : OS_NEW;
        } else {
            if      (finish_sv == "filled")           rcmd.body.orderResponse.orderStatus = OS_FILLED;
            else if (finish_sv == "cancelled" || finish_sv == "liquidated" ||
                     finish_sv == "ioc"       || finish_sv == "auto_deleveraged" ||
                     finish_sv == "reduce_only")     rcmd.body.orderResponse.orderStatus = OS_CANCELED;
            else                                     rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
        }

        rcmd.body.orderResponse.updateTime    = crypto::getCurrentTime();
        rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd)
    }
}


// ---- futures.balances update ----
void GateioUSTradeUnit::handleBalancesUpdate(simdjson::ondemand::value& result) {
    simdjson::ondemand::array arr;
    if (result.get_array().get(arr) != simdjson::SUCCESS) return;

    std::vector<pubsub::RCommand> pending;
    for (auto it : arr) {
        auto o = it.get_object();
        if (o.error()) continue;

        std::string_view cur_sv, text_sv, bal_sv;
        o["currency"].get(cur_sv);
        o["text"].get(text_sv);
        o["balance"].get(bal_sv);

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
        rcmd.body.balance.exchangeTypeEnum = GATEIO;
        rcmd.body.balance.instTypeEnum     = SPOT;   // 老逻辑走 SPOT (未拆分 USDT_SWAP balance)
        crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);

        if (!cur_sv.empty()) {
            crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(cur_sv)));
        } else if (crypto::has_str(std::string(text_sv).c_str(), "USDT")) {
            crypto::copy_sv_to_char_array(rcmd.body.balance.currency, std::string("USDT"));
        } else {
            continue;
        }
        if (!bal_sv.empty()) rcmd.body.balance.total = crypto::fast_atod(bal_sv);
        rcmd.body.balance.updateTime = crypto::getCurrentTime();
        rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;
        pending.emplace_back(rcmd);
    }
    for (size_t i = 0; i < pending.size(); ++i) {
        pending[i].body.balance.isLast = (i + 1 == pending.size());
        PUSH_RCMD(pending[i])
    }
}


// ---- futures.positions update ----
void GateioUSTradeUnit::handlePositionsUpdate(simdjson::ondemand::value& result) {
    simdjson::ondemand::array arr;
    if (result.get_array().get(arr) != simdjson::SUCCESS) return;

    std::vector<pubsub::RCommand> pending;
    for (auto it : arr) {
        auto o = it.get_object();
        if (o.error()) continue;

        std::string_view contract_sv, size_sv, margin_sv, entry_sv, up_sv, mark_sv, liq_sv, adl_sv;
        if (o["contract"].get(contract_sv) != simdjson::SUCCESS) continue;
        o["size"].get(size_sv);
        o["margin"].get(margin_sv);
        o["entry_price"].get(entry_sv);
        o["unrealised_pnl"].get(up_sv);
        o["mark_price"].get(mark_sv);
        o["liq_price"].get(liq_sv);
        o["adl_ranking"].get(adl_sv);

        std::string originInstId(contract_sv);
        md::InstrumentInfo info;
        if (!smc->get_instrument_info(GATEIO, USDT_SWAP, originInstId.c_str(), info)) continue;

        double sz = crypto::fast_atod(size_sv);
        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
        rcmd.body.position.exchangeTypeEnum = GATEIO;
        rcmd.body.position.instTypeEnum     = USDT_SWAP;
        crypto::copy_sv_to_char_array(rcmd.body.position.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.position.instId,     std::string_view(info.instId));
        rcmd.body.position.direction     = sz >= 0 ? DT_LONG : DT_SHORT;
        rcmd.body.position.volume        = std::fabs(sz) * info.magnifyNumber;
        rcmd.body.position.maintMargin   = crypto::fast_atod(margin_sv);
        rcmd.body.position.avgPrice      = crypto::fast_atod(entry_sv) * info.reduceNumber;
        rcmd.body.position.unrealizedPnl = crypto::fast_atod(up_sv);
        rcmd.body.position.markPrice     = crypto::fast_atod(mark_sv) * info.reduceNumber;
        rcmd.body.position.liquidPrice   = crypto::fast_atod(liq_sv)  * info.reduceNumber;
        rcmd.body.position.adlQuantile   = mapAdlRanking(static_cast<int>(crypto::fast_atod(adl_sv)));
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
// query_account —— 无实现 (老代码也是空)
// ============================================================================
void GateioUSTradeUnit::query_account(const pubsub::TCommand&) {}


// ============================================================================
// query_balance —— GET /api/v4/futures/usdt/accounts (array)
// ============================================================================
void GateioUSTradeUnit::query_balance(const pubsub::TCommand&) {
    if (!pRestClient) return;
    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign     = crypto::getGateioSignatureRest("GET", balanceUrl, time_str, "", "", acc.secretKey);
    auto headers = gate_auth_headers(acc.apiKey, time_str, sign);

    asyncRequest(boost::beast::http::verb::get, balanceUrl, "", "", std::move(headers),
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) { LOG_ERROR("TB {} Gate US query_balance ec: {}", acc.accountId, ec.message()); return; }
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
                    std::string_view cur_sv, avail_sv, om_sv, pm_sv, tot_sv;
                    o["currency"].get(cur_sv);
                    o["available"].get(avail_sv);
                    o["order_margin"].get(om_sv);
                    o["position_margin"].get(pm_sv);
                    o["total"].get(tot_sv);

                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = GATEIO;
                    rcmd.body.balance.instTypeEnum     = SPOT;
                    crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.currency,   crypto::to_upper(std::string(cur_sv)));
                    rcmd.body.balance.available = crypto::fast_atod(avail_sv);
                    rcmd.body.balance.frozen    = crypto::fast_atod(om_sv) + crypto::fast_atod(pm_sv);
                    rcmd.body.balance.total     = crypto::fast_atod(tot_sv);
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
                LOG_ERROR("TB {} Gate US query_balance cb exc: {}", acc.accountId, e.what());
            }
        });
}


// ============================================================================
// query_position —— GET /api/v4/futures/usdt/positions
// ============================================================================
void GateioUSTradeUnit::query_position(const pubsub::TCommand&) {
    if (!pRestClient) return;
    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign     = crypto::getGateioSignatureRest("GET", positionUrl, time_str, "", "", acc.secretKey);
    auto headers = gate_auth_headers(acc.apiKey, time_str, sign);

    asyncRequest(boost::beast::http::verb::get, positionUrl, "", "", std::move(headers),
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) { LOG_ERROR("TB {} Gate US query_position ec: {}", acc.accountId, ec.message()); return; }
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

                    std::string_view contract_sv, size_sv, margin_sv, entry_sv, up_sv, mark_sv, liq_sv, adl_sv;
                    if (o["contract"].get(contract_sv) != simdjson::SUCCESS) continue;
                    o["size"].get(size_sv);
                    o["margin"].get(margin_sv);
                    o["entry_price"].get(entry_sv);
                    o["unrealised_pnl"].get(up_sv);
                    o["mark_price"].get(mark_sv);
                    o["liq_price"].get(liq_sv);
                    o["adl_ranking"].get(adl_sv);

                    std::string originInstId(contract_sv);
                    md::InstrumentInfo info;
                    if (!smc->get_instrument_info(GATEIO, USDT_SWAP, originInstId.c_str(), info)) continue;

                    double sz = crypto::fast_atod(size_sv);
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                    rcmd.body.position.exchangeTypeEnum = GATEIO;
                    rcmd.body.position.instTypeEnum     = USDT_SWAP;
                    crypto::copy_sv_to_char_array(rcmd.body.position.accountId,  acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.instId,     std::string_view(info.instId));
                    rcmd.body.position.direction     = sz >= 0 ? DT_LONG : DT_SHORT;
                    rcmd.body.position.volume        = std::fabs(sz) * info.magnifyNumber;
                    rcmd.body.position.maintMargin   = crypto::fast_atod(margin_sv);
                    rcmd.body.position.avgPrice      = crypto::fast_atod(entry_sv) * info.reduceNumber;
                    rcmd.body.position.unrealizedPnl = crypto::fast_atod(up_sv);
                    rcmd.body.position.markPrice     = crypto::fast_atod(mark_sv) * info.reduceNumber;
                    rcmd.body.position.liquidPrice   = crypto::fast_atod(liq_sv)  * info.reduceNumber;
                    rcmd.body.position.adlQuantile   = mapAdlRanking(static_cast<int>(crypto::fast_atod(adl_sv)));
                    rcmd.body.position.updateTime    = crypto::getCurrentTime();
                    rcmd.body.position.apiSourceEnum = AS_REST;
                    pending.emplace_back(rcmd);
                }
                if (pending.empty()) {
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                    rcmd.body.position.exchangeTypeEnum = GATEIO;
                    rcmd.body.position.instTypeEnum     = USDT_SWAP;
                    crypto::copy_sv_to_char_array(rcmd.body.position.accountId,  acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.instId,     std::string_view("BTC-USDT"));
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

    double price  = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice  * info.magnifyNumber, info.tickSize);
    double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber,  info.lotSize);

    // Gate futures 用带正负号的 size (正=多, 负=空), tif 走 gtc/ioc/poc/fok
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

    // size 正负规则:
    //   OPEN + LONG   → +vol   (买多)
    //   OPEN + SHORT  → -vol   (卖空)
    //   CLOSE + LONG  → -vol   (卖平多)
    //   CLOSE + SHORT → +vol   (买平空)
    double sizeSigned = 0;
    if (tcmd.body.newOrder.offsetFlag == OF_OPEN) {
        if      (tcmd.body.newOrder.direction == DT_LONG)  sizeSigned =  volume;
        else if (tcmd.body.newOrder.direction == DT_SHORT) sizeSigned = -volume;
    } else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
        if      (tcmd.body.newOrder.direction == DT_LONG)  sizeSigned = -volume;
        else if (tcmd.body.newOrder.direction == DT_SHORT) sizeSigned =  volume;
    }
    if (sizeSigned == 0) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId     =
            (tcmd.body.newOrder.offsetFlag == OF_OPEN || tcmd.body.newOrder.offsetFlag == OF_CLOSE)
                ? DirectionError : OffsetFlagError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    std::string price_str = priceZero ? "0" : fmt::format("{}", price);
    std::string size_str  = fmt::format("{}", sizeSigned);

    std::string body = fmt::format(
        R"({{"text":"{}","contract":"{}","price":"{}","size":{},"tif":"{}","reduce_only":{}}})",
        escape_json(tcmd.body.newOrder.orderSysId), info.originInstId, price_str, size_str, tif,
        tcmd.body.newOrder.reduceOnly ? "true" : "false");

    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign     = crypto::getGateioSignatureRest("POST", newOrderUrl, time_str, "", body, acc.secretKey);
    auto headers = gate_auth_headers(acc.apiKey, time_str, sign);

    LOG_INFO("TB {} Gate US add_new_order body={}", acc.accountId, body);

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

                std::string_view id_sv, label_sv, fill_sv, left_sv;
                bool has_id    = (doc.find_field_unordered("id").get(id_sv)    == simdjson::SUCCESS);
                bool has_label = (doc.find_field_unordered("label").get(label_sv) == simdjson::SUCCESS);

                if (has_id) {
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, id_sv);
                    rcmd.body.orderResponse.errorId = NoError;
                    doc.find_field_unordered("fill_price").get(fill_sv);
                    doc.find_field_unordered("left").get(left_sv);
                    if (!fill_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(fill_sv);
                    double left = std::fabs(crypto::fast_atod(left_sv));
                    rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;

                    // 用 status/finish_as 判 orderStatus, 逻辑与 query_order 一致
                    std::string_view status_sv, finish_sv;
                    doc.find_field_unordered("status").get(status_sv);
                    doc.find_field_unordered("finish_as").get(finish_sv);
                    if (status_sv == "open") {
                        rcmd.body.orderResponse.orderStatus =
                            (rcmd.body.orderResponse.volumeTraded > ZERO_NUM) ? OS_PARTFILLED : OS_NEW;
                    } else if (finish_sv == "filled") {
                        rcmd.body.orderResponse.orderStatus = OS_FILLED;
                    } else if (finish_sv == "cancelled" || finish_sv == "ioc") {
                        rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                    } else if (rcmd.body.orderResponse.orderType == OT_IOC) {
                        rcmd.body.orderResponse.orderStatus =
                            (rcmd.body.orderResponse.volumeTraded < rcmd.body.orderResponse.volumeTotal)
                                ? OS_CANCELED : OS_FILLED;
                    } else {
                        rcmd.body.orderResponse.orderStatus = OS_NEW;
                    }
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
                LOG_ERROR("TB {} Gate US add_new_order cb exc: {}", acc.accountId, e.what());
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                rcmd.body.orderResponse.errorId     = NetworkError;
                rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            }
        });
}


// ============================================================================
// cancel_order —— DELETE /api/v4/futures/usdt/orders/{id}
// ============================================================================
void GateioUSTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
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
    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign     = crypto::getGateioSignatureRest("DELETE", pathBase, time_str, "", "", acc.secretKey);
    auto headers = gate_auth_headers(acc.apiKey, time_str, sign);

    LOG_INFO("TB {} Gate US cancel_order: {}", acc.accountId, pathBase);

    auto info_captured = info;

    asyncRequest(boost::beast::http::verb::delete_, pathBase, "", "", std::move(headers),
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

                std::string_view status_sv, id_sv, fill_sv, size_sv, left_sv, label_sv;
                bool has_status = (doc.find_field_unordered("status").get(status_sv) == simdjson::SUCCESS);
                bool has_label  = (doc.find_field_unordered("label").get(label_sv)   == simdjson::SUCCESS);

                if (has_status) {
                    doc.find_field_unordered("id").get(id_sv);
                    doc.find_field_unordered("fill_price").get(fill_sv);
                    doc.find_field_unordered("size").get(size_sv);
                    doc.find_field_unordered("left").get(left_sv);

                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, id_sv);
                    if (!fill_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(fill_sv);
                    double sz   = std::fabs(crypto::fast_atod(size_sv));
                    double left = std::fabs(crypto::fast_atod(left_sv));
                    rcmd.body.orderResponse.volumeTraded = sz - left;
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
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum,
                                  tcmd.body.queryOrder.instTypeEnum,
                                  tcmd.body.queryOrder.instId, info)) {
        LOG_INFO("TB {} Gate US query_order smc miss: {}", acc.accountId, tcmd.body.queryOrder.instId);
        return;
    }

    std::string idSeg;
    // Fix: 老代码 str_cmp 语义反了 (少了 !), 补正。
    if (!crypto::str_cmp(tcmd.body.queryOrder.orderId, "")) {
        idSeg = tcmd.body.queryOrder.orderId;
    } else if (!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")) {
        idSeg = tcmd.body.queryOrder.orderSysId;
    } else {
        return;
    }

    std::string pathBase = queryOrderUrl + "/" + idSeg;
    std::string time_str = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign     = crypto::getGateioSignatureRest("GET", pathBase, time_str, "", "", acc.secretKey);
    auto headers = gate_auth_headers(acc.apiKey, time_str, sign);

    LOG_INFO("TB {} Gate US query_order: {}", acc.accountId, pathBase);

    auto info_captured = info;

    asyncRequest(boost::beast::http::verb::get, pathBase, "", "", std::move(headers),
        [this, rcmd, info_captured](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
            if (ec) return;
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;

                std::string_view status_sv;
                if (doc.find_field_unordered("status").get(status_sv) != simdjson::SUCCESS) {
                    rcmd.body.orderResponse.errorId = crypto::get_gateio_errorid(resp.body.c_str());
                    rcmd.body.orderResponse.orderStatus =
                        (rcmd.body.orderResponse.errorId == OrderNotFoundError) ? OS_REJECTED : OS_UNKNOWN;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;
                }

                std::string_view id_sv, text_sv, size_sv, price_sv, left_sv, fill_sv, finish_sv;
                doc.find_field_unordered("id").get(id_sv);
                doc.find_field_unordered("text").get(text_sv);
                doc.find_field_unordered("size").get(size_sv);
                doc.find_field_unordered("price").get(price_sv);
                doc.find_field_unordered("left").get(left_sv);
                doc.find_field_unordered("fill_price").get(fill_sv);
                doc.find_field_unordered("finish_as").get(finish_sv);

                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId,    id_sv);
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, text_sv);
                rcmd.body.orderResponse.volumeTotal  = std::fabs(crypto::fast_atod(size_sv));
                rcmd.body.orderResponse.limitPrice   = crypto::fast_atod(price_sv);
                double left = std::fabs(crypto::fast_atod(left_sv));
                rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
                if (!fill_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(fill_sv);

                if (status_sv == "open") {
                    rcmd.body.orderResponse.orderStatus =
                        (rcmd.body.orderResponse.volumeTotal > rcmd.body.orderResponse.volumeTraded && rcmd.body.orderResponse.volumeTraded > ZERO_NUM)
                            ? OS_PARTFILLED : OS_NEW;
                } else {
                    if      (finish_sv == "filled")                            rcmd.body.orderResponse.orderStatus = OS_FILLED;
                    else if (finish_sv == "cancelled" || finish_sv == "liquidated" ||
                             finish_sv == "ioc"       || finish_sv == "auto_deleveraged" ||
                             finish_sv == "reduce_only" || finish_sv == "reduce_out")
                                                                              rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                    else                                                       rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                }
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} Gate US query_order cb exc: {}", acc.accountId, e.what());
            }
        });
}