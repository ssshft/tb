#include "binance/BinanceSpotTrade.h"
#include <algorithm>
#include <fmt/format.h>
#include <simdjson.h>


BinanceSpotTradeUnit::BinanceSpotTradeUnit(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {

}

BinanceSpotTradeUnit::~BinanceSpotTradeUnit() {

}

std::string BinanceSpotTradeUnit::buildSignedPath(std::string_view basePath, const std::vector<std::pair<std::string, std::string>>& kvs) const {
    // caller 已按预期顺序传入; sign 什么就 send 什么, Binance 只校验一致
    std::string qs;
    qs.reserve(256);
    for (size_t i = 0; i < kvs.size(); ++i) {
        if (i) {
            qs.push_back('&');
        }
        qs += kvs[i].first;
        qs.push_back('=');
        qs += kvs[i].second;
    }
    std::string sig = crypto::getBinanceSignatureRest(acc.secretKey, qs);

    std::string full;
    full.reserve(basePath.size() + 1 + qs.size() + 11 + sig.size());
    full.append(basePath);
    full.push_back('?');
    full.append(qs);
    full.append("&signature=");
    full.append(sig);
    return full;
}

std::string BinanceSpotTradeUnit::buildWsSigPayload(std::vector<std::pair<std::string, std::string>> kvs) const {
    std::sort(kvs.begin(), kvs.end(), [](const auto& a, const auto& b) { 
        return a.first < b.first; 
    });

    std::string out;
    out.reserve(256);
    for (size_t i = 0; i < kvs.size(); ++i) {
        if (i) {
            out.push_back('&');
        }
        out += kvs[i].first;
        out.push_back('=');
        out += kvs[i].second;
    }
    return out;
}

std::string BinanceSpotTradeUnit::buildSubscribeJson(long ts_ms, const std::string& signature) const {
    // 手拼 JSON, 免拉 rapidjson。 apiKey / signature 需要 JSON 转义 (虽然通常都是 [A-Za-z0-9])。
    return fmt::format(
        R"({{"id":"{}","method":"userDataStream.subscribe.signature",)"
        R"("params":{{"apiKey":"{}","timestamp":{},"recvWindow":5000,"signature":"{}"}}}})",
        ts_ms, acc.apiKey, ts_ms, signature);
}


// ============================================================================
// subWebsocekt: 建 REST + WS
// ============================================================================
void BinanceSpotTradeUnit::subWebsocekt() {
    // ---- REST ----
    std::string restHost = host_of(acc.restUrl);
    initRestClient(restHost, {{"X-MBX-APIKEY", acc.apiKey}}, 4);

    // ---- WS ----
    // Binance ws-api server 主动 PING → beast 自动回 PONG (ServerOnly)。
    net::WsConfig cfg;
    cfg.url = acc.wsUrl;
    cfg.ping_mode = net::WsConfig::PingMode::ServerOnly;
    cfg.auto_reconnect = true;
    cfg.idle_timeout_sec = 60;
    // subscribe 不能塞进 cfg.subscribe_messages —— timestamp 5s 内失效, 重连必 stale。
    // 我们在 onOpen 里当场签发。

    LOG_INFO("TB {} spot ws {} rest {}", acc.accountId, acc.wsUrl, restHost);
    subWebsocketWithConfig(std::move(cfg));
}


// ============================================================================
// onOpen: 每次连上 (含重连) 都发 fresh-signed subscribe.signature
// ============================================================================
void BinanceSpotTradeUnit::onOpen() {
    BaseTradeUnit::onOpen();  // 更新 isConnected + log

    long ts = crypto::getCurrentTimeMilli();
    std::string payload = buildWsSigPayload({
        {"apiKey", acc.apiKey},
        {"recvWindow", "5000"},
        {"timestamp", std::to_string(ts)}
    });
    std::string signature = crypto::getBinanceSignatureRest(acc.secretKey, payload);
    std::string msg = buildSubscribeJson(ts, signature);

    LOG_INFO("TB {} ws subscribe: {}", acc.accountId, msg);
    if (pWsClient) {
        pWsClient->send_text(std::move(msg));
    }
}


// ============================================================================
// onWebsocketMsg: simdjson 解析
// ============================================================================
void BinanceSpotTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool /*isBinary*/, int64_t /*recv_ns*/) {
    try {
        std::string msg(reinterpret_cast<const char*>(data), len);
        std::cout << "onWebsocketMsg: " << msg << std::endl;

        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc = g_parser.iterate(padded);
        if (doc.error()) {
            LOG_DEBUG("TB {} ws parse err: {}", acc.accountId, simdjson::error_message(doc.error()));
            return;
        }

        // ws-api 回复外层: {"id":"...","status":200,"result":{...}} 也可能命中,
        // userDataStream event 外层: {"event":{"e":"...", ...}}
        simdjson::ondemand::object ev;
        if (doc["event"].get(ev) != simdjson::SUCCESS) {
            // subscribe.signature 的 ACK / 心跳等, 无 event 字段, 忽略。
            return;
        }

        std::string_view e_sv;
        if (ev["e"].get(e_sv) != simdjson::SUCCESS) {
            return;
        }

        if (e_sv == "outboundAccountPosition") {
            handleAccountPosition(ev);
        }
        else if (e_sv == "executionReport") {
            handleExecutionReport(ev);
        }
        // 其他 e 值 (balanceUpdate / listStatus 等) 目前策略不消费, 跳过。
    }
    catch (const std::exception& e) {
        LOG_ERROR("TB {} ws msg exception: {}", acc.accountId, e.what());
    }
}

// ---- outboundAccountPosition ----
//   { "e":"outboundAccountPosition", "E":ts, "u":ts,
//     "B":[{"a":"BTC","f":"1.0","l":"0.0"},...] }
void BinanceSpotTradeUnit::handleAccountPosition(simdjson::ondemand::object& ev) {
    simdjson::ondemand::array balances;
    if (ev["B"].get(balances) != simdjson::SUCCESS) {
        return;
    }

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


// ---- executionReport ----
//   { "e":"executionReport", "s":"BTCUSDT", "i":<orderId>, "c":<clientOrderId>,
//     "C":<origClientOrderId>, "S":"BUY"/"SELL", "f":"GTC"/... , "o":"LIMIT"/... ,
//     "X":"NEW"/"FILLED"/... , "l":"lastQty", "L":"lastPx", "z":"cumQty",
//     "Z":"cumQuoteQty", "q":"origQty", "p":"limitPx" }
void BinanceSpotTradeUnit::handleExecutionReport(simdjson::ondemand::object& ev) {
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

    for (auto field : ev) {
        std::string_view k = field.unescaped_key().value_unsafe();

        switch (k[0]) {
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


// ============================================================================
// Trade API
// ============================================================================
void BinanceSpotTradeUnit::query_account(const pubsub::TCommand& tcmd) {
    // 走 query_balance 已够, 这里保留占位
    query_balance(tcmd);
}

void BinanceSpotTradeUnit::query_position(const pubsub::TCommand& tcmd) {
    // Spot 无 position
}

// ---- GET /api/v3/account?recvWindow=5000&timestamp=...&signature=... ----
void BinanceSpotTradeUnit::query_balance(const pubsub::TCommand& tcmd) {
    if (!pRestClient) {
        LOG_ERROR("TB {} query_balance: rest not ready", acc.accountId);
        return;
    }
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"recvWindow", "5000"},
        {"timestamp",  std::to_string(crypto::getCurrentTimeMilli())},
    };
    const std::string& path = buildSignedPath(balanceUrl, kvs);

    asyncRequest(boost::beast::http::verb::get, std::move(path), /*body=*/"", /*ct=*/"", [this](boost::system::error_code ec, ::net::HttpResponse resp) {
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
                crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(asset_sv)));
                rcmd.body.balance.available = crypto::fast_atod(free_sv);
                rcmd.body.balance.frozen = crypto::fast_atod(locked_sv);
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

// ---- POST /api/v3/order?... ----
void BinanceSpotTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
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

    // side
    const char* side = nullptr;
    if (tcmd.body.newOrder.offsetFlag == OF_OPEN) {
        if (tcmd.body.newOrder.direction == DT_LONG)  {
            side = "BUY";
        }
        else if (tcmd.body.newOrder.direction == DT_SHORT) {
            side = "SELL";
        }
    }
    else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
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

    double price = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice * info.magnifyNumber, info.tickSize);
    double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber,  info.lotSize);

    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(10);
    kvs.emplace_back("recvWindow", "5000");
    kvs.emplace_back("newClientOrderId", tcmd.body.newOrder.orderSysId);
    kvs.emplace_back("symbol", info.originInstId);
    kvs.emplace_back("timestamp", std::to_string(crypto::getCurrentTimeMilli()));
    kvs.emplace_back("side", side);

    // type / timeInForce / price / quantity / newOrderRespType
    switch (tcmd.body.newOrder.orderType) {
        case OT_LIMIT:
            kvs.emplace_back("type", "LIMIT");
            kvs.emplace_back("timeInForce", "GTC");
            kvs.emplace_back("price", fmt::format("{}", price));
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "ACK");
            break;
        case OT_MARKET:
            kvs.emplace_back("type", "MARKET");
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "RESULT");
            break;
        case OT_POST_ONLY:
            kvs.emplace_back("type", "LIMIT_MAKER");
            kvs.emplace_back("price", fmt::format("{}", price));
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "ACK");
            break;
        case OT_FOK:
            kvs.emplace_back("type", "LIMIT");
            kvs.emplace_back("timeInForce", "FOK");
            kvs.emplace_back("price", fmt::format("{}", price));
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "RESULT");
            break;
        case OT_IOC:
            kvs.emplace_back("type", "LIMIT");
            kvs.emplace_back("timeInForce", "IOC");
            kvs.emplace_back("price", fmt::format("{}", price));
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "RESULT");
            break;
        default:
            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            rcmd.body.orderResponse.errorId = OrderTypeError;
            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
            return;
    }

    const std::string& path = buildSignedPath(newOrderUrl, kvs);
    LOG_INFO("TB {} add_new_order: {}", acc.accountId, path);

    asyncRequest(boost::beast::http::verb::post, std::move(path), /*body=*/"", /*ct=*/"",
        [this, rcmd, info](boost::system::error_code ec, net::HttpResponse resp) mutable {
            if (ec) {
                LOG_ERROR("TB {} add_new_order ec: {}", acc.accountId, ec.message());
                rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                rcmd.body.orderResponse.errorId = NetworkError;
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, ec.message());
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;
            }
            // 触发 TESTCLIENTORDERID 时业务侧不上报
            if (rcmd.body.orderResponse.clientOrderId == TESTCLIENTORDERID) {
                return;
            }

            try {
                std::cout << "add new order: " << resp.body << std::endl;
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) {
                    LOG_ERROR("TB {} add_new_order parse err: {}", acc.accountId, resp.body);
                    rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                    rcmd.body.orderResponse.errorId = UnknownError;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;
                }

                int64_t code = 0;
                if (doc.find_field_unordered("code").get(code) == simdjson::SUCCESS) {
                    std::string_view msg_sv;
                    doc.find_field_unordered("msg").get(msg_sv);
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    rcmd.body.orderResponse.errorId = crypto::get_binance_errorid(static_cast<int>(code));
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, msg_sv);
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;
                }

                int64_t oid = 0;
                if (doc.find_field_unordered("orderId").get(oid) == simdjson::SUCCESS) {
                    fmt::format_to(rcmd.body.orderResponse.orderId, "{}", oid);

                    std::string_view execQ_sv, cumQ_sv;
                    doc.find_field_unordered("executedQty").get(execQ_sv);
                    doc.find_field_unordered("cummulativeQuoteQty").get(cumQ_sv);
                    if (!execQ_sv.empty()) {
                        rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(execQ_sv) * info.magnifyNumber;
                    }
                    if (rcmd.body.orderResponse.volumeTraded > 0 && !cumQ_sv.empty()) {
                        double cumQ = crypto::fast_atod(cumQ_sv);
                        rcmd.body.orderResponse.tradePrice = cumQ / rcmd.body.orderResponse.volumeTraded * info.reduceNumber;
                    }

                    if (rcmd.body.orderResponse.orderType == OT_IOC) {
                        rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.volumeTraded < rcmd.body.orderResponse.volumeTotal) ? OS_CANCELED : OS_FILLED;
                    } else {
                        if (rcmd.body.orderResponse.volumeTraded < ZERO_NUM) {
                            rcmd.body.orderResponse.orderStatus = OS_NEW;
                        } else if (rcmd.body.orderResponse.volumeTraded < rcmd.body.orderResponse.volumeTotal) {
                            rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
                        } else {
                            rcmd.body.orderResponse.orderStatus = OS_FILLED;
                        }
                    }
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                } else {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    rcmd.body.orderResponse.errorId = UnknownError;
                    rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                }
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} add_new_order cb exception: {}", acc.accountId, e.what());
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                rcmd.body.orderResponse.errorId     = NetworkError;
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, std::string_view(e.what()));
                rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            }
        });
}


// ---- DELETE /api/v3/order?... ----
void BinanceSpotTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.cancelOrder.exchangeTypeEnum, tcmd.body.cancelOrder.instTypeEnum, tcmd.body.cancelOrder.instId, info)) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = SMCInstrumentNotExistError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(5);
    kvs.emplace_back("recvWindow", "5000");
    kvs.emplace_back("symbol", info.originInstId);
    kvs.emplace_back("timestamp", std::to_string(crypto::getCurrentTimeMilli()));

    if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
        kvs.emplace_back("orderId", tcmd.body.cancelOrder.orderId);
    } else if (!crypto::str_cmp(tcmd.body.cancelOrder.orderSysId, "")) {
        kvs.emplace_back("origClientOrderId", tcmd.body.cancelOrder.orderSysId);
    } else {
        LOG_ERROR("TB {} cancel_order need orderId or orderSysId", acc.accountId);
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = OrderIdError;
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, std::string_view("need orderId or clientOrderId"));
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const std::string& path = buildSignedPath(cancelOrderUrl, kvs);
    LOG_INFO("TB {} cancel_order: {}", acc.accountId, path);

    asyncRequest(boost::beast::http::verb::delete_, std::move(path), /*body=*/"", /*ct=*/"",
        [this, rcmd, info](boost::system::error_code ec, net::HttpResponse resp) mutable {
            if (ec) {
                LOG_ERROR("TB {} cancel_order ec: {}", acc.accountId, ec.message());
                rcmd.body.orderResponse.orderStatus = OS_FAILED;
                rcmd.body.orderResponse.errorId = NetworkError;
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, ec.message());
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;
            }
            try {
                std::cout << "cancel order: " << resp.body << std::endl;
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) {
                    LOG_ERROR("TB {} cancel_order parse err: {}", acc.accountId, resp.body);
                    return;
                }

                int64_t code = 0;
                if (doc.find_field_unordered("code").get(code) == simdjson::SUCCESS) {
                    std::string_view msg_sv;
                    doc.find_field_unordered("msg").get(msg_sv);
                    rcmd.body.orderResponse.errorId = crypto::get_binance_errorid(static_cast<int>(code));
                    rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.errorId == OrderNotFoundError) ? OS_REJECTED : OS_FAILED;
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, msg_sv);
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;
                }

                int64_t oid = 0;
                if (doc.find_field_unordered("orderId").get(oid) == simdjson::SUCCESS) {
                    fmt::format_to(rcmd.body.orderResponse.orderId, "{}", oid);
                }
                std::string_view execQ_sv, cumQ_sv;
                doc.find_field_unordered("executedQty").get(execQ_sv);
                doc.find_field_unordered("cummulativeQuoteQty").get(cumQ_sv);
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
            catch (const std::exception& e) {
                LOG_ERROR("TB {} cancel_order cb exception: {}", acc.accountId, e.what());
            }
        });
}


// ---- GET /api/v3/order?... ----
void BinanceSpotTradeUnit::query_order(const pubsub::TCommand& tcmd) {
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
        LOG_ERROR("TB {} query_order need orderId or orderSysId, {}", acc.accountId, tcmd.body.queryOrder.instId);
        return;
    }

    const std::string& path = buildSignedPath(queryOrderUrl, kvs);
    LOG_INFO("TB {} query_order: {}", acc.accountId, path);

    asyncRequest(boost::beast::http::verb::get, std::move(path), /*body=*/"", /*ct=*/"",
        [this, rcmd, info](boost::system::error_code ec, net::HttpResponse resp) mutable {
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

                int64_t code = 0;
                if (doc.find_field_unordered("code").get(code) == simdjson::SUCCESS) {
                    long now = crypto::getCurrentTime();
                    if (rcmd.body.orderResponse.clientOrderId > 0 && now - rcmd.body.orderResponse.clientOrderId > ORDER_REJECTED_TIME_OUT) {
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
            catch (const std::exception& e) {
                LOG_ERROR("TB {} query_order cb exception: {}", acc.accountId, e.what());
            }
        });
}
