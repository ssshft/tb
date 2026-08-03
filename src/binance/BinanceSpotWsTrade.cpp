#include "binance/BinanceSpotWsTrade.h"



BinanceSpotWsTrade::BinanceSpotWsTrade(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {
    newOrderUrl = "/api/v3/order";
    cancelOrderUrl = "/api/v3/order";
    queryOrderUrl = "/api/v3/order";
    balanceUrl = "/api/v3/account";

    if (!init_ed25519_key_from_cfg()) {
        LOG_ERROR();
        return;
    }

    m_tradeWsUrl = "wss:://ws-api.binance./comm/ws-api/v3"
    if () {
        m_tradeWsUrl = m_curcfg.wsTradeBaseUrl;
    }
    init_trade_ws_client();

    m_userWsUrl = m_curcfg.wsBaseUrl;
    if () {
        m_userWsUrl = m_tradeUsUrl;
    }
    init_user_ws_client();
}

void BinanceSpotWsTrade::init_trade_ws_client() {

}

std::string base64_encode_openssl(const unsigned char* input, size_t length) {
    BIO* bmem = BIO_new(BIO_s_mem());
    BIO* b64 = BIO_new(BIO_f_base64());
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
    b64 = BIO_push(b64, bmem);
    BIO_write(b64, input, static_cast<int>(length));
    BIO_flush(b64);
    BUF_MEM* bptr = nullptr;
    BIO_get_mem_ptr(b64, &bptr);
    std::string encodes(bptr->data, bptr->length);
    BIO_free_all(64);
    return encoded;
}

void BinanceSpotWsTrade::sign_ed25519_base64(const std::string& payload) {
    EVP_MD_CTX* mdctx = EVP_MD_CTX_new();
    if (!mdctx) {
        throw std::runtime_error("EVP_MD_CTX_new failed");
    }

    if (EVP_DigestSignInit(mdctx, nullptr, nullptr, nullptr, m_ed25519_pkey) <= 0) {
        EVP_MD_CTX_Free(mdctx);
        throw std::runtime_error("EVP_DigestSignInit failed");
    }

    size_t siglen = 0;
    if (EVP_DigestSign(mdctx, nullptr, &siglen, reinterpret_cast<const unsigned char*>(payload.data()), payload.size()) <= 0) {
        EVP_MD_CTX_Free(mdctx);
        throw std::runtime_error("EVP_DigestSign siglen failed");
    }

    std::vector<unsigned char> sig(siglen);
    if (EVP_DigestSign(mdctx, sig.data(), &siglen, reinterpret_cast<const unsigned char*>(payload.data()), payload.size()) <= 0) {
        EVP_MD_CTX_Free(mdctx);
        throw std::runtime_error("EVP_DigestSign failed");
    }

    EVP_MD_CTX_free(mdctx);

    return base64_encode_openssl(sig.data(), siglen);
}

BinanceSpotTradeUnit::~BinanceSpotTradeUnit() {

}

std::string BinanceSpotTradeUnit::buildWsSigPayload(std::vector<std::pair<std::string, std::string>> kvs) {
    std::sort(kvs.begin(), kvs.end(), [](auto& a, auto& b){ return a.first < b.first;});

    std::string out = "";
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


void user_ws_subscribe_signature() {
    std::string payload;
    payload.reserve(160);
    payload.append("apikey=");
    payload.append(m_curcfg.apiKey);
    payload.append("&recvWindow=5000");
    payload.append("&timestamp=");
    payload.append(ts_buf);

    std::string sig = sign_ed25519_base64(payload);

    std::string params;
    params.reserve(220 + sig.size());
    params.append("{\"apiKey\":\"");
    params.append(m_curcfg.apiKey);
    params.append("\",\"timestamp\":");
    params.append(ts_buf);
    params.append(",\"recvWindow\":");
    params.append(recvWindow);
    params.append(",\"signature\":\"");
    params.append(sig);
    params.append("\"}");

    m_userWsClient->send_request_task("userDataStream.subscribe.signature", params, std::move(on_resp));

    auto on_resp = [this](const std::string& resp) {
        rapidjson::Value& raw = d.parse(resp);
        int status_code = 0;
        if (raw.HasMember("status")) {
            const auto& st = raw["status"];
            if (st.IsInt()) {
               status_code = st.GetInt(); 
            }
            else if (st.IsInt64()) {
                status_code = static_cast<int>(st.GetInt64()); 
            }
        }

        if (raw.HasMember("result") && raw["result"].IsObject()) {
            const auto& r = raw["result"];
            int sid = -1;
            if (r.HasMember("subscriptionId")) {
                const auto& v = r["subscriptionId"];
                if (v.IsInt()) {
                    sid = v.GetInt(); 
                }
                else if (v.IsInt64()) {
                    sid = static_cast<int>(v.GetInt64()); 
                }
            }
            m_userWsSubscriptionId.store(sid, std::memory_order_release);
        }
    };

}

void on_user_ws_msg(const std::string& body) {
    if (body.find("\"event\"") == std::string::npos) {
        return;
    }

    rapidjson::Value& raw = d.Parse(body);
    auto& ev = raw["event"];
    if (!ev.IsObject() || ! ev.HasMember("e")) {
        return;
    }

    auto& e = ev["e"].GetString();
    if (e == "outboundAccountPosition") {

    }
    else if (e == "executionReport") {
        
    }
}


void add_new_order_ws() {
    long ts_ms = crypto::getCurrentTimeMilli();


    static thread_local std::string msg;
    msg.clear();
    msg.reserve(512 + price.size() + amount.size());
    msg.append("\"id\":");
    msg.append(wsid_buf);
    msg.append(",\"method\":\"order.place\",\"params\":{");
    msg.append("\"newClientOrderId\":\"");
    msg.append(tcmd.body.newOrder.orderSysId);
    msg.append("\",");

    try {
        tradeWsClient->send_text_async(msg);
    }
    catch() {

    }
}

void cancel_order_ws() {
    long ts_ms = crypto::getCurrentTimeMilli();

    static thread_local std::string msg;
    msg.clear();
    msg.reserve(512 + price.size() + amount.size());
    msg.append("\"id\":");
    msg.append(wsid_buf);
    msg.append(",\"method\":\"order.cancel\",\"params\":{");
    msg.append("\"newClientOrderId\":\"");
    msg.append(tcmd.body.newOrder.orderSysId);
    msg.append("\",");

    try {
        tradeWsClient->send_text_async(msg);
    }
    catch() {

    }   
}

void BinanceSpotTradeUnit::subWebsocekt() {
    web::http::uri_builder builder(acc.wsUrl);
    START_SUB_WEBSOCKET(builder);

    if (isConnected) {
        latestPingPongTime.store(crypto::getCurrentTimeSeconds());

        long ts = crypto::getCurrentTimeMilli();
        std::string recvWindow = "5000";
        std::string payload = buildWsSigPayload({
            {"apiKey", acc.apiKey},
            {"recvWindow", recvWindow},
            {"timestamp", std::to_string(ts)}
        });

        std::string signature = crypto::getBinanceSignatureRest(acc.secretKey, payload);

        rapidjson::Document d;
        d.SetObject();
        auto& al = d.GetAllocator();
        d.AddMember("id", rapidjson::Value(std::to_string(ts).c_str(), al), al);
        d.AddMember("method", rapidjson::Value("userDataStream.subscribe.signature", al), al);
        rapidjson::Value params(rapidjson::kObjectType);
        params.AddMember("apiKey", rapidjson::Value(acc.apiKey.c_str(), al), al);
        params.AddMember("timestamp", ts, al);
        params.AddMember("recvWindow", 5000, al);
        params.AddMember("signature", rapidjson::Value(signature.c_str(), al), al);
        d.AddMember("params", params, al);

        rapidjson::StringBuffer sb;
        rapidjson::Writer<rapidjson::StringBuffer> wr(sb);
        d.Accept(wr);

        web:;websockets::client::websocket_outgoing_message out;
        out.set_utf8_message(sb.GetString());
        LOG_INFO("TB {} ws subscribe payload: {}, signature:{}", acc.accountId, payload, signature);
        pWsClient->send(out).then([this]() {
            std::cout << "spot ws url subscribe successfully!" << std::endl;
        }).wait();
    }
    else {
        LOG_ERROR("{} ws: {} connect failed, cannot sub.", acc.accountId, acc.wsUrl);
    }
}

void BinanceSpotTradeUnit::onWebsocketMsg(const websocket_incoming_message& msg) {
    try {
        latestPingPongTime.store(crypto::getCurrentTimeSeconds());
        auto ty = msg.message_type();
        if (ty == websocket_message_type::text_message) [[likely]] {
            const std::string s = msg.extract_string().get();

            std::cout << "------------"  << s << std::endl;
            return;

            rapidjson::Document d;
            d.Parse<rapidjson::kParseNumbersAsStringsFlag>(s.c_str());
            if (d.HasParseError() || !d.IsObject() || !d.HasMember("event")) {
                return;
            }

            const rapidjson::Value& ev = d["event"];
            if (!ev.IsObject() || !ev.HasMember("e")) {
                return;
            }

            const std::string& e = ev["e"].GetString();

            LOG_DEBUG("{} on_websocket_msg: {}", acc.accountId, s.c_str());

            if (crypto::str_cmp(e.c_str(), "outboundAccountPosition")) {
                if (!ev.HasMember("B") || !ev["B"].IsArray()) {
                    return;
                }

                const rapidjson::Value& data = ev["B"];
                for (rapidjson::SizeType i = 0; i < data.Size(); i++) {
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = BINANCE;
                    rcmd.body.balance.instTypeEnum = SPOT;
                    
                    strncpy(rcmd.body.balance.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                    strncpy(rcmd.body.balance.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);
                    strncpy(rcmd.body.balance.currency, crypto::to_upper(data[i]["a"].GetString()).c_str(), INSTID_SIZE);
                    rcmd.body.balance.available = std::stod(data[i]["f"].GetString());
                    rcmd.body.balance.frozen = std::stod(data[i]["l"].GetString());
                    rcmd.body.balance.total = rcmd.body.balance.available + rcmd.body.balance.frozen;
                    rcmd.body.balance.isLast = data.Size() - 1;
                    rcmd.body.balance.updateTime = crypto::getCurrentTime();
                    rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;

                    std::cout << "onWebsocketMsg: " << rcmd.getString() << std::endl;
                    PUSH_RCMD(rcmd)
                }
            }
            else if(crypto::str_cmp(e.c_str()," executionReport")) {
                if (!ev.HasMember("s")) {
                    return;
                }

                const std::string& originInstId = ev["s"].GetString();
                md::InstrumentInfo info;
                if (smc->get_instrument_info(BINANCE, SPOT, originInstId.c_str(), info)) {
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                    rcmd.body.orderResponse.exchangeTypeEnum = BINANCE;
                    rcmd.body.orderResponse.instTypeEnum = SPOT;
                    
                    strncpy(rcmd.body.orderResponse.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                    strncpy(rcmd.body.orderResponse.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);
                    strncpy(rcmd.body.orderResponse.instId, info.instId, INSTID_SIZE);

                    if (ev["i"].IsString()) {
                        strncpy(rcmd.body.orderResponse.orderId, ev["i"].GetString(), ORDER_SIZE);
                    }
                    else {
                        strncpy(rcmd.body.orderResponse.orderId, std::to_string(ev["i"].GetInt64()).c_str(), ORDER_SIZE);
                    }

                    if (ev.HasMember("C") && !ev["C"].IsNull() && ev["C"].GetStringLength() > 0) {
                        strncpy(rcmd.body.orderResponse.orderSysId, ev["C"].GetString(), ORDER_SIZE);
                    }
                    else if (ev.HasMember("c")) {
                        strncpy(rcmd.body.orderResponse.orderSysId, ev["c"].GetString(), ORDER_SIZE);
                    }

                    rcmd.body.orderResponse.offsetFlag = OF_OPEN;
                    if (ev.HasMember("S")) {
                        if (ev["S"].GetString()[0] == 'B') {
                             rcmd.body.orderResponse.direction = DT_LONG;
                        }
                        else {
                             rcmd.body.orderResponse.direction = DT_SHORT;
                        }
                    }
                   
                    std::string tif = "";
                    if (ev.HasMember("f")) {
                        tif = ev["f"].GetString();
                    }

                    std::string ty = "";
                    if (ev.HasMember("o")) {
                        ty = ev["o"].GetString();
                    }

                    rcmd.body.orderResponse.orderType = crypto::get_binance_ordertype(tif.c_str(), ty.c_str());
                    
                    if (ev.HasMember("X")) {
                        rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(ev["X"].GetString());
                    }

                    if (ev.HasMember("l")) {
                        rcmd.body.orderResponse.tradeDiff = stod(ev["l"].GetString()) * info.magnifyNumber;
                    }

                    if (ev.HasMember("L")) {
                        rcmd.body.orderResponse.fillPrice = stod(ev["L"].GetString()) * info.reduceNumber;
                    }

                    if (ev.HasMember("z")) {
                        rcmd.body.orderResponse.volumeTraded = stod(ev["z"].GetString()) * info.magnifyNumber;
                    }

                    if (rcmd.body.orderResponse.volumeTraded > 0 && ev.HasMember("Z")) {
                        rcmd.body.orderResponse.tradePrice = stod(ev["Z"].GetString()) / rcmd.body.orderResponse.volumeTraded * info.reduceNumber;
                    }

                    if (ev.HasMember("q")) {
                        rcmd.body.orderResponse.volumeTotal = stod(ev["q"].GetString()) * info.magnifyNumber;
                    }

                    if (ev.HasMember("p")) {
                        rcmd.body.orderResponse.limitPrice = stod(ev["p"].GetString()) * info.reduceNumber;
                    }

                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
                    PUSH_RCMD(rcmd)
                }
                else {
                    LOG_ERROR("cannot find smc info, originInstId: {}", originInstId);
                }
            }     
        }
        else if (ty == websocket_message_type::ping) {
            try {
                const std::string& payload = msg.extract_string().get();
                LOG_INFO("TB {} got ping msg: {}, will reply pong.", acc.accountId, payload);
                pong(payload);
            }
            catch (std::exception& e) {
                isConnected = false;
            }
        }
        else if(ty == websocket_message_type::close) {
            LOG_INFO("TB {} get close msg.", acc.accountId);
            isConnected = false;
        }
    }
    catch(std::exception& e) {
        LOG_ERROR("{}", e.what());
        isConnected = false;
    }
    catch (...) {
        LOG_ERROR("unknown error, {}", acc.accountId);
        isConnected = false;
    }
}

void BinanceSpotTradeUnit::ping() {

}

void BinanceSpotTradeUnit::pong() {

}

void BinanceSpotTradeUnit::pong(const std::string& payload) {
    web::websockets::client::websocket_outgoing_message outMsg;
    if (payload.empty()) {
        outMsg.set_pong_message();
    }
    else {
        LOG_INFO("TB {} send pong msg: {}", acc.accountId, payload);
        outMsg.set_pong_message(utility::conversions::to_string_t(payload));
    }
    if (pWsClient) {
        latestPingPongTime.store(crypto::getCurrentTimeSeconds());
        pWsClient->send(outMsg);
    }
}

void BinanceSpotTradeUnit::query_account(const pubsub::TCommand& tcmd) {

}

void BinanceSpotTradeUnit::query_position(const pubsub::TCommand& tcmd) {

}

void BinanceSpotTradeUnit::query_balance(const pubsub::TCommand& tcmd) { // 也需要 magnifyNumber/reduceNumber
    try {
        web::http::http_request request(web::http::methods::GET);
        FROMAT_BINANCE_REQUEST(request)
        web::http::uri_builder builder(balanceUrl);
        builder.append_query("recvWindow", 5000);
        builder.append_query("timestamp", crypto::getCurrentTimeMilli());
        std::string signature = crypto::getBinanceSignatureRest(acc.secretKey, builder.query());
        builder.append_query("signature", signature);
        request.set_request_uri(builder.to_string());

        auto& restClient = *pRestClient;
        START_FORMAT_RESPONSE(request)
            const web::json::value& v = previousTask.get();
            if (v.has_field("balances")) {
                auto& array = v.at("balances").as_array();
                std::vector<pubsub::RCommand> needPushRcmd;
                for (auto& it : array) {
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = BINANCE;
                    rcmd.body.balance.instTypeEnum = SPOT;
                    strncpy(rcmd.body.balance.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                    strncpy(rcmd.body.balance.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);
                    
                    strncpy(rcmd.body.balance.currency, crypto::to_upper(it.at("asset").as_string().c_str()).c_str(), INSTID_SIZE);
                    rcmd.body.balance.available = std::stod(it.at("free").as_string().c_str());
                    rcmd.body.balance.frozen = std::stod(it.at("locked").as_string().c_str());
                    rcmd.body.balance.total = rcmd.body.balance.available + rcmd.body.balance.frozen;
                    rcmd.body.balance.updateTime = crypto::getCurrentTime();
                    rcmd.body.balance.apiSourceEnum = AS_REST;

                    needPushRcmd.emplace_back(rcmd);
                }

                if (needPushRcmd.size() > 0) {
                    size_t count = 0;
                    size_t size = needPushRcmd.size();
                    for (size_t i = 0; i < needPushRcmd.size(); ++i) {
                        count++;
                        needPushRcmd[i].body.balance.isLast = count == size;
                        PUSH_RCMD(needPushRcmd[i]);
                    }
                }
                else {
                    LOG_INFO("{} no balance pushed, will push usdt balance=0 rcmd.", acc.accountId);
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = BINANCE;
                    rcmd.body.balance.instTypeEnum = SPOT;
                    strncpy(rcmd.body.balance.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                    strncpy(rcmd.body.balance.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);

                    strncpy(rcmd.body.balance.currency, "USDT", INSTID_SIZE);
                    rcmd.body.balance.updateTime = crypto::getCurrentTime();
                    rcmd.body.balance.apiSourceEnum = AS_REST;
                    rcmd.body.balance.isLast = true;
                    PUSH_RCMD(rcmd);
                }
            }
            else {
                LOG_ERROR("{}", v.serialize());
            }
        END_FORMAT_RESPONSE(request)
    }
    catch (std::exception& e) {
        LOG_ERROR("{}", e.what());
    }
}


void BinanceSpotTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED; 
        rcmd.body.orderResponse.errorId = TBDisconnectError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    try {
        md::InstrumentInfo info;
        if (smc->get_instrument_info(tcmd.body.newOrder.exchangeTypeEnum, tcmd.body.newOrder.instTypeEnum, tcmd.body.newOrder.instId, info)) {
            web::http::http_request request(web::http::methods::POST);
            FROMAT_BINANCE_REQUEST(request)

            web::http::uri_builder builder(newOrderUrl);
            builder.append_query("recvWindow", 5000);
            builder.append_query("newClientOrderId", tcmd.body.newOrder.orderSysId);
            builder.append_query("symbol", info.originInstId);
            builder.append_query("timestamp", crypto::getCurrentTimeMilli());

            double price = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice * info.magnifyNumber, info.tickSize);
            double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber, info.lotSize);
    
            if (tcmd.body.newOrder.offsetFlag == OF_OPEN) {
                if (tcmd.body.newOrder.direction == DT_LONG) {
                    builder.append_query("side", "BUY");
                }
                else if (tcmd.body.newOrder.direction == DT_SHORT) {
                    builder.append_query("side", "SELL");
                }
                else {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED; 
                    rcmd.body.orderResponse.errorId = DirectionError;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;
                }
            }
            else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
                if (tcmd.body.newOrder.direction == DT_LONG) {
                    builder.append_query("side", "SELL");
                }
                else if (tcmd.body.newOrder.direction == DT_SHORT) {
                    builder.append_query("side", "BUY");
                }
                else {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED; 
                    rcmd.body.orderResponse.errorId = DirectionError;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;
                }
            }
            else {
                rcmd.body.orderResponse.orderStatus = OS_REJECTED; 
                rcmd.body.orderResponse.errorId = OffsetFlagError;
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;
            }

            switch (tcmd.body.newOrder.orderType) {
                case OT_LIMIT: {
                    builder.append_query("type", "LIMIT");
                    builder.append_query("timeInForce", "GTC");
                    builder.append_query("price", price);
                    builder.append_query("quantity", volume);
                    builder.append_query("newOrderRespType", "ACK");
                    break;  
                }
                case OT_MARKET: {
                    builder.append_query("type", "MARKET");
                    builder.append_query("quantity", volume);
                    builder.append_query("newOrderRespType", "RESULT");
                    break;
                }
                case OT_POST_ONLY: {
                    builder.append_query("type", "LIMIT_MAKER");
                    builder.append_query("price", price);
                    builder.append_query("quantity", volume);
                    builder.append_query("newOrderRespType", "ACK");
                    break;
                }
                case OT_FOK: {
                    builder.append_query("type", "LIMIT");
                    builder.append_query("timeInForce", "FOK");
                    builder.append_query("price", price);
                    builder.append_query("quantity", volume);  
                    builder.append_query("newOrderRespType", "RESULT");
                    break;
                }
                case OT_IOC: {
                    builder.append_query("type", "LIMIT");
                    builder.append_query("timeInForce", "IOC");
                    builder.append_query("price", price);
                    builder.append_query("quantity", volume); 
                    builder.append_query("newOrderRespType", "RESULT");
                    break;
                }
                default: {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED; 
                    rcmd.body.orderResponse.errorId = OrderTypeError;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;    
                }
            }

            std::string signature = crypto::getBinanceSignatureRest(acc.secretKey, builder.query());
            builder.append_query("signature", signature);
            request.set_request_uri(builder.to_string());

            LOG_INFO("add_new_order builder: {}", builder.to_string());

            auto& restClient = *pRestClient;
            START_FORMAT_RESPONSE(request)
                if (tcmd.body.newOrder.clientOrderId == TESTCLIENTORDERID) {
                    return;
                }
                const web::json::value& v = previousTask.get();
                LOG_INFO("add_new_order response: {}", v.serialize().c_str());

                if (v.has_field("code")) {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED; 
                    rcmd.body.orderResponse.errorId = crypto::get_binance_errorid(v.at("code").as_integer());
                    strncpy(rcmd.body.orderResponse.originMsg, v.at("msg").as_string().c_str(), ORIGINMSG_SIZE);
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                }
                else if (v.has_field("orderId")) {
                    strncpy(rcmd.body.orderResponse.orderId, v.at("orderId").as_string().c_str(), INSTID_SIZE);
                    if (v.has_field("executedQty")) {
                        rcmd.body.orderResponse.volumeTraded = std::stod(v.at("executedQty").as_string().c_str()) * info.magnifyNumber;
                    }

                    if (v.has_field("cummulativeQuoteQty")) {
                        double cummulativeQuoteQty = std::stod(v.at("cummulativeQuoteQty").as_string().c_str());
                        if (rcmd.body.orderResponse.volumeTraded > 0) {
                            rcmd.body.orderResponse.tradePrice = cummulativeQuoteQty / rcmd.body.orderResponse.volumeTraded * info.reduceNumber;
                        }
                    }

                    if (rcmd.body.orderResponse.orderType == OT_IOC) {
                        if (rcmd.body.orderResponse.volumeTraded < rcmd.body.orderResponse.volumeTotal) {
                            rcmd.body.orderResponse.orderStatus = OS_CANCELED; 
                        }
                        else {
                            rcmd.body.orderResponse.orderStatus = OS_FILLED; 
                        }
                    }
                    else {
                        if (rcmd.body.orderResponse.volumeTraded < ZERO_NUM) {
                            rcmd.body.orderResponse.orderStatus = OS_NEW; 
                        }
                        else if (rcmd.body.orderResponse.volumeTraded < rcmd.body.orderResponse.volumeTotal) {
                            rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
                        }
                        else {
                            rcmd.body.orderResponse.orderStatus = OS_FILLED; 
                        }
                    }

                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                }
                else {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED; 
                    rcmd.body.orderResponse.errorId = UnknownError;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)    
                }

            END_FORMAT_RESPONSE(request)
        }
        else {
            rcmd.body.orderResponse.orderStatus = OS_REJECTED; 
            rcmd.body.orderResponse.errorId = SMCInstrumentNotExistError;
            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
        }
    }
    catch (std::exception& e) {
        LOG_ERROR("add_new_order exception: {}", e.what());
        rcmd.body.orderResponse.orderStatus = OS_REJECTED; 
        rcmd.body.orderResponse.errorId = NetworkError;
        strncpy(rcmd.body.orderResponse.originMsg, e.what(), ORIGINMSG_SIZE);
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
    }
}

void BinanceSpotTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    try {
        md::InstrumentInfo info;
        if (smc->get_instrument_info(tcmd.body.cancelOrder.exchangeTypeEnum, tcmd.body.cancelOrder.instTypeEnum, tcmd.body.cancelOrder.instId, info)) {
            web::http::http_request request(web::http::methods::DEL);
            FROMAT_BINANCE_REQUEST(request)

            web::http::uri_builder builder(cancelOrderUrl);
            builder.append_query("recvWindow", 5000);
            builder.append_query("symbol", info.originInstId);
            builder.append_query("timestamp", crypto::getCurrentTimeMilli());
          
            if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
                builder.append_query("orderId", tcmd.body.cancelOrder.orderId);
            }
            else if (!crypto::str_cmp(tcmd.body.cancelOrder.orderSysId, "")) {
                builder.append_query("origClientOrderId", tcmd.body.cancelOrder.orderSysId);
            }
            else {
                LOG_ERROR("cancel order need orderId or orderSysId" );
                rcmd.body.orderResponse.orderStatus = OS_FAILED;
                rcmd.body.orderResponse.errorId = OrderIdError;
                strcpy(rcmd.body.orderResponse.originMsg, "cancel order need orderId or clientOrderId");
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;
            }

            std::string signature = crypto::getBinanceSignatureRest(acc.secretKey, builder.query());
            builder.append_query("signature", signature);
            request.set_request_uri(builder.to_string());

            LOG_INFO("cancel_order builder: {}", builder.to_string());

            auto& restClient = *pRestClient;
            START_FORMAT_RESPONSE(request)
                const web::json::value& v = previousTask.get();
                LOG_INFO("cancel_order response: {}", v.serialize().c_str());

                if (v.has_field("code")) {
                    rcmd.body.orderResponse.orderStatus = OS_FAILED;
                    rcmd.body.orderResponse.errorId = crypto::get_binance_errorid(v.at("code").as_integer());
                    if (rcmd.body.orderResponse.errorId == OrderNotFoundError) {
                        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    }
                    strncpy(rcmd.body.orderResponse.originMsg, v.at("msg").as_string().c_str(), ORIGINMSG_SIZE);
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd);
                }
                else {
                    strncpy(rcmd.body.orderResponse.orderId, v.at("orderId").as_string().c_str(), ORDER_SIZE);
                    rcmd.body.orderResponse.volumeTraded = std::stod(v.at("executedQty").as_string().c_str()) * info.magnifyNumber;

                    if (v.has_field("cummulativeQuoteQty")) {
                        double cummulativeQuoteQty = std::stod(v.at("cummulativeQuoteQty").as_string().c_str());
                        if (rcmd.body.orderResponse.volumeTraded > 0) {
                            rcmd.body.orderResponse.tradePrice = cummulativeQuoteQty / rcmd.body.orderResponse.volumeTraded * info.reduceNumber;
                        }
                    }

                    rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd);
                }

            END_FORMAT_RESPONSE(request)   
        }
        else {
            rcmd.body.orderResponse.orderStatus = OS_FAILED; 
            rcmd.body.orderResponse.errorId = SMCInstrumentNotExistError;
            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
        }
    }
    catch (const std::exception& e) {
        LOG_ERROR("cancel_order exception: {}", e.what());
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = NetworkError;
        strncpy(rcmd.body.orderResponse.originMsg, e.what(), ORIGINMSG_SIZE);
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
    }
}

void BinanceSpotTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);
    try {
        md::InstrumentInfo info;
        if (smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum, tcmd.body.queryOrder.instTypeEnum, tcmd.body.queryOrder.instId, info)) {
            web::http::http_request request(web::http::methods::GET);
            FROMAT_BINANCE_REQUEST(request)

            web::http::uri_builder builder(queryOrderUrl);
            builder.append_query("recvWindow", 5000);
            builder.append_query("symbol", info.originInstId);
            builder.append_query("timestamp", crypto::getCurrentTimeMilli());
          
            if (!crypto::str_cmp(tcmd.body.queryOrder.orderId, "")) {
                builder.append_query("orderId", tcmd.body.queryOrder.orderId);
            }
            else if (!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")) {
                builder.append_query("origClientOrderId", tcmd.body.queryOrder.orderSysId);
            }
            else {
                LOG_ERROR("query order need orderId or orderSysId {}", tcmd.body.queryOrder.instId);
                return;
            }

            std::string signature = crypto::getBinanceSignatureRest(acc.secretKey, builder.query());
            builder.append_query("signature", signature);
            request.set_request_uri(builder.to_string());

            LOG_INFO("query_order builder: {}", builder.to_string());

            auto& restClient = *pRestClient;
            START_FORMAT_RESPONSE(request)
                const web::json::value& v = previousTask.get();
                LOG_INFO("query_order response: {}", v.serialize().c_str());

                if (v.has_field("code")) {
                    long now = crypto::getCurrentTime();
                    if (rcmd.body.orderResponse.clientOrderId > 0 && now - rcmd.body.orderResponse.clientOrderId > ORDER_REJECTED_TIME_OUT) {
                        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                        rcmd.body.orderResponse.errorId = crypto::get_binance_errorid(v.at("code").as_integer());
                        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                        PUSH_RCMD(rcmd);
                    }
                }
                else {
                    if (v.has_field("orderId")) {
                        strncpy(rcmd.body.orderResponse.orderId, v.at("orderId").as_string().c_str(), ORDER_SIZE);
                    }

                    rcmd.body.orderResponse.volumeTotal = std::stod(v.at("origQty").as_string().c_str()) * info.magnifyNumber;
                    rcmd.body.orderResponse.limitPrice = std::stod(v.at("price").as_string().c_str()) * info.reduceNumber;
                    rcmd.body.orderResponse.volumeTraded = std::stod(v.at("executedQty").as_string().c_str()) * info.magnifyNumber;

                    if (v.has_field("cummulativeQuoteQty")) {
                        double cummulativeQuoteQty = std::stod(v.at("cummulativeQuoteQty").as_string().c_str());
                        if (rcmd.body.orderResponse.volumeTraded > 0) {
                            rcmd.body.orderResponse.tradePrice = cummulativeQuoteQty / rcmd.body.orderResponse.volumeTraded * info.reduceNumber;
                        }
                    }

                    const std::string& status = v.at("status").as_string();
                    rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(status);
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd);
                }

            END_FORMAT_RESPONSE(request)   
        }
        else {
            LOG_INFO("query_order smc not found: {}", tcmd.body.queryOrder.instId);
        }
    }
    catch (std::exception& e) {
        LOG_ERROR("query_order exception: {}", e.what());
    }
}
