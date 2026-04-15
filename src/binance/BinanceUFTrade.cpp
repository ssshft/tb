#include "binance/BinanceUFTrade.h"

BinanceUFTradeUnit::BinanceUFTradeUnit(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {
    newOrderUrl = "/fapi/v1/order";
    cancelOrderUrl = "/fapi/v1/order";
    queryOrderUrl = "/fapi/v1/order";
    balanceUrl = "/fapi/v3/account";
    positionUrl = "/fapi/v3/positionRisk";
    wsSubUrl = "/private/ws/";
    listenKeyUrl = "/fapi/v1/listenKey";

    listenKey = "";
}

BinanceUFTradeUnit::~BinanceUFTradeUnit() {

}

void BinanceUFTradeUnit::monitorWs() {
    constexpr int pingPongInterval = 10; //s
    constexpr int listenInterval = 30 * 60; //s
    while (1) {
        try {
            LOG_INFO("TB {} start to connect ws: {}", acc.accountId, acc.wsUrl);
            long lastListenKeyTime = crypto::getCurrentTimeSeconds();
            long lastPingPongTime = crypto::getCurrentTimeSeconds();
            genListenKey();
            if (crypto::str_cmp(listenKey, "")) {
                LOG_ERROR("TB {} failed generate listenkey", acc.accountId);
                isConnected = false;
                std::this_thread::sleep_for(std::chrono::seconds(5));
                continue;
            }
            subWebsocekt();
            std::this_thread::sleep_for(std::chrono::seconds(5));
            while (isConnected) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                long now = crypto::getCurrentTimeSeconds();
                long latestTime = latestPingPongTime.load();

                long diff = now - latestTime;
                if (diff > kTradeTimeOutTime) {
                    LOG_ERROR("trade: {} lastPingPongTime: {} too old, time diff: {} seconds, will reconnect now.", acc.accountId, latestTime, diff);
                    break;
                }

                if (!isConnected) {
                    LOG_WARN("trade: {}, wsUrl: {} address disconnected, connecting now", acc.accountId, acc.wsUrl);
                    break;
                }
                else {
                    if (now - lastPingPongTime > pingPongInterval) {
                        switch (acc.exchangeTypeEnum) {
                            case BINANCE: {
                                break;
                            }
                            default: {
                                LOG_INFO("{}.{}.{} ws is connected, will send ping!", ExchangeTypeEnum2StrMap[acc.exchangeTypeEnum], InstTypeEnum2StrMap[acc.instTypeEnum], acc.accountId);
                                ping();
                                break;
                            }  
                        }
                        lastPingPongTime = now;
                    }

                    if (now - lastListenKeyTime > listenInterval) {
                        LOG_INFO("TB {} usdt swap will renew listen key.", acc.accountId);
                        keepListenKey();
                        lastListenKeyTime = now;
                    }
  
                }
            }
        }
        catch (const std::exception& e) {
            isConnected = false;
            LOG_ERROR("ws connect exception: {}", e.what());
        }

        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }   
}

void BinanceUFTradeUnit::genListenKey() {
    try {
        web::http::uri_builder builder(listenKeyUrl);
        web::http::http_request request(web::http::methods::POST);
        FROMAT_BINANCE_REQUEST(request)
        request.set_request_uri(builder.to_string());

        auto& restClient = *pRestClient;
        web::http::http_response response = restClient.request(request).get();
        if (response.status_code() == web::http::status_codes::OK) {
            const web::json::value& v = response.extract_json().get();
            if (v.has_field("listenKey")){
                listenKey = v.at("listenKey").as_string();
                LOG_DEBUG("accountId:{}, listenkey:{}", acc.accountId, listenKey);
            }
            else {
                LOG_ERROR("TB {} failed to get listenkey, response: {}", acc.accountId, v.serialize());
                isConnected = false;
            }
        }
        else {
            LOG_ERROR("TB {} failed to get listenkey, response: {}", acc.accountId, response.extract_string().get());
            isConnected = false;
        }
    }
    catch (const exception& e) {
        LOG_ERROR("TB {} Error exception: {}", acc.accountId, e.what());
        isConnected = false;
        
    }
}

void BinanceUFTradeUnit::keepListenKey() {
    try {
        web::http::uri_builder builder(listenKeyUrl);
        web::http::http_request request(web::http::methods::POST);
        FROMAT_BINANCE_REQUEST(request)
        request.set_request_uri(builder.to_string());

        web::json::value obj;
        obj["listenKey"] = json::value::string(listenKey);
        request.set_body(obj.serialize());
        LOG_DEBUG("TB {} keepListenKey, body: {}", acc.accountId, obj.serialize());

        auto& restClient = *pRestClient;
        START_FORMAT_RESPONSE(request)
            const web::json::value& v = previousTask.get();
            auto errCode = 0;
            std::string errMsg = "";
            if (v.has_field("code")) {
                errCode = v.at("code").as_integer();
            }
            if (v.has_field("msg")) {
                errMsg = v.at("msg").as_string();
            }
            LOG_INFO("TB {} keepListenKey errCode: {}, errMsg: {}", acc.accountId, errCode, errMsg);
            
            if (errCode != 0) {
                isConnected = false;
                genListenKey();
            }
        END_FORMAT_RESPONSE(request)
    }
    catch (const exception& e) {
        LOG_ERROR("TB {} Error exception: {}", acc.accountId, e.what());
        isConnected = false;
    }
}

void BinanceUFTradeUnit::subWebsocekt() {
    web::http::uri_builder builder(acc.wsUrl);
    builder.append_path(wsSubUrl.to_string());
    builder.append_path(listenKey);
    START_SUB_WEBSOCKET(builder);    
}

void BinanceUFTradeUnit::onWebsocketMsg(const websocket_incoming_message& msg) {
    try {
        latestPingPongTime.store(crypto::getCurrentTimeSeconds());
        auto ty = msg.message_type();
        if (ty == websocket_message_type::text_message) [[likely]] {
            const std::string s = msg.extract_string().get();

            std::cout << "------------"  << s << std::endl;
            LOG_DEBUG("{} on_websocket_msg: {}", acc.accountId, s.c_str());

            rapidjson::Document d;
            rapidjson::Value& rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(s.c_str());
            if (d.HasParseError() || !rawData.IsObject() || !rawData.HasMember("e")) {
                return;
            }

            const std::string& e = rawData["e"].GetString();
            if (e[0] == 'A') {
                const rapidjson::Value& data = rawData["a"]["B"];
                for (rapidjson::SizeType i = 0; i < data.Size(); i++) {
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = BINANCE;
                    rcmd.body.balance.instTypeEnum = USDT_SWAP;
                    
                    strncpy(rcmd.body.balance.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                    strncpy(rcmd.body.balance.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);
                    strncpy(rcmd.body.balance.currency, crypto::to_upper(data[i]["a"].GetString()).c_str(), INSTID_SIZE);
                    rcmd.body.balance.available = std::stod(data[i]["cw"].GetString());
                    rcmd.body.balance.frozen = std::stod(data[i]["bc"].GetString());
                    rcmd.body.balance.total = std::stod(data[i]["wb"].GetString());
                    rcmd.body.balance.isLast = data.Size() - 1;
                    rcmd.body.balance.updateTime = crypto::getCurrentTime();
                    rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;

                    std::cout << "onWebsocketMsg: " << rcmd.getString() << std::endl;
                    PUSH_RCMD(rcmd)
                }

                const rapidjson::Value& pData = rawData["a"]["P"];
                for (rapidjson::SizeType i = 0; i < pData.Size(); i++) {
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                    rcmd.body.position.exchangeTypeEnum = BINANCE;
                    strncpy(rcmd.body.position.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                    strncpy(rcmd.body.position.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);

                    const std::string& originInstId = pData[i]["s"].GetString();
                    double positionAmt = std::stod(pData[i]["pa"].GetString());
                    const std::string& positionSide = pData[i]["ps"].GetString();
                    if (positionSide[0] == 'B') {
                        rcmd.body.position.direction = positionAmt >= 0 ? DT_LONG : DT_SHORT;
                        md::InstrumentInfo info;
                        if (smc->get_instrument_info(BINANCE, USDT_SWAP, originInstId.c_str(), info)) {
                            rcmd.body.position.instTypeEnum = USDT_SWAP;
                            strncpy(rcmd.body.position.instId, info.instId, INSTID_SIZE);
                        }
                        else if (smc->get_instrument_info(BINANCE, USDT_FUTURES, originInstId.c_str(), info)) {
                            rcmd.body.position.instTypeEnum = USDT_FUTURES;
                            strncpy(rcmd.body.position.instId, info.instId, INSTID_SIZE);
                        }
                        else {
                            continue;
                        }

                        positionAmt = positionAmt >= 0 ? positionAmt : -positionAmt;
                        rcmd.body.position.volume = positionAmt * info.magnifyNumber;
                        rcmd.body.position.maintMargin = std::stod(pData[i]["iw"].GetString());
                        rcmd.body.position.avgPrice = std::stod(pData[i]["ep"].GetString()) * info.reduceNumber;
                        rcmd.body.position.unrealizedPnl = std::stod(pData[i]["up"].GetString());
                        rcmd.body.position.isLast = pData.Size() - 1;
                        rcmd.body.position.updateTime = crypto::getCurrentTime();
                        rcmd.body.position.apiSourceEnum = AS_WEBSOCKET;
                    }
                }
            }
            else if (e[0] == 'O') {
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_NEW_ORDER;
                rcmd.body.orderResponse.exchangeTypeEnum = BINANCE;

                strncpy(rcmd.body.orderResponse.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                strncpy(rcmd.body.orderResponse.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);

                const std::string& cOrderId = rawData["o"]["c"].GetString();
                strncpy(rcmd.body.orderResponse.orderSysId, cOrderId.c_str(), ORDER_SIZE);

                const std::string& originInstId = rawData["o"]["s"].GetString();
                md::InstrumentInfo info;
                if (smc->get_instrument_info(BINANCE, USDT_SWAP, originInstId.c_str(), info)) {
                    rcmd.body.orderResponse.instTypeEnum = USDT_SWAP;
                    strncpy(rcmd.body.orderResponse.instId, info.instId, INSTID_SIZE);
                }
                else if (smc->get_instrument_info(BINANCE, USDT_FUTURES, originInstId.c_str(), info)) {
                    rcmd.body.orderResponse.instTypeEnum = USDT_FUTURES;
                    strncpy(rcmd.body.orderResponse.instId, info.instId, INSTID_SIZE);
                }
                else {
                    return;
                }

                rcmd.body.orderResponse.offsetFlag = OF_OPEN;
                const std::string& side = rawData["o"]["S"].GetString();
                rcmd.body.orderResponse.direction = side[0] == 'B' ? DT_LONG : DT_SHORT;

                const std::string& timeInForce = rawData["o"]["f"].GetString();
                const std::string& orderType = rawData["o"]["o"].GetString();
                rcmd.body.orderResponse.orderType = crypto::get_binance_ordertype(timeInForce.c_str(), orderType.c_str());

                rcmd.body.orderResponse.volumeTotal = std::stod(rawData["o"]["q"].GetString()) * info.magnifyNumber;
                rcmd.body.orderResponse.limitPrice = std::stod(rawData["o"]["p"].GetString()) * info.reduceNumber;
                rcmd.body.orderResponse.volumeTraded = std::stod(rawData["o"]["z"].GetString()) * info.magnifyNumber;
                rcmd.body.orderResponse.tradePrice = std::stod(rawData["o"]["ap"].GetString()) * info.reduceNumber;
                rcmd.body.orderResponse.tradeDiff = std::stod(rawData["o"]["l"].GetString()) * info.magnifyNumber;
                rcmd.body.orderResponse.fillPrice= std::stod(rawData["o"]["L"].GetString()) * info.reduceNumber;

                rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(rawData["o"]["X"].GetString());

                if (cOrderId[0] == 'a' && cOrderId[0] == 't') {
                    rcmd.body.orderResponse.errorId = LiquidationError;
                }
                else if (cOrderId[0] == 'a' && cOrderId[0] == 'l') {
                    rcmd.body.orderResponse.errorId = ADLError;
                }

                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
                PUSH_RCMD(rcmd)
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

void BinanceUFTradeUnit::ping() {

}

void BinanceUFTradeUnit::pong() {

}

void BinanceUFTradeUnit::pong(const std::string& payload) {
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

void BinanceUFTradeUnit::query_account(const pubsub::TCommand& tcmd) {

}

void BinanceUFTradeUnit::query_balance(const pubsub::TCommand& tcmd) { // 也需要 magnifyNumber/reduceNumber
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
            if (v.has_field("assets")) {
                auto& array = v.at("assets").as_array();
                std::vector<pubsub::RCommand> needPushRcmd;
                for (auto& it : array) {
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = BINANCE;
                    rcmd.body.balance.instTypeEnum = USDT_SWAP;
                    strncpy(rcmd.body.balance.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                    strncpy(rcmd.body.balance.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);
                    
                    strncpy(rcmd.body.balance.currency, crypto::to_upper(it.at("asset").as_string().c_str()).c_str(), INSTID_SIZE);
                    rcmd.body.balance.available = std::stod(it.at("availableBalance").as_string().c_str());
                    rcmd.body.balance.frozen = std::stod(it.at("marginBalance").as_string().c_str());
                    rcmd.body.balance.total = std::stod(it.at("walletBalance").as_string().c_str());
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
                    rcmd.body.balance.instTypeEnum = USDT_SWAP;
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

void BinanceUFTradeUnit::query_position(const pubsub::TCommand& tcmd) {
    try {
        web::http::http_request request(web::http::methods::GET);
        FROMAT_BINANCE_REQUEST(request)
        web::http::uri_builder builder(positionUrl);
        builder.append_query("recvWindow", 5000);
        builder.append_query("timestamp", crypto::getCurrentTimeMilli());
        std::string signature = crypto::getBinanceSignatureRest(acc.secretKey, builder.query());
        builder.append_query("signature", signature);
        request.set_request_uri(builder.to_string());

        auto& restClient = *pRestClient;
        START_FORMAT_RESPONSE(request)
            const web::json::value& v = previousTask.get();
            if (v.is_array()) {
                auto& array = v.as_array();
                std::vector<pubsub::RCommand> needPushRcmd;
                for (auto& it : array) {
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                    rcmd.body.position.exchangeTypeEnum = BINANCE;
                    strncpy(rcmd.body.position.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                    strncpy(rcmd.body.position.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);


                    const std::string& originInstId = it.at("symbol").as_string();
                    double positionAmt = std::stod(it.at("positionAmt").as_string().c_str());
                    const std::string& positionSide = it.at("positionSide").as_string();
                    if (positionSide[0] == 'B') {
                        rcmd.body.position.direction = positionAmt >= 0 ? DT_LONG : DT_SHORT;
                        md::InstrumentInfo info;
                        if (smc->get_instrument_info(BINANCE, USDT_SWAP, originInstId.c_str(), info)) {
                            rcmd.body.position.instTypeEnum = USDT_SWAP;
                            strncpy(rcmd.body.position.instId, info.instId, INSTID_SIZE);
                        }
                        else if (smc->get_instrument_info(BINANCE, USDT_FUTURES, originInstId.c_str(), info)) {
                            rcmd.body.position.instTypeEnum = USDT_FUTURES;
                            strncpy(rcmd.body.position.instId, info.instId, INSTID_SIZE);
                        }
                        else {
                            continue;
                        }

                        positionAmt = positionAmt >= 0 ? positionAmt : -positionAmt;
                        rcmd.body.position.volume = positionAmt * info.magnifyNumber;
                        rcmd.body.position.maintMargin = std::stod(it.at("maintMargin").as_string().c_str());
                        rcmd.body.position.avgPrice = std::stod(it.at("entryPrice").as_string().c_str()) * info.reduceNumber;
                        rcmd.body.position.unrealizedPnl = std::stod(it.at("unRealizedProfit").as_string().c_str());
                        rcmd.body.position.markPrice = std::stod(it.at("markPrice").as_string().c_str()) * info.reduceNumber;
                        rcmd.body.position.liquidPrice = std::stod(it.at("liquidationPrice").as_string().c_str()) * info.reduceNumber;
                        rcmd.body.position.adlQuantile = it.at("adl").as_integer() + 1;

                        rcmd.body.position.updateTime = crypto::getCurrentTime();
                        rcmd.body.position.apiSourceEnum = AS_REST;
                        needPushRcmd.emplace_back(rcmd);
                    }
                }

                if (needPushRcmd.size() > 0) {
                    size_t count = 0;
                    size_t size = needPushRcmd.size();
                    for (size_t i = 0; i < needPushRcmd.size(); ++i) {
                        count++;
                        needPushRcmd[i].body.position.isLast = count == size;
                        PUSH_RCMD(needPushRcmd[i]);
                    }
                }
                else {
                    LOG_INFO("{} no position pushed, will push usdt position=0 rcmd.", acc.accountId);
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                    rcmd.body.position.exchangeTypeEnum = BINANCE;
                    rcmd.body.position.instTypeEnum = USDT_SWAP;
                    strncpy(rcmd.body.position.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                    strncpy(rcmd.body.position.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);

                    strncpy(rcmd.body.position.instId, "BTC-USDT", INSTID_SIZE);
                    rcmd.body.position.updateTime = crypto::getCurrentTime();
                    rcmd.body.position.apiSourceEnum = AS_REST;
                    rcmd.body.position.isLast = true;
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

void BinanceUFTradeUnit::add_new_order(const pubsub::TCommand &tcmd){
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
                    builder.append_query("type", "LIMIT");
                    builder.append_query("timeInForce", "GTX");
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

            builder.append_query("reduceOnly", tcmd.body.newOrder.reduceOnly ? "true" : "false"); 

            std::string signature = crypto::getBinanceSignatureRest(acc.secretKey, builder.query());
            builder.append_query("signature", signature);
            request.set_request_uri(builder.to_string());

            LOG_INFO("add_new_order builder: {}", builder.to_string());

            auto& restClient = *pRestClient;
            START_FORMAT_RESPONSE(request)
                if (tcmd.body.newOrder.clientOrderId == TESTCLIENTORDERID) {
                    return;
                }
                const std::string& s = previousTask.get().serialize();
                LOG_INFO("add_new_order response: {}", s);
                
                rapidjson::Document d;
                rapidjson::Value& rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(s.c_str());
                if (!d.IsObject() || d.HasParseError()) {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED; 
                    rcmd.body.orderResponse.errorId = UnknownError;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;      
                }

                if (rawData.HasMember("orderId")) {
                    strncpy(rcmd.body.orderResponse.orderId, rawData["orderId"].GetString(), INSTID_SIZE);
                    rcmd.body.orderResponse.volumeTraded = std::stod(rawData["executedQty"].GetString()) * info.magnifyNumber;
                    rcmd.body.orderResponse.tradePrice = std::stod(rawData["avgPrice"].GetString()) * info.reduceNumber;
                    
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
                else if (rawData.HasMember("code")) {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED; 
                    rcmd.body.orderResponse.errorId = crypto::get_binance_errorid(std::stoi(rawData["code"].GetString()));
                    strncpy(rcmd.body.orderResponse.originMsg, s.c_str(), ORIGINMSG_SIZE);
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

void BinanceUFTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
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
          
            if (crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
                builder.append_query("orderId", tcmd.body.cancelOrder.orderId);
            }
            else if (crypto::str_cmp(tcmd.body.cancelOrder.orderSysId, "")) {
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
                const std::string& s = previousTask.get().serialize();
                LOG_INFO("cancel_order response: {}", s);

                rapidjson::Document d;
                rapidjson::Value& rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(s.c_str());
                if (!d.IsObject() || d.HasParseError()) {
                    rcmd.body.orderResponse.orderStatus = OS_UNKNOWN; 
                    rcmd.body.orderResponse.errorId = UnknownError;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;      
                }

                if (rawData.HasMember("code")) {
                    rcmd.body.orderResponse.orderStatus = OS_FAILED;
                    rcmd.body.orderResponse.errorId = crypto::get_binance_errorid(std::stoi(rawData["code"].GetString()));
                    if (rcmd.body.orderResponse.errorId == OrderNotFoundError) {
                        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    }
                    strncpy(rcmd.body.orderResponse.originMsg, s.c_str(), ORIGINMSG_SIZE);
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd);
                }
                else {
                    strncpy(rcmd.body.orderResponse.orderId, rawData["orderId"].GetString(), INSTID_SIZE);
                    rcmd.body.orderResponse.volumeTraded = std::stod(rawData["executedQty"].GetString()) * info.magnifyNumber;
                    rcmd.body.orderResponse.tradePrice = std::stod(rawData["avgPrice"].GetString()) * info.reduceNumber;

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
    catch (std::exception& e) {
        LOG_ERROR("cancel_order exception: {}", e.what());
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = NetworkError;
        strncpy(rcmd.body.orderResponse.originMsg, e.what(), ORIGINMSG_SIZE);
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
    }
}


void BinanceUFTradeUnit::query_order(const pubsub::TCommand& tcmd) {
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
                    }
                    else {
                        rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;      
                    }

                    rcmd.body.orderResponse.errorId = crypto::get_binance_errorid(v.at("code").as_integer());
                    strncpy(rcmd.body.orderResponse.originMsg, v.serialize().c_str(), ORIGINMSG_SIZE);
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd);
                }
                else {
                    if (v.has_field("orderId")) {
                        strncpy(rcmd.body.orderResponse.orderId, v.at("orderId").as_string().c_str(), ORDER_SIZE);
                    }
                    rcmd.body.orderResponse.volumeTotal = std::stod(v.at("origQty").as_string().c_str()) * info.magnifyNumber;
                    rcmd.body.orderResponse.limitPrice = std::stod(v.at("price").as_string().c_str()) * info.reduceNumber;
                    rcmd.body.orderResponse.volumeTraded = std::stod(v.at("executedQty").as_string().c_str()) * info.magnifyNumber;
                    rcmd.body.orderResponse.tradePrice = std::stod(v.at("avgPrice").as_string().c_str()) * info.reduceNumber;

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
