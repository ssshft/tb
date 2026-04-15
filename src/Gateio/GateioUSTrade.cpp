#include "gateio/GateioUSTrade.h"

GateioUSTradeUnit::GateioUSTradeUnit(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {
    newOrderUrl = "/api/v4/futures/usdt/orders";
    cancelOrderUrl = "/api/v4/futures/usdt/orders";
    queryOrderUrl = "/api/v4/futures/usdt/orders";
    balanceUrl = "/api/v4/futures/usdt/accounts";
    positionUrl = "/api/v4/futures/usdt/positions";
}

GateioUSTradeUnit::~GateioUSTradeUnit() {

}

void GateioUSTradeUnit::subWebsocekt() {
    web::http::uri_builder builder(acc.wsUrl);
    START_SUB_WEBSOCKET(builder);

    if (isConnected) {
    #ifdef USE_GATEIO_UNIFIED  // 统一账户不需要订阅现货的balance推送
        // websocket_outgoing_message unifiedBalanceOutMsg = sub_balance_channel();   
        // pWsClient->send(unifiedBalanceOutMsg).wait(); 
    #else
        web::websockets::client::websocket_outgoing_message balanceOutMsg = sub_balance_channel();
        pWsClient->send(balanceOutMsg).wait();
    #endif

        web::websockets::client::websocket_outgoing_message orderOutMsg = sub_orders_channel();
        pWsClient->send(orderOutMsg).wait();

        web::websockets::client::websocket_outgoing_message positionOutMsg = sub_positions_channel();
        pWsClient->send(positionOutMsg).wait();
      
        // websocket_outgoing_message tradesOutMsg  = sub_trades_channel();
        // pWsClient->send(tradesOutMsg).wait();

        LOG_INFO("connected with gateio spot api: {}", builder.to_string());

    }
    else {
        LOG_ERROR("{} ws: {} connect failed, cannot sub.", acc.accountId, acc.wsUrl);
    }
}


web::websockets::client::websocket_outgoing_message GateioUSTradeUnit::sub_orders_channel() {
    std::string channel = "futures.orders";
    web::websockets::client::websocket_outgoing_message outMsg;
    web::json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = web::json::value::string(channel);
    subValue["event"] = web::json::value::string("subscribe");
    subValue["payload"][0] = web::json::value::string(acc.userId);
    subValue["payload"][1] = web::json::value::string("!all");

    std::string time = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign = crypto::getGateioSignatureWs(channel, "subscribe", time, acc.secretKey);
    web::json::value signValue;
    signValue["method"] = web::json::value::string("api_key");
    signValue["KEY"] = web::json::value::string(acc.apiKey);
    signValue["SIGN"] = web::json::value::string(sign);
    subValue["auth"] = signValue;
    outMsg.set_utf8_message(subValue.serialize().c_str());
    return outMsg;
}

web::websockets::client::websocket_outgoing_message GateioUSTradeUnit::sub_positions_channel() {
    std::string channel = "futures.positions";
    web::websockets::client::websocket_outgoing_message outMsg;
    web::json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = web::json::value::string(channel);
    subValue["event"] = web::json::value::string("subscribe");
    subValue["payload"][0] = web::json::value::string(acc.userId);
    subValue["payload"][1] = web::json::value::string("!all");

    std::string time = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign = crypto::getGateioSignatureWs(channel, "subscribe", time, acc.secretKey);
    web::json::value signValue;
    signValue["method"] = web::json::value::string("api_key");
    signValue["KEY"] = web::json::value::string(acc.apiKey);
    signValue["SIGN"] = web::json::value::string(sign);
    subValue["auth"] = signValue;
    outMsg.set_utf8_message(subValue.serialize().c_str());
    return outMsg;
}


web::websockets::client::websocket_outgoing_message GateioUSTradeUnit::sub_balance_channel() {
    std::string channel = "futures.balances";
    web::websockets::client::websocket_outgoing_message outMsg;
    web::json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = web::json::value::string(channel);
    subValue["event"] = web::json::value::string("subscribe");
    subValue["payload"][0] = web::json::value::string(acc.userId);
    std::string time = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign = crypto::getGateioSignatureWs(channel, "subscribe", time, acc.secretKey);
    web::json::value signValue;
    signValue["method"] = web::json::value::string("api_key");
    signValue["KEY"] = web::json::value::string(acc.apiKey);
    signValue["SIGN"] = web::json::value::string(sign);
    subValue["auth"] = signValue;
    outMsg.set_utf8_message(subValue.serialize().c_str());
    return outMsg;
}

web::websockets::client::websocket_outgoing_message GateioUSTradeUnit::sub_trades_channel() {
    std::string channel = "futures.usertrades";
    web::websockets::client::websocket_outgoing_message outMsg;
    web::json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = web::json::value::string(channel);
    subValue["event"] = web::json::value::string("subscribe");
    subValue["payload"][0] = web::json::value::string(acc.userId);
    subValue["payload"][1] = web::json::value::string("!all");
    std::string time = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign = crypto::getGateioSignatureWs(channel, "subscribe", time, acc.secretKey);
    web::json::value signValue;
    signValue["method"] = web::json::value::string("api_key");
    signValue["KEY"] = web::json::value::string(acc.apiKey);
    signValue["SIGN"] = web::json::value::string(sign);
    subValue["auth"] = signValue;
    outMsg.set_utf8_message(subValue.serialize().c_str());
    return outMsg;
}


void GateioUSTradeUnit::onWebsocketMsg(const web::websockets::client::websocket_incoming_message& msg) {
    try {
        // text_message, binary_message, close, ping, pong
        latestPingPongTime.store(crypto::getCurrentTimeSeconds());
        auto ty = msg.message_type();
        if (ty == web::websockets::client::websocket_message_type::text_message) {
            const std::string s = msg.extract_string().get();

            rapidjson::Document d;
            rapidjson::Value& rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(s.c_str());
            if (d.HasParseError() || !rawData.IsObject()) {
                return;
            }

            if (!rawData.HasMember("channel")) {
                return;
            }

            const std::string& channel = rawData["channel"].GetString();
            if (crypto::str_cmp(channel.c_str(), "futures.pong")) {
                return;
            }

            if (!rawData.HasMember("event") || !crypto::str_cmp(rawData["event"].GetString(), "update")) {
                return;
            }


            if (crypto::str_cmp(channel.c_str(), "futures.orders")) {
                const rapidjson::Value& data = rawData["result"];
                for (rapidjson::SizeType i = 0; i < data.Size(); i++) {
                    if(!data[i].HasMember("contract")){
                        continue;
                    }
                    std::string originInstId = data[i]["contract"].GetString();
                    md::InstrumentInfo info;
                    if (smc->get_instrument_info(GATEIO, USDT_SWAP, originInstId.c_str(), info)) {
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                        rcmd.body.orderResponse.exchangeTypeEnum = GATEIO;
                        rcmd.body.orderResponse.instTypeEnum = USDT_SWAP;
                        
                        strncpy(rcmd.body.orderResponse.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                        strncpy(rcmd.body.orderResponse.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);
                        strncpy(rcmd.body.orderResponse.instId, info.instId, INSTID_SIZE);

                        if(data[i].HasMember("id")){
                            strncpy(rcmd.body.orderResponse.orderId, data[i]["id"].GetString(), ORDER_SIZE);
                        }
                        if(data[i].HasMember("text")) {
                            std::string text = data[i]["text"].GetString();
                            strncpy(rcmd.body.orderResponse.orderSysId, text.c_str(), ORDER_SIZE);

                            if (text == "auto_deleveraging") {
                                rcmd.body.orderResponse.errorId = ADLError;
                            }
                            else if (text == "liquidation") {
                                rcmd.body.orderResponse.errorId = LiquidationError;
                            }
                        }
                        if(data[i].HasMember("is_close")){
                            rcmd.body.orderResponse.offsetFlag = data[i]["is_close"].GetBool() ? OF_CLOSE : OF_OPEN;
                        }
                        if(data[i].HasMember("size")){
                            double size  = stod(data[i]["size"].GetString());
                            rcmd.body.orderResponse.direction = size > 0 ? DT_LONG : DT_SHORT ;
                            rcmd.body.orderResponse.volumeTotal  = size > 0 ? size : -size ;
                        }
                        if(data[i].HasMember("price")){
                            rcmd.body.orderResponse.limitPrice = stod(data[i]["price"].GetString());
                        }

                        std::string tif = data[i]["tif"].GetString();
                        if (tif[0] == 'g') {
                            rcmd.body.orderResponse.orderType = OT_LIMIT;
                        }
                        else if (tif[0] == 'i') {
                            rcmd.body.orderResponse.orderType = OT_IOC;
                        }
                        else if (tif[0] == 'p') {
                            rcmd.body.orderResponse.orderType = OT_POST_ONLY;
                        }


                        if(data[i].HasMember("left")){
                            double left = stod(data[i]["left"].GetString());
                            left = left > 0 ? left : -left;
                            rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
                        }
                        //成交均价
                        if(data[i].HasMember("fill_price")){
                            rcmd.body.orderResponse.tradePrice = stod(data[i]["fill_price"].GetString());
                        }

                        const std::string& status = rawData["status"].GetString();
                        if (crypto::str_cmp(status.c_str(), "open")) {
                            if (rcmd.body.orderResponse.volumeTotal > rcmd.body.orderResponse.volumeTraded && rcmd.body.orderResponse.volumeTraded > ZERO_NUM) {
                                rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
                            }
                            else {
                                rcmd.body.orderResponse.orderStatus = OS_NEW;
                            }
                        }
                        else {
                            std::string finish_as = rawData["finish_as"].GetString();
                            if (crypto::str_cmp(finish_as.c_str(), "filled")) {
                                rcmd.body.orderResponse.orderStatus = OS_FILLED;
                            }
                            else if(crypto::str_cmp(finish_as.c_str(), "cancelled")
                            || crypto::str_cmp(finish_as.c_str(), "liquidated")//- 强制平仓撤销
                            || crypto::str_cmp(finish_as.c_str(), "ioc")//未立即完全成交，因为tif设置为ioc
                            || crypto::str_cmp(finish_as.c_str(), "auto_deleveraged")//自动减仓撤销
                            || crypto::str_cmp(finish_as.c_str(), "reduce_only")//: 增持仓位撤销，因为设置reduce_only或平仓
                            ) {
                                rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                            }
                            else {
                                rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                            }
                        }

                        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                        PUSH_RCMD(rcmd)
                    }
                    else{
                        LOG_ERROR("not found GATEIO.SWAP.%s smc info", originInstId.c_str());
                    }
                }
            }
            else if (crypto::str_cmp(channel.c_str(),"futures.balances")) {
                const rapidjson::Value& data = rawData["result"];
                for (rapidjson::SizeType i = 0; i < data.Size(); i++) {
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = GATEIO;
                    rcmd.body.balance.instTypeEnum = SPOT;
                    strncpy(rcmd.body.balance.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                    strncpy(rcmd.body.balance.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);

                    if (data[i].HasMember("text")) {
                        const std::string &text = data[i]["text"].GetString();
                        if (data[i].HasMember("currency")) {
                            strncpy(rcmd.body.balance.currency, crypto::to_upper(data[i]["currency"].GetString()).c_str(), INSTID_SIZE);
                        }
                        else if (crypto::has_str(text.c_str(), "USDT")) {
                            strncpy(rcmd.body.balance.currency, "USDT", INSTID_SIZE);
                        }
                        else {
                            continue;
                        }
                    }

                    if (data[i].HasMember("balance") && data[i]["balance"].IsString()) {
                        rcmd.body.balance.total = std::stod(data[i]["balance"].GetString());
                    }

                    rcmd.body.balance.updateTime = crypto::getCurrentTime();
                    rcmd.body.balance.isLast = i == data.Size() - 1;
                    rcmd.body.balance.apiSourceEnum = AS_REST;
                    PUSH_RCMD(rcmd);       
                }
            }
            else if (crypto::str_cmp(channel.c_str(),"futures.positions")) {
                const rapidjson::Value &data = rawData["result"];
                for(rapidjson::SizeType i = 0; i < data.Size(); i++){
                    if(!data[i].HasMember("contract")){
                        continue;
                    }

                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                    rcmd.body.position.exchangeTypeEnum = GATEIO;
                    strncpy(rcmd.body.position.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                    strncpy(rcmd.body.position.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);

                    const string &originInstId = data[i]["contract"].GetString();
                    double size = std::stod(data[i]["size"].GetString());
                    rcmd.body.position.direction = size >= 0 ? DT_LONG : DT_SHORT;
                    md::InstrumentInfo info;
                    if (smc->get_instrument_info(GATEIO, USDT_SWAP, originInstId.c_str(), info)) {
                        rcmd.body.position.instTypeEnum = USDT_SWAP;
                        strncpy(rcmd.body.position.instId, info.instId, INSTID_SIZE);
                    }
                    else {
                        continue;
                    }

                    size = size >= 0 ? size : -size;
                    rcmd.body.position.volume = size * info.magnifyNumber;
                    rcmd.body.position.maintMargin = std::stod(data[i]["margin"].GetString());
                    rcmd.body.position.avgPrice = std::stod(data[i]["entry_price"].GetString()) * info.reduceNumber;
                    rcmd.body.position.unrealizedPnl = std::stod(data[i]["unrealised_pnl"].GetString());
                    rcmd.body.position.markPrice = std::stod(data[i]["mark_price"].GetString()) * info.reduceNumber;
                    rcmd.body.position.liquidPrice = std::stod(data[i]["liq_price"].GetString()) * info.reduceNumber;
                    
                    double adl = std::stod(data[i]["adl_ranking"].GetString());
                    if(adl >= 5){
                        adl = 1;
                    }
                    else if(adl == 3){
                        adl = 3;
                    }
                    else if(adl == 2){
                        adl = 4;
                    }
                    else if(adl == 4 || adl == 1){
                        adl = 5 - adl;
                    }
                    else{
                        adl = 1;
                    }
                    rcmd.body.position.adlQuantile = adl;

                    rcmd.body.position.updateTime = crypto::getCurrentTime();
                    rcmd.body.position.isLast = i == data.Size() - 1;
                    rcmd.body.position.apiSourceEnum = AS_REST;
                    PUSH_RCMD(rcmd);
                }
            }
        }
        else if (msg.message_type() == web::websockets::client::websocket_message_type::close) {
            isConnected = false;
        }
    }
    catch (const std::exception& e) {
        LOG_ERROR("{}", e.what());
        isConnected = false;
    } 
}

void GateioUSTradeUnit::ping() {
    try {
        web::websockets::client::websocket_outgoing_message outMsg;
        web::json::value swapPingSubValue ;
        swapPingSubValue ["time"] = crypto::getCurrentTimeSeconds();
        swapPingSubValue ["channel"] = web::json::value::string("futures.ping");
        outMsg.set_utf8_message(swapPingSubValue .serialize().c_str());
        pWsClient->send(outMsg).wait();
    }
    catch(const std::exception& e) {
        LOG_ERROR("%s", e.what());
        isConnected = false;
    }
}

void GateioUSTradeUnit::pong() {

}

void GateioUSTradeUnit::query_account(const pubsub::TCommand& tcmd) {

}

void GateioUSTradeUnit::query_balance(const pubsub::TCommand& tcmd) { // 也需要 magnifyNumber/reduceNumber
    try {
        web::http::http_request request(web::http::methods::GET);
        FORMAT_REQUEST(request)
        std::string time = std::to_string(crypto::getCurrentTimeSeconds() );
        std::string sign = crypto::getGateioSignatureRest("GET", balanceUrl.to_string(), time, "", "", acc.secretKey);
        request.headers().add("KEY", acc.apiKey);
        request.headers().add("Timestamp", time);
        request.headers().add("SIGN", sign);

        web::http::uri_builder builder(balanceUrl);
        request.set_request_uri(builder.to_string());

        auto& restClient = *pRestClient;
        START_FORMAT_RESPONSE(request)
            const web::json::value& v = previousTask.get();
            auto& array = v.as_array();
            std::vector<pubsub::RCommand> needPushRcmd;
            for (auto& it : array) {
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                rcmd.body.balance.exchangeTypeEnum = GATEIO;
                rcmd.body.balance.instTypeEnum = SPOT;
                strncpy(rcmd.body.balance.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                strncpy(rcmd.body.balance.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);

                strncpy(rcmd.body.balance.currency, crypto::to_upper(it.at("currency").as_string().c_str()).c_str(), INSTID_SIZE);
                rcmd.body.balance.available = std::stod(it.at("available").as_string().c_str());
                rcmd.body.balance.frozen = std::stod(it.at("order_margin").as_string().c_str()) + stod(it.at("position_margin").as_string().c_str());
                rcmd.body.balance.total = std::stod(it.at("total").as_string().c_str());
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
                rcmd.body.balance.exchangeTypeEnum = GATEIO;
                rcmd.body.balance.instTypeEnum = SPOT;
                strncpy(rcmd.body.balance.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                strncpy(rcmd.body.balance.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);

                strncpy(rcmd.body.balance.currency, "USDT", INSTID_SIZE);
                rcmd.body.balance.updateTime = crypto::getCurrentTime();
                rcmd.body.balance.apiSourceEnum = AS_REST;
                rcmd.body.balance.isLast = true;
                PUSH_RCMD(rcmd);
            }
      
        END_FORMAT_RESPONSE(request)
    }
    catch (std::exception& e) {
        LOG_ERROR("{}", e.what());
    }
}

void GateioUSTradeUnit::query_position(const pubsub::TCommand& tcmd) {
    try {
        web::http::http_request request(web::http::methods::GET);
        FORMAT_REQUEST(request)
        std::string time = std::to_string(crypto::getCurrentTimeSeconds() );
        std::string sign = crypto::getGateioSignatureRest("GET", positionUrl.to_string(), time, "", "", acc.secretKey);
        request.headers().add("KEY", acc.apiKey);
        request.headers().add("Timestamp", time);
        request.headers().add("SIGN", sign);

        web::http::uri_builder builder(positionUrl);
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
                    rcmd.body.position.exchangeTypeEnum = GATEIO;
                    strncpy(rcmd.body.position.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                    strncpy(rcmd.body.position.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);

                    const std::string& originInstId = it.at("contract").as_string();
                    double size = std::stod(it.at("size").as_string().c_str());
      
                    rcmd.body.position.direction = size >= 0 ? DT_LONG : DT_SHORT;
                    md::InstrumentInfo info;
                    if (smc->get_instrument_info(GATEIO, USDT_SWAP, originInstId.c_str(), info)) {
                        rcmd.body.position.instTypeEnum = USDT_SWAP;
                        strncpy(rcmd.body.position.instId, info.instId, INSTID_SIZE);
                    }
                    else {
                        continue;
                    }

                    size = size >= 0 ? size : -size;
                    rcmd.body.position.volume = size * info.magnifyNumber;
                    rcmd.body.position.maintMargin = std::stod(it.at("margin").as_string().c_str());
                    rcmd.body.position.avgPrice = std::stod(it.at("entry_price").as_string().c_str()) * info.reduceNumber;
                    rcmd.body.position.unrealizedPnl = std::stod(it.at("unrealised_pnl").as_string().c_str());
                    rcmd.body.position.markPrice = std::stod(it.at("mark_price").as_string().c_str()) * info.reduceNumber;
                    rcmd.body.position.liquidPrice = std::stod(it.at("liq_price").as_string().c_str()) * info.reduceNumber;
                    
                    double adl = std::stod(it.at("adl_ranking").as_string().c_str());
                    if(adl >= 5){
                        adl = 1;
                    }
                    else if(adl == 3){
                        adl = 3;
                    }
                    else if(adl == 2){
                        adl = 4;
                    }
                    else if(adl == 4 || adl == 1){
                        adl = 5 - adl;
                    }
                    else{
                        adl = 1;
                    }
                    rcmd.body.position.adlQuantile = adl;

                    rcmd.body.position.updateTime = crypto::getCurrentTime();
                    rcmd.body.position.apiSourceEnum = AS_REST;
                    needPushRcmd.emplace_back(rcmd);
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


void GateioUSTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
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
            FORMAT_REQUEST(request)
            web::http::uri_builder builder(newOrderUrl);

            double price = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice * info.magnifyNumber, info.tickSize);
            double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber, info.lotSize);
    
            web::json::value value;
            value["text"] = json::value::string(tcmd.body.newOrder.orderSysId);
            value["contract"] = json::value::string(info.originInstId);
            value["price"] = json::value::string(std::to_string(price));

            switch (tcmd.body.newOrder.orderType) {
                case OT_LIMIT: {
                    value["tif"] = web::json::value::string("gtc");
                    break;  
                }
                case OT_MARKET: {
                    value["tif"] = web::json::value::string("ioc");
                    value["price"] = web::json::value::string("0");
                    break;
                }
                case OT_POST_ONLY: {
                    value["tif"] = web::json::value::string("poc");
                    break;
                }
                case OT_FOK: {
                    value["tif"] = web::json::value::string("fok");
                    break;
                }
                case OT_IOC: {
                    value["tif"] = web::json::value::string("ioc");
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

            if (tcmd.body.newOrder.offsetFlag == OF_OPEN) {
                if (tcmd.body.newOrder.direction == DT_LONG) {
                    value["size"] = web::json::value::string(std::to_string(volume));
                }
                else if (tcmd.body.newOrder.direction == DT_SHORT) {
                    value["size"] = web::json::value::string(std::to_string(-volume));
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
                    value["size"] = web::json::value::string(std::to_string(-volume));
                }
                else if (tcmd.body.newOrder.direction == DT_SHORT) {
                    value["size"] = web::json::value::string(std::to_string(volume));
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
            value["reduce_only"] = tcmd.body.newOrder.reduceOnly ? web::json::value::string("true") : web::json::value::string("false");

            std::string time = std::to_string(crypto::getCurrentTimeSeconds());
            std::string sign = crypto::getGateioSignatureRest("POST", newOrderUrl.to_string(), time, "", value.serialize(), acc.secretKey);
            request.headers().add("KEY", acc.apiKey);
            request.headers().add("Timestamp", time);
            request.headers().add("SIGN", sign);
            request.set_body(value);
            request.set_request_uri(builder.to_string());

            auto& restClient = *pRestClient;
            START_FORMAT_RESPONSE(request)
                if (tcmd.body.newOrder.clientOrderId == TESTCLIENTORDERID) {
                    return;
                }
  
                std::string s = previousTask.get().serialize();
                LOG_DEBUG("add_new_order reponse: {}", s);
                rapidjson::Document d;
                rapidjson::Value& rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(s.c_str());

                if (!d.IsObject() || d.HasParseError()) {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED; 
                    rcmd.body.orderResponse.errorId = UnknownError;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;      
                }

                if (rawData.HasMember("id")) {
                    rcmd.body.orderResponse.orderStatus = crypto::get_gateio_orderstatus(rcmd.body.orderResponse.instTypeEnum, rawData);
                    strcpy(rcmd.body.orderResponse.orderId, rawData["id"].GetString());
                    rcmd.body.orderResponse.errorId = NoError;
                    rcmd.body.orderResponse.tradePrice = stod(rawData["fill_price"].GetString());
                    
                    
                    double left = std::stod(rawData["left"].GetString());
                    left = left > 0 ? left : -left;
                    rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                }
                else if (rawData.HasMember("label")) {
                    rcmd.body.orderResponse.errorId = crypto::get_gateio_errorid(s.c_str());
                    strncpy(rcmd.body.orderResponse.originMsg, rawData["label"].GetString(), sizeof(rcmd.body.orderResponse.originMsg));

                    rcmd.body.orderResponse.orderStatus = OS_REJECTED; 
                    rcmd.body.orderResponse.errorId = UnknownError;
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

void GateioUSTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    try {
        md::InstrumentInfo info;
        if (smc->get_instrument_info(tcmd.body.cancelOrder.exchangeTypeEnum, tcmd.body.cancelOrder.instTypeEnum, tcmd.body.cancelOrder.instId, info)) {
            web::http::http_request request(web::http::methods::DEL);
            FORMAT_REQUEST(request)
        
            std::string queryStr = "";

            std::string cancelOrderUrlStr = cancelOrderUrl.to_string();
            if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
                cancelOrderUrlStr.append("/").append(tcmd.body.cancelOrder.orderId);
            }
            else if (!crypto::str_cmp(tcmd.body.cancelOrder.orderSysId, "")) {
                cancelOrderUrlStr.append("/").append(tcmd.body.cancelOrder.orderSysId);
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

            std::string time = std::to_string(crypto::getCurrentTimeSeconds());
            std::string sign = crypto::getGateioSignatureRest("DELETE", cancelOrderUrlStr, time, queryStr, "", acc.secretKey);
            request.headers().add("KEY", acc.apiKey);
            request.headers().add("Timestamp", time);
            request.headers().add("SIGN", sign);
     
            cancelOrderUrlStr.append("?").append(queryStr);
            web::uri clOrdUrl = cancelOrderUrlStr;
            uri_builder builder(clOrdUrl);
            request.set_request_uri(builder.to_string());

            LOG_INFO("cancel_order builder: {}", builder.to_string());

            auto& restClient = *pRestClient;
            START_FORMAT_RESPONSE(request)
                const web::json::value& v = previousTask.get();
                LOG_INFO("cancel_order response: {}", v.serialize().c_str());
                if (v.has_field("status")) {
                    rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                    strncpy(rcmd.body.orderResponse.orderId, v.at("id").as_string().c_str(), ORDER_SIZE);
                    rcmd.body.orderResponse.tradePrice = std::stod(v.at("fill_price").as_string().c_str());
                    
                    double size = std::stod(v.at("size").as_string().c_str());
                    size = size > 0 ? size : -size;
                    double left = std::stod(v.at("left").as_string().c_str());
                    left = left > 0 ? left : -left;
                    rcmd.body.orderResponse.volumeTraded = size - left;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                }
                else if (v.has_field("label")) {
                    rcmd.body.orderResponse.orderStatus = OS_FAILED;
                    rcmd.body.orderResponse.errorId = crypto::get_gateio_errorid(v.serialize().c_str());
                    if (rcmd.body.orderResponse.errorId == OrderNotFoundError) {
                        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    }
                    strncpy(rcmd.body.orderResponse.originMsg, v.at("label").as_string().c_str(), ORIGINMSG_SIZE);
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
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

void GateioUSTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);
    try {
        md::InstrumentInfo info;
        if (smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum, tcmd.body.queryOrder.instTypeEnum, tcmd.body.queryOrder.instId, info)) {
            web::http::http_request request(web::http::methods::GET);
            FORMAT_REQUEST(request)

            std::string queryStr = "";

            std::string queryOrderUrlStr = queryOrderUrl.to_string();          
            if (crypto::str_cmp(tcmd.body.queryOrder.orderId, "")) {
                queryOrderUrlStr.append("/").append(tcmd.body.queryOrder.orderId);
            }
            else if (crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")) {
                queryOrderUrlStr.append("/").append(tcmd.body.queryOrder.orderSysId);
            }
            else {
                LOG_ERROR("query order need orderId or orderSysId {}", tcmd.body.queryOrder.instId);
                return;
            }

            std::string time = std::to_string(crypto::getCurrentTimeSeconds());
            std::string sign = crypto::getGateioSignatureRest("GET", queryOrderUrlStr, time, queryStr, "", acc.secretKey);
            request.headers().add("KEY", acc.apiKey);
            request.headers().add("Timestamp", time);
            request.headers().add("SIGN", sign);
     
            queryOrderUrlStr.append("?").append(queryStr);
            web::uri quOrdUrl = queryOrderUrlStr;
            uri_builder builder(quOrdUrl);
            request.set_request_uri(builder.to_string());

            LOG_INFO("query_order builder: {}", builder.to_string());

            auto& restClient = *pRestClient;
            START_FORMAT_RESPONSE(request)
                const web::json::value& v = previousTask.get();
                LOG_INFO("query_order response: {}", v.serialize().c_str());
                
                rapidjson::Document d;
                rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(v.serialize().c_str());
                if (rawData.HasMember("status")) {
                    strncpy(rcmd.body.orderResponse.orderId, rawData["id"].GetString(), ORDER_SIZE);
                    strncpy(rcmd.body.orderResponse.orderSysId, rawData["text"].GetString(), ORDER_SIZE);
                    rcmd.body.orderResponse.volumeTotal = fabs(std::stod(rawData["size"].GetString()));
                    rcmd.body.orderResponse.limitPrice = std::stod(rawData["price"].GetString());
                    double left = fabs(std::stod(rawData["left"].GetString()));
                    rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
                    rcmd.body.orderResponse.tradePrice = std::stod(rawData["fill_price"].GetString());
                    
                    std::string status = rawData["status"].GetString();
                    if (crypto::str_cmp(status.c_str(), "open")) {
                        if (rcmd.body.orderResponse.volumeTotal > rcmd.body.orderResponse.volumeTraded && rcmd.body.orderResponse.volumeTraded > ZERO_NUM) {
                            rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;  
                        }
                        else {
                            rcmd.body.orderResponse.orderStatus = OS_NEW;
                        }
                    }
                    else if (crypto::str_cmp(status.c_str(), "close")) {
                        rcmd.body.orderResponse.orderStatus = OS_FILLED;
                    }
                    else if (crypto::str_cmp(status.c_str(), "cancelled")) {
                        rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                    }
                    else {
                        std::string finish_as = rawData["finish_as"].GetString();
                        if (crypto::str_cmp(finish_as.c_str(), "filled")) {
                            rcmd.body.orderResponse.orderStatus = OS_FILLED;
                        }
                        else if (crypto::str_cmp(finish_as.c_str(), "cancelled")
                        || crypto::str_cmp(finish_as.c_str(), "liquidated")
                        || crypto::str_cmp(finish_as.c_str(), "ioc")
                        || crypto::str_cmp(finish_as.c_str(), "auto_deleveraged")
                        || crypto::str_cmp(finish_as.c_str(), "reduce_only")
                        || crypto::str_cmp(finish_as.c_str(), "reduce_out")) {
                            rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                        }
                        else {
                            rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                        }
                    }
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                }
                else{
                    LOG_ERROR("%s",v.serialize().c_str());
                    rcmd.body.orderResponse.errorId = crypto::get_gateio_errorid(v.serialize().c_str());
                    if (rcmd.body.orderResponse.errorId == OrderNotFoundError) {
                        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    }
                    else {
                        rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                    }
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
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
