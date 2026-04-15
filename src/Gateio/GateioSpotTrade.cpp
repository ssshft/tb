#include "gateio/GateioSpotTrade.h"

GateioSpotTradeUnit::GateioSpotTradeUnit(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {
    newOrderUrl = "/api/v3/order";
    cancelOrderUrl = "/api/v3/order";
    queryOrderUrl = "/api/v3/order";
    balanceUrl = "/api/v3/account";
    unifiedUrl = "";
}

GateioSpotTradeUnit::~GateioSpotTradeUnit() {

}

void GateioSpotTradeUnit::subWebsocekt() {
    web::http::uri_builder builder(acc.wsUrl);
    START_SUB_WEBSOCKET(builder);

    if (isConnected) {
    #ifdef USE_GATEIO_UNIFIED  // 统一账户不需要订阅现货的balance推送
        // websocket_outgoing_message unifiedBalanceOutMsg = sub_unified_balance_channel();   
        // pWsClient->send(unifiedBalanceOutMsg).wait(); 
    #else
        web::websockets::client::websocket_outgoing_message balanceOutMsg = sub_balance_channel();
        pWsClient->send(balanceOutMsg).wait();
    #endif

        web::websockets::client::websocket_outgoing_message orderOutMsg = sub_orders_channel();
        pWsClient->send(orderOutMsg).wait();
      
        // websocket_outgoing_message tradesOutMsg  = sub_trades_channel();
        // pWsClient->send(tradesOutMsg).wait();

        LOG_INFO("connected with gateio spot api: {}", builder.to_string());

    }
    else {
        LOG_ERROR("{} ws: {} connect failed, cannot sub.", acc.accountId, acc.wsUrl);
    }
}

web::websockets::client::websocket_outgoing_message GateioSpotTradeUnit::sub_balance_channel() {
    std::string channel = "spot.balances";
    web::websockets::client::websocket_outgoing_message outMsg;
    web::json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = web::json::value::string(channel);
    subValue["event"] = web::json::value::string("subscribe");
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

web::websockets::client::websocket_outgoing_message GateioSpotTradeUnit::sub_orders_channel() {
    std::string channel = "spot.orders";
    web::websockets::client::websocket_outgoing_message outMsg;
    web::json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = web::json::value::string(channel);
    subValue["event"] = web::json::value::string("subscribe");
    subValue["payload"][0] = web::json::value::string("BTC_USDT");
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


/*
websocket_outgoing_message GateioSpotTradingClient::sub_cross_balance_channel() {
     string channel = "spot.cross_balances";
    websocket_outgoing_message outMsg;
    json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = json::value::string(channel);
    subValue["event"] = json::value::string("subscribe");
    string time = to_string(crypto::getCurrentTimeSeconds());
    string sign = get_signature_ws(channel.c_str(), "subscribe", time.c_str());
    json::value signValue;
    signValue["method"] = json::value::string("api_key");
    signValue["KEY"] = json::value::string(m_curcfg.apiKey);
    signValue["SIGN"] = json::value::string(sign);
    subValue["auth"] = signValue;
    outMsg.set_utf8_message(subValue.serialize().c_str());
    return outMsg;
}

websocket_outgoing_message GateioSpotTradingClient::sub_unified_balance_channel() {
     string channel = "unified.balances";
    websocket_outgoing_message outMsg;
    json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = json::value::string(channel);
    subValue["event"] = json::value::string("subscribe");
    string time = to_string(crypto::getCurrentTimeSeconds());
    string sign = get_signature_ws(channel.c_str(), "subscribe", time.c_str());
    json::value signValue;
    signValue["method"] = json::value::string("api_key");
    signValue["KEY"] = json::value::string(m_curcfg.apiKey);
    signValue["SIGN"] = json::value::string(sign);
    subValue["auth"] = signValue;
    outMsg.set_utf8_message(subValue.serialize().c_str());
    return outMsg;
}

websocket_outgoing_message GateioSpotTradingClient::sub_trades_channel() {
     string channel = "spot.usertrades";
    websocket_outgoing_message outMsg;
    json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = json::value::string(channel);
    subValue["event"] = json::value::string("subscribe");
    subValue["payload"][0] = json::value::string("BTC_USDT");
    //If you want to subscribe to all user trades updates in all currency pairs,
    // you can include !all in currency pair list.
    subValue["payload"][1] = json::value::string("!all");
    string time = to_string(crypto::getCurrentTimeSeconds());
    string sign = get_signature_ws(channel.c_str(), "subscribe", time.c_str());
    json::value signValue;
    signValue["method"] = json::value::string("api_key");
    signValue["KEY"] = json::value::string(m_curcfg.apiKey);
    signValue["SIGN"] = json::value::string(sign);
    subValue["auth"] = signValue;
    outMsg.set_utf8_message(subValue.serialize().c_str());
    return outMsg;
}
*/

void GateioSpotTradeUnit::onWebsocketMsg(const web::websockets::client::websocket_incoming_message& msg) {
    try {
        // text_message, binary_message, close, ping, pong
        latestPingPongTime.store(crypto::getCurrentTimeSeconds());
        auto ty = msg.message_type();
        if (ty == web::websockets::client::websocket_message_type::text_message) {
            const std::string s = msg.extract_string().get();

            LOG_DEBUG("on_websocket_msg:%s", s.c_str());
            rapidjson::Document d;
            rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(s.c_str());
            if (d.HasParseError()) {
                return;
            }

            //订阅成功的回报和ping pong之类的消息不用特别处理
            std::string channel = "";
            if (rawData.HasMember("channel")) {
                channel = rawData["channel"].GetString();
            }
            else {
                return;
            }

            if (crypto::str_cmp(channel.c_str(), "spot.pong")) {
                return;
            }
            //没有有效信息无需处理
            if (rawData.HasMember("event")) {
                if (!crypto::str_cmp(rawData["event"].GetString(), "update")) {
                    return;
                }
            }
            else {
                return;
            }
        
            if (crypto::str_cmp(channel.c_str(), "spot.orders")) {
                const rapidjson::Value &data = rawData["result"];
                for(rapidjson::SizeType i = 0; i < data.Size(); i++){
                    std::string originInstId = data[i]["currency_pair"].GetString();
                    md::InstrumentInfo info;
                    if(this->smc->get_instrument_info(GATEIO, SPOT, originInstId.c_str(), info)) {
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                        rcmd.body.orderResponse.exchangeTypeEnum = BINANCE;
                        rcmd.body.orderResponse.instTypeEnum = SPOT;
                        
                        strncpy(rcmd.body.orderResponse.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                        strncpy(rcmd.body.orderResponse.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);
                        strncpy(rcmd.body.orderResponse.instId, info.instId, INSTID_SIZE);

                        strncpy(rcmd.body.orderResponse.orderId, data[i]["id"].GetString(), ORDER_SIZE);

                        if (data[i].HasMember("text")) {
                            strncpy(rcmd.body.orderResponse.orderSysId, data[i]["text"].GetString(), ORDER_SIZE);
                        }

                        rcmd.body.orderResponse.limitPrice = std::stod(data[i]["price"].GetString()) * info.reduceNumber;
                        rcmd.body.orderResponse.volumeTotal = std::stod(data[i]["amount"].GetString()) * info.magnifyNumber;

                        rcmd.body.orderResponse.offsetFlag = OF_OPEN;
                        rcmd.body.orderResponse.direction = data[i]["side"].GetString()[0] == 's' ? DT_SHORT : DT_LONG;

                        std::string tif = data[i]["time_in_force"].GetString();
                        if (tif[0] == 'g') {
                            rcmd.body.orderResponse.orderType = OT_LIMIT;
                        }
                        else if (tif[0] == 'i') {
                            rcmd.body.orderResponse.orderType = OT_IOC;
                        }
                        else if (tif[0] == 'p') {
                            rcmd.body.orderResponse.orderType = OT_POST_ONLY;
                        }

                        //成交数量
                        double left = std::stod(data[i]["left"].GetString());
                        left = left > 0 ? left : -left;
                        rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;

                        if (data[i].HasMember("avg_deal_price")) {
                            rcmd.body.orderResponse.tradePrice = std::stod(data[i]["avg_deal_price"].GetString());
                        }
                        
                        std::string event = data[i]["event"].GetString();
                        if (event[0] == 'p') {//put
                            rcmd.body.orderResponse.orderStatus = OS_NEW;
                        }
                        else if (event[0] == 'u') {//update
                            rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
                        }
                        else if (event[0] == 'f') {//finish
                            //成交数量等于挂单数量
                            if (rcmd.body.orderResponse.volumeTotal - rcmd.body.orderResponse.volumeTraded < ZERO_NUM) {
                                rcmd.body.orderResponse.orderStatus = OS_FILLED;
                            }
                            else {
                                rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                            }
                        }
                        else {
                            rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                        }
                        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                        PUSH_RCMD(rcmd)
                    }
                    else {
                        LOG_ERROR("not found GATEIO.SPOT.{} smc info", originInstId);
                    }
                }
            }
            // else if(crypto::str_cmp(channel.c_str(),"spot.usertrades") == true){
    //                 const rapidjson::Value &data = rawData["result"];
    //                 for(rapidjson::SizeType i = 0; i < data.Size(); i++){
    //                     string originInstId = data[i]["currency_pair"].GetString();
    //                     md::InstrumentInfo info;
    //                     if(this->smc->get_instrument_info("GATEIO", "SPOT",originInstId.c_str(), info)){
    // //                        MsgExt msgExt;
    //                     }
    //                     else{
    //                         LOG_ERROR("not found GATEIO.SPOT.%s smc info", originInstId.c_str());
    //                     }
    //                 }
            // }
            else if (crypto::str_cmp(channel.c_str(), "spot.balances")) {
                const rapidjson::Value &data = rawData["result"];
                for (rapidjson::SizeType i = 0; i < data.Size(); i++) {
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = GATEIO;
                    rcmd.body.balance.instTypeEnum = SPOT;
                    
                    strncpy(rcmd.body.balance.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                    strncpy(rcmd.body.balance.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);
                    strncpy(rcmd.body.balance.currency, crypto::to_upper(data[i]["currency"].GetString()).c_str(), INSTID_SIZE);
                    rcmd.body.balance.available = std::stod(data[i]["available"].GetString());
                    rcmd.body.balance.total = std::stod(data[i]["total"].GetString());
                    rcmd.body.balance.isLast = data.Size() - 1;
                    rcmd.body.balance.updateTime = crypto::getCurrentTime();
                    rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;

                    PUSH_RCMD(rcmd)
                }
            }
            else if (crypto::str_cmp(channel.c_str(), "spot.cross_balances")) {
                const rapidjson::Value &data = rawData["result"];
                for (rapidjson::SizeType i = 0; i < data.Size(); i++) {
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = GATEIO;
                    rcmd.body.balance.instTypeEnum = SPOT;
                    
                    strncpy(rcmd.body.balance.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                    strncpy(rcmd.body.balance.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);
                    strncpy(rcmd.body.balance.currency, crypto::to_upper(data[i]["currency"].GetString()).c_str(), INSTID_SIZE);
                    rcmd.body.balance.available = std::stod(data[i]["available"].GetString());
                    rcmd.body.balance.total = std::stod(data[i]["total"].GetString());
                    rcmd.body.balance.isLast = data.Size() - 1;
                    rcmd.body.balance.updateTime = crypto::getCurrentTime();
                    rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;

                    PUSH_RCMD(rcmd)
                }
            }
            else {
                LOG_ERROR("%s",s.c_str());
                return;
            }
        
        }
        else if (msg.message_type() == web::websockets::client::websocket_message_type::close) {
            isConnected = false;
        }
        else {
        }
    }
    catch (const std::exception& e) {
        LOG_ERROR("{}", e.what());
        isConnected = false;
    }
}


void GateioSpotTradeUnit::ping(){
    if (isConnected) {
        web::websockets::client::websocket_outgoing_message outMsg;
        web::json::value spotPingSubValue;
        spotPingSubValue["time"] = crypto::getCurrentTimeSeconds();
        spotPingSubValue["channel"] = web::json::value::string("spot.ping");
        outMsg.set_utf8_message(spotPingSubValue.serialize().c_str());
        pWsClient->send(outMsg).wait();
    }
}

void GateioSpotTradeUnit::pong(){
    if (isConnected) {
        web::websockets::client::websocket_outgoing_message outMsg;
        outMsg.set_pong_message();
        pWsClient->send(outMsg).wait();
    }
}

void GateioSpotTradeUnit::query_account(const pubsub::TCommand& tcmd) {
    try {
        web::http::http_request request(web::http::methods::GET);
        FORMAT_REQUEST(request)
        std::string time = std::to_string(crypto::getCurrentTimeSeconds() );
        std::string sign = crypto::getGateioSignatureRest("GET", unifiedUrl.to_string(), time, "", "", acc.secretKey);
        request.headers().add("KEY", acc.apiKey);
        request.headers().add("Timestamp", time);
        request.headers().add("SIGN", sign);

        web::http::uri_builder builder(unifiedUrl);
        request.set_request_uri(builder.to_string());

        auto& restClient = *pRestClient;
        START_FORMAT_RESPONSE(request)
            const web::json::value& v = previousTask.get();
            auto balances = v.at("balances").as_object();
            std::vector<pubsub::RCommand> needPushRcmd;
            for (auto iter = balances.begin(); iter != balances.end(); ++iter) {
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                rcmd.body.balance.exchangeTypeEnum = GATEIO;
                rcmd.body.balance.instTypeEnum = SPOT;
                strncpy(rcmd.body.balance.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
                strncpy(rcmd.body.balance.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);

                strncpy(rcmd.body.balance.currency, crypto::to_upper(iter->first).c_str(), INSTID_SIZE);
                rcmd.body.balance.available = std::stod(iter->second.at("available").as_string().c_str());
                rcmd.body.balance.frozen = std::stod(iter->second.at("freeze").as_string().c_str());
                rcmd.body.balance.total = std::stod(iter->second.at("equity").as_string().c_str());
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

            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
            rcmd.body.totalAccount.exchangeTypeEnum = GATEIO;
            rcmd.body.totalAccount.instTypeEnum = SPOT;
            strncpy(rcmd.body.totalAccount.accountId, acc.accountId.c_str(), ACCOUNTID_SIZE);
            strncpy(rcmd.body.totalAccount.strategyId, acc.strategyId.c_str(), STRATEGYID_SIZE);

            rcmd.body.totalAccount.totalEquity = stod(v.at("unified_account_total_equity").as_string());
            rcmd.body.totalAccount.adjEquity = stod(v.at("total_margin_balance").as_string());
            rcmd.body.totalAccount.mmr = stod(v.at("total_maintenance_margin").as_string());
            std::string mgnStr = v.at("total_maintenance_margin_rate").as_string();
            if (mgnStr != "") {
                rcmd.body.totalAccount.mgnRatio = stod(mgnStr);
            } else {
                rcmd.body.totalAccount.mgnRatio = 9999;
            }
            rcmd.body.totalAccount.apiSourceEnum = AS_REST;
    
            PUSH_RCMD(rcmd)  
      
        END_FORMAT_RESPONSE(request)
    }
    catch (const std::exception& e) {
        LOG_ERROR("get_unified_account {}", e.what());
    }
}

void GateioSpotTradeUnit::query_balance(const pubsub::TCommand& tcmd) { // 也需要 magnifyNumber/reduceNumber
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

void GateioSpotTradeUnit::query_position(const pubsub::TCommand& tcmd) {

}

void GateioSpotTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
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
            value["currency_pair"] = json::value::string(info.originInstId);
            value["price"] = json::value::string(std::to_string(price));
            value["amount"] = json::value::string(std::to_string(volume));

            if (tcmd.body.newOrder.offsetFlag == OF_OPEN) {
                if (tcmd.body.newOrder.direction == DT_LONG) {
                    value["side"] = json::value::string("buy");
                }
                else if (tcmd.body.newOrder.direction == DT_SHORT) {
                    value["side"] = json::value::string("sell");
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
                    value["side"] = json::value::string("sell");
                }
                else if (tcmd.body.newOrder.direction == DT_SHORT) {
                    value["side"] = json::value::string("buy");
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
                    value["time_in_force"] = json::value::string("gtc");
                    break;  
                }
                case OT_MARKET: {
                    value["time_in_force"] = json::value::string("ioc");
                    value["price"] = json::value::string("0");
                    break;
                }
                case OT_POST_ONLY: {
                    value["time_in_force"] = json::value::string("poc");
                    break;
                }
                case OT_FOK: {
                    value["time_in_force"] = json::value::string("fok");
                    break;
                }
                case OT_IOC: {
                    value["time_in_force"] = json::value::string("ioc");
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

            if(tcmd.body.newOrder.instTypeEnum == SPOT){
                value["account"] = json::value::string("spot");
            }
            else if(tcmd.body.newOrder.instTypeEnum == MARGIN){
                value["account"] = json::value::string("margin");
            }
            else{
                rcmd.body.orderResponse.errorId = InstTypeError;
                PUSH_RCMD(rcmd)
                return;
            }

        #ifdef USE_GATEIO_UNIFIED
            value["account"] = json::value::string("unified");
            value["auto_borrow"] = json::value::boolean(true);
            value["auto_repay"] = json::value::boolean(true);
        #endif

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
                rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(s.c_str());

                if(rawData.HasMember("status")){
                    rcmd.body.orderResponse.orderStatus = crypto::get_gateio_orderstatus(rcmd.body.orderResponse.instTypeEnum, rawData);
                    strcpy(rcmd.body.orderResponse.orderId, rawData["id"].GetString());
                    rcmd.body.orderResponse.errorId = NoError;

                    if (rawData.HasMember("avg_deal_price")) {
                        rcmd.body.orderResponse.tradePrice = stod(rawData["avg_deal_price"].GetString());
                    }
                    
                    double left = std::stod(rawData["left"].GetString());
                    left = left > 0 ? left : -left;
                    rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                }
                else if(rawData.HasMember("label")){
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

void GateioSpotTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    try {
        md::InstrumentInfo info;
        if (smc->get_instrument_info(tcmd.body.cancelOrder.exchangeTypeEnum, tcmd.body.cancelOrder.instTypeEnum, tcmd.body.cancelOrder.instId, info)) {
            web::http::http_request request(web::http::methods::DEL);
            FORMAT_REQUEST(request)
        
            std::string queryStr{"currency_pair="};
            queryStr.append(info.originInstId);

        #ifdef USE_GATEIO_UNIFIED
            queryStr.append("&account=unified");
        #endif

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
                    if (v.has_field("avg_deal_price")) {
                        rcmd.body.orderResponse.tradePrice = std::stod(v.at("avg_deal_price").as_string().c_str());
                    }
                    double amount = std::stod(v.at("amount").as_string().c_str());
                    double left = std::stod(v.at("left").as_string().c_str());
                    left = left > 0 ? left : -left;
                    rcmd.body.orderResponse.volumeTraded = amount - left;
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

void GateioSpotTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);
    try {
        md::InstrumentInfo info;
        if (smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum, tcmd.body.queryOrder.instTypeEnum, tcmd.body.queryOrder.instId, info)) {
            web::http::http_request request(web::http::methods::GET);
            FORMAT_REQUEST(request)

            std::string queryStr{"currency_pair="};
            queryStr.append(info.originInstId);

        #ifdef USE_UNIFIED
            queryStr.append("&account=unified");
        #endif

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
                    rcmd.body.orderResponse.volumeTotal = std::stod(rawData["amount"].GetString());
                    rcmd.body.orderResponse.limitPrice = std::stod(rawData["price"].GetString());
                    double left = std::stod(rawData["left"].GetString());
                    left = left > 0 ? left : -left;
                    rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;

                    if (rawData.HasMember("avg_deal_price")) {
                        rcmd.body.orderResponse.tradePrice = std::stod(rawData["avg_deal_price"].GetString());
                    }

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
                        if(crypto::str_cmp(finish_as.c_str(), "filled")){
                            return rcmd.body.orderResponse.orderStatus = OS_FILLED;
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
