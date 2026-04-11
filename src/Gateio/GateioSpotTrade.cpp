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
        // wsClient.send(unifiedBalanceOutMsg).wait(); 
    #else
        web:;websockets::client::websocket_outgoing_message balanceOutMsg = sub_balance_channel();
        pWsClient->send(balanceOutMsg).wait();
    #endif

        web:;websockets::client::websocket_outgoing_message orderOutMsg = sub_orders_channel();
        pWsClient->send(orderOutMsg).wait();
      
        // websocket_outgoing_message tradesOutMsg  = sub_trades_channel();
        // wsClient.send(tradesOutMsg).wait();

        LOG_INFO("connected with gateio spot api: {}", builder.to_string());

    }
    else {
        LOG_ERROR("{} ws: {} connect failed, cannot sub.", acc.accountId, acc.wsUrl);
    }
}

web:;websockets::client::websocket_outgoing_message GateioSpotTradeUnit::sub_balance_channel() {
    std::string channel = "spot.balances";
    web:;websockets::client::websocket_outgoing_message outMsg;
    web::json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = web::json::value::string(channel);
    subValue["event"] = web::json::value::string("subscribe");
    std::string time = std::to_string(crypto::getCurrentTimeSeconds());
    std;:string sign = get_signature_ws(channel.c_str(), "subscribe", time.c_str());
    web::json::value signValue;
    signValue["method"] = web::json::value::string("api_key");
    signValue["KEY"] = web::json::value::string(m_curcfg.apiKey);
    signValue["SIGN"] = web::json::value::string(sign);
    subValue["auth"] = signValue;
    outMsg.set_utf8_message(subValue.serialize().c_str());
    return outMsg;
}

web:;websockets::client::websocket_outgoing_message GateioSpotTradeUnit::sub_orders_channel() {
    std::string channel = "spot.orders";
    web:;websockets::client::websocket_outgoing_message outMsg;
    web::json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = web::json::value::string(channel);
    subValue["event"] = web::json::value::string("subscribe");
    subValue["payload"][0] = web::json::value::string("BTC_USDT");
    //If you want to subscribe to all orders updates in all currency pairs,
    // you can include !all in currency pair list.
    subValue["payload"][1] = web::json::value::string("!all");
    std::string time = to_string(crypto::getCurrentTimeSeconds());
    std::string sign = get_signature_ws(channel.c_str(), "subscribe", time.c_str());
    web::json::value signValue;
    signValue["method"] = web::json::value::string("api_key");
    signValue["KEY"] = web::json::value::string(m_curcfg.apiKey);
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

void GateioSpotTradeUnit::onWebsocketMsg(const web::websockets::client::websocket_incoming_message& msg)
try{
    // text_message, binary_message, close, ping, pong
    if (msg.message_type() == websocket_message_type::text_message){
        msg.extract_string().then([&](const string s){
            LOG_DEBUG("on_websocket_msg:%s", s.c_str());
            rapidjson::Document d;
            rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(s.c_str());
            if(d.HasParseError() ){
                return;
            }
          
            #ifdef USE_WEBSOCKET_API
            if (rawData.HasMember("request_id")) {
                const string &channel = rawData["header"]["channel"].GetString();
                if(crypto::str_cmp(channel.c_str(), "spot.order_place") == true || crypto::str_cmp(channel.c_str(), "spot.order_cancel") == true || crypto::str_cmp(channel.c_str(), "spot.order_status") == true) {
                    const rapidjson::Value &data = rawData["data"];
                    if (data.HasMember("result")) {
                        const rapidjson::Value &res = data["result"];
                        if (!res.HasMember("req_param")) {
                            string originInstId = res["contract"].GetString();
                            md::InstrumentInfo info;
                            if(this->smc->get_instrument_info("GATEIO", "InstType_SPOT", originInstId.c_str(), info)){
                                pubsub::RCommand rcmd;
                                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                                rcmd.header.cmdTime = crypto::getCurrentTime();
                                rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                                rcmd.header.instTypeEnum = InstType_SPOT;
                                rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                                strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                                strcpy(rcmd.header.accountId, m_curcfg.accountId);
                                strcpy(rcmd.body.orderResponse.instId, info.instId);
                                strcpy(rcmd.body.orderResponse.orderId, res["id"].GetString());

                                if(res.HasMember("text")){
                                    strcpy(rcmd.body.orderResponse.orderSysId, res["text"].GetString());
                                }

                                rcmd.body.orderResponse.offsetFlag = OffsetFlag_OPEN;//crypto::str_cmp(data[i][""].GetString())
                                rcmd.body.orderResponse.direction = res["side"].GetString()[0] == 's' ? Direction_SHORT : Direction_LONG;
                                string tif = res["time_in_force"].GetString();
                                if(tif[0] == 'g'){
                                    rcmd.body.orderResponse.orderType = OrderType_LIMIT;
                                }
                                else if(tif[0] == 'i'){
                                    rcmd.body.orderResponse.orderType = OrderType_IOC;
                                }
                                else if(tif[0] == 'p'){
                                    rcmd.body.orderResponse.orderType = OrderType_POST_ONLY;//maker order only
                                }
                                else{
                                    rcmd.body.orderResponse.orderStatus = OrderStatus_UNKNOWN;
                                }
                                rcmd.body.orderResponse.limitPrice   = stod(res["price"].GetString());
                                rcmd.body.orderResponse.volumeTotal  = stod(res["amount"].GetString());
                                //成交数量
                                double left = stod(res["left"].GetString());
                                left = left > 0 ? left : -left;
                                rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;

                                if(res.HasMember("fill_price")){
                                    rcmd.body.orderResponse.tradePrice = stod(res["fill_price"].GetString());
                                }
                                rcmd.body.orderResponse.orderStatus = crypto::get_gateio_orderstatus(rcmd.header.instTypeEnum, res);
                                PUSH_RCMD(rcmd)
                            }
                            else{
                                LOG_ERROR("not found GATEIO.SWAP.%s smc info", originInstId.c_str());
                            }
                        }
                    } else if (data.HasMember("errs")) {
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.header.cmdTime = crypto::getCurrentTime();
                        rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                        rcmd.header.instTypeEnum = InstType_SPOT;
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);

                        string orderId = rawData["request_id"].GetString();
                        auto iter = orderId.find("t-");
                        if (iter != string::npos) {
                            strcpy(rcmd.body.orderResponse.orderSysId, orderId.c_str());
                        } else {
                            strcpy(rcmd.body.orderResponse.orderId, orderId.c_str());
                        }

                        const rapidjson::Value &errs = data["errs"];
                        const string& label = errs["label"].GetString();
                        // const string& message = errs["message"].GetString();
                        // rcmd.body.newOrder.ErrorID = crypto::get_gateio_errorid(label.c_str());
                        // strncpy(rcmd.body.newOrder.originMsg, label.c_str(), sizeof(rcmd.body.newOrder.originMsg));
                        rcmd.body.orderResponse.orderStatus = OrderStatus_REJECTED;
                        PUSH_RCMD(rcmd)
                    }
                }
            }
            #endif


            //订阅成功的回报和ping pong之类的消息不用特别处理
            string channel;
            if(rawData.HasMember("channel")){
                channel = rawData["channel"].GetString();
            }
            else{
                return;
            }

            // return;
            if(crypto::str_cmp(channel.c_str(), "spot.pong") == true){
                return;
            }
            //没有有效信息无需处理
            if(rawData.HasMember("event")){
                if(crypto::str_cmp(rawData["event"].GetString(), "update") == false){
                    return;
                }
            }
            else{
                return;
            }
      
            if(crypto::str_cmp(channel.c_str(), "spot.orders") == true){
                const rapidjson::Value &data = rawData["result"];
                for(rapidjson::SizeType i = 0; i < data.Size(); i++){
                    string originInstId = data[i]["currency_pair"].GetString();
                    md::InstrumentInfo info;
                    if(this->smc->get_instrument_info("GATEIO", "InstType_SPOT", originInstId.c_str(), info)){
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.header.cmdTime = crypto::getCurrentTime();
                        rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                        rcmd.header.instTypeEnum = InstType_SPOT;
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        strcpy(rcmd.body.orderResponse.instId, info.instId);
                        strcpy(rcmd.body.orderResponse.orderId, data[i]["id"].GetString());

                        if(data[i].HasMember("text")){
                            strcpy(rcmd.body.orderResponse.orderSysId, data[i]["text"].GetString());
                        }

                        rcmd.body.orderResponse.offsetFlag = OffsetFlag_OPEN;//crypto::str_cmp(data[i][""].GetString())
                        rcmd.body.orderResponse.direction = data[i]["side"].GetString()[0] == 's' ? Direction_SHORT : Direction_LONG;
                        string tif = data[i]["time_in_force"].GetString();
                        if(tif[0] == 'g'){
                            rcmd.body.orderResponse.orderType = OrderType_LIMIT;
                        }
                        else if(tif[0] == 'i'){
                            rcmd.body.orderResponse.orderType = OrderType_IOC;
                        }
                        else if(tif[0] == 'p'){
                            rcmd.body.orderResponse.orderType = OrderType_POST_ONLY;//maker order only
                        }
                        else{
                            rcmd.body.orderResponse.orderStatus = OrderStatus_UNKNOWN;
                        }
                        rcmd.body.orderResponse.limitPrice   = stod(data[i]["price"].GetString());
                        rcmd.body.orderResponse.volumeTotal  = stod(data[i]["amount"].GetString());
                        //成交数量
                        double left = stod(data[i]["left"].GetString());
                        left = left > 0 ? left : -left;
                        rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;

                        if(data[i].HasMember("avg_deal_price")){
                            rcmd.body.orderResponse.tradePrice = stod(data[i]["avg_deal_price"].GetString());
                        }
                        
                        //成交均价
                        // if(rcmd.body.orderResponse.volumeTraded > 0){
                        //     if(data[i].HasMember("fill_price")){
                        //         rcmd.body.orderResponse.tradePrice = stod(data[i]["fill_price"].GetString());
                        //     }
                        // }
                        string event = data[i]["event"].GetString();
                        if(event[0] == 'p'){//put
                            rcmd.body.orderResponse.orderStatus = OrderStatus_NEW;
                        }
                        else if(event[0] == 'u'){//update
                            rcmd.body.orderResponse.orderStatus = OrderStatus_PARTFILLED;
                        }
                        else if(event[0] == 'f'){//finish
                            //成交数量等于挂单数量
                            if(rcmd.body.orderResponse.volumeTotal == rcmd.body.orderResponse.volumeTraded ){
                                rcmd.body.orderResponse.orderStatus = OrderStatus_FILLED;
                            }
                            else{
                                rcmd.body.orderResponse.orderStatus = OrderStatus_CANCELED;
                            }
                        }
                        else{
                            rcmd.body.orderResponse.orderStatus = OrderStatus_UNKNOWN;
                        }
                        PUSH_RCMD(rcmd)
                    }
                    else{
                        LOG_ERROR("not found GATEIO.SPOT.%s smc info", originInstId.c_str());
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
            else if(crypto::str_cmp(channel.c_str(), "spot.balances") == true){
                const rapidjson::Value &data = rawData["result"];
                for(rapidjson::SizeType i = 0; i < data.Size(); i++){
//                    MsgExt msgExt;
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.header.cmdTime = crypto::getCurrentTime();
                    rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                    rcmd.header.instTypeEnum = InstType_SPOT;
                    rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                    strcpy(rcmd.header.accountId, m_curcfg.accountId);
                    strcpy(rcmd.body.balance.currency, data[i]["currency"].GetString());
                    rcmd.body.balance.available = stod(data[i]["available"].GetString());
                    rcmd.body.balance.total = stod(data[i]["total"].GetString());
                    if(crypto::is_zeronum(rcmd.body.balance.total)){
                        continue;
                    }
                    rcmd.body.balance.frozen = stod(data[i]["freeze"].GetString());
                    rcmd.body.balance.apiSourceEnum = ApiSource_WEBSOCKET;
                    PUSH_RCMD(rcmd)
                }
            }
            else if(crypto::str_cmp(channel.c_str(), "spot.cross_balances") == true){
                const rapidjson::Value &data = rawData["result"];
                for(rapidjson::SizeType i = 0; i < data.Size(); i++){
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.header.cmdTime = crypto::getCurrentTime();
                    rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                    rcmd.header.instTypeEnum = InstType_SPOT;
                    rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                    strcpy(rcmd.header.accountId, m_curcfg.accountId);
                    strcpy(rcmd.body.balance.currency, data[i]["currency"].GetString());
                    rcmd.body.balance.available = stod(data[i]["available"].GetString());
                    rcmd.body.balance.total = stod(data[i]["total"].GetString());
                    if(crypto::is_zeronum(rcmd.body.balance.total)){
                        continue;
                    }
                    rcmd.body.balance.frozen = stod(data[i]["freeze"].GetString());
                    rcmd.body.balance.apiSourceEnum = ApiSource_WEBSOCKET;
                    PUSH_RCMD(rcmd)
                }
            }
            else{
                LOG_ERROR("%s",s.c_str());
                return;
            }
        });
    }
    else if(msg.message_type() == websocket_message_type::close){
        m_IsConnected = false;
    }
    else{
        =
    }
}
catch(exception &e){
    std::cout << "SubWebSocket Error:" << e.what() << endl;
    LOG_ERROR("%s", e.what());
    m_IsConnected = false;
}

void GateioSpotTradeUnit::ping(){
    if (isConnected) {
        web:;websockets::client::websocket_outgoing_message outMsg;
        web::json::value spotPingSubValue;
        spotPingSubValue["time"] = crypto::getCurrentTimeSeconds();
        spotPingSubValue["channel"] = web::json::value::string("spot.ping");
        outMsg.set_utf8_message(spotPingSubValue.serialize().c_str());
        wsClient.send(outMsg).wait();
    }
}

void GateioSpotTradeUnit::pong(){
    if (isConnected) {
        web:;websockets::client::websocket_outgoing_message outMsg;
        outMsg.set_pong_message();
        wsClient.send(outMsg).wait();
    }
}

bool GateioSpotTradeUnit::query_account(const pubsub::TCommand& tcmd) {
    try {
        web::http::http_request request(web::http::methods::GET);
        FORMAT_REQUEST(request)
        std::string time = std::to_string(crypto::getCurrentTimeSeconds() );
        std::string sign = crypto::getGateioSignatureRest("GET", unifiedUrl.to_string(), time, "", "");
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
        std::string sign = crypto::getGateioSignatureRest("GET", balanceUrl.to_string(), time, "", "");
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
                rcmd.body.newOrder.ErrorID = ERROR_InstTypeError;
                PUSH_RCMD(rcmd)
                return;
            }

        #ifdef USE_GATEIO_UNIFIED
            value["account"] = json::value::string("unified");
            value["auto_borrow"] = json::value::boolean(true);
            value["auto_repay"] = json::value::boolean(true);
        #endif

            std::string time = std::to_string(crypto::getCurrentTimeSeconds());
            std::string sign = get_signature_rest("POST", orderUrl.to_string(), time, "", value.serialize());
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
                    rcmd.body.orderResponse.orderStatus = crypto::get_gateio_orderstatus(rcmd.header.instTypeEnum, rawData);
                    strcpy(rcmd.body.orderResponse.orderId, rawData["id"].GetString());
                    rcmd.body.orderResponse.errorId = NoError;

                    if (rawData.HasMember("avg_deal_price")) {
                        rcmd.body.orderResponse.tradePrice = stod(rawData["avg_deal_price"].GetString());
                    }
                    
                    double left = std::stod(rawData["left"].GetString());
                    left = left > 0 ? left : -left;
                    rcmd.body.orderResponse.volumeTraded = rcmd.body.newOrder.volumeTotal - left;
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


void GateioSpotTradingClient::cancel_order(pubsub::TCommand &tcmd){
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)
    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.cancelOrder.instId, info)){
            // http_client restclient(m_curcfg.restBaseUrl);//bUrl
            http_request request(methods::DEL);
            request.headers().add("Accept","application/json");
            request.headers().add("Content-Type","application/json");
            string queryStr{"currency_pair="};
            queryStr.append(info.originInstId);

            #ifdef USE_UNIFIED
            queryStr.append("&account=unified");
            #endif

            string time = to_string(crypto::getCurrentTimeSeconds());
            if(tcmd.body.cancelOrder.cancelOrderTypeEnum == pubsub::COT_ONE_INST){
                string cancelOrderUrl = m_orderUrl.to_string();
                if(crypto::str_cmp(tcmd.body.cancelOrder.orderId,"") == false){
                    cancelOrderUrl.append("/").append(tcmd.body.cancelOrder.orderId);
                }
                else
                if( tcmd.body.cancelOrder.clientOrderId != 0){
                    cancelOrderUrl.append("/").append(tcmd.body.cancelOrder.orderSysId);
                }
                else{
                    LOG_ERROR("cancel order need orderId or clientOrderId:%s",tcmd.getString().c_str());
                    strcpy(rcmd.body.cancelOrder.originMsg, "cancel order need orderId or clientOrderId");
                    rcmd.body.cancelOrder.ErrorID = ERROR_NoOrderIdError;
                    rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                    PUSH_RCMD(rcmd)
                    return;
                }
                string sign = get_signature_rest("DELETE", cancelOrderUrl.c_str(), time.c_str(),
                                                 queryStr.c_str(), "");
                cancelOrderUrl.append("?").append(queryStr);
                web::uri m_cancelOrderUrl = cancelOrderUrl;
                uri_builder builder(m_cancelOrderUrl);// bUrl m_balanceUrl
                request.headers().add("KEY",m_curcfg.apiKey);
                request.headers().add("Timestamp",time);
                request.headers().add("SIGN",sign);
                request.set_request_uri(builder.to_string());
                LOG_DEBUG("%s:%s",__FUNCTION__, builder.to_string().c_str());
                const http_response &response = hotHttpClient->request(request);
                rcmd.body.cancelOrder.tsNet = crypto::getCurrentTime();
                auto code = response.status_code();
                if(code == status_codes::Created || code == status_codes::OK || code == status_codes::BadRequest){
                    json::value const &v = response.extract_json().get();
                    LOG_DEBUG("cancel_order response:%s", v.serialize().c_str());
                    if(v.has_field("status")){
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_CANCELED;
                        strcpy(rcmd.body.cancelOrder.orderId, v.at("id").as_string().c_str());
                        strcpy(rcmd.body.cancelOrder.orderSysId, v.at("text").as_string().c_str());
                        if (v.has_field("avg_deal_price")) {
                            rcmd.body.cancelOrder.tradePrice = stod(v.at("avg_deal_price").as_string().c_str());
                        }
                        double amount = stod(v.at("amount").as_string().c_str());
                        double left = stod(v.at("left").as_string().c_str());
                        left = left > 0 ? left : -left;
                        rcmd.body.cancelOrder.volumeTraded = amount - left;
                        PUSH_RCMD(rcmd)
                        return;
                    }
                    else if(v.has_field("label")){
                        LOG_ERROR("%s",v.serialize().c_str());
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                        rcmd.body.cancelOrder.ErrorID = crypto::get_gateio_errorid(v.serialize().c_str());
                        if (rcmd.body.cancelOrder.ErrorID == ERROR_OrderNotFoundError){
                            rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                        }
                        strncpy(rcmd.body.cancelOrder.originMsg, v.at("label").as_string().c_str(), sizeof(rcmd.body.cancelOrder.originMsg));

                        PUSH_RCMD(rcmd)
                        return;
                    }
                }
                else{
                    auto v = response.extract_json().get();
                    LOG_ERROR("%s", v.serialize().c_str());
                    rcmd.body.cancelOrder.ErrorID = code;
                    if(v.has_field("label")){
                        strncpy(rcmd.body.cancelOrder.originMsg, v.at("label").as_string().c_str(), sizeof(rcmd.body.cancelOrder.originMsg));
                    }
                    rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                    PUSH_RCMD(rcmd)
                }
#if 0
                .then([&](const http_response &response) -> pplx::task<json::value> { auto code = response.status_code();
                    rcmd.body.cancelOrder.tsNet = crypto::getCurrentTime();
//                        LOG_DEBUG("cancel_order response:'%s' ",response.to_string().c_str());
                    if(code == status_codes::Created || code == status_codes::OK || code == status_codes::BadRequest){
                        return response.extract_json();
                    }
                    else{
                        auto v = response.extract_json().get();
                        LOG_ERROR("%s", v.serialize().c_str());
                        rcmd.body.cancelOrder.ErrorID = code;
                        if(v.has_field("label")){
                            strncpy(rcmd.body.cancelOrder.originMsg, v.at("label").as_string().c_str(), sizeof(rcmd.body.cancelOrder.originMsg));
                        }
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                        PUSH_RCMD(rcmd)
                        return pplx::task_from_result(json::value());
                    }
                })
                .then([&](pplx::task<json::value> previousTask) {
                    json::value const &v = previousTask.get();
                    LOG_DEBUG("cancel_order response:%s", v.serialize().c_str());
                    if(v.has_field("status")){
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_CANCELED;
                        PUSH_RCMD(rcmd)
                        return;
                    }
                    else if(v.has_field("label")){
                        LOG_ERROR("%s",v.serialize().c_str());
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                        rcmd.body.cancelOrder.ErrorID = crypto::get_gateio_errorid(v.serialize().c_str());
                        if (rcmd.body.cancelOrder.ErrorID == ERROR_OrderNotFoundError){
                            rcmd.body.cancelOrder.orderStatus = OrderStatus_REJECTED;
                        }
                        strncpy(rcmd.body.cancelOrder.originMsg, v.at("label").as_string().c_str(), sizeof(rcmd.body.cancelOrder.originMsg));

                        PUSH_RCMD(rcmd)
                        return;
                    }
                })
                .wait();
#endif
            }
            else if(tcmd.body.cancelOrder.cancelOrderTypeEnum == pubsub::COT_MULTI_INST){

            }
            else{
                rcmd.body.cancelOrder.ErrorID = ERROR_CancelOrQueryTypeError;
                rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                PUSH_RCMD(rcmd)
                return;
            }
        }
        else{
            rcmd.body.cancelOrder.ErrorID = ERROR_SMCInstrumentNotExistError;
            rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
            PUSH_RCMD(rcmd)
            return;
        }
    }
    catch(exception &e) {
        rcmd.body.cancelOrder.ErrorID = ERROR_OrderNotFoundError;
        rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
//        strncpy(rcmd.body.cancelOrder.originMsg, e.what(), sizeof(rcmd.body.newOrder.originMsg));
        LOG_ERROR("%s", e.what());
        PUSH_RCMD(rcmd)
    }
    return;
}

void GateioSpotTradingClient::query_order(pubsub::TCommand &tcmd){
    #if 1
    QUERY_ORDER_TCMD_2_RCMD(tcmd)

    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.queryOrder.instId, info)){
            http_client restclient(m_curcfg.restBaseUrl);//bUrl
            http_request request(methods::GET);
//            request.set_method(http::method::DELETE);
            request.headers().add("Accept","application/json");
            request.headers().add("Content-Type","application/json");
            string queryStr{"currency_pair="};
            queryStr.append(info.originInstId);

            #ifdef USE_UNIFIED
            queryStr.append("&account=unified");
            #endif

            string time = to_string(crypto::getCurrentTimeSeconds());
            string queryOrderUrl = m_orderUrl.to_string();
            if(!crypto::str_cmp(tcmd.body.queryOrder.orderId,"")){
                queryOrderUrl.append("/").append(tcmd.body.queryOrder.orderId);
            }
            else if(tcmd.body.queryOrder.clientOrderId != 0){
                queryOrderUrl.append("/").append(tcmd.body.queryOrder.orderSysId);
            }
            else{
                LOG_ERROR("query_order need orderId or clientOrderId" );
                rcmd.body.queryOrder.ErrorID = ERROR_NoOrderIdError;
                PUSH_RCMD(rcmd)
                return;
            }
            string sign = get_signature_rest("GET", queryOrderUrl.c_str(), time.c_str(),
                                             queryStr.c_str(), "");
            queryOrderUrl.append("?").append(queryStr);
            web::uri m_queryOrderUrl  = queryOrderUrl;
            uri_builder builder(m_queryOrderUrl);// bUrl m_balanceUrl
//            LOG_DEBUG("%s url:%s",__FUNCTION__, m_cancelOrderUrl.to_string().c_str());
            request.headers().add("KEY",m_curcfg.apiKey);
            request.headers().add("Timestamp",time);
            request.headers().add("SIGN",sign);
            request.set_request_uri(builder.to_string());

            restclient.request(request)
            .then([](const http_response &response) -> pplx::task<json::value> { auto code = response.status_code();
                if(code == status_codes::Created || code == status_codes::OK || code == status_codes::BadRequest)
                    return response.extract_json();
                return pplx::task_from_result(json::value());
            }) // continue when the JSON value is available
            .then([&](pplx::task<json::value> previousTask) {
                json::value const &v = previousTask.get();
                LOG_DEBUG("query_order response:%s", v.serialize().c_str());
                rapidjson::Document d;
                rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(v.serialize().c_str());
                if(rawData.HasMember("status")){
                    strcpy(rcmd.body.queryOrder.instId, info.instId);
                    strcpy(rcmd.body.queryOrder.orderId, rawData["id"].GetString());
                    strcpy(rcmd.body.queryOrder.orderSysId, rawData["text"].GetString());
                    rcmd.body.queryOrder.volumeTotal = stod(rawData["amount"].GetString());
                    rcmd.body.queryOrder.limitPrice = stod(rawData["price"].GetString());
                    double left = stod(rawData["left"].GetString());
                    left = left > 0 ? left : -left;
                    rcmd.body.queryOrder.volumeTraded = rcmd.body.queryOrder.volumeTotal - left;

                    if (rawData.HasMember("avg_deal_price")) {
                        rcmd.body.queryOrder.tradePrice = stod(rawData["avg_deal_price"].GetString());
                    }

                    string status = rawData["status"].GetString();
                    if(crypto::str_cmp(status.c_str(),"open")){
                        //成交数量等于挂单数量
                        if (rcmd.body.queryOrder.volumeTotal == rcmd.body.queryOrder.volumeTraded) {
                            rcmd.body.queryOrder.orderStatus = OrderStatus_FILLED;
                        }
                        else{
                            rcmd.body.queryOrder.orderStatus = OrderStatus_PARTFILLED;
                        }
                    }
                    else if(crypto::str_cmp(status.c_str(),"cancelled")){
                        rcmd.body.queryOrder.orderStatus = OrderStatus_CANCELED;
                    }
                    else {
                        string finish_as = rawData["finish_as"].GetString();
                        if(crypto::str_cmp(finish_as.c_str(), "filled")){
                            return OrderStatus_FILLED;
                        }
                        else{
                            rcmd.body.queryOrder.orderStatus = OrderStatus_UNKNOWN;
                        }
                    }
                    PUSH_RCMD(rcmd)
                }
                else{
                    LOG_ERROR("%s",v.serialize().c_str());
                    rcmd.body.queryOrder.ErrorID = crypto::get_gateio_errorid(v.serialize().c_str());
                    // strncpy(rcmd.body.queryOrder.originMsg, v.serialize().c_str(), sizeof(rcmd.body.queryOrder.originMsg));
                    PUSH_RCMD(rcmd)
                }
            })
            .wait();
        }
        else{
            rcmd.body.queryOrder.ErrorID = ERROR_SMCInstrumentNotExistError;
            PUSH_RCMD(rcmd)
            LOG_ERROR("not found GATEIO.SPOT.%s smc info", tcmd.body.queryOrder.instId);
        }
    }
    catch(exception &e) {
        rcmd.body.queryOrder.ErrorID = ERROR_NetworkError;
        strncpy(rcmd.body.queryOrder.originMsg, e.what(), sizeof(rcmd.body.queryOrder.originMsg));
        PUSH_RCMD(rcmd)
        LOG_ERROR("%s", e.what());
    }
    #endif
}

#ifdef USE_WEBSOCKET_API
void GateioSpotTradingClient::ws_add_new_order(pubsub::TCommand &tcmd){
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (m_IsConnected == false){//&& m_IsConnectedBtc == false
        rcmd.body.newOrder.ErrorID = ERROR_TBDisconnectError;
        PUSH_RCMD(rcmd)
        return;
    }

    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.newOrder.instId, info)){
            json::value value;

            value["text"] = json::value::string(tcmd.body.newOrder.orderSysId);
            value["currency_pair"] = json::value::string(info.originInstId);
            value["amount"] = json::value::string(crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal, info.lotSize));//to_string(order.volumeTotal);
            value["price"] = json::value::string(crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice, info.tickSize));

            if(tcmd.body.newOrder.orderType == OrderType_LIMIT){
                value["time_in_force"] = json::value::string("gtc");
            }
            else if(tcmd.body.newOrder.orderType == OrderType_IOC){
                value["time_in_force"] = json::value::string("ioc");
            }
            else if(tcmd.body.newOrder.orderType == OrderType_POST_ONLY){
                value["time_in_force"] = json::value::string("poc");
            }
            else if(tcmd.body.newOrder.orderType == OrderType_FOK){
                value["time_in_force"] = json::value::string("fok");
            }
            else if(tcmd.body.newOrder.orderType == OrderType_MARKET){
                value["time_in_force"] = json::value::string("ioc");
                value["price"] = json::value::string("0");
            }
            else{
                rcmd.body.newOrder.ErrorID = ERROR_OrderTypeError;
                PUSH_RCMD(rcmd)
                return ;
            }
            if(tcmd.header.instTypeEnum == InstType_SPOT){
                value["account"] = json::value::string("spot");
            }
            else if(tcmd.header.instTypeEnum == InstType_MARGIN){
                value["account"] = json::value::string("margin");
            }
            else{
                rcmd.body.newOrder.ErrorID = ERROR_InstTypeError;
                PUSH_RCMD(rcmd)
                return;
            }

            #ifdef USE_UNIFIED
            value["account"] = json::value::string("unified");
            value["auto_borrow"] = json::value::boolean(true);
            value["auto_repay"] = json::value::boolean(true);
            #endif

            //open
            if(tcmd.body.newOrder.offsetFlag == OffsetFlag_OPEN){
                if(tcmd.body.newOrder.direction == Direction_LONG){
                    value["side"] = json::value::string("buy");
                }
                else if(tcmd.body.newOrder.direction == Direction_SHORT){
                    value["side"] = json::value::string("sell");
                }
                else{
                    rcmd.body.newOrder.ErrorID = ERROR_DirectionError;
                    PUSH_RCMD(rcmd)
                    return;
                }
            }
            else if(tcmd.body.newOrder.offsetFlag == OffsetFlag_CLOSE){
                if(tcmd.body.newOrder.direction == Direction_LONG){
                    value["side"] = json::value::string("sell");
                }
                else if(tcmd.body.newOrder.direction == Direction_SHORT){
                    value["side"] = json::value::string("buy");
                }
                else{
                    rcmd.body.newOrder.ErrorID = ERROR_DirectionError;
                    PUSH_RCMD(rcmd)
                    return;
                }
            }
            else{
                rcmd.body.newOrder.ErrorID = ERROR_OffsetFlagError;
                PUSH_RCMD(rcmd)
                return;
            }

            string channel = "spot.order_place";
            websocket_outgoing_message outMsg;
            json::value subValue;
            subValue["time"] = crypto::getCurrentTimeSeconds();
            subValue["channel"] = json::value::string(channel);
            subValue["event"] = json::value::string("api");

            string time = to_string(crypto::getCurrentTimeSeconds());
            string sign = get_signature_ws_api(channel.c_str(), "api", time.c_str());

            string reqId = to_string(crypto::getCurrentTimeMilli());

            json::value payloadValue;
            payloadValue["req_param"] = value;
            payloadValue["req_id"] = json::value::string(tcmd.body.newOrder.orderSysId);
            subValue["payload"] = payloadValue;
            outMsg.set_utf8_message(subValue.serialize().c_str());

            LOG_DEBUG("ws_add_new_order url:%s%s, params:%s",m_curcfg.restBaseUrl.c_str(), m_orderUrl.to_string().c_str(), value.serialize().c_str());
            wsClient.send(outMsg);;
      
            if(tcmd.body.newOrder.clientOrderId == TESTCLIENTORDERID){
                return;
            }
            rcmd.body.newOrder.tsNet = crypto::getCurrentTime();
            LOG_DEBUG("GATEIO,%s,internetDelay,%ld", __FUNCTION__, rcmd.body.newOrder.tsNet - rcmd.body.newOrder.tsSent);
        }
        else{
            rcmd.body.newOrder.ErrorID = ERROR_SMCInstrumentNotExistError;
            PUSH_RCMD(rcmd)
            return;
        }
    }
    catch(exception &e){
        rcmd.body.newOrder.ErrorID = ERROR_NetworkError;
        strncpy(rcmd.body.newOrder.originMsg, e.what(), sizeof(rcmd.body.newOrder.originMsg));
        LOG_ERROR("%s", e.what());
        PUSH_RCMD(rcmd)
        return;
    }
}

void GateioSpotTradingClient::ws_cancel_order(pubsub::TCommand &tcmd){
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.cancelOrder.instId, info)){
            json::value value;
            value["currency_pair"] = json::value::string(info.originInstId);
            if(tcmd.body.cancelOrder.cancelOrderTypeEnum == pubsub::COT_ONE_INST){
                string cancelOrderUrl = m_orderUrl.to_string();
                if(crypto::str_cmp(tcmd.body.cancelOrder.orderId, "") == false){
                    value["order_id"] = json::value::string(tcmd.body.cancelOrder.orderId);
                }
                else if( tcmd.body.cancelOrder.clientOrderId != 0){
                    value["order_id"] = json::value::string(tcmd.body.cancelOrder.orderSysId);
                }
                else{
                    LOG_ERROR("cancel order need orderId or clientOrderId" );
                    strcpy(rcmd.body.cancelOrder.originMsg, "cancel order need orderId or clientOrderId");
                    rcmd.body.cancelOrder.ErrorID = ERROR_NoOrderIdError;
                    rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                    PUSH_RCMD(rcmd)
                    return;
                }

                string channel = "spot.order_cancel";
                websocket_outgoing_message outMsg;
                json::value subValue;
                subValue["time"] = crypto::getCurrentTimeSeconds();
                subValue["channel"] = json::value::string(channel);
                subValue["event"] = json::value::string("api");

                string time = to_string(crypto::getCurrentTimeSeconds());
                string sign = get_signature_ws_api(channel.c_str(), "api", time.c_str());

                string reqId = to_string(crypto::getCurrentTimeMilli());

                json::value payloadValue;
                payloadValue["req_param"] = value;
                // payloadValue["req_id"] = json::value::string(reqId);

                if(crypto::str_cmp(tcmd.body.cancelOrder.orderId, "") == false){
                    payloadValue["req_id"] = json::value::string(tcmd.body.cancelOrder.orderId);
                }
                else if( tcmd.body.cancelOrder.clientOrderId != 0){
                    payloadValue["req_id"] = json::value::string(tcmd.body.cancelOrder.orderSysId);
                }

                subValue["payload"] = payloadValue;

                outMsg.set_utf8_message(subValue.serialize().c_str());

                wsClient.send(outMsg);
                rcmd.body.cancelOrder.tsNet = crypto::getCurrentTime();
                LOG_DEBUG("GATEIO,%s,internetDelay,%ld", __FUNCTION__, rcmd.body.cancelOrder.tsNet - rcmd.body.cancelOrder.tsSent);
            }
            else if(tcmd.body.cancelOrder.cancelOrderTypeEnum == pubsub::COT_MULTI_INST){
                // TODO
            }
            else{
                rcmd.body.cancelOrder.ErrorID = ERROR_CancelOrQueryTypeError;
                rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                PUSH_RCMD(rcmd)
                return;
            }
        }
        else{
           rcmd.body.cancelOrder.ErrorID = ERROR_SMCInstrumentNotExistError;
           rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
           LOG_ERROR("not found GATEIO SWAP %s smc info", tcmd.body.cancelOrder.instId);
           PUSH_RCMD(rcmd)
           return;
        }
    }
    catch(exception &e) {
        rcmd.body.cancelOrder.ErrorID = ERROR_OrderNotFoundError;
        rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
        LOG_ERROR("%s", e.what());
        PUSH_RCMD(rcmd)
        return;
    }
    return;
}


void GateioSpotTradingClient::ws_query_order(pubsub::TCommand &tcmd){
    QUERY_ORDER_TCMD_2_RCMD(tcmd)
    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.queryOrder.instId, info)){
            json::value value;
            value["currency_pair"] = json::value::string(info.originInstId);
            if(crypto::str_cmp(tcmd.body.queryOrder.orderId, "") == false){
                value["order_id"] = json::value::string(tcmd.body.queryOrder.orderId);
            }
            else if(!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")){
                value["order_id"] = json::value::string(tcmd.body.queryOrder.orderSysId);
            }
            else{
                string errMsg = "query_order need orderId or orderSysId";
                rcmd.body.queryOrder.ErrorID = ERROR_NoOrderIdError;
                rcmd.body.queryOrder.orderStatus = OrderStatus_REJECTED;
                strncpy(rcmd.body.queryOrder.originMsg, errMsg.c_str(), sizeof(rcmd.body.queryOrder.originMsg));
                LOG_ERROR("%s", errMsg.c_str());
                PUSH_RCMD(rcmd)
                return;
            }

            string channel = "spot.order_status";
            websocket_outgoing_message outMsg;
            json::value subValue;
            subValue["time"] = crypto::getCurrentTimeSeconds();
            subValue["channel"] = json::value::string(channel);
            subValue["event"] = json::value::string("api");

            string time = to_string(crypto::getCurrentTimeSeconds());
            string sign = get_signature_ws_api(channel.c_str(), "api", time.c_str());

            string reqId = to_string(crypto::getCurrentTimeMilli());

            json::value payloadValue;
            payloadValue["req_param"] = value;
            // payloadValue["req_id"] = json::value::string(reqId);

            if(crypto::str_cmp(tcmd.body.queryOrder.orderId, "") == false){
                payloadValue["req_id"] = json::value::string(tcmd.body.queryOrder.orderId);
            }
            else if(!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")){
                payloadValue["req_id"] = json::value::string(tcmd.body.queryOrder.orderSysId);
            }

            subValue["payload"] = payloadValue;
            outMsg.set_utf8_message(subValue.serialize().c_str());
            wsClient.send(outMsg);
        }
        else{
            rcmd.body.queryOrder.ErrorID = ERROR_SMCInstrumentNotExistError;
            rcmd.body.queryOrder.orderStatus = OrderStatus_UNKNOWN;
            LOG_ERROR("not found GATEIO.SWAP.%s smc info", tcmd.body.queryOrder.instId);
            PUSH_RCMD(rcmd)
            return;
        } 
    }
    catch(exception &e) {
        rcmd.body.queryOrder.ErrorID = ERROR_NetworkError;
        rcmd.body.queryOrder.orderStatus = OrderStatus_UNKNOWN;
        strncpy(rcmd.body.queryOrder.originMsg, e.what(), sizeof(rcmd.body.queryOrder.originMsg));
        LOG_ERROR("%s", e.what());
        PUSH_RCMD(rcmd)
        return;
    }
}

#endif