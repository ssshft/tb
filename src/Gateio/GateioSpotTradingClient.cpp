#include "api/Gateio/GateioSpotTradingClient.h"


GateioSpotTradingClient::GateioSpotTradingClient(){

}

GateioSpotTradingClient::~GateioSpotTradingClient(){
//    delete smc;
}

bool GateioSpotTradingClient::Initialize(AccountCfg& cfg, sm::SecurityManager *smc){
    this->smc = smc;
    m_balanceUrl = "/api/v4/spot/accounts";
    m_orderUrl = "/api/v4/spot/orders";
    m_unifiedUrl = "/api/v4/unified/accounts";
//    m_cancelOrderUrl = "/spot/orders/";
    m_curcfg = cfg;
    // addNewOrderRestClient = new http_client(m_curcfg.restBaseUrl);
    // cancelOrderRestClient = new http_client(m_curcfg.restBaseUrl);
//    smc = new sm::SecurityManager(curcfg.smcIp, curcfg.smcPort);
    // hotHttpClient = new http_client(m_curcfg.restBaseUrl);
    hotHttpClient = new crypto::RestClientCPP(m_curcfg.restBaseUrl.c_str());
    return true;
}

void GateioSpotTradingClient::Run() {
    std::thread monitorThread(&GateioSpotTradingClient::monitor, this);
    monitorThread.detach();
}

void GateioSpotTradingClient::sub_websocket()//{// websocket_client websocket_callback_client &wsclient)
try{
    uri_builder builder(m_curcfg.wsBaseUrl);

    wsClient.close();
    wsClient.connect(builder.to_string())
    .then([&]() {
        std::function<void (const websocket_incoming_message &msg)> f;
        f = std::bind(&GateioSpotTradingClient::on_websocket_msg, this, placeholders::_1);
        wsClient.set_message_handler(f);
        std::function<void (websocket_close_status close_status,
        const utility::string_t& reason, const std::error_code& error)> c;
        c =  std::bind(&GateioSpotTradingClient::on_close_msg,this
                ,placeholders::_1,placeholders::_2,placeholders::_3);
        wsClient.set_close_handler(c);
    })
    .wait();

    #ifdef USE_UNIFIED  // 统一账户不需要订阅现货的balance推送
    // websocket_outgoing_message unifiedBalanceOutMsg = sub_unified_balance_channel();   
    // wsClient.send(unifiedBalanceOutMsg).wait(); 
    #else
    websocket_outgoing_message balanceOutMsg = sub_balance_channel();
    wsClient.send(balanceOutMsg).wait();
    #endif

    #ifdef USE_WEBSOCKET_API
    websocket_outgoing_message loginOutMsg  = sub_login();
    wsClient.send(loginOutMsg).wait();
    #else
    websocket_outgoing_message orderOutMsg   = sub_orders_channel();
    wsClient.send(orderOutMsg).wait();
    #endif
    // websocket_outgoing_message tradesOutMsg  = sub_trades_channel();
    // wsClient.send(tradesOutMsg).wait();

    m_IsConnected = true;
    LOG_INFO("connected with gateio spot api: %s", builder.to_string().c_str());
//    TIMERTAIL(m_curcfg.accountid,"SubWebSocket");
}
catch(exception &e) {
    m_IsConnected = false;
    LOG_ERROR("%s", e.what());
}

websocket_outgoing_message GateioSpotTradingClient::sub_orders_channel() {
     string channel = "spot.orders";
    websocket_outgoing_message outMsg;
    json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = json::value::string(channel);
    subValue["event"] = json::value::string("subscribe");
    subValue["payload"][0] = json::value::string("BTC_USDT");
    //If you want to subscribe to all orders updates in all currency pairs,
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

websocket_outgoing_message GateioSpotTradingClient::sub_balance_channel() {
     string channel = "spot.balances";
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

#ifdef USE_WEBSOCKET_API
websocket_outgoing_message GateioSpotTradingClient::sub_login() {
    string channel = "spot.login";
    websocket_outgoing_message outMsg;
    json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = json::value::string(channel);
    subValue["event"] = json::value::string("api");

    string time = to_string(crypto::getCurrentTimeSeconds());
    string sign = get_signature_ws_api(channel.c_str(), "api", time.c_str());

    string reqId = to_string(crypto::getCurrentTimeMilli());

    json::value payloadValue;
    payloadValue["api_key"] = json::value::string(m_curcfg.apiKey);
    payloadValue["signature"] = json::value::string(sign);
    payloadValue["timestamp"] = json::value::string(time);
    payloadValue["req_id"] = json::value::string(reqId);

    subValue["payload"] = payloadValue;

    outMsg.set_utf8_message(subValue.serialize().c_str());
    return outMsg;
}
#endif

void GateioSpotTradingClient::monitor() {
    while(1){
        try{
            LOG_INFO("start to connect with gateio spot api");
            sub_websocket();
            sleep(5);
            while(m_IsConnected){
                sleep(10);
                if(!m_IsConnected){
                    LOG_ERROR("gateio spot ws disconnected, will reconnect it now");
                    break;
                }
                else{
//                    LOG_DEBUG("gateio spot ws is conntected now, will send ping to it!");
                    ping();
                }
            }
        }
        catch(exception &e){
            LOG_ERROR("%s", e.what());
        }
    }
}

void GateioSpotTradingClient::on_websocket_msg(const websocket_incoming_message& msg)
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
        DEBUGLINE
    }
}
catch(exception &e){
    std::cout << "SubWebSocket Error:" << e.what() << endl;
    LOG_ERROR("%s", e.what());
    m_IsConnected = false;
}


void GateioSpotTradingClient::on_close_msg(websocket_close_status close_status,
                  const utility::string_t& reason, const std::error_code& error)
try{
    // fprintf(stderr, "I am happy,%s,%d\n", __FUNCTION__ , __LINE__);
    m_IsConnected = false;
    LOG_ERROR("Spot recv CloseMsg, reason:%s ",reason.c_str() );
}
catch (exception &e){
    LOG_ERROR("%s", e.what());
}


string GateioSpotTradingClient::get_signature_ws(const char *channel, const char *event, const char * time){
    string s("");
    s.append("channel=").append(channel).append("&event=")
            .append(event).append("&time=").append(time);

    string hmacsha512hex = crypto::encryptWithHMACForGateio(m_curcfg.apiSecret, s);
    return hmacsha512hex;
}


string GateioSpotTradingClient::get_signature_rest(const char *method, const char *url, const char * time,
                                              const char *queryString , const char *payloadString ){

    string s("");
    string hashed_payload = crypto::sha512(payloadString);
    s.append(method).append("\n").append(url).append("\n")
            .append(queryString).append("\n").append(hashed_payload).append("\n").append(time) ;
    // LOG_DEBUG("get_signature_rest:%s", s.c_str());
    string hmacsha512hex = crypto::encryptWithHMACForGateio(m_curcfg.apiSecret, s);
    return hmacsha512hex;

}

string GateioSpotTradingClient::get_signature_ws_api(const char *channel, const char *event, const char * time, const char* reqPara){
    string s("");
    s.append(event).append("\n").append(channel).append("\n").append(reqPara).append("\n").append(time);
    string hmacsha512hex = crypto::encryptWithHMACForGateio(m_curcfg.apiSecret, s);
    return hmacsha512hex;
}


void GateioSpotTradingClient::ping(){
    if(m_IsConnected){
        websocket_outgoing_message outMsg;
        json::value spotPingSubValue;
        spotPingSubValue["time"] = crypto::getCurrentTimeSeconds();
        spotPingSubValue["channel"] = json::value::string("spot.ping");
//        LOG_DEBUG( "%s", spotPingSubValue.serialize().c_str());
        outMsg.set_utf8_message(spotPingSubValue.serialize().c_str());
        wsClient.send(outMsg).wait();
    }
}

void GateioSpotTradingClient::pong(){
    if(m_IsConnected){
        websocket_outgoing_message outMsg;
        outMsg.set_pong_message();
        wsClient.send(outMsg).wait();
    }
}

bool GateioSpotTradingClient::get_balances()//vector<Balance> &balanceVec
try {
    // http_client restclient(m_curcfg.restBaseUrl);//bUrl
    http_request request(methods::GET);
    // request.headers().add("Accept","application/json");
    // request.headers().add("Content-Type","application/json");
    string queryStr = "";
    string time = to_string(crypto::getCurrentTimeSeconds() );
//    cout << m_curcfg.restBaseUrl<< m_balanceUrl.to_string() << endl;
    string sign = get_signature_rest("GET", m_balanceUrl.to_string().c_str(), time.c_str(), "", "");

    request.headers().add("KEY",m_curcfg.apiKey);
    request.headers().add("Timestamp",time);
    request.headers().add("SIGN",sign);
    uri_builder builder(m_balanceUrl);// bUrl m_balanceUrl

    request.set_request_uri(builder.to_string());
    // restclient.request(request)
    const http_response &response = hotHttpClient->request(request);
    auto code = response.status_code();
    if(code == status_codes::OK || code == status_codes::BadRequest
        || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
        json::value const &v = response.extract_json().get();
        LOG_INFO("get_balances: %s", v.serialize().c_str());
        if(v.is_array()){
            auto array = v.as_array();
            for(auto it : array){
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.header.cmdTime = crypto::getCurrentTime();
                rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                rcmd.header.instTypeEnum = InstType_SPOT;
                rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                strcpy(rcmd.header.accountId, m_curcfg.accountId);
                strcpy(rcmd.body.balance.currency, crypto::to_upper(it.at("currency").as_string().c_str()).c_str());
                rcmd.body.balance.available = stod(it.at("available").as_string().c_str());
                rcmd.body.balance.frozen    = stod(it.at("locked").as_string().c_str());
                rcmd.body.balance.total = rcmd.body.balance.available + rcmd.body.balance.frozen;
                if(crypto::is_zeronum(rcmd.body.balance.total)){
                    continue;
                }
                rcmd.body.balance.apiSourceEnum = ApiSource_REST;
                PUSH_RCMD(rcmd)
                if(crypto::str_cmp(rcmd.body.balance.currency, "USDT")){
                    LOG_INFO("exchId:%s,instType:%s,strategyId:%s,usdt available:%.2f",
                             ExchangeTypeEnum2StrMap[rcmd.header.exchangeTypeEnum].c_str(),
                             InstTypeEnum2StrMap[rcmd.header.instTypeEnum].c_str(),
                             rcmd.header.strategyId, rcmd.body.balance.available);
                }
            }
        }
    }
#if 0
    .then([&](const http_response &response) -> pplx::task<json::value> {
        auto code = response.status_code();
        if(code == status_codes::OK || code == status_codes::BadRequest
            /*|| code == status_codes::TooManyRequests || code == status_codes::Unauthorized*/)
            return response.extract_json();
        return pplx::task_from_result(json::value());
    })
    .then([&](pplx::task<json::value> previousTask) {
        json::value const &v = previousTask.get();
        if(v.is_array()){
            auto array = v.as_array();
            for(auto it : array){
                pubsub::RCommand rcmd;
                rcmd.header.cmdTime = crypto::getCurrentTime();
                rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                rcmd.header.instTypeEnum = InstType_SPOT;
                rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                strcpy(rcmd.header.accountId, m_curcfg.accountId);
                strcpy(rcmd.body.balance.currency, crypto::to_upper(it.at("currency").as_string().c_str()).c_str());
                rcmd.body.balance.available = stod(it.at("available").as_string().c_str());
                rcmd.body.balance.frozen    = stod(it.at("locked").as_string().c_str());
                rcmd.body.balance.total = rcmd.body.balance.available + rcmd.body.balance.frozen;
                if(crypto::is_zeronum(rcmd.body.balance.total)){
                    continue;
                }
                rcmd.body.balance.apiSourceEnum = ApiSource_REST;
                PUSH_RCMD(rcmd)
                if(crypto::str_cmp(rcmd.body.balance.currency, "USDT")){
                    LOG_INFO("exchId:%s,instType:%s,strategyId:%s,usdt available:%.2f",
                             ExchangeTypeEnum2StrMap[rcmd.header.exchangeTypeEnum].c_str(),
                             InstTypeEnum2StrMap[rcmd.header.instTypeEnum].c_str(),
                             rcmd.header.strategyId, rcmd.body.balance.available);
                }
            }
        }
        return true;
    })
    .wait();
#endif
    return true;
}
catch(exception &e){
    LOG_ERROR("%s", e.what());
    return false;
}

bool GateioSpotTradingClient::get_unified_account()
try {
    http_request request(methods::GET);
    string queryStr = "";
    string time = to_string(crypto::getCurrentTimeSeconds() );
    string sign = get_signature_rest("GET", m_unifiedUrl.to_string().c_str(), time.c_str(), "", "");

    request.headers().add("KEY",m_curcfg.apiKey);
    request.headers().add("Timestamp",time);
    request.headers().add("SIGN",sign);
    uri_builder builder(m_unifiedUrl);

    request.set_request_uri(builder.to_string());
    const http_response &response = hotHttpClient->request(request);
    auto code = response.status_code();
    if(code == status_codes::OK || code == status_codes::BadRequest
        || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
        json::value const &v = response.extract_json().get();
        LOG_INFO("get_unified_account: %s", v.serialize().c_str());
        
        auto balances = v.at("balances").as_object();
        for (auto iter = balances.begin(); iter != balances.end(); ++iter) {
            string currency = iter->first;
            double total = stod(iter->second.at("equity").as_string());
            double available = stod(iter->second.at("available").as_string());
            double freeze = stod(iter->second.at("freeze").as_string());

            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.header.cmdTime = crypto::getCurrentTime();
            rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
            rcmd.header.instTypeEnum = InstType_SPOT;
            rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
            strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
            strcpy(rcmd.header.accountId, m_curcfg.accountId);
            strcpy(rcmd.body.balance.currency, crypto::to_upper(currency).c_str());
            rcmd.body.balance.available = available;
            rcmd.body.balance.frozen    = freeze;
            rcmd.body.balance.total = total;
            if(crypto::is_zeronum(rcmd.body.balance.total)){
                continue;
            }
            rcmd.body.balance.apiSourceEnum = ApiSource_REST;
            PUSH_RCMD(rcmd)
        }

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.header.cmdTime = crypto::getCurrentTime();
        rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
        rcmd.header.instTypeEnum = InstType_SPOT;
        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
        strcpy(rcmd.header.accountId, m_curcfg.accountId);
        rcmd.body.totalAccount.totalEquity = stod(v.at("unified_account_total_equity").as_string());
        rcmd.body.totalAccount.adjEquity = stod(v.at("total_margin_balance").as_string());
        rcmd.body.totalAccount.mmr = stod(v.at("total_maintenance_margin").as_string());
        string mgnStr = v.at("total_maintenance_margin_rate").as_string();
        if (mgnStr != "") {
            rcmd.body.totalAccount.mgnRatio = stod(mgnStr);
        } else {
            rcmd.body.totalAccount.mgnRatio = 9999;
        }
        rcmd.body.totalAccount.apiSourceEnum = ApiSource_REST;
        LOG_INFO("rcmd: %s", rcmd.getString().c_str());
        PUSH_RCMD(rcmd)
        
    } else {
        LOG_INFO("get_unified_account request error! code:%d", code);
    }
    return true;
}
catch(exception &e){
    LOG_ERROR("get_unified_account %s", e.what());
    return false;
}

void GateioSpotTradingClient::add_new_order(pubsub::TCommand &tcmd){
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)
    if(!m_IsConnected){
        rcmd.body.newOrder.ErrorID = ERROR_TBDisconnectError;
        PUSH_RCMD(rcmd)
        return;
    }

    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.newOrder.instId, info)){
            // http_client restclient(m_curcfg.restBaseUrl);//bUrl
            http_request request(methods::POST);
            // request.headers().add("Accept","application/json");
            // request.headers().add("Content-Type","application/json");
            string queryStr = "";
            string time = to_string(crypto::getCurrentTimeSeconds());
            uri_builder builder(m_orderUrl);// bUrl m_balanceUrl
//            std::unordered_map<std::string, std::string> params;
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
//                builder.append_query("time_in_force","poc");
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

            string sign = get_signature_rest("POST", m_orderUrl.to_string().c_str(), time.c_str(),
                                             "", value.serialize().c_str());
            request.headers().add("KEY",m_curcfg.apiKey);
            request.headers().add("Timestamp",time);
            request.headers().add("SIGN",sign);

            request.set_body(value);
            request.set_request_uri(builder.to_string());
            LOG_DEBUG("add_new_order :%s%s,params:%s",m_curcfg.restBaseUrl.c_str(),m_orderUrl.to_string().c_str(), value.serialize().c_str());
            const http_response &response = hotHttpClient->request(request);
            rcmd.body.newOrder.tsNet = crypto::getCurrentTime();
            LOG_DEBUG("GATEIO,%s,internetDelay,%ld", __FUNCTION__, rcmd.body.newOrder.tsNet - rcmd.body.newOrder.tsSent);
            auto code = response.status_code();
            if(code == status_codes::Created || code == status_codes::OK || code == status_codes::BadRequest){
                string v = response.extract_string().get();
                LOG_DEBUG("add_new_order reponse: %s", v.c_str());
                rapidjson::Document d;
                rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(v.c_str());
                if(d.HasParseError()){
                    rcmd.body.newOrder.orderStatus =  OrderStatus_UNKNOWN;
                    PUSH_RCMD(rcmd)
                    return;
                }
                if(rawData.HasMember("status")){
                    rcmd.body.newOrder.orderStatus = crypto::get_gateio_orderstatus(rcmd.header.instTypeEnum, rawData);
                    strcpy(rcmd.body.newOrder.orderId, rawData["id"].GetString());
                    rcmd.body.newOrder.ErrorID = ERROR_NoError;
                    strcpy(rcmd.body.newOrder.orderSysId, rawData["text"].GetString());
                    rcmd.body.newOrder.volumeTotal = stod(rawData["amount"].GetString());
                    rcmd.body.newOrder.limitPrice = stod(rawData["price"].GetString());

                    if (rawData.HasMember("avg_deal_price")) {
                        rcmd.body.newOrder.tradePrice = stod(rawData["avg_deal_price"].GetString());
                    }
                    
                    double left = stod(rawData["left"].GetString());
                    left = left > 0 ? left : -left;
                    rcmd.body.newOrder.volumeTraded = rcmd.body.newOrder.volumeTotal - left;
                    PUSH_RCMD(rcmd)
                }
                else if(rawData.HasMember("label")){
                    rcmd.body.newOrder.ErrorID = crypto::get_gateio_errorid(v.c_str());
                    strncpy(rcmd.body.newOrder.originMsg, rawData["label"].GetString(), sizeof(rcmd.body.newOrder.originMsg));
                    rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN;
                    LOG_ERROR("%s",v.c_str());
                    PUSH_RCMD(rcmd)
                }
            }
            else{
                string v = response.extract_string().get();
                LOG_DEBUG("add_new_order reponse: %s", v.c_str());
                rapidjson::Document d;
                rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(v.c_str());
                if(d.HasParseError()){
                    rcmd.body.newOrder.orderStatus =  OrderStatus_UNKNOWN;
                    PUSH_RCMD(rcmd)
                    return;
                }
                // auto v = response.extract_json().get();
                rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED;
                rcmd.body.newOrder.ErrorID = code;
                if(rawData.HasMember("label")){
                    strncpy(rcmd.body.newOrder.originMsg, rawData["label"].GetString(), sizeof(rcmd.body.newOrder.originMsg));
                }
                PUSH_RCMD(rcmd)
            }
#if 0
            hotHttpClient->request(request)
            .then([&](const http_response &response) -> pplx::task<json::value> {
                DEBUGLOG
                auto code = response.status_code();
                rcmd.body.newOrder.tsNet = crypto::getCurrentTime();
                DEBUGLOG
                // if(code == status_codes::Created || code == status_codes::OK || code == status_codes::BadRequest){
                if( code == status_codes::OK ){
                    DEBUGLOG
                    return response.extract_json();
                }
                else{
                    auto v = response.extract_json().get();
                    rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED;
                    rcmd.body.newOrder.ErrorID = code;
                    if(v.has_field("label")){
                        strncpy(rcmd.body.newOrder.originMsg, v.at("label").as_string().c_str(), sizeof(rcmd.body.newOrder.originMsg));
                    }
                    LOG_ERROR("%s", v.serialize().c_str());
                    PUSH_RCMD(rcmd)
                    return pplx::task_from_result(json::value());
                }
            })
            .then([&](pplx::task<json::value> previousTask) {
                json::value const &v = previousTask.get();
                LOG_DEBUG("add_new_order reponse: %s", v.serialize().c_str());
                DEBUGLOG
                rapidjson::Document d;
                rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(v.serialize().c_str());
                DEBUGLOG
                if(rawData.HasMember("status")){
                    strcpy(rcmd.body.newOrder.orderId, rawData["id"].GetString());
                    // strcpy(rcmd.body.newOrder.orderSysId, rawData["text"].GetString());
                    rcmd.body.newOrder.orderStatus =  OrderStatus_REST_NEW;
                    rcmd.body.newOrder.volumeTotal = stod(rawData["amount"].GetString());
                    rcmd.body.newOrder.limitPrice = stod(rawData["price"].GetString());
                    DEBUGLOG
                    PUSH_RCMD(rcmd)
                    DEBUGLOG
                }
                else if(v.has_field("label")){
                    rcmd.body.newOrder.ErrorID = crypto::get_gateio_errorid(v.serialize().c_str());
                    strncpy(rcmd.body.newOrder.originMsg, v.at("label").as_string().c_str(), sizeof(rcmd.body.newOrder.originMsg));

                    LOG_ERROR("%s",v.serialize().c_str());
                    PUSH_RCMD(rcmd)
                }
            })
            .wait();
#endif
        }
        else{
            rcmd.body.newOrder.ErrorID = ERROR_SMCInstrumentNotExistError;
            // g_rptInnerQueue.push(rcmd);
            PUSH_RCMD(rcmd)
        }
    }
    catch(exception &e) {
        rcmd.body.newOrder.ErrorID = ERROR_NetworkError;
        strncpy(rcmd.body.newOrder.originMsg, e.what(), sizeof(rcmd.body.newOrder.originMsg));
        LOG_ERROR("%s", e.what());
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