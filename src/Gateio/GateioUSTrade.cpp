#include "api/Gateio/GateioUSwapTradingClient.h"

GateioUSwapTradingClient::GateioUSwapTradingClient(){

}

GateioUSwapTradingClient::~GateioUSwapTradingClient(){
//    delete smc;
}

bool GateioUSwapTradingClient::Initialize(AccountCfg& cfg, sm::SecurityManager *smc){
    this->smc = smc;
    m_positionsUrl = "/api/v4/futures/usdt/positions";
    m_positionUrl = "/api/v4/futures/usdt/positions/";
    m_accountsUrl = "/api/v4/futures/usdt/accounts";
    m_orderUrl = "/api/v4/futures/usdt/orders";

    // m_positionsUrlBtc = "/api/v4/futures/btc/positions";
    // m_accountsUrlBtc = "/api/v4/futures/btc/accounts";
    // m_orderUrlBtc = "/api/v4/futures/btc/orders";

    m_curcfg = cfg;
    // addNewOrderRestClient = new http_client(m_curcfg.restBaseUrl);
    // cancelOrderRestClient = new http_client(m_curcfg.restBaseUrl);
    // hotHttpClient = new http_client(m_curcfg.restBaseUrl);
    hotHttpClient = new crypto::RestClientCPP(m_curcfg.restBaseUrl.c_str());
    return true;
}

void GateioUSwapTradingClient::Run() {
    std::thread monitorThread(&GateioUSwapTradingClient::monitor, this);
    monitorThread.detach();
}

// string GateioUSwapTradingClient::get_signature_rest(const char *method, const char *url, const char * time,
//                                               const char *queryString , const char *payloadString ){

//     string s("");
//     string hashed_payload = crypto::sha512(payloadString);
//     s.append(method).append("\n").append(url).append("\n")
//             .append(queryString).append("\n").append(hashed_payload).append("\n").append(time) ;
//     LOG_DEBUG("%s:%s", __FUNCTION__, s.c_str());
//     string hmacsha512hex = crypto::encryptWithHMACForGateio(m_curcfg.apiSecret, s);
//     return hmacsha512hex;

// }

void GateioUSwapTradingClient::sub_websocket()
try{
    uri_builder builder(m_curcfg.wsBaseUrl);

    wsClient.close();
    wsClient.connect(builder.to_string())
    .then([&](){
        std::function<void (const websocket_incoming_message &msg)> f;
        f = std::bind(&GateioUSwapTradingClient::on_websocket_msg, this, placeholders::_1);
        wsClient.set_message_handler(f);
        std::function<void (websocket_close_status close_status,
        const utility::string_t& reason, const std::error_code& error)> c;
        c =  std::bind(&GateioUSwapTradingClient::on_close_msg,this ,placeholders::_1,placeholders::_2,placeholders::_3);
        wsClient.set_close_handler(c);
    })
    .wait();

    #ifdef USE_UNIFIED  // 统一账户不需要订阅合约balance推送
    #else
    websocket_outgoing_message balanceOutMsg = sub_balance_channel();
    wsClient.send(balanceOutMsg).wait();
    #endif
    // websocket_outgoing_message tradesOutMsg  = sub_trades_channel();
    // wsClientUsdt.send(tradesOutMsg).wait();
    websocket_outgoing_message positionsOutMsg  = sub_positions_channel();
    wsClient.send(positionsOutMsg).wait();

    #ifdef USE_WEBSOCKET_API
    websocket_outgoing_message loginOutMsg  = sub_login();
    wsClient.send(loginOutMsg).wait();
    #else
    websocket_outgoing_message orderOutMsg   = sub_orders_channel();
    wsClient.send(orderOutMsg).wait();
    #endif

    // websocket_outgoing_message positionCloseOutMsg  = sub_position_close_channel();
    // wsClientUsdt.send(positionCloseOutMsg).wait();
    LOG_INFO("connected with gateio usdt swap api: %s", builder.to_string().c_str());
    m_IsConnected = true;

    // else{
    //     try{
    //         uri_builder builderBtc(m_curcfg.wsBaseUrl + "btc");

    //         wsClientBtc.close();
    //         wsClientBtc.connect(builderBtc.to_string())
    //         .then([&](){
    //             std::function<void (const websocket_incoming_message &msg)> f;
    //             f = std::bind(&GateioUSwapTradingClient::on_websocket_msg, this, placeholders::_1);
    //             wsClientBtc.set_message_handler(f);
    //             std::function<void (websocket_close_status close_status,
    //             const utility::string_t& reason, const std::error_code& error)> c;
    //             c =  std::bind(&GateioUSwapTradingClient::on_close_msg_btc, this ,placeholders::_1,placeholders::_2,placeholders::_3);
    //             wsClientBtc.set_close_handler(c);
    //         })
    //         .wait();
    //         websocket_outgoing_message balanceOutMsg = sub_balance_channel();
    //         wsClientBtc.send(balanceOutMsg).wait();
    //         websocket_outgoing_message orderOutMsg   = sub_orders_channel();
    //         wsClientBtc.send(orderOutMsg).wait();
    //         // websocket_outgoing_message tradesOutMsg  = sub_trades_channel();
    //         // wsClientBtc.send(tradesOutMsg).wait();
    //         websocket_outgoing_message positionsOutMsg  = sub_positions_channel();
    //         wsClientBtc.send(positionsOutMsg).wait();
    //         // websocket_outgoing_message positionCloseOutMsg  = sub_position_close_channel();
    //         // wsClientBtc.send(positionCloseOutMsg).wait();
    //         LOG_INFO("connected with gateio swap api: %s", builderBtc.to_string().c_str());
    //         m_IsConnectedBtc = true;
    //     }
    //     catch(exception &e){
    //         m_IsConnectedBtc = false;
    //         LOG_ERROR("%s", e.what());
    //     }
    // }
}
catch(exception &e){
    m_IsConnected = false;
    LOG_ERROR("%s", e.what());
}

websocket_outgoing_message GateioUSwapTradingClient::sub_orders_channel() {
    string channel = "futures.orders";
    websocket_outgoing_message outMsg;
    json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = json::value::string(channel);
    subValue["event"] = json::value::string("subscribe");
    subValue["payload"][0] = json::value::string(m_curcfg.userId);
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

websocket_outgoing_message GateioUSwapTradingClient::sub_positions_channel() {
    string channel = "futures.positions";
    websocket_outgoing_message outMsg;
    json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = json::value::string(channel);
    subValue["event"] = json::value::string("subscribe");
    subValue["payload"][0] = json::value::string(m_curcfg.userId);
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

// websocket_outgoing_message GateioUSwapTradingClient::sub_position_close_channel() {
//     string channel = "futures.position_close";
//     websocket_outgoing_message outMsg;
//     json::value subValue;
//     subValue["time"] = crypto::getCurrentTimeSeconds();
//     subValue["channel"] = json::value::string(channel);
//     subValue["event"] = json::value::string("subscribe");
//     subValue["payload"][0] = json::value::string(m_curcfg.userId);
//     subValue["payload"][1] = json::value::string("!all");

//     string time = to_string(crypto::getCurrentTimeSeconds());
//     string sign = get_signature_ws(channel.c_str(), "subscribe", time.c_str());
//     json::value signValue;
//     signValue["method"] = json::value::string("api_key");
//     signValue["KEY"] = json::value::string(m_curcfg.apiKey);
//     signValue["SIGN"] = json::value::string(sign);
//     subValue["auth"] = signValue;
//     outMsg.set_utf8_message(subValue.serialize().c_str());
//     return outMsg;
// }


websocket_outgoing_message GateioUSwapTradingClient::sub_balance_channel() {
    string channel = "futures.balances";
    websocket_outgoing_message outMsg;
    json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = json::value::string(channel);
    subValue["event"] = json::value::string("subscribe");
    subValue["payload"][0] = json::value::string(m_curcfg.userId);
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

// websocket_outgoing_message GateioUSwapTradingClient::sub_trades_channel() {
//      string channel = "futures.usertrades";
//     websocket_outgoing_message outMsg;
//     json::value subValue;
//     subValue["time"] = crypto::getCurrentTimeSeconds();
//     subValue["channel"] = json::value::string(channel);
//     subValue["event"] = json::value::string("subscribe");
//     subValue["payload"][0] = json::value::string(m_curcfg.userId);
//     subValue["payload"][1] = json::value::string("!all");
//     string time = to_string(crypto::getCurrentTimeSeconds());
//     string sign = get_signature_ws(channel.c_str(), "subscribe", time.c_str());
//     json::value signValue;
//     signValue["method"] = json::value::string("api_key");
//     signValue["KEY"] = json::value::string(m_curcfg.apiKey);
//     signValue["SIGN"] = json::value::string(sign);
//     subValue["auth"] = signValue;
//     outMsg.set_utf8_message(subValue.serialize().c_str());
//     return outMsg;
// }

#ifdef USE_WEBSOCKET_API
websocket_outgoing_message GateioUSwapTradingClient::sub_login() {
    string channel = "futures.login";
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

void GateioUSwapTradingClient::monitor(){
    while(1){
        try{
            LOG_INFO("start to connect with gateio usdt swap api");
            sub_websocket();
            sleep(3);
            while (m_IsConnected){
                sleep(15);
                if (!m_IsConnected){
                    LOG_ERROR("gateio usdt swap ws disconnected, will reconnect it now");
                    break;
                }
                else{
                    ping();
                }
            }
        }
        catch(exception &e){
            LOG_ERROR("%s", e.what());
        }
        sleep(2);
    }
}

void GateioUSwapTradingClient::on_websocket_msg(const websocket_incoming_message& msg)
try{
    // text_message, binary_message, close, ping, pong
    if (msg.message_type() == websocket_message_type::text_message){
        msg.extract_string().then([&](const string s){
            LOG_DEBUG("on_websocket_msg:%s", s.c_str());
            rapidjson::Document d;
            rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(s.c_str());
            if(d.HasParseError() || !rawData.IsObject()){
                return;
            }

            #ifdef USE_WEBSOCKET_API
            if (rawData.HasMember("request_id")) {
                const string &channel = rawData["header"]["channel"].GetString();
                if(crypto::str_cmp(channel.c_str(), "futures.order_place") == true || crypto::str_cmp(channel.c_str(), "futures.order_cancel") == true || crypto::str_cmp(channel.c_str(), "futures.order_status") == true) {
                    const rapidjson::Value &data = rawData["data"];

                    if (data.HasMember("result")) {
                        const rapidjson::Value &res = data["result"];
                        if (!res.HasMember("req_param")) {
                            string originInstId = res["contract"].GetString();
                            md::InstrumentInfo info;
                            if(this->smc->get_instrument_info("GATEIO", "InstType_USDT_SWAP", originInstId.c_str(), info)){
                                pubsub::RCommand rcmd;
                                memset(&rcmd, 0 , sizeof(pubsub::RCommand));
                                rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                                rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                                rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                                strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                                strcpy(rcmd.header.accountId, m_curcfg.accountId);
                                strcpy(rcmd.body.orderResponse.instId, info.instId);
                                if(res.HasMember("id")){
                                    strcpy(rcmd.body.orderResponse.orderId, res["id"].GetString());
                                }
                                if(res.HasMember("text")){
                                    strcpy(rcmd.body.orderResponse.orderSysId, res["text"].GetString());
                                }
                                if(res.HasMember("is_close")){
                                    rcmd.body.orderResponse.offsetFlag = res["is_close"].GetBool() == true ? OffsetFlag_CLOSE : OffsetFlag_OPEN;
                                }
                                if(res.HasMember("size")){
                                    double size  = stod(res["size"].GetString());
                                    rcmd.body.orderResponse.direction = size > 0 ? Direction_LONG : Direction_SHORT ;
                                    rcmd.body.orderResponse.volumeTotal  = size > 0 ? size : -size ;
                                }
                                if(res.HasMember("price")){
                                    rcmd.body.orderResponse.limitPrice = stod(res["price"].GetString());
                                }

                                if(res.HasMember("tif")){
                                    const string &tif = res["tif"].GetString();
                                    GET_ORDERTYPE(rcmd)
                                }

                                if(res.HasMember("left")){
                                    double left = stod(res["left"].GetString());
                                    left = left > 0 ? left : -left;
                                    //成交数量
                                    rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
                                }
                                //成交均价
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
                        memset(&rcmd, 0 , sizeof(pubsub::RCommand));
                        rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                        rcmd.header.instTypeEnum = InstType_USDT_SWAP;
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
                        rcmd.body.newOrder.ErrorID = crypto::get_gateio_errorid(label.c_str());
                        strncpy(rcmd.body.newOrder.originMsg, label.c_str(), sizeof(rcmd.body.newOrder.originMsg));
                        rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN;
                        PUSH_RCMD(rcmd)
                    }
                }
            }
            #endif


            //订阅成功的回报和ping pong之类的消息不用特别处理
            if(!rawData.HasMember("channel")){
                return;
            }
            const string &channel = rawData["channel"].GetString();
            //没有有效信息无需处理
            if(crypto::str_cmp(channel.c_str(), "futures.pong") == true){
                return;
            }

            if(!rawData.HasMember("event")
            || crypto::str_cmp(rawData["event"].GetString(), "update") == false){
                return;
            }
            
            if(crypto::str_cmp(channel.c_str(), "futures.orders") == true){
                const rapidjson::Value &data = rawData["result"];
                for(rapidjson::SizeType i = 0; i < data.Size(); i++){
                    if(!data[i].HasMember("contract")){
                        continue;
                    }
                    string originInstId = data[i]["contract"].GetString();
                    md::InstrumentInfo info;
                    if(this->smc->get_instrument_info("GATEIO", "InstType_USDT_SWAP", originInstId.c_str(), info)){
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0 , sizeof(pubsub::RCommand));
                        // rcmd.header.cmdTime = crypto::getCurrentTime();
                        rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                        rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                        // TODO
                        // if(crypto::has_str(originInstId.c_str(), "USDT")){
                        //     rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                        // }
                        // else{
                        //     rcmd.header.instTypeEnum = InstType_BTC_SWAP;
                        // }

                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        strcpy(rcmd.body.orderResponse.instId, info.instId);
                        if(data[i].HasMember("id")){
                            strcpy(rcmd.body.orderResponse.orderId, data[i]["id"].GetString());
                        }
                        if(data[i].HasMember("text")){
                            strcpy(rcmd.body.orderResponse.orderSysId, data[i]["text"].GetString());
                        }
                        if(data[i].HasMember("is_close")){
                            rcmd.body.orderResponse.offsetFlag = data[i]["is_close"].GetBool() == true ? OffsetFlag_CLOSE : OffsetFlag_OPEN;
                        }
                        if(data[i].HasMember("size")){
                            double size  = stod(data[i]["size"].GetString());
                            rcmd.body.orderResponse.direction = size > 0 ? Direction_LONG : Direction_SHORT ;
                            rcmd.body.orderResponse.volumeTotal  = size > 0 ? size : -size ;
                        }
                        if(data[i].HasMember("price")){
                            rcmd.body.orderResponse.limitPrice = stod(data[i]["price"].GetString());
                        }

                        if(data[i].HasMember("tif")){
                            const string &tif = data[i]["tif"].GetString();
                            GET_ORDERTYPE(rcmd)
                        }

                        if(data[i].HasMember("left")){
                            double left = stod(data[i]["left"].GetString());
                            left = left > 0 ? left : -left;
                            //成交数量
                            rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
                        }
                        //成交均价
                        if(data[i].HasMember("fill_price")){
                            rcmd.body.orderResponse.tradePrice = stod(data[i]["fill_price"].GetString());
                        }
                        rcmd.body.orderResponse.orderStatus = crypto::get_gateio_orderstatus(rcmd.header.instTypeEnum, data[i]);

                        PUSH_RCMD(rcmd)
                    }
                    else{
                        LOG_ERROR("not found GATEIO.SWAP.%s smc info", originInstId.c_str());
                    }
                }
            }
            // else if(crypto::str_cmp(channel.c_str(),"futures.usertrades") == true){
            //     const rapidjson::Value &data = rawData["result"];
            //     for(rapidjson::SizeType i = 0; i < data.Size(); i++){
            //         string originInstId = data[i]["contract"].GetString();
            //         md::InstrumentInfo info;
            //         if(this->smc->get_instrument_info("GATEIO", "InstType_USDT_SWAP", originInstId.c_str(), info)){
            //             pubsub::RCommand rcmd;
            //             memset(&rcmd,0,sizeof(pubsub::RCommand));
            //             rcmd.header.cmdTime = crypto::getCurrentTime();
            //             rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
            //             rcmd.header.instTypeEnum = InstType_USDT_SWAP;
            //             rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_TRADE;
            //             strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
            //             strcpy(rcmd.header.accountId, m_curcfg.accountId);
            //             strcpy(rcmd.body.trade.instId, info.instId);
            //             strcpy(rcmd.body.trade.tradeId, data[i]["id"].GetString());
            //             strcpy(rcmd.body.trade.orderId, data[i]["order_id"].GetString());
            //             if(data[i].HasMember("text")){
            //                 strcpy(rcmd.body.trade.orderSysId, data[i]["text"].GetString());
            //             }
            //             int size = stoi(data[i]["size"].GetString());
            //             rcmd.body.trade.volumeTraded = size > 0 ? size : -size;
            //             // cmd.body.trade.offsetFlag = OffsetFlag_OPEN;
            //             // cmd.body.trade.direction = size > 0 ? Direction_LONG : Direction_SHORT;
            //             rcmd.body.trade.tradePrice   = stod(data[i]["price"].GetString());
            //             //TODO
            //             // cmd.body.trade.tradeTime = stoll(data[i]["create_time_ms"].GetString())*1000;
            //             string maker = data[i]["role"].GetString();
            //             rcmd.body.trade.isMaker = maker[0] == 'm' ? true : false;
            //             g_rptInnerQueue.push(rcmd);
            //         }
            //         else{
            //             LOG_ERROR("not found GATEIO.SWAP.%s smc info", originInstId.c_str());
            //         }
            //     }
            // }
            else if(crypto::str_cmp(channel.c_str(),"futures.balances") == true){
                const rapidjson::Value &data = rawData["result"];
                for(rapidjson::SizeType i = 0; i < data.Size(); i++){
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    // rcmd.header.cmdTime = crypto::getCurrentTime();
                    rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                    rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                    strcpy(rcmd.header.accountId, m_curcfg.accountId);
                    // cmd.body.balance.updateTime = stoll(data[i]["time_ms"].GetString())*1000;
                    if(data[i].HasMember("text")){
                        const string &text = data[i]["text"].GetString();
                        rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                        // if(crypto::has_str(text.c_str(),"USDT")){
                        //     rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                        // }
                        // else{
                        //     rcmd.header.instTypeEnum = InstType_BTC_SWAP;
                        // }
                        if(data[i].HasMember("currency")){
                            string currencyUpper = crypto::to_upper(data[i]["currency"].GetString());
                            strcpy(rcmd.body.balance.currency, currencyUpper.c_str());
                        }
                        else if(crypto::has_str(text.c_str(), "USDT")){
                            strcpy(rcmd.body.balance.currency, "USDT");
                        }
                        else{
                            LOG_ERROR("%s",s.c_str());
                            continue;
                        }
                    }
                    if(data[i].HasMember("balance") && data[i]["balance"].IsString()){
                        rcmd.body.balance.total = stod(data[i]["balance"].GetString());
                    }
                    else{
                        LOG_ERROR("gateio balance fields error:%s", s.c_str());
                    }
                    rcmd.body.balance.apiSourceEnum = ApiSource_WEBSOCKET;
                    PUSH_RCMD(rcmd)
                }
            }
            else if(crypto::str_cmp(channel.c_str(),"futures.positions") == true){
                const rapidjson::Value &data = rawData["result"];
                for(rapidjson::SizeType i = 0; i < data.Size(); i++){
                    if(!data[i].HasMember("contract")){
                        continue;
                    }
                    const string &originInstId = data[i]["contract"].GetString();
                    md::InstrumentInfo info;
                    if(this->smc->get_instrument_info("GATEIO", "InstType_USDT_SWAP", originInstId.c_str(), info)) {
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        // rcmd.header.cmdTime = crypto::getCurrentTime();
                        rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                        rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                        // if(crypto::has_str(originInstId.c_str(), "USDT")){
                        //     rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                        // }
                        // else{
                        //     rcmd.header.instTypeEnum = InstType_BTC_SWAP;
                        // }
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        strcpy(rcmd.body.position.instId, info.instId);
                        if(data[i].HasMember("size")){
                            int size = stoi(data[i]["size"].GetString());
                            rcmd.body.position.direction = size >= 0 ? Direction_LONG : Direction_SHORT;
                            rcmd.body.position.volume = size >= 0 ? size : -size;
                        }
                        if(data[i].HasMember("margin")){
                            rcmd.body.position.maintMargin = stod(data[i]["margin"].GetString());
                        }
                        if(data[i].HasMember("entry_price")){
                            rcmd.body.position.avgPrice = stod(data[i]["entry_price"].GetString());
                        }
                        if(data[i].HasMember("unrealised_pnl")){
                            rcmd.body.position.unrealizedPnl = stod(data[i]["unrealised_pnl"].GetString());
                        }
                        if(data[i].HasMember("liq_price")){
                            rcmd.body.position.liquidPrice = stod(data[i]["liq_price"].GetString());
                        }
                        if(data[i].HasMember("mark_price")){
                            rcmd.body.position.markPrice = stod(data[i]["mark_price"].GetString());
                        }
                        rcmd.body.position.apiSourceEnum = ApiSource_WEBSOCKET;
                        PUSH_RCMD(rcmd)
                    }
                    else{
                        LOG_ERROR("not found GATEIO.SWAP.%s smc info", originInstId.c_str());
                    }
                }
            }
            else if(crypto::str_cmp(channel.c_str(),"futures.position_closes") == true){
                // const rapidjson::Value &data = rawData["result"];
                // for(rapidjson::SizeType i = 0; i < data.Size(); i++){

                // }
            }
            else{
                LOG_ERROR("%s",s.c_str());
                return;
            }
        });
    }
    else if(msg.message_type() == websocket_message_type::ping){
        // DEBUGLINE
    }
    else if(msg.message_type() == websocket_message_type::close){
    //    DEBUGLINE
        m_IsConnected = false;
    }
}
catch(exception &e){
    m_IsConnected = false;
    LOG_ERROR("%s", e.what());
}

void GateioUSwapTradingClient::on_close_msg(websocket_close_status close_status,
                  const utility::string_t& reason, const std::error_code& error)
try{
    m_IsConnected = false;
    LOG_ERROR("swap recv CloseMsg, reason:%s, code:%d ",reason.c_str(), error.value());
}
catch (exception &e){
    LOG_ERROR("%s", e.what());
}

void GateioUSwapTradingClient::ping(){
    try{
        websocket_outgoing_message outMsg;
        json::value swapPingSubValue ;
        swapPingSubValue ["time"] = crypto::getCurrentTimeSeconds();
        swapPingSubValue ["channel"] = json::value::string("futures.ping");
        outMsg.set_utf8_message(swapPingSubValue .serialize().c_str());
        wsClient.send(outMsg).wait();
    }
    catch(exception &e){
        m_IsConnected = false;
        LOG_ERROR("%s", e.what());
    }
}

bool GateioUSwapTradingClient::get_position(pubsub::TCommand &tcmd)
try {
    // http_client restclient(m_curcfg.restBaseUrl);//bUrl
    http_request request(methods::GET);
    request.headers().add("Accept","application/json");
    request.headers().add("Content-Type","application/json");
    string queryStr = "";
    string time = to_string(crypto::getCurrentTimeSeconds() );
    md::InstrumentInfo info;
    string pUrl = m_positionUrl.to_string();
    if(!smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.queryPosition.instId, info)) {
        // builder.append_query("symbol", info.originInstId);//
        return false;
    }
    pUrl.append(info.originInstId);
    string sign = get_signature_rest("GET", pUrl.c_str(), time.c_str(), "", "");

    request.headers().add("KEY",m_curcfg.apiKey);
    request.headers().add("Timestamp",time);
    request.headers().add("SIGN",sign);

    web::uri pUrlw = pUrl;
    uri_builder builder(pUrlw);// bUrl m_balanceUrl

    request.set_request_uri(builder.to_string());
    // restclient.request(request)
    const http_response &response = hotHttpClient->request(request);
    // const http_response &response = restclient.request(request).get();
    auto code = response.status_code();
    if(code == status_codes::OK || code == status_codes::BadRequest
        || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
        json::value const &v = response.extract_json().get();
        LOG_DEBUG("get_position:%s", v.serialize().c_str());
        if(v.has_field("label")){
            string label = v.at("label").as_string();
            if(crypto::str_cmp(label.c_str(), "POSITION_NOT_FOUND")){
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                // rcmd.header.cmdTime = crypto::getCurrentTime();
                rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                strcpy(rcmd.header.accountId, m_curcfg.accountId);
                strcpy(rcmd.body.position.instId, info.instId);
                rcmd.body.position.direction = Direction_LONG;
                PUSH_RCMD(rcmd)
                return true;
            }
            else{
                LOG_ERROR("%s",v.serialize().c_str());
                return false;
            }
        }
        else{
            int size = v.at("size").as_integer();

            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            // rcmd.header.cmdTime = crypto::getCurrentTime();
            rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
            rcmd.header.instTypeEnum = InstType_USDT_SWAP;
            rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
            strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
            strcpy(rcmd.header.accountId, m_curcfg.accountId);
            strcpy(rcmd.body.position.instId, info.instId);

            rcmd.body.position.direction = size >= 0 ? Direction_LONG : Direction_SHORT;
            rcmd.body.position.volume = size >= 0 ? size : -size;
            if(v.has_field("margin")) {
                rcmd.body.position.maintMargin = stod(v.at("margin").as_string().c_str());
            }
            if(v.has_field("entry_price")) {
                rcmd.body.position.avgPrice = stod(v.at("entry_price").as_string().c_str());
            }
            if(v.has_field("unrealised_pnl")) {
                rcmd.body.position.unrealizedPnl = stod(v.at("unrealised_pnl").as_string().c_str());
            }
            if(v.has_field("liq_price")) {
                rcmd.body.position.liquidPrice = stod(v.at("liq_price").as_string().c_str());
            }
            if(v.has_field("mark_price")) {
                rcmd.body.position.markPrice = stod(v.at("mark_price").as_string().c_str());
            }
            if(v.has_field("adl_ranking")) {
                double adl = stod(v.at("adl_ranking").serialize().c_str());
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
            }
            rcmd.body.position.apiSourceEnum = ApiSource_REST;
            PUSH_RCMD(rcmd)

            return true;
        
        }
    }
    else{
        LOG_ERROR("%s,%s", __FUNCTION__, response.extract_string().get().c_str());
    }
    return true;
}
catch(exception &e){
    LOG_ERROR("%s", e.what());
    return false;
}

bool GateioUSwapTradingClient::get_positions()
try {
    // http_client restclient(m_curcfg.restBaseUrl);//bUrl
    http_request request(methods::GET);
    request.headers().add("Accept","application/json");
    request.headers().add("Content-Type","application/json");
    string queryStr = "";
    string time = to_string(crypto::getCurrentTimeSeconds() );
    string sign = get_signature_rest("GET", m_positionsUrl.to_string().c_str(), time.c_str(), "", "");

    request.headers().add("KEY",m_curcfg.apiKey);
    request.headers().add("Timestamp",time);
    request.headers().add("SIGN",sign);

    uri_builder builder(m_positionsUrl);// bUrl m_balanceUrl

    request.set_request_uri(builder.to_string());
    // restclient.request(request)
    const http_response &response = hotHttpClient->request(request);
    // const http_response &response = restclient.request(request).get();
    auto code = response.status_code();
    if(code == status_codes::OK || code == status_codes::BadRequest
        || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
        unordered_map<string, pubsub::RCommand> mCurrentPositions;
        json::value const &v = response.extract_json().get();
        if(v.is_array()){
            auto array = v.as_array();
            int size = array.size();
            int i = 0;
            for(auto it : array){
                i++;
                string originInstId = it["contract"].as_string();
                md::InstrumentInfo info;
                if(smc->get_instrument_info("GATEIO", "InstType_USDT_SWAP", originInstId.c_str(),info)) {
                    int size = it["size"].as_integer();
                    if(crypto::is_zeronum(size)){
                        continue;
                    }
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    // rcmd.header.cmdTime = crypto::getCurrentTime();
                    rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                    rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                    rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                    strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                    strcpy(rcmd.header.accountId, m_curcfg.accountId);
                    strcpy(rcmd.body.position.instId, info.instId);

                    rcmd.body.position.direction = size > 0 ? Direction_LONG : Direction_SHORT;
                    rcmd.body.position.volume = size > 0 ? size : -size;
                    if(it.has_field("margin")) {
                        rcmd.body.position.maintMargin = stod(it["margin"].as_string().c_str());
                    }
                    if(it.has_field("entry_price")) {
                        rcmd.body.position.avgPrice = stod(it["entry_price"].as_string().c_str());
                    }
                    if(it.has_field("unrealised_pnl")) {
                        rcmd.body.position.unrealizedPnl = stod(it["unrealised_pnl"].as_string().c_str());
                    }
                    if(it.has_field("liq_price")) {
                        rcmd.body.position.liquidPrice = stod(it["liq_price"].as_string().c_str());
                    }
                    if(it.has_field("mark_price")) {
                        rcmd.body.position.markPrice = stod(it["mark_price"].as_string().c_str());
                    }
                    if(it.has_field("adl_ranking")) {
                        double adl = stod(it["adl_ranking"].serialize().c_str());
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
                        // rcmd.body.position.adlQuantile = stod(it["adl_ranking"].serialize().c_str());
                    }
                    rcmd.body.position.apiSourceEnum = ApiSource_REST;

                    if (i == size) {
                        rcmd.body.position.isLast = 1;
                    } else {
                        rcmd.body.position.isLast = 0;
                    }
                    
                    PUSH_RCMD(rcmd)
                }
            }
        }
        else{
            LOG_ERROR("%s,%s",__FUNCTION__, v.serialize().c_str());
        }
        return true;
    }
    else{
        LOG_ERROR("%s,%s", __FUNCTION__, response.extract_string().get().c_str());
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
        try{
            json::value const &v = previousTask.get();
            if(v.is_array()){
                auto array = v.as_array();
                for(auto it : array){
                    string originInstId = it["contract"].as_string();
                    md::InstrumentInfo info;
                    if(smc->get_instrument_info("GATEIO", "InstType_USDT_SWAP", originInstId.c_str(),info)) {
                        int size = it["size"].as_integer();
                        if(crypto::is_zeronum(size)){
                            continue;
                        }
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        // rcmd.header.cmdTime = crypto::getCurrentTime();
                        rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                        rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        strcpy(rcmd.body.position.instId, info.instId);

                        rcmd.body.position.direction = size > 0 ? Direction_LONG : Direction_SHORT;
                        rcmd.body.position.volume = size > 0 ? size : -size;
                        if(it.has_field("margin")) {
                            rcmd.body.position.maintMargin = stod(it["margin"].as_string().c_str());
                        }
                        if(it.has_field("entry_price")) {
                            rcmd.body.position.avgPrice = stod(it["entry_price"].as_string().c_str());
                        }
                        if(it.has_field("unrealised_pnl")) {
                            rcmd.body.position.unrealizedPnl = stod(it["unrealised_pnl"].as_string().c_str());
                        }
                        if(it.has_field("liq_price")) {
                            rcmd.body.position.liquidPrice = stod(it["liq_price"].as_string().c_str());
                        }
                        if(it.has_field("mark_price")) {
                            rcmd.body.position.markPrice = stod(it["mark_price"].as_string().c_str());
                        }
                        if(it.has_field("adl_ranking")) {
                            rcmd.body.position.adlQuantile = stod(it["adl_ranking"].serialize().c_str());
                        }
                        rcmd.body.position.apiSourceEnum = ApiSource_REST;
                        PUSH_RCMD(rcmd)
                        // string instId = rcmd.body.position.instId;
                        // if(g_filterSymbolsMap.count(instId) > 0){
                        //     g_rptInnerQueue.push(rcmd);
                        // }
                    }
                }
            }
            else{
                // LOG_ERROR("%s",v.serialize().c_str());
            }
            return true;
        }
        catch (exception &e){
            LOG_ERROR("%s",e.what());
            return false;
        }
    })
    .wait();
#endif
    return true;
}
catch(exception &e){
    LOG_ERROR("%s", e.what());
    return false;
}

bool GateioUSwapTradingClient::get_account(pubsub::TCommand &tcmd)
try {
    // http_client restclient(m_curcfg.restBaseUrl);//bUrl
    http_request request(methods::GET);
    request.headers().add("Accept","application/json");
    request.headers().add("Content-Type","application/json");
    string queryStr = "";
    string time = to_string(crypto::getCurrentTimeSeconds() );

    string sign = get_signature_rest("GET", m_accountsUrl.to_string().c_str(), time.c_str(), "", "");

    request.headers().add("KEY",m_curcfg.apiKey);
    request.headers().add("Timestamp",time);
    request.headers().add("SIGN",sign);
    uri_builder builder(m_accountsUrl);

    request.set_request_uri(builder.to_string());
    // restclient.request(request)
    const http_response &response = hotHttpClient->request(request);
    // const http_response &response = restclient.request(request).get();
    auto code = response.status_code();
    if(code == status_codes::OK || code == status_codes::BadRequest
            || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
        json::value const &v = response.extract_json().get();
        LOG_INFO("get account:%s", v.serialize().c_str());
        if(v.has_field("currency")){
            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.header.cmdTime = crypto::getCurrentTime();
            rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
            rcmd.header.instTypeEnum = InstType_USDT_SWAP;
            rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
            strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
            strcpy(rcmd.header.accountId, m_curcfg.accountId);
            string ccy = crypto::to_upper(v.at("currency").as_string().c_str());

            strcpy(rcmd.body.balance.currency, ccy.c_str());
            // cmd.body.balance.updateTime = crypto::getCurrentTime();
            rcmd.body.balance.total = stod(v.at("total").as_string().c_str());
            rcmd.body.balance.unrealizedPnl = stod(v.at("unrealised_pnl").as_string().c_str());
            rcmd.body.balance.available = stod(v.at("available").as_string().c_str());
            rcmd.body.balance.frozen = stod(v.at("order_margin").as_string().c_str()) +
                    stod(v.at("position_margin").as_string().c_str());
            rcmd.body.balance.apiSourceEnum = ApiSource_REST;
            PUSH_RCMD(rcmd)

            if(crypto::str_cmp(rcmd.body.balance.currency, "USDT")){
                LOG_INFO("exchId:%s,instType:%s,accountId:%s,strategyId:%s,usdt available:%.2f,frozen:%.2f",
                    ExchangeTypeEnum2StrMap[rcmd.header.exchangeTypeEnum].c_str(),
                    InstTypeEnum2StrMap[rcmd.header.instTypeEnum].c_str(),
                    rcmd.header.accountId, rcmd.header.strategyId,
                    rcmd.body.balance.available, rcmd.body.balance.frozen
                );
            }
        }
        else{
            LOG_ERROR("%s", v.serialize().c_str());
        }
        return true;
    }
    else{
        LOG_ERROR("%s,%s",__FUNCTION__, response.extract_string().get().c_str());
    }
#if 0
    hotHttpClient->request(request)
    .then([&](const http_response &response) -> pplx::task<json::value> {
        auto code = response.status_code();
        if(code == status_codes::OK || code == status_codes::BadRequest
            || code == status_codes::TooManyRequests || code == status_codes::Unauthorized)
            return response.extract_json();
        return pplx::task_from_result(json::value());
    })
    .then([&](pplx::task<json::value> previousTask) {
        json::value const &v = previousTask.get();
        if(v.has_field("currency")){
            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.header.cmdTime = crypto::getCurrentTime();
            rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
            rcmd.header.instTypeEnum = InstType_USDT_SWAP;
            rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
            strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
            strcpy(rcmd.header.accountId, m_curcfg.accountId);
            string ccy = crypto::to_upper(v.at("currency").as_string().c_str());
            strcpy(rcmd.body.balance.currency, ccy.c_str());
            // cmd.body.balance.updateTime = crypto::getCurrentTime();
            rcmd.body.balance.total = stod(v.at("total").as_string().c_str());
            rcmd.body.balance.unrealizedPnl = stod(v.at("unrealised_pnl").as_string().c_str());
            rcmd.body.balance.available = stod(v.at("available").as_string().c_str());
            rcmd.body.balance.frozen = stod(v.at("order_margin").as_string().c_str()) +
                    stod(v.at("position_margin").as_string().c_str());
            rcmd.body.balance.apiSourceEnum = ApiSource_REST;
            PUSH_RCMD(rcmd)

            if(crypto::str_cmp(rcmd.body.balance.currency, "USDT")){
                LOG_INFO("exchId:%s,instType:%s,accountId:%s,strategyId:%s,usdt available:%.2f,frozen:%.2f",
                    ExchangeTypeEnum2StrMap[rcmd.header.exchangeTypeEnum].c_str(),
                    InstTypeEnum2StrMap[rcmd.header.instTypeEnum].c_str(),
                    rcmd.header.accountId, rcmd.header.strategyId,
                    rcmd.body.balance.available, rcmd.body.balance.frozen
                );
            }
        }
        else{
            // LOG_ERROR("%s", v.serialize().c_str());
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

void GateioUSwapTradingClient::add_new_order(pubsub::TCommand &tcmd){
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (m_IsConnected == false){//&& m_IsConnectedBtc == false
        rcmd.body.newOrder.ErrorID = ERROR_TBDisconnectError;
        PUSH_RCMD(rcmd)
        return;
    }

    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.newOrder.instId, info)){
            // http_client restclient(m_curcfg.restBaseUrl);
            http_request request(methods::POST);
            request.headers().add("Accept","application/json");
            request.headers().add("Content-Type","application/json");
            string queryStr = "";
            string time = to_string(crypto::getCurrentTimeSeconds());

            static uri_builder builder(m_orderUrl);
            json::value value;

            value["text"] = json::value::string(tcmd.body.newOrder.orderSysId);
            value["contract"] = json::value::string(info.originInstId);

            value["price"] = json::value::string(crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice, info.tickSize));

            if(tcmd.body.newOrder.orderType == OrderType_LIMIT){
                value["tif"] = json::value::string("gtc");
            }
            else if(tcmd.body.newOrder.orderType == OrderType_IOC){
                value["tif"] = json::value::string("ioc");
            }
            else if(tcmd.body.newOrder.orderType == OrderType_POST_ONLY){
                value["tif"] = json::value::string("poc");
            }
            else if(tcmd.body.newOrder.orderType == OrderType_FOK){
                value["tif"] = json::value::string("ioc");
            }
            else if(tcmd.body.newOrder.orderType == OrderType_MARKET){
                value["tif"] = json::value::string("ioc");
                value["price"] =json::value::string("0");
            }
            else{
                rcmd.body.newOrder.ErrorID = ERROR_OrderTypeError;
                PUSH_RCMD(rcmd)
                return;
            }

            //open
            if(tcmd.body.newOrder.offsetFlag == OffsetFlag_OPEN){
                //交易数量，正数为买入，负数为卖出。平仓委托则设置为0。
                if(tcmd.body.newOrder.direction == Direction_LONG){
                    value["size"] = json::value::string(crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal, info.lotSize));
                }
                else if(tcmd.body.newOrder.direction == Direction_SHORT){
                    value["size"] = json::value::string(crypto::getFixedPrecision(-tcmd.body.newOrder.volumeTotal, info.lotSize));
                }
                else{
                    rcmd.body.newOrder.ErrorID = ERROR_DirectionError;
                    PUSH_RCMD(rcmd)
                    return;
                }
            }//close
            else if(tcmd.body.newOrder.offsetFlag == OffsetFlag_CLOSE){
                //交易数量，close与open相反 正数为买入，负数为卖出。平仓委托则设置为0。
                if(tcmd.body.newOrder.direction == Direction_LONG){
                    value["size"] = json::value::string(crypto::getFixedPrecision(-tcmd.body.newOrder.volumeTotal, info.lotSize));
                }
                else if(tcmd.body.newOrder.direction == Direction_SHORT){
                    value["size"] = json::value::string(crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal, info.lotSize));
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
            value["reduce_only"] = tcmd.body.newOrder.reduceOnly ? json::value::string("true") : json::value::string("false");
            string sign = get_signature_rest("POST", m_orderUrl.to_string().c_str(), time.c_str(),
                                             "", value.serialize().c_str());

            request.headers().add("KEY",m_curcfg.apiKey);
            request.headers().add("Timestamp",time);
            request.headers().add("SIGN",sign);
            // request.headers()["KEY"] =  m_curcfg.apiKey;
            // request.headers()["Timestamp"] =  time;
            // request.headers()["SIGN"] =  sign;
            request.set_body(value);
            request.set_request_uri(builder.to_string());
            LOG_DEBUG("add_new_order url:%s%s, params:%s",m_curcfg.restBaseUrl.c_str(), m_orderUrl.to_string().c_str(), value.serialize().c_str());
            const http_response &response = hotHttpClient->request(request);
            if(tcmd.body.newOrder.clientOrderId == TESTCLIENTORDERID){
                return;
            }
            rcmd.body.newOrder.tsNet = crypto::getCurrentTime();
            LOG_DEBUG("GATEIO,%s,internetDelay,%ld", __FUNCTION__, rcmd.body.newOrder.tsNet - rcmd.body.newOrder.tsSent);

            // cout << response.to_string() << endl;
            // const http_response &response = addNewOrderRestClient->request(request).get();
            auto code = response.status_code();

            if(code == status_codes::Created || code == status_codes::OK
                    || code == status_codes::Unauthorized || code == status_codes::NotFound
                    || code == status_codes::BadRequest){
                const string &v = response.extract_string().get();
                LOG_DEBUG("add_new_order reponse: %s", v.c_str());
                rapidjson::Document d;
                rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(v.c_str());
                if(d.HasParseError()){
                    rcmd.body.newOrder.ErrorID = ERROR_UnknownError;
                    rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN;
                    PUSH_RCMD(rcmd)
                    return;
                }
                if(rawData.HasMember("id") ){
                    //有id说明下单成功了，没有的话说明失败了
                    string orderId = rawData["id"].GetString();//::to_string(v.at("id").as_number().to_int64());
                    strcpy(rcmd.body.newOrder.orderId, orderId.c_str());
                    rcmd.body.newOrder.ErrorID = ERROR_NoError;
                    double size = stod(rawData["size"].GetString());
                    rcmd.body.newOrder.volumeTotal = size > 0 ? size : -size;
                    rcmd.body.newOrder.limitPrice = stod(rawData["price"].GetString());
                    rcmd.body.newOrder.tradePrice = stod(rawData["fill_price"].GetString());//stod(v.at("fill_price").as_string().c_str());
                    double left = stod(rawData["left"].GetString());//v.at("left").as_double();
                    left = left > 0 ? left : -left;
                    rcmd.body.newOrder.volumeTraded = rcmd.body.newOrder.volumeTotal - left;
                    rcmd.body.newOrder.orderStatus = crypto::get_gateio_orderstatus(rcmd.header.instTypeEnum, rawData);

                    PUSH_RCMD(rcmd)
                    return;
                }
                else if(rawData.HasMember("label")){
                    //{"label":"INVALID_PARAM_VALUE","message":"text content not starting with `t-`"}
                    rcmd.body.newOrder.ErrorID = crypto::get_gateio_errorid(v.c_str());
                    strncpy(rcmd.body.newOrder.originMsg, rawData["label"].GetString(), sizeof(rcmd.body.newOrder.originMsg));
                    rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN;
                    PUSH_RCMD(rcmd)
                    return;
                }
                else{
                    LOG_ERROR("%s", v.c_str());
                }
            }
            else{
                rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN;
                rcmd.body.newOrder.ErrorID = code;
                const string &v = response.extract_string().get();
                LOG_ERROR("add_new_order reponse: %s", v.c_str());
                PUSH_RCMD(rcmd)
            }
#if 0
            hotHttpClient->hotHttpClient->request(request)
            .then([&](http_response response){
                rcmd.body.newOrder.tsNet = crypto::getCurrentTime();
                LOG_DEBUG("GATEIO,%s,internetDelay,%ld", __FUNCTION__, rcmd.body.newOrder.tsNet - rcmd.body.newOrder.tsSent);

                // cout << response.to_string() << endl;
                // const http_response &response = addNewOrderRestClient->request(request).get();
                auto code = response.status_code();

                if(code == status_codes::Created || code == status_codes::OK
                        || code == status_codes::Unauthorized || code == status_codes::NotFound
                        || code == status_codes::BadRequest){
                    const string &v = response.extract_string().get();
                    LOG_DEBUG("add_new_order reponse: %s", v.c_str());
                    rapidjson::Document d;
                    rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(v.c_str());
                    if(d.HasParseError()){
                        rcmd.body.newOrder.ErrorID = ERROR_UnknownError;
                        rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN;
                        PUSH_RCMD(rcmd)
                        return;
                    }
                    if(rawData.HasMember("id") ){
                        //有id说明下单成功了，没有的话说明失败了
                        string orderId = rawData["id"].GetString();//::to_string(v.at("id").as_number().to_int64());
                        strcpy(rcmd.body.newOrder.orderId, orderId.c_str());
                        rcmd.body.newOrder.ErrorID = ERROR_NoError;
                        double size = stod(rawData["size"].GetString());
                        rcmd.body.newOrder.volumeTotal = size > 0 ? size : -size;
                        rcmd.body.newOrder.limitPrice = stod(rawData["price"].GetString());
                        rcmd.body.newOrder.tradePrice = stod(rawData["fill_price"].GetString());//stod(v.at("fill_price").as_string().c_str());
                        double left = stod(rawData["left"].GetString());//v.at("left").as_double();
                        left = left > 0 ? left : -left;
                        rcmd.body.newOrder.volumeTraded = rcmd.body.newOrder.volumeTotal - left;
                        rcmd.body.newOrder.orderStatus = crypto::get_gateio_orderstatus(rcmd.header.instTypeEnum, rawData);

                        PUSH_RCMD(rcmd)
                        return;
                    }
                    else if(rawData.HasMember("label")){
                        //{"label":"INVALID_PARAM_VALUE","message":"text content not starting with `t-`"}
                        rcmd.body.newOrder.ErrorID = crypto::get_gateio_errorid(v.c_str());
                        strncpy(rcmd.body.newOrder.originMsg, rawData["label"].GetString(), sizeof(rcmd.body.newOrder.originMsg));
                        rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN;
                        PUSH_RCMD(rcmd)
                        return;
                    }
                    else{
                        LOG_ERROR("%s", v.c_str());
                    }
                }
                else{
                    rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN;
                    rcmd.body.newOrder.ErrorID = code;
                    const string &v = response.extract_string().get();
                    LOG_ERROR("add_new_order reponse: %s", v.c_str());
                    PUSH_RCMD(rcmd)
                }
            });

            hotHttpClient->request(request)
            .then([&](const http_response &response) -> pplx::task<json::value> { auto code = response.status_code();
                rcmd.body.newOrder.tsNet = crypto::getCurrentTime();
                //200 201 401 404
                if(code == status_codes::Created || code == status_codes::OK
                    || code == status_codes::Unauthorized || code == status_codes::NotFound
                    || code == status_codes::BadRequest){
                    return response.extract_json();
                }
                else{
                    auto s = response.extract_string().get();
                    rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN;
                    rcmd.body.newOrder.ErrorID = code;
                    strncpy(rcmd.body.newOrder.originMsg, s.c_str(), sizeof(rcmd.body.newOrder.originMsg));
                    LOG_ERROR("%s", s.c_str());
                    g_rptInnerQueue.push(rcmd);
                    return pplx::task_from_result(json::value());
                }
            }) // continue when the JSON value is available
            .then([&](pplx::task<json::value> previousTask) {
                // get the JSON value from the task and display content from it
                json::value const &v = previousTask.get();
                LOG_DEBUG("add_new_order reponse: %s", v.serialize().c_str());
                if(v.has_field("id") ){
                    //有id说明下单成功了，没有的话说明失败了
                    string orderId = std::to_string(v.at("id").as_number().to_int64());
                    strcpy(rcmd.body.newOrder.orderId, orderId.c_str());
                    strcpy(rcmd.body.newOrder.orderSysId, v.at("text").as_string().c_str());

                    rcmd.body.newOrder.ErrorID = ERROR_NoError;
                    double size = v.at("size").as_double();//stod(v.at("size").serialize().c_str());
                    rcmd.body.newOrder.volumeTotal = size > 0 ? size : -size;
                    rcmd.body.newOrder.limitPrice = stod(v.at("price").as_string().c_str());
                    rcmd.body.newOrder.tradePrice = stod(v.at("fill_price").as_string().c_str());
                    double left = v.at("left").as_double();
                    left = left > 0 ? left : -left;
                    rcmd.body.newOrder.volumeTraded = rcmd.body.newOrder.volumeTotal - left;

                    if(rcmd.body.newOrder.volumeTraded < ZERO_NUM){
                        rcmd.body.newOrder.orderStatus =  OrderStatus_REST_NEW;
                    }
                    else if(rcmd.body.newOrder.volumeTraded < rcmd.body.newOrder.volumeTotal){
                        if(tcmd.body.newOrder.orderType == OrderType_IOC){
                            rcmd.body.newOrder.orderStatus = OrderStatus_CANCELED;
                        }
                        else{
                            rcmd.body.newOrder.orderStatus =  OrderStatus_PARTFILLED;
                        }
                    }
                    else{
                        rcmd.body.newOrder.orderStatus =  OrderStatus_FILLED;
                    }

                    PUSH_RCMD(rcmd)
                    return;
                }
                else if(v.has_field("label")){
                    //{"label":"INVALID_PARAM_VALUE","message":"text content not starting with `t-`"}
                    rcmd.body.newOrder.ErrorID = crypto::get_gateio_errorid(v.serialize().c_str());
                    strncpy(rcmd.body.newOrder.originMsg, v.at("label").as_string().c_str(), sizeof(rcmd.body.newOrder.originMsg));
                    LOG_ERROR("%s",v.serialize().c_str());
                    PUSH_RCMD(rcmd)
                    return;
                }
                else{
                     LOG_ERROR("%s",v.serialize().c_str());
                }
            })
            .wait();
#endif
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

void GateioUSwapTradingClient::cancel_order(pubsub::TCommand &tcmd){
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.cancelOrder.instId, info)){
            // http_client restclient(m_curcfg.restBaseUrl);//bUrl
            http_request request(methods::DEL);
            // request.headers().add("Accept","application/json");
            // request.headers().add("Connection", "Keep-Alive");
            // request.headers().add("Keep-Alive", "timeout=90, max=100000");
            // request.headers().add("Content-Type","application/json");
            string queryStr{""};
            string time = to_string(crypto::getCurrentTimeSeconds());
            if(tcmd.body.cancelOrder.cancelOrderTypeEnum == pubsub::COT_ONE_INST){
                string cancelOrderUrl = m_orderUrl.to_string();
                if(crypto::str_cmp(tcmd.body.cancelOrder.orderId, "") == false){
                    cancelOrderUrl.append("/").append(tcmd.body.cancelOrder.orderId);
                }
                else if( tcmd.body.cancelOrder.clientOrderId != 0){
                    cancelOrderUrl.append("/").append(tcmd.body.cancelOrder.orderSysId);
                }
                else{
                    LOG_ERROR("cancel order need orderId or clientOrderId" );
                    strcpy(rcmd.body.cancelOrder.originMsg, "cancel order need orderId or clientOrderId");
                    rcmd.body.cancelOrder.ErrorID = ERROR_NoOrderIdError;
                    rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                    PUSH_RCMD(rcmd)
                    return;
                }
                string sign = get_signature_rest("DELETE", cancelOrderUrl.c_str(), time.c_str(), queryStr.c_str(), "");
                cancelOrderUrl.append("?").append(queryStr);
                web::uri m_cancelOrderUrl = cancelOrderUrl;
                uri_builder builder(m_cancelOrderUrl);
                request.headers().add("KEY", m_curcfg.apiKey);
                request.headers().add("Timestamp", time);
                request.headers().add("SIGN", sign);
                request.set_request_uri(builder.to_string());
                LOG_DEBUG("%s:%s",__FUNCTION__, builder.to_string().c_str());
                const http_response &response = hotHttpClient->request(request);
                // const http_response &response = restclient.request(request).get();
                rcmd.body.cancelOrder.tsNet = crypto::getCurrentTime();
                LOG_DEBUG("GATEIO,%s,internetDelay,%ld", __FUNCTION__, rcmd.body.cancelOrder.tsNet - rcmd.body.cancelOrder.tsSent);
                auto code = response.status_code();
                if(code == status_codes::Created || code == status_codes::OK
                    || code == status_codes::Unauthorized || code == status_codes::NotFound
                    || code == status_codes::BadRequest){
                    const string &v = response.extract_string().get();
                    LOG_DEBUG("cancel_order response:%s", v.c_str());
                    rapidjson::Document d;
                    rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(v.c_str());
                    if(d.HasParseError()){
                        rcmd.body.cancelOrder.ErrorID = ERROR_UnknownError;
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                        PUSH_RCMD(rcmd)
                        return;
                    }
                    if(rawData.HasMember("id") && rawData.HasMember("status")){
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_CANCELED;
                        string orderId = rawData["id"].GetString();//std::to_string(v.at("id").as_number().to_int64());
                        strcpy(rcmd.body.cancelOrder.orderId, orderId.c_str());
                        // strcpy(rcmd.body.cancelOrder.orderSysId, v.at("text").as_string().c_str());
                        // rcmd.body.cancelOrder.offsetFlag = OffsetFlag_OPEN;//crypto::str_cmp(data[i][""].GetString())
                        double size  = stod(rawData["size"].GetString());//stod(v.at("size").serialize().c_str());
                        size = size > 0 ? size : -size ;
                        double left = stod(rawData["left"].GetString());//v.at("left").as_integer();
                        left = left > 0 ? left : -left;
                        rcmd.body.cancelOrder.volumeTraded = size - left;
                        rcmd.body.cancelOrder.tradePrice   = stod(rawData["fill_price"].GetString());//stod(v.at("fill_price").as_string().c_str() );

                        PUSH_RCMD(rcmd)
                        return;
                    }
                    else if(rawData.HasMember("label")){
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                        rcmd.body.cancelOrder.ErrorID = crypto::get_gateio_errorid(v.c_str());
                        if(rcmd.body.cancelOrder.ErrorID == ERROR_OrderNotFoundError){
                            rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                        }
                        strncpy(rcmd.body.cancelOrder.originMsg, rawData["label"].GetString(), sizeof(rcmd.body.cancelOrder.originMsg));

                        PUSH_RCMD(rcmd)
                        return;
                    }
                }
                else{
                    auto s = response.extract_string().get();
                    rcmd.body.cancelOrder.ErrorID = code;
                    // strncpy(rcmd.body.cancelOrder.originMsg, s.c_str(), sizeof(rcmd.body.cancelOrder.originMsg));
                    LOG_ERROR("%s", s.c_str());
                    rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                    PUSH_RCMD(rcmd)
                }
#if 0
                hotHttpClient->hotHttpClient->request(request)
                .then([&](http_response response){
                    rcmd.body.cancelOrder.tsNet = crypto::getCurrentTime();
                    LOG_DEBUG("GATEIO,cancel_order,internetDelay,%ld", __FUNCTION__, rcmd.body.cancelOrder.tsNet - rcmd.body.cancelOrder.tsSent);
                    auto code = response.status_code();
                    if(code == status_codes::Created || code == status_codes::OK
                        || code == status_codes::Unauthorized || code == status_codes::NotFound
                        || code == status_codes::BadRequest){
                        const string &v = response.extract_string().get();
                        LOG_DEBUG("cancel_order response:%s", v.c_str());
                        rapidjson::Document d;
                        rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(v.c_str());
                        if(d.HasParseError()){
                            rcmd.body.cancelOrder.ErrorID = ERROR_UnknownError;
                            rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                            PUSH_RCMD(rcmd)
                            return;
                        }

                        if(rawData.HasMember("id") && rawData.HasMember("status")){
                            rcmd.body.cancelOrder.orderStatus = OrderStatus_CANCELED;
                            string orderId = rawData["id"].GetString();//std::to_string(v.at("id").as_number().to_int64());
                            strcpy(rcmd.body.cancelOrder.orderId, orderId.c_str());
                            // strcpy(rcmd.body.cancelOrder.orderSysId, v.at("text").as_string().c_str());
                            // rcmd.body.cancelOrder.offsetFlag = OffsetFlag_OPEN;//crypto::str_cmp(data[i][""].GetString())
                            double size  = stod(rawData["size"].GetString());//stod(v.at("size").serialize().c_str());
                            size = size > 0 ? size : -size ;
                            double left = stod(rawData["left"].GetString());//v.at("left").as_integer();
                            left = left > 0 ? left : -left;
                            rcmd.body.cancelOrder.volumeTraded = size - left;
                            rcmd.body.cancelOrder.tradePrice   = stod(rawData["fill_price"].GetString());//stod(v.at("fill_price").as_string().c_str() );

                            PUSH_RCMD(rcmd)
                            return;
                        }
                        else if(rawData.HasMember("label")){
                            rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                            rcmd.body.cancelOrder.ErrorID = crypto::get_gateio_errorid(v.c_str());
                            if(rcmd.body.cancelOrder.ErrorID == ERROR_OrderNotFoundError){
                                rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                            }
                            strncpy(rcmd.body.cancelOrder.originMsg, rawData["label"].GetString(), sizeof(rcmd.body.cancelOrder.originMsg));

                            PUSH_RCMD(rcmd)
                            return;
                        }
                    }
                    else{
                        auto s = response.extract_string().get();
                        rcmd.body.cancelOrder.ErrorID = code;
                        // strncpy(rcmd.body.cancelOrder.originMsg, s.c_str(), sizeof(rcmd.body.cancelOrder.originMsg));
                        LOG_ERROR("%s", s.c_str());
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                        PUSH_RCMD(rcmd)
                    }
                });

                hotHttpClient->request(request)
                .then([&](const http_response &response) -> pplx::task<json::value> {
                    rcmd.body.cancelOrder.tsNet = crypto::getCurrentTime();
                    auto code = response.status_code();
                    if(code == status_codes::Created || code == status_codes::OK
                       || code == status_codes::Unauthorized || code == status_codes::NotFound
                       || code == status_codes::BadRequest){
                        return response.extract_json();
                    }
                    else{
                        auto s = response.extract_string().get();
                        rcmd.body.cancelOrder.ErrorID = code;
                        strncpy(rcmd.body.cancelOrder.originMsg, s.c_str(), sizeof(rcmd.body.cancelOrder.originMsg));
                        LOG_ERROR("%s", s.c_str());
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                        PUSH_RCMD(rcmd)
                        return pplx::task_from_result(json::value());
                    }
                })
                .then([&](pplx::task<json::value> previousTask) {
                    json::value const &v = previousTask.get();
                    LOG_DEBUG("cancel_order response:%s", v.serialize().c_str());
                    if(v.has_field("id") && v.has_field("status")){
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_CANCELED;
                        string orderId = std::to_string(v.at("id").as_number().to_int64());
                        strcpy(rcmd.body.cancelOrder.orderId, orderId.c_str());
                        strcpy(rcmd.body.cancelOrder.orderSysId, v.at("text").as_string().c_str());
                        // rcmd.body.cancelOrder.offsetFlag = OffsetFlag_OPEN;//crypto::str_cmp(data[i][""].GetString())
                        double size  = stod(v.at("size").serialize().c_str());
                        size = size > 0 ? size : -size ;
                        double left = v.at("left").as_integer();
                        left = left > 0 ? left : -left;
                        rcmd.body.cancelOrder.volumeTraded = size - left;
                        rcmd.body.cancelOrder.tradePrice   = stod(v.at("fill_price").as_string().c_str() );

                        PUSH_RCMD(rcmd)
                        return;
                    }
                    else if(v.has_field("label")){
                        LOG_ERROR("%s",v.serialize().c_str());
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                        rcmd.body.cancelOrder.ErrorID = crypto::get_gateio_errorid(v.serialize().c_str());
                        if(rcmd.body.cancelOrder.ErrorID == ERROR_OrderNotFoundError){
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

void GateioUSwapTradingClient::query_multi_orders(pubsub::TCommand &tcmd){

}

void GateioUSwapTradingClient::query_one_order(pubsub::TCommand &tcmd){
    QUERY_ORDER_TCMD_2_RCMD(tcmd)
    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.queryOrder.instId, info)){
            http_client restclient(m_curcfg.restBaseUrl);
            http_request request(methods::GET);
            request.headers().add("Accept","application/json");
            request.headers().add("Content-Type","application/json");
            string queryStr{""};
            string time = to_string(crypto::getCurrentTimeSeconds());
            string queryOrderUrl = m_orderUrl.to_string();
            if(crypto::str_cmp(tcmd.body.queryOrder.orderId, "") == false){
                queryOrderUrl.append("/").append(tcmd.body.queryOrder.orderId);
            }
            else if(!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")){
                queryOrderUrl.append("/").append(tcmd.body.queryOrder.orderSysId);
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
            string sign = get_signature_rest("GET", queryOrderUrl.c_str(), time.c_str(), queryStr.c_str(), "");
//            queryOrderUrl.append("?").append(queryStr);
            web::uri m_queryOrderUrl  = queryOrderUrl;
            uri_builder builder(m_queryOrderUrl);
            LOG_DEBUG("%s url:%s,params:%s,",__FUNCTION__, m_queryOrderUrl.to_string().c_str(),queryStr.c_str());
            request.headers().add("KEY",m_curcfg.apiKey);
            request.headers().add("Timestamp",time);
            request.headers().add("SIGN",sign);
            request.set_request_uri(builder.to_string());
            // const http_response &response = hotHttpClient->request(request);//restclient
            const http_response &response = restclient.request(request).get();
            auto code = response.status_code();
            if(code == status_codes::Created || code == status_codes::OK
                    || code == status_codes::Unauthorized || code == status_codes::NotFound
                    || code == status_codes::BadRequest) {
                json::value const &v = response.extract_json().get();
                LOG_DEBUG("query_one_order response:%s", v.serialize().c_str());
                if(v.has_field("id") && v.has_field("status")){
                    strcpy(rcmd.body.queryOrder.instId, info.instId);
                    string orderId = std::to_string(v.at("id").as_number().to_int64());
                    strcpy(rcmd.body.queryOrder.orderId, orderId.c_str());
                    strcpy(rcmd.body.queryOrder.orderSysId, v.at("text").as_string().c_str());
                    double size  = v.at("size").as_integer();

                    rcmd.body.queryOrder.volumeTotal  = size > 0 ? size : -size ;
                    rcmd.body.queryOrder.limitPrice   = stod(v.at("price").as_string().c_str());

                    double left = stod(v.at("left").serialize().c_str());
                    left = left > 0 ? left : -left;
                    rcmd.body.queryOrder.volumeTraded = rcmd.body.queryOrder.volumeTotal - left;
                    rcmd.body.queryOrder.tradePrice   = stod(v.at("fill_price").as_string().c_str());
                    // rcmd.body.orderResponse.reduceOnly = v.at("is_reduce_only").as_bool();
                    if(crypto::str_cmp(v.at("status").as_string().c_str() ,"open")){
                        //成交数量<挂单数量 && 成交数量大于0 --> 部分成交
                        if(rcmd.body.queryOrder.volumeTotal > rcmd.body.queryOrder.volumeTraded
                           && rcmd.body.queryOrder.volumeTraded > 0 ){
                            rcmd.body.queryOrder.orderStatus = OrderStatus_PARTFILLED;
                        }
                        else{
                            rcmd.body.queryOrder.orderStatus = OrderStatus_NEW;
                        }
                    }
                    else{
                        if(v.has_field("finish_as")){
                            string finish_as = v.at("finish_as").as_string();
                            if(crypto::str_cmp(finish_as.c_str(),"filled")){
                                rcmd.body.queryOrder.orderStatus = OrderStatus_FILLED;
                            }
                            else if(crypto::str_cmp(finish_as.c_str(),"cancelled")
                                    || crypto::str_cmp(finish_as.c_str(),"liquidated")//- 强制平仓撤销
                                    || crypto::str_cmp(finish_as.c_str(),"ioc")//未立即完全成交，因为tif设置为ioc
                                    || crypto::str_cmp(finish_as.c_str(),"auto_deleveraged")//自动减仓撤销
                                    || crypto::str_cmp(finish_as.c_str(),"reduce_only")//: 增持仓位撤销，因为设置reduce_only或平仓
                                    ){
                                rcmd.body.queryOrder.orderStatus = OrderStatus_CANCELED;
                            }
                        }
                        else{
                            rcmd.body.queryOrder.orderStatus = OrderStatus_UNKNOWN;
                        }
                    }

                    PUSH_RCMD(rcmd)
                    return;
                }
                else if(v.has_field("label")) {
                    rcmd.body.queryOrder.ErrorID = crypto::get_gateio_errorid(v.serialize().c_str());
                    rcmd.body.queryOrder.orderStatus = OrderStatus_REJECTED;
                    strncpy(rcmd.body.queryOrder.originMsg, v.at("label").as_string().c_str(), sizeof(rcmd.body.queryOrder.originMsg));
                    LOG_ERROR("query_one_order:%s",v.serialize().c_str());

                    PUSH_RCMD(rcmd)
                    return;
                }
                else{
                    LOG_ERROR("can not parse gateio query order json:%s", v.as_string().c_str());
                }
            }
            else{
                const string &s = response.extract_string().get();
                rcmd.body.queryOrder.ErrorID = code;
                rcmd.body.queryOrder.orderStatus = OrderStatus_UNKNOWN;
                // strncpy(rcmd.body.queryOrder.originMsg, s.c_str(), sizeof(rcmd.body.queryOrder.originMsg));
                LOG_ERROR("query_one_order:code:%d,response:%s",code, s.c_str());

                PUSH_RCMD(rcmd)
            }
#if 0
            // restclient.request(request)
            hotHttpClient->request(request)
            .then([&](const http_response &response) -> pplx::task<json::value> { auto code = response.status_code();
                if(code == status_codes::Created || code == status_codes::OK
                       || code == status_codes::Unauthorized || code == status_codes::NotFound
                       || code == status_codes::BadRequest) {
                    return response.extract_json();
                }
                else{
                    const string &s = response.extract_string().get();
                    rcmd.body.queryOrder.ErrorID = code;
                    rcmd.body.queryOrder.orderStatus = OrderStatus_UNKNOWN;
                    strncpy(rcmd.body.queryOrder.originMsg, s.c_str(), sizeof(rcmd.body.queryOrder.originMsg));
                    LOG_ERROR("query_one_order:code:%d,response:%s",code, s.c_str());

                    PUSH_RCMD(rcmd)
                    return pplx::task_from_result(json::value());
                }
            })
            .then([&](pplx::task<json::value> previousTask) {
                json::value const &v = previousTask.get();
                LOG_DEBUG("query_one_order response:%s", v.serialize().c_str());
                if(v.has_field("id") && v.has_field("status")){
                    strcpy(rcmd.body.queryOrder.instId, info.instId);
                    string orderId = std::to_string(v.at("id").as_number().to_int64());
                    strcpy(rcmd.body.queryOrder.orderId, orderId.c_str());
                    strcpy(rcmd.body.queryOrder.orderSysId, v.at("text").as_string().c_str());
                    double size  = v.at("size").as_integer();

                    rcmd.body.queryOrder.volumeTotal  = size > 0 ? size : -size ;
                    rcmd.body.queryOrder.limitPrice   = stod(v.at("price").as_string().c_str());

                    double left = stod(v.at("left").serialize().c_str());
                    left = left > 0 ? left : -left;
                    rcmd.body.queryOrder.volumeTraded = rcmd.body.queryOrder.volumeTotal - left;
                    rcmd.body.queryOrder.tradePrice   = stod(v.at("fill_price").as_string().c_str());
                    // rcmd.body.orderResponse.reduceOnly = v.at("is_reduce_only").as_bool();
                    if(crypto::str_cmp(v.at("status").as_string().c_str() ,"open")){
                        //成交数量<挂单数量 && 成交数量大于0 --> 部分成交
                        if(rcmd.body.queryOrder.volumeTotal > rcmd.body.queryOrder.volumeTraded
                           && rcmd.body.queryOrder.volumeTraded > 0 ){
                            rcmd.body.queryOrder.orderStatus = OrderStatus_PARTFILLED;
                        }
                        else{
                            rcmd.body.queryOrder.orderStatus = OrderStatus_NEW;
                        }
                    }
                    else{
                        if(v.has_field("finish_as")){
                            string finish_as = v.at("finish_as").as_string();
                            if(crypto::str_cmp(finish_as.c_str(),"filled")){
                                rcmd.body.queryOrder.orderStatus = OrderStatus_FILLED;
                            }
                            else if(crypto::str_cmp(finish_as.c_str(),"cancelled")
                                    || crypto::str_cmp(finish_as.c_str(),"liquidated")//- 强制平仓撤销
                                    || crypto::str_cmp(finish_as.c_str(),"ioc")//未立即完全成交，因为tif设置为ioc
                                    || crypto::str_cmp(finish_as.c_str(),"auto_deleveraged")//自动减仓撤销
                                    || crypto::str_cmp(finish_as.c_str(),"reduce_only")//: 增持仓位撤销，因为设置reduce_only或平仓
                                    ){
                                rcmd.body.queryOrder.orderStatus = OrderStatus_CANCELED;
                            }
                        }
                        else{
                            rcmd.body.queryOrder.orderStatus = OrderStatus_UNKNOWN;
                        }
                    }

                    PUSH_RCMD(rcmd)
                    return;
                }
                else if(v.has_field("label")) {
                    rcmd.body.queryOrder.ErrorID = crypto::get_gateio_errorid(v.serialize().c_str());
                    rcmd.body.queryOrder.orderStatus = OrderStatus_REJECTED;
                    strncpy(rcmd.body.queryOrder.originMsg, v.serialize().c_str(), sizeof(rcmd.body.queryOrder.originMsg));
                    LOG_ERROR("query_one_order:%s",v.serialize().c_str());

                    PUSH_RCMD(rcmd)
                    return;
                }
            })
            .wait();
#endif
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

void GateioUSwapTradingClient::query_order(pubsub::TCommand &tcmd){
    if(tcmd.body.queryOrder.queryOrderTypeEnum == QOT_ONE_INST){
        query_one_order(tcmd);
    }
    else if(tcmd.body.queryOrder.queryOrderTypeEnum == QOT_MULTI_INST){
        // query_multi_orders(cmd);
    }
    else{
        LOG_ERROR("query_order got unknown queryOrderType:%d", tcmd.body.queryOrder.queryOrderTypeEnum);
    }
}

#ifdef USE_WEBSOCKET_API
void GateioUSwapTradingClient::ws_add_new_order(pubsub::TCommand &tcmd){
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
            value["contract"] = json::value::string(info.originInstId);

            value["price"] = json::value::string(crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice, info.tickSize));

            if(tcmd.body.newOrder.orderType == OrderType_LIMIT){
                value["tif"] = json::value::string("gtc");
            }
            else if(tcmd.body.newOrder.orderType == OrderType_IOC){
                value["tif"] = json::value::string("ioc");
            }
            else if(tcmd.body.newOrder.orderType == OrderType_POST_ONLY){
                value["tif"] = json::value::string("poc");
            }
            else if(tcmd.body.newOrder.orderType == OrderType_FOK){
                value["tif"] = json::value::string("ioc");
            }
            else if(tcmd.body.newOrder.orderType == OrderType_MARKET){
                value["tif"] = json::value::string("ioc");
                value["price"] =json::value::string("0");
            }
            else{
                rcmd.body.newOrder.ErrorID = ERROR_OrderTypeError;
                PUSH_RCMD(rcmd)
                return;
            }

            //open
            if(tcmd.body.newOrder.offsetFlag == OffsetFlag_OPEN){
                //交易数量，正数为买入，负数为卖出。平仓委托则设置为0。
                if(tcmd.body.newOrder.direction == Direction_LONG){
                    value["size"] = json::value::number(stol(crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal, info.lotSize)));
                }
                else if(tcmd.body.newOrder.direction == Direction_SHORT){
                    value["size"] = json::value::number(stol(crypto::getFixedPrecision(-tcmd.body.newOrder.volumeTotal, info.lotSize)));
                }
                else{
                    rcmd.body.newOrder.ErrorID = ERROR_DirectionError;
                    PUSH_RCMD(rcmd)
                    return;
                }
            }//close
            else if(tcmd.body.newOrder.offsetFlag == OffsetFlag_CLOSE){
                //交易数量，close与open相反 正数为买入，负数为卖出。平仓委托则设置为0。
                if(tcmd.body.newOrder.direction == Direction_LONG){
                    value["size"] = json::value::number(stol(crypto::getFixedPrecision(-tcmd.body.newOrder.volumeTotal, info.lotSize)));
                }
                else if(tcmd.body.newOrder.direction == Direction_SHORT){
                    value["size"] = json::value::number(stol(crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal, info.lotSize)));
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
            //value["reduce_only"] = tcmd.body.newOrder.reduceOnly ? json::value::string("true") : json::value::string("false");
            value["reduce_only"] = json::value::boolean(tcmd.body.newOrder.reduceOnly);

            string channel = "futures.order_place";
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

void GateioUSwapTradingClient::ws_cancel_order(pubsub::TCommand &tcmd){
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.cancelOrder.instId, info)){
            json::value value;
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

                string channel = "futures.order_cancel";
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


void GateioUSwapTradingClient::ws_query_order(pubsub::TCommand &tcmd){
    QUERY_ORDER_TCMD_2_RCMD(tcmd)
    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.queryOrder.instId, info)){
            json::value value;
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

            string channel = "futures.order_status";
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