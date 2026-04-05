#include "GateioUSwapTradingClient.h"

GateioUSwapTradingClient::GateioUSwapTradingClient(){

}

GateioUSwapTradingClient::~GateioUSwapTradingClient(){
//    delete smc;
}

bool GateioUSwapTradingClient::Initialize(AccountCfg& cfg, sm::SecurityManager *smc){
    this->smc = smc;
    m_positionsUrl = "/api/v4/futures/usdt/positions";
    m_accountsUrl = "/api/v4/futures/usdt/accounts";
    m_orderUrl = "/api/v4/futures/usdt/orders";
//    m_cancelOrderUrl = "/api/v4/futures/usdt/orders/";
    m_curcfg = cfg;
    return true;
}

void GateioUSwapTradingClient::Run() {
    std::thread monitorThread(&GateioUSwapTradingClient::monitor, this);
    monitorThread.detach();
//    go [&]{
//        monitor();
//    };
}

void GateioUSwapTradingClient::sub_websocket()//{// websocket_client websocket_callback_client &wsclient)
try
{
//    fprintf(stdout,"I am happy,%s,%d\n", __FUNCTION__ , __LINE__);
    //    TIMERHEADER;
    uri_builder builder(m_curcfg.wsBaseUrl);
    wsClient.close();
    wsClient.connect(builder.to_string())
        .then([&]() {
            std::function<void (const websocket_incoming_message &msg)> f;
            f = std::bind(&GateioUSwapTradingClient::on_websocket_msg, this, placeholders::_1);
            wsClient.set_message_handler(f);
            std::function<void (websocket_close_status close_status,
            const utility::string_t& reason, const std::error_code& error)> c;
            c =  std::bind(&GateioUSwapTradingClient::on_close_msg,this
                    ,placeholders::_1,placeholders::_2,placeholders::_3);
            wsClient.set_close_handler(c);
        })
        .wait();
    websocket_outgoing_message balanceOutMsg = sub_balance_channel();
    wsClient.send(balanceOutMsg).wait();
    websocket_outgoing_message orderOutMsg   = sub_orders_channel();
    wsClient.send(orderOutMsg).wait();
    websocket_outgoing_message tradesOutMsg  = sub_trades_channel();
    wsClient.send(tradesOutMsg).wait();
    websocket_outgoing_message positionsOutMsg  = sub_positions_channel();
    wsClient.send(positionsOutMsg).wait();
    websocket_outgoing_message positionCloseOutMsg  = sub_position_close_channel();
    wsClient.send(positionCloseOutMsg).wait();

    m_IsConnected = true;
    LOG_INFO("connected with gateio usdt swap api");
//    TIMERTAIL(m_curcfg.accountid,"SubWebSocket");
}
catch(exception &e){
    m_IsConnected = false;
//    std::cout << "SubWebSocket Error:" << e.what() << endl;
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
    //If you want to subscribe to all orders updates in all currency pairs,
    // you can include !all in currency pair list.
//    subValue["payload"][1] = json::value::string("!all");
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
    //If you want to subscribe to all orders updates in all currency pairs,
    // you can include !all in currency pair list.
//    subValue["payload"][1] = json::value::string("!all");
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

websocket_outgoing_message GateioUSwapTradingClient::sub_position_close_channel() {
    string channel = "futures.position_close";
    websocket_outgoing_message outMsg;
    json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = json::value::string(channel);
    subValue["event"] = json::value::string("subscribe");
    subValue["payload"][0] = json::value::string(m_curcfg.userId);
    subValue["payload"][1] = json::value::string("!all");
    //If you want to subscribe to all orders updates in all currency pairs,
    // you can include !all in currency pair list.
//    subValue["payload"][1] = json::value::string("!all");
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

websocket_outgoing_message GateioUSwapTradingClient::sub_trades_channel() {
     string channel = "futures.usertrades";
    websocket_outgoing_message outMsg;
    json::value subValue;
    subValue["time"] = crypto::getCurrentTimeSeconds();
    subValue["channel"] = json::value::string(channel);
    subValue["event"] = json::value::string("subscribe");
    subValue["payload"][0] = json::value::string(m_curcfg.userId);
//    subValue["payload"][1] = json::value::string("BTC_USDT");
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

void GateioUSwapTradingClient::monitor() {
    while(1){
        try{
            LOG_INFO("start to connect with gateio usdt swap api");
            sub_websocket();

            while(m_IsConnected){
                sleep(10);
                if(!m_IsConnected){
                    LOG_ERROR("gateio usdt swap ws disconnected, will reconnect it now");
                    break;
                }
                else{
//                    LOG_DEBUG("gateio usdt swap ws is conntected now, will send ping to it!");
                    ping();
                }
            }
        }
        catch(exception &e){
            LOG_ERROR("%s", e.what());
        }
        sleep(5);
    }
}

void GateioUSwapTradingClient::on_websocket_msg(const websocket_incoming_message& msg)
try{
    // text_message, binary_message, close, ping, pong
    if (msg.message_type() == websocket_message_type::text_message){
        msg.extract_string().then([&](const string s){
//            msgExt.msg = s;
            rapidjson::Document d;
            rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(s.c_str());
            //订阅成功的回报和ping pong之类的消息不用特别处理
            string channel = rawData["channel"].GetString();
            //没有有效信息无需处理
            if(crypto::str_cmp(channel.c_str(),"futures.pong") == true
            || !rawData.HasMember("event")
            || crypto::str_cmp(rawData["event"].GetString(),"update") == false){
                return;
            }
            LOG_DEBUG("on_websocket_msg:%s", s.c_str());
            if(crypto::str_cmp(channel.c_str(), "futures.orders") == true){
                //{"id":null,"time":1648831308,"channel":"futures.orders","event":"update","error":null,
                // "result":[{"contract":"DOGE_USDT","create_time":1648831308,"create_time_ms":1648831308897,
                // "fill_price":0,"finish_as":"_new","finish_time":1648831308,"finish_time_ms":1648831308897,
                // "iceberg":0,"id":143814067568,"is_close":false,"is_liq":false,"is_reduce_only":false,
                // "left":10,"mkfr":-0.00005,"price":0.1,"refr":0,"refu":0,"size":10,"status":"open",
                // "text":"app","tif":"gtc","tkfr":0.00048,"user":"1047221"}]}
//                return;
                const rapidjson::Value &data = rawData["result"];
                for(rapidjson::SizeType i = 0; i < data.Size(); i++){
                    string originInstId = data[i]["contract"].GetString();
                    md::InstrumentInfo info;
                    if(this->smc->get_instrument_info("GATEIO","SWAP",originInstId.c_str(), info)){
//                        MsgExt msgExt;
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0 , sizeof(pubsub::RCommand));
                        rcmd.header.insertTime = crypto::getCurrentTime();
                        rcmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                        rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        strcpy(rcmd.body.orderResponse.instId, info.instId);

                        strcpy(rcmd.body.orderResponse.orderId, data[i]["id"].GetString());
                        if(data[i].HasMember("text")){
                            strcpy(rcmd.body.orderResponse.clientOrderId, data[i]["text"].GetString());
                        }

                        rcmd.body.orderResponse.offsetFlag = data[i]["is_close"].GetBool() == true ? OffsetFlag_CLOSE : OffsetFlag_OPEN;
                        if(data[i].HasMember("is_reduce_only")) {
                            rcmd.body.orderResponse.reduceOnly = data[i]["is_reduce_only"].GetBool();
                        }
                        double size  = stod(data[i]["size"].GetString());
                        rcmd.body.orderResponse.direction = size > 0 ? Direction_LONG : Direction_SHORT ;

                        rcmd.body.orderResponse.volumeTotal  = size > 0 ? size : -size ;
                        rcmd.body.orderResponse.limitPrice   = stod(data[i]["price"].GetString());

                        string tif = data[i]["tif"].GetString();
                        GET_ORDERTYPE(rcmd)
                        double left = stod(data[i]["left"].GetString());
                        left = left > 0 ? left : -left;
                        //成交数量
                        rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
                        //成交均价
                        if(rcmd.body.orderResponse.volumeTraded > ZERO_NUM){
                            if(data[i].HasMember("filled_total")){
                                rcmd.body.orderResponse.tradePrice = stod(data[i]["filled_total"].GetString())
                                                                     / rcmd.body.orderResponse.volumeTraded;
                            }
                            else{
                                if(data[i].HasMember("fill_price")){
                                    rcmd.body.orderResponse.tradePrice = stod(data[i]["fill_price"].GetString());
                                }
                            }
                        }

                        if(crypto::str_cmp(data[i]["status"].GetString(),"open")){
                            //成交数量<挂单数量 && 成交数量大于0 --> 部分成交
                            if(rcmd.body.orderResponse.volumeTotal > rcmd.body.orderResponse.volumeTraded
                               && rcmd.body.orderResponse.volumeTraded > 0
                            ){
                                rcmd.body.orderResponse.orderStatus = OrderStatus_PARTFILLED;
                            }
                            else{
                                rcmd.body.orderResponse.orderStatus = OrderStatus_NEW;
                            }
                        }
                        else{
                            string finish_as = data[i]["finish_as"].GetString();
                            if(crypto::str_cmp(finish_as.c_str(),"filled")){
                                rcmd.body.orderResponse.orderStatus = OrderStatus_FILLED;
                            }
                            //
                            else if(crypto::str_cmp(finish_as.c_str(),"cancelled")
                            || crypto::str_cmp(finish_as.c_str(),"liquidated")//- 强制平仓撤销
                            || crypto::str_cmp(finish_as.c_str(),"ioc")//未立即完全成交，因为tif设置为ioc
                            || crypto::str_cmp(finish_as.c_str(),"auto_deleveraged")//自动减仓撤销
                            || crypto::str_cmp(finish_as.c_str(),"reduce_only")//: 增持仓位撤销，因为设置reduce_only或平仓
                            ){
                                rcmd.body.orderResponse.orderStatus = OrderStatus_CANCELED;
                            }
                            else{
                                rcmd.body.orderResponse.orderStatus = OrderStatus_UNKNOWN;
                            }
                        }

                        rcmd.body.orderResponse.insertTime = stoll(data[i]["create_time_ms"].GetString()) * 1000;
                        rcmd.body.orderResponse.updateTime = stoll(data[i]["finish_time_ms"].GetString()) * 1000;
                        rcmd.body.orderResponse.tsParse = crypto::getCurrentTime();
                        g_rptInnerQueue.push(rcmd);
                    }
                    else{
                        LOG_ERROR("not found GATEIO.SWAP.%s smc info", originInstId.c_str());
                    }
                }
            }
            else if(crypto::str_cmp(channel.c_str(),"futures.usertrades") == true){
                //{"time":1650556082,"channel":"futures.usertrades","event":"update","result":[{"id":"3619351","order_id":"617426002",
                // "contract":"BTC_USDT","create_time":1650556082,"create_time_ms":1650556082369,"size":-1,"role":"taker",
                // "price":"42500","text":"t-WVG7TYUDYQ06X36ST644NPAG"}]}
                //实盘数据
                //{"id":null,"time":1650565147,"channel":"futures.usertrades","event":"update",
                // "error":null,"result":[{"id":"88227719","create_time":1650565147,
                // "create_time_ms":1650565147244,"contract":"BTC_USDT","order_id":"150937751242",
                // "size":1,"price":"41536.5","role":"maker"}]}
//                return;
                const rapidjson::Value &data = rawData["result"];
                for(rapidjson::SizeType i = 0; i < data.Size(); i++){
                    string originInstId = data[i]["contract"].GetString();
                    md::InstrumentInfo info;
                    if(this->smc->get_instrument_info("GATEIO","SWAP",originInstId.c_str(), info)){
                        pubsub::RCommand cmd;
                        memset(&cmd,0,sizeof(pubsub::RCommand));
                        cmd.header.insertTime = crypto::getCurrentTime();
                        cmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                        cmd.header.instTypeEnum = InstType_USDT_SWAP;
                        cmd.header.cmdTypeEnum = pubsub::CMD_RPT_TRADE;
                        strcpy(cmd.header.accountId, m_curcfg.accountId);
                        strcpy(cmd.body.trade.instId, info.instId);
                        strcpy(cmd.body.trade.tradeId, data[i]["id"].GetString());
                        strcpy(cmd.body.trade.orderId, data[i]["order_id"].GetString());
                        if(data[i].HasMember("text")){//实盘没有这个字段 @20220710
                            strcpy(cmd.body.trade.clientOrderId, data[i]["text"].GetString());
                        }
                        int size = stoi(data[i]["size"].GetString());
                        cmd.body.trade.volumeTraded = size > 0 ? size : -size;
                        cmd.body.trade.offsetFlag = OffsetFlag_OPEN;
                        cmd.body.trade.direction = size > 0 ? Direction_LONG : Direction_SHORT;
                        cmd.body.trade.tradePrice   = stod(data[i]["price"].GetString());
                        //TODO
                        cmd.body.trade.tradeTime = stoll(data[i]["create_time_ms"].GetString())*1000;
                        string maker = data[i]["role"].GetString();
                        cmd.body.trade.isMaker = maker[0] == 'm' ? true : false;
                        g_rptInnerQueue.push(cmd);
                    }
                    else{
                        LOG_ERROR("not found GATEIO.SWAP.%s smc info", originInstId.c_str());
                    }
                }
            }
            else if(crypto::str_cmp(channel.c_str(),"futures.balances") == true){
                //{"time":1650555418,"channel":"futures.balances","event":"update","result":[{"balance":989.374310681771,"change":-0.19632649413,"currency":"usdt","text":"BTC_USDT:617410632","time":1650555418,"time_ms":1650555418114,"type":"pnl","user":"1047221"}]}
                //{"id":null,"time":1650565147,"channel":"futures.balances","event":"update","error":null,"result":[{"balance":450.024235109984,"change":0.0486475,
                // "text":"BTC_USDT:150937751242","time":1650565147,"time_ms":1650565147244,"type":"pnl",
                // "user":"1047221"}]}
                const rapidjson::Value &data = rawData["result"];
                for(rapidjson::SizeType i = 0; i < data.Size(); i++){
                    pubsub::RCommand cmd;
                    memset(&cmd,0,sizeof(pubsub::RCommand));
                    cmd.header.insertTime = crypto::getCurrentTime();
                    cmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                    cmd.header.instTypeEnum = InstType_USDT_SWAP;
                    cmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    strcpy(cmd.header.accountId, m_curcfg.accountId);
                    cmd.body.balance.updateTime = stoll(data[i]["time_ms"].GetString())*1000;
                    string text = data[i]["text"].GetString();
                    if(data[i].HasMember("currency")){
                        string currencyUpper = crypto::to_upper(data[i]["currency"].GetString());
                        strcpy(cmd.body.balance.currency, currencyUpper.c_str());
                    }
                    else if(crypto::has_str(text.c_str(),"USDT")){
                        strcpy(cmd.body.balance.currency, "USDT");
                    }
                    else{
                        LOG_ERROR("%s",s.c_str());
                        continue;
                    }

                    cmd.body.balance.available = stod(data[i]["balance"].GetString());
                    cmd.body.balance.frozen = 0 ;
                    g_rptInnerQueue.push(cmd);

                }
            }
            else if(crypto::str_cmp(channel.c_str(),"futures.positions") == true){
                //做多4张
                //{"time":1649425643,"channel":"futures.positions","event":"update","result":[{"contract":"BTC_USDT",
                // "entry_price":44517.26666666667,"history_pnl":0,"history_point":0,"last_close_pnl":0,
                // "leverage":0,"leverage_max":100,"liq_price":0,"maintenance_rate":0.005,"margin":999.8521412063,
                // "mode":"single","realised_pnl":-0.1034121662,"realised_point":0,"risk_limit":1000000,
                // "size":6,"time":1649425643,"time_ms":1649425643255,"user":"1047221"}]}
                //{"id":null,"time":1650565147,"channel":"futures.positions","event":"update","error":null,"result":[{"contract":"BTC_USDT","cross_leverage_limit":0,"entry_price":42022.975,"history_pnl":0,"history_point":0,"last_close_pnl":0,"leverage":0,"leverage_max":100,"liq_price":245147.73,"maintenance_rate":0.005,"margin":449.975587609984,"mode":"single","realised_pnl":0.024235109984,"realised_point":0,"risk_limit":1000000,"size":-22,"time":1650565147,"time_ms":1650565147244,"user":"1047221"}]}
                const rapidjson::Value &data = rawData["result"];
                for(rapidjson::SizeType i = 0; i < data.Size(); i++){
                    string originInstId = data[i]["contract"].GetString();
                    md::InstrumentInfo info;
                    if(this->smc->get_instrument_info("GATEIO","SWAP",originInstId.c_str(), info)) {
                        pubsub::RCommand cmd;
                        memset(&cmd,0,sizeof(pubsub::RCommand));
                        cmd.header.insertTime = crypto::getCurrentTime();
                        cmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                        cmd.header.instTypeEnum = InstType_USDT_SWAP;
                        cmd.header.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                        strcpy(cmd.header.accountId, m_curcfg.accountId);
                        strcpy(cmd.body.position.instId, info.instId);
                        cmd.body.position.updateTime = stoll(data[i]["time_ms"].GetString()) *1000;
                        int size = stoi(data[i]["size"].GetString());

                        cmd.body.position.direction = size > 0 ? Direction_LONG : Direction_SHORT;
                        cmd.body.position.volume = size > 0 ? size : -size;
                        cmd.body.position.maintMargin = stod(data[i]["margin"].GetString());
                        cmd.body.position.avgPrice = stod(data[i]["entry_price"].GetString());
                        if(data[i].HasMember("unrealised_pnl")){
                            cmd.body.position.unrealizedPnl = stod(data[i]["unrealised_pnl"].GetString());
                        }
                        if(data[i].HasMember("liq_price")) {
                            cmd.body.position.liquidPrice = stod(data[i]["liq_price"].GetString());
                        }
                        if(data[i].HasMember("mark_price")) {
                            cmd.body.position.markPrice = stod(data[i]["mark_price"].GetString());
                        }
                        //单向持仓模式
                        g_rptInnerQueue.push(cmd);
//                        if(cmd.body.position.available > ZERO_NUM){
//                            g_rptInnerQueue.push(cmd);
//                        }
                    }
                    else{
                        LOG_ERROR("not found GATEIO.SWAP.%s smc info", originInstId.c_str());
                    }
                }
            }
            else if(crypto::str_cmp(channel.c_str(),"futures.position_closes") == true){
                const rapidjson::Value &data = rawData["result"];
                for(rapidjson::SizeType i = 0; i < data.Size(); i++){

                }
            }
            else{
                LOG_ERROR("%s",s.c_str());
                return;
            }
        });
    }
//    else if(msg.message_type() == websocket_message_type::pong){
//        DEBUGLINE
//        ping();
//    }
//    else if(msg.message_type() == websocket_message_type::ping){
//        DEBUGLINE
//        pong();
//    }
    else if(msg.message_type() == websocket_message_type::close){
//        DEBUGLINE
        m_IsConnected = false;
    }
}
catch(exception &e){
//    std::cout << "SubWebSocket Error:" << e.what() << endl;
    LOG_ERROR("%s", e.what());
    m_IsConnected = false;
}


void GateioUSwapTradingClient::on_close_msg(websocket_close_status close_status,
                  const utility::string_t& reason, const std::error_code& error)
try{
    fprintf(stderr, "I am happy,%s,%d\n", __FUNCTION__ , __LINE__);
    m_IsConnected = false;
    LOG_ERROR("U swap recv CloseMsg, reason:%s ",reason.c_str() );
}
catch (exception &e){
    LOG_ERROR("%s", e.what());
}

void GateioUSwapTradingClient::ping(){
    if(m_IsConnected){
        websocket_outgoing_message outMsg;
        json::value swapPingSubValue ;
        swapPingSubValue ["time"] = crypto::getCurrentTimeSeconds();
        swapPingSubValue ["channel"] = json::value::string("futures.ping");
//        LOG_DEBUG( "%s", spotPingSubValue.serialize().c_str());
        outMsg.set_utf8_message(swapPingSubValue .serialize().c_str());
        wsClient.send(outMsg).wait();
    }
}

void GateioUSwapTradingClient::pong(){
    if(m_IsConnected){
        websocket_outgoing_message outMsg;
        outMsg.set_pong_message();
        wsClient.send(outMsg).wait();
    }
}

bool GateioUSwapTradingClient::get_positions()//vector<Balance> &balanceVec
try {
    http_client restclient(m_curcfg.restBaseUrl);//bUrl
    http_request request(methods::GET);
    request.headers().add("Accept","application/json");
    request.headers().add("Content-Type","application/json");
    string queryStr = "";
    string time = to_string(crypto::getCurrentTimeSeconds() );
//    cout <<  m_balanceUrl.to_string() << endl;
    string sign = get_signature_rest("GET", m_positionsUrl.to_string().c_str(), time.c_str(), "", "");

    request.headers().add("KEY",m_curcfg.apiKey);
    request.headers().add("Timestamp",time);
    request.headers().add("SIGN",sign);
    uri_builder builder(m_positionsUrl);// bUrl m_balanceUrl

    request.set_request_uri(builder.to_string());
    restclient.request(request)
    .then([&](http_response response) -> pplx::task<json::value> {
        auto code = response.status_code();
        if(code == status_codes::OK || code == status_codes::BadRequest
            /*|| code == status_codes::TooManyRequests || code == status_codes::Unauthorized*/)
            return response.extract_json();
        // return an empty JSON value
        return pplx::task_from_result(json::value());
    }) // continue when the JSON value is available
    .then([&](pplx::task<json::value> previousTask) {
        try{
            // get the JSON value from the task and display content from it
            json::value const &v = previousTask.get();
            if(v.is_array()){
                auto array = v.as_array();
                for(auto it : array){
                    string originInstId = it["contract"].as_string();
                    md::InstrumentInfo info;
                    if(smc->get_instrument_info("GATEIO","SWAP",originInstId.c_str(),info)) {
                        pubsub::RCommand cmd;
                        cmd.header.insertTime = crypto::getCurrentTime();
                        cmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
                        cmd.header.instTypeEnum = InstType_USDT_SWAP;
                        cmd.header.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                        strcpy(cmd.header.accountId, m_curcfg.accountId);
                        strcpy(cmd.body.position.instId, info.instId);
                        cmd.body.position.updateTime = crypto::getCurrentTime();
                        int size = it["size"].as_integer();

                        cmd.body.position.direction = size > 0 ? Direction_LONG : Direction_SHORT;
                        cmd.body.position.volume = size > 0 ? size : -size;
                        if(it.has_field("margin")) {
                            cmd.body.position.maintMargin = stod(it["margin"].as_string().c_str());
                        }
                        if(it.has_field("entry_price")) {
                            cmd.body.position.avgPrice = stod(it["entry_price"].as_string().c_str());
                        }
                        if(it.has_field("unrealised_pnl")) {
                            cmd.body.position.unrealizedPnl = stod(it["unrealised_pnl"].as_string().c_str());
                        }
                        if(it.has_field("liq_price")) {
                            cmd.body.position.liquidPrice = stod(it["liq_price"].as_string().c_str());
                        }
                        if(it.has_field("mark_price")) {
                            cmd.body.position.markPrice = stod(it["mark_price"].as_string().c_str());
                        }
                        string instId = cmd.body.position.instId;
                        if(g_filterSymbolsMap.count(instId) > 0){
                            g_rptInnerQueue.push(cmd);
                        }
//                        g_rptInnerQueue.push(cmd);
                    }
                }
            }
            else{
                LOG_ERROR("%s",v.serialize().c_str());
            }
            return true;
        }
        catch (exception &e){
            LOG_ERROR("%s",e.what());
            return false;
        }
    })
    .wait();
    return true;
}
catch(exception &e){
    LOG_ERROR("%s", e.what());
    return false;
}

bool GateioUSwapTradingClient::get_accounts()
try {
    http_client restclient(m_curcfg.restBaseUrl);//bUrl
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
    restclient.request(request)
    .then([&](http_response response) -> pplx::task<json::value> {
        auto code = response.status_code();
        if(code == status_codes::OK || code == status_codes::BadRequest
            /*|| code == status_codes::TooManyRequests || code == status_codes::Unauthorized*/)
            return response.extract_json();
        // return an empty JSON value
        return pplx::task_from_result(json::value());
    }) // continue when the JSON value is available
    .then([&](pplx::task<json::value> previousTask) {
        // get the JSON value from the task and display content from it
        json::value const &v = previousTask.get();
//                LOG_DEBUG("%s", v.serialize().c_str());
        if(v.has_field("currency")){
            pubsub::RCommand cmd;
            cmd.header.insertTime = crypto::getCurrentTime();
            cmd.header.exchangeTypeEnum = ExchangeType_GATEIO;
            cmd.header.instTypeEnum = InstType_USDT_SWAP;
            cmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
            strcpy(cmd.header.accountId, m_curcfg.accountId);
            strcpy(cmd.body.balance.currency, v.at("currency").as_string().c_str());
            cmd.body.balance.updateTime = crypto::getCurrentTime();

            cmd.body.balance.available = stod(v.at("available").as_string().c_str());
//                    cmd.body.balance.total = stod(v.at("total").as_string().c_str());
            cmd.body.balance.frozen = stod(v.at("order_margin").as_string().c_str()) +
                    stod(v.at("position_margin").as_string().c_str());
            cmd.body.balance.total = cmd.body.balance.available + cmd.body.balance.frozen;
            string ccy = cmd.body.balance.currency;
            if(g_filterSymbolsMap.count(ccy) > 0){
                g_rptInnerQueue.push(cmd);
            }
            if(crypto::str_cmp(cmd.body.balance.currency, "USDT")){
                LOG_INFO("exchId:%s,instType:%s,accountId:%s,usdt available:%.2f",
                         ExchangeTypeEnum2StrMap[cmd.header.exchangeTypeEnum].c_str(),
                         InstTypeEnum2StrMap[cmd.header.instTypeEnum].c_str(),
                         cmd.header.accountId, cmd.body.balance.available
                );
            }
        }
        else{
            LOG_ERROR("%s",v.serialize().c_str());
        }
        return true;
    })
    .wait();
    return true;
}
catch(exception &e){
    LOG_ERROR("%s", e.what());
    return false;
}

void GateioUSwapTradingClient::add_new_order(pubsub::TCommand &cmd){
    pubsub::RCommand rcmd;
    memset(&rcmd,0,sizeof(pubsub::RCommand));
    rcmd.header.insertTime = crypto::getCurrentTime();
    strcpy(rcmd.header.accountId, cmd.header.accountId);
    rcmd.header.instTypeEnum = cmd.header.instTypeEnum;
    rcmd.header.exchangeTypeEnum = cmd.header.exchangeTypeEnum;
    rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_NEW_ORDER;
    strcpy(rcmd.body.newOrder.instId,cmd.body.newOrder.instId);

    rcmd.body.newOrder.offsetFlag = cmd.body.newOrder.offsetFlag;
    rcmd.body.newOrder.direction = cmd.body.newOrder.direction;
    rcmd.body.newOrder.orderType = cmd.body.newOrder.orderType;
    rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED;
    strcpy(rcmd.body.newOrder.clientOrderId, cmd.body.newOrder.clientOrderId);
    rcmd.body.newOrder.reduceOnly = cmd.body.newOrder.reduceOnly;
    if(!m_IsConnected){
        rcmd.body.newOrder.ErrorID = ERROR_TBDisconnectError;
        rcmd.body.newOrder.insertTime = cmd.body.newOrder.insertTime;
        rcmd.body.newOrder.updateTime = crypto::getCurrentTime();
        g_rptInnerQueue.push(rcmd);
        return;
    }
    if(!(cmd.body.newOrder.clientOrderId[0] == 't' && cmd.body.newOrder.clientOrderId[1] == '-')){
        rcmd.body.newOrder.ErrorID = ERROR_OrderParamError;
        strcpy(rcmd.body.newOrder.originMsg, "gateio clientOrderId have to start with 't-'");
        rcmd.body.newOrder.insertTime = cmd.body.newOrder.insertTime;
        rcmd.body.newOrder.updateTime = crypto::getCurrentTime();
        g_rptInnerQueue.push(rcmd);
        return;
    }
    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info("GATEIO","SWAP",cmd.body.newOrder.instId,info)){
            http_client restclient(m_curcfg.restBaseUrl);//bUrl
            http_request request(methods::POST);
            request.headers().add("Accept","application/json");
            request.headers().add("Content-Type","application/json");
            string queryStr = "";
            string time = to_string(crypto::getCurrentTimeSeconds());
            uri_builder builder(m_orderUrl);// bUrl m_balanceUrl
//            std::unordered_map<std::string, std::string> params;
            json::value value;

            value["text"] = json::value::string(cmd.body.newOrder.clientOrderId);
            value["contract"] = json::value::string(info.originInstId);
            if(tcmd.body.newOrder.reduceOnly == false){
                value["price"] = json::value::string(crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice, info.tickSize));
            }
            else{
                value["price"] = json::value::string(to_string(tcmd.body.newOrder.limitPrice));
            }

            
            if(cmd.body.newOrder.orderType == OrderType_LIMIT){
                value["tif"] = json::value::string("gtc");
            }
            else if(cmd.body.newOrder.orderType == OrderType_IOC){
                value["tif"] = json::value::string("ioc");
            }
            else if(cmd.body.newOrder.orderType == OrderType_POST_ONLY){
                value["tif"] = json::value::string("poc");
            }
            else if(cmd.body.newOrder.orderType == OrderType_FOK){
                value["tif"] = json::value::string("ioc");
            }
            else if(cmd.body.newOrder.orderType == OrderType_MARKET){
                value["tif"] = json::value::string("ioc");
                value["price"] =json::value::string("0");
            }
            else{
                rcmd.body.newOrder.ErrorID = ERROR_OrderTypeError;
                g_rptInnerQueue.push(rcmd);
                return;
            }

            //open
            if(tcmd.body.newOrder.offsetFlag == OffsetFlag_OPEN){
                //交易数量，正数为买入，负数为卖出。平仓委托则设置为0。
                if(tcmd.body.newOrder.direction == Direction_LONG){
                    if(tcmd.body.newOrder.reduceOnly){
                        value["size"] = json::value::string(to_string(tcmd.body.newOrder.volumeTotal));
                    }
                    else{
                        value["size"] = json::value::string(crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal, info.lotSize));
                    }
                    
                }//
                else if(tcmd.body.newOrder.direction == Direction_SHORT){
                    if(tcmd.body.newOrder.reduceOnly){
                        value["size"] = json::value::string(to_string(-tcmd.body.newOrder.volumeTotal));
                    }
                    else{
                        value["size"] = json::value::string(crypto::getFixedPrecision(-tcmd.body.newOrder.volumeTotal, info.lotSize));
                    }
                    
                }//
                else{
                    rcmd.body.newOrder.ErrorID = ERROR_DirectionError;
                    g_rptInnerQueue.push(rcmd);
                    return;
                }
            }//close
            else if(tcmd.body.newOrder.offsetFlag == OffsetFlag_CLOSE){
                //交易数量，close与open相反 正数为买入，负数为卖出。平仓委托则设置为0。
                if(tcmd.body.newOrder.direction == Direction_LONG){
                    if(tcmd.body.newOrder.reduceOnly){
                        value["size"] = json::value::string(to_string(-tcmd.body.newOrder.volumeTotal));
                    }
                    else{
                        value["size"] = json::value::string(crypto::getFixedPrecision(-tcmd.body.newOrder.volumeTotal, info.lotSize));
                    }
                }//
                else if(tcmd.body.newOrder.direction == Direction_SHORT){
                    if(tcmd.body.newOrder.reduceOnly){
                        value["size"] = json::value::string(to_string(tcmd.body.newOrder.volumeTotal));
                    }
                    else{
                        value["size"] = json::value::string(crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal, info.lotSize));
                    }
                }//
                else{
                    rcmd.body.newOrder.ErrorID = ERROR_DirectionError;
                    g_rptInnerQueue.push(rcmd);
                    return;
                }
            }
            else{
                rcmd.body.newOrder.ErrorID = ERROR_OffsetFlagError;
                g_rptInnerQueue.push(rcmd);
                return;
            }

            value["reduce_only"] = cmd.body.newOrder.reduceOnly ? json::value::string("true") : json::value::string("false");
            string sign = get_signature_rest("POST", m_orderUrl.to_string().c_str(), time.c_str(),
                                             "", value.serialize().c_str());
            request.headers().add("KEY",m_curcfg.apiKey);
            request.headers().add("Timestamp",time);
            request.headers().add("SIGN",sign);

            request.set_body(value);
            request.set_request_uri(builder.to_string());
            LOG_DEBUG("add_new_order url:%s%s, params:%s",m_curcfg.restBaseUrl.c_str(), m_orderUrl.to_string().c_str(), value.serialize().c_str());
            restclient.request(request)
            .then([&](http_response response) -> pplx::task<json::value> { auto code = response.status_code();
                //200 201 401 404
                if(code == status_codes::Created || code == status_codes::OK
                || code == status_codes::Unauthorized || code == status_codes::NotFound
                || code == status_codes::BadRequest){
                    return response.extract_json();
                }
//                else if(code == status_codes::ServiceUnavailable){//503
//                    auto s = response.extract_string().get();
//                    rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN ;
//                    rcmd.body.newOrder.ErrorID = ERROR_NetworkUnknownError;
//                    strncpy(rcmd.body.newOrder.originMsg, s.c_str(), sizeof(rcmd.body.newOrder.originMsg));
//                    LOG_ERROR("%s", s.c_str());
//                    g_rptInnerQueue.push(rcmd);
//                    return pplx::task_from_result(json::value());
//                }
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
                    strcpy(rcmd.body.newOrder.orderId,orderId.c_str());
                    strcpy(rcmd.body.newOrder.clientOrderId,v.at("text").as_string().c_str());
                    rcmd.body.newOrder.orderStatus =  OrderStatus_REST_NEW;
                    rcmd.body.newOrder.ErrorID = ERROR_NoError;
                    double size = v.at("size").as_double();//stod(v.at("size").serialize().c_str());
                    rcmd.body.newOrder.volumeTotal = size > 0 ? size : -size;
                    rcmd.body.newOrder.limitPrice = stod(v.at("price").as_string().c_str());
                    if(v.has_field("left")){
                        double left = v.at("left").as_integer();
                        left = left > 0 ? left : -left;
                        rcmd.body.newOrder.volumeTraded = rcmd.body.newOrder.volumeTotal - left;
                    }
                    if(v.has_field("fill_price")) {
                        rcmd.body.newOrder.tradePrice = stod(v.at("fill_price").as_string().c_str());
                    }
                    rcmd.body.newOrder.insertTime = (long long)( v.at("create_time").as_double() * 1000000) ;
                    rcmd.body.newOrder.updateTime = crypto::getCurrentTime();
                    g_rptInnerQueue.push(rcmd);
                }
                else if( v.has_field("label")){
                    //{"label":"INVALID_PARAM_VALUE","message":"text content not starting with `t-`"}
                    rcmd.body.newOrder.ErrorID = crypto::get_gateio_errorid(v.serialize().c_str());
                    strncpy(rcmd.body.newOrder.originMsg,v.serialize().c_str(), sizeof(rcmd.body.newOrder.originMsg));
                    rcmd.body.newOrder.insertTime = cmd.body.newOrder.insertTime;
                    rcmd.body.newOrder.updateTime = crypto::getCurrentTime();
                    LOG_ERROR("%s",v.serialize().c_str());
                    g_rptInnerQueue.push(rcmd);
                }
            })
            .wait();
        }
        else{
            rcmd.body.newOrder.ErrorID = ERROR_SMCInstrumentNotExistError;
            g_rptInnerQueue.push(rcmd);
        }
    }
    catch(exception &e){
        rcmd.body.newOrder.ErrorID = ERROR_NetworkError;
        strncpy(rcmd.body.newOrder.originMsg, e.what(), sizeof(rcmd.body.newOrder.originMsg));
        LOG_ERROR("%s", e.what());
        g_rptInnerQueue.push(rcmd);
    }
}

void GateioUSwapTradingClient::cancel_order(pubsub::TCommand &cmd){
    pubsub::RCommand rcmd;
    memset(&rcmd, 0, sizeof(pubsub::RCommand));
    rcmd.header.exchangeTypeEnum = cmd.header.exchangeTypeEnum;
    rcmd.header.instTypeEnum = cmd.header.instTypeEnum;
    strcpy(rcmd.header.accountId, cmd.header.accountId);
    rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_CANCEL_ORDER;
    rcmd.header.insertTime = crypto::getCurrentTime();
    rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
    strcpy(rcmd.body.cancelOrder.instId, cmd.body.cancelOrder.instId);
    strcpy(rcmd.body.cancelOrder.orderId, cmd.body.cancelOrder.orderId);
    strcpy(rcmd.body.cancelOrder.clientOrderId, cmd.body.cancelOrder.clientOrderId);
    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info("GATEIO","SWAP", cmd.body.cancelOrder.instId, info)){
            http_client restclient(m_curcfg.restBaseUrl);//bUrl
            http_request request(methods::DEL);
            request.headers().add("Accept","application/json");
            request.headers().add("Content-Type","application/json");
            string queryStr{""};
            string time = to_string(crypto::getCurrentTimeSeconds());
            if(cmd.body.cancelOrder.cancelOrderTypeEnum == pubsub::COT_ONE_INST){
                string cancelOrderUrl = m_orderUrl.to_string();
                if(!crypto::str_cmp(cmd.body.cancelOrder.orderId,"")){
                    cancelOrderUrl.append("/").append(cmd.body.cancelOrder.orderId);
                }
                else if(!crypto::str_cmp(cmd.body.cancelOrder.clientOrderId,"")){
                    cancelOrderUrl.append("/").append(cmd.body.cancelOrder.clientOrderId);
                }
                else{
                    LOG_ERROR("cancel order need orderId or clientOrderId" );
                    strcpy(rcmd.body.cancelOrder.originMsg, "cancel order need orderId or clientOrderId");
                    rcmd.body.cancelOrder.ErrorID = ERROR_NoOrderId;
                    rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                    g_rptInnerQueue.push(rcmd);
                    return;
                }
//            cout << queryStr << endl;
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
                restclient.request(request)
                .then([&](http_response response) -> pplx::task<json::value> {
                    auto code = response.status_code();
                    if(code == status_codes::Created || code == status_codes::OK
                       || code == status_codes::Unauthorized || code == status_codes::NotFound
                       || code == status_codes::BadRequest) {
//                    if(code == status_codes::Created || code == status_codes::OK || code == status_codes::BadRequest)
                        return response.extract_json();
                    }
                    else{
                        auto s = response.extract_string().get();
                        rcmd.body.cancelOrder.ErrorID = code;
                        strncpy(rcmd.body.cancelOrder.originMsg, s.c_str(), sizeof(rcmd.body.cancelOrder.originMsg));
                        LOG_ERROR("%s", s.c_str());
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                        g_rptInnerQueue.push(rcmd);
                        return pplx::task_from_result(json::value());
                    }
//                    string msg{"response code is not 2xx, but:"};
//                    msg.append(to_string(code));
//                    throw crypto_exception(msg.c_str());
//                    return pplx::task_from_result(json::value());
                }) // continue when the JSON value is available
                .then([&](pplx::task<json::value> previousTask) {
                    json::value const &v = previousTask.get();
                    LOG_DEBUG("cancel_order response:%s", v.serialize().c_str());
                    if(v.has_field("id") && v.has_field("status")){
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_CANCELED;
                        string orderId = std::to_string(v.at("id").as_number().to_int64());
                        strcpy(rcmd.body.cancelOrder.orderId, orderId.c_str());
                        strcpy(rcmd.body.cancelOrder.clientOrderId,v.at("text").as_string().c_str());
                        rcmd.body.cancelOrder.offsetFlag = OffsetFlag_OPEN;//crypto::str_cmp(data[i][""].GetString())
                        double size  = stod(v.at("size").serialize().c_str());

                        rcmd.body.cancelOrder.direction = size > 0 ? Direction_LONG : Direction_SHORT ;

                        rcmd.body.cancelOrder.volumeTotal  = size > 0 ? size : -size ;
                        rcmd.body.cancelOrder.limitPrice   = stod(v.at("price").as_string().c_str());

                        if(v.has_field("tif")){
                            string tif = v.at("tif").as_string();
                            if(crypto::str_cmp(tif.c_str(),"gtc")){
                                rcmd.body.cancelOrder.orderType = OrderType_LIMIT;
                            }
                            else if(crypto::str_cmp(tif.c_str(),"ioc")){
                                if(rcmd.body.cancelOrder.limitPrice == 0){
                                    rcmd.body.cancelOrder.orderType = OrderType_MARKET;
                                }
                                else{
                                    rcmd.body.cancelOrder.orderType = OrderType_IOC;
                                }
                            }
                            else if(crypto::str_cmp(tif.c_str(),"poc")){
                                rcmd.body.cancelOrder.orderType = OrderType_POST_ONLY;
                            }
                            else{
                                rcmd.body.cancelOrder.orderType = OrderType_UNKNOWN;
                            }
                        }
                        else{
                            LOG_ERROR("no tif info in:%s",v.serialize().c_str());
                        }

                        double left = v.at("left").as_integer();
                        left = left > 0 ? left : -left;
                        rcmd.body.cancelOrder.volumeTraded = rcmd.body.cancelOrder.volumeTotal - left;
                        rcmd.body.cancelOrder.tradePrice   = stod(v.at("fill_price").as_string().c_str() );

                        rcmd.body.cancelOrder.insertTime = stol(v.at("create_time").serialize().c_str()) * 1000*1000;
                        rcmd.body.cancelOrder.updateTime = stol(v.at("finish_time").serialize().c_str()) * 1000*1000;

                        g_rptInnerQueue.push(rcmd);
                    }
                    else{
                        LOG_ERROR("%s",v.serialize().c_str());
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                        rcmd.body.cancelOrder.ErrorID = crypto::get_gateio_errorid(v.serialize().c_str());
                        if(rcmd.body.cancelOrder.ErrorID == ERROR_OrderNotFoundError){
                            rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                        }
                        rcmd.body.cancelOrder.insertTime = cmd.body.cancelOrder.insertTime;
                        rcmd.body.cancelOrder.updateTime = crypto::getCurrentTime();
                        strncpy(rcmd.body.cancelOrder.originMsg,v.serialize().c_str(), sizeof(rcmd.body.cancelOrder.originMsg));
                        g_rptInnerQueue.push(rcmd);
                    }

                })
                .wait();
            }
            else if(cmd.body.cancelOrder.cancelOrderTypeEnum == pubsub::COT_MULTI_INST){
                // TODO
            }
            else{
                rcmd.body.cancelOrder.ErrorID = ERROR_CancelOrQueryTypeError;
                rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                g_rptInnerQueue.push(rcmd);
            }
        }
        else{
            rcmd.body.cancelOrder.ErrorID = ERROR_SMCInstrumentNotExistError;
            rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
            LOG_ERROR("not found GATEIO SWAP %s smc info", cmd.body.cancelOrder.instId);
            g_rptInnerQueue.push(rcmd);
        }
    }
    catch(exception &e) {
        rcmd.body.cancelOrder.ErrorID = ERROR_OrderNotFoundError;
        rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
        rcmd.body.cancelOrder.insertTime = cmd.body.cancelOrder.insertTime;
        rcmd.body.cancelOrder.updateTime = crypto::getCurrentTime();
        LOG_ERROR("%s", e.what());
        g_rptInnerQueue.push(rcmd);
    }
    return;
}

void GateioUSwapTradingClient::query_multi_orders(pubsub::TCommand &cmd){
    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info("GATEIO","SWAP",cmd.body.queryOrder.instId, info)){
            http_client restclient(m_curcfg.restBaseUrl);//bUrl
            http_request request(methods::GET);
            request.headers().add("Accept","application/json");
            request.headers().add("Content-Type","application/json");
            string queryStr{""};
            queryStr.append("/contract=").append(info.originInstId)
                    .append("&status=open");
            string time = to_string(crypto::getCurrentTimeSeconds());
            string queryOrderUrl = m_orderUrl.to_string();

            string sign = get_signature_rest("GET", queryOrderUrl.c_str(), time.c_str(),
                                             queryStr.c_str(), "");
            queryOrderUrl.append("?").append(queryStr);
            web::uri m_queryOrderUrl  = queryOrderUrl;
            uri_builder builder(m_queryOrderUrl);// bUrl m_balanceUrl
            LOG_DEBUG("%s url:%s,params:%s",__FUNCTION__, m_queryOrderUrl.to_string().c_str(),
                      queryStr.c_str());
            request.headers().add("KEY",m_curcfg.apiKey);
            request.headers().add("Timestamp",time);
            request.headers().add("SIGN",sign);
            request.set_request_uri(builder.to_string());

            restclient.request(request)
            .then([](http_response response) -> pplx::task<json::value> { auto code = response.status_code();
                if(code == status_codes::Created || code == status_codes::OK || code == status_codes::BadRequest)
                    return response.extract_json();
                return pplx::task_from_result(json::value());
            }) // continue when the JSON value is available
            .then([&](pplx::task<json::value> previousTask) {
                // get the JSON value from the task and display content from it
                json::value const &data = previousTask.get();
                LOG_DEBUG("query_multi_orders response:%s", data.serialize().c_str());
                if(data.is_array()){
                    for(auto &v : data.as_array()){
                        pubsub::RCommand rcmd;
                        memset(&rcmd,0,sizeof(pubsub::RCommand));
                        strcpy(rcmd.header.accountId, cmd.header.accountId);
                        rcmd.header.instTypeEnum = cmd.header.instTypeEnum;
                        rcmd.header.exchangeTypeEnum = cmd.header.exchangeTypeEnum;
                        rcmd.header.insertTime = crypto::getCurrentTime();
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;

                        strcpy(rcmd.body.orderResponse.instId, info.instId);
                        string orderId = std::to_string(v.at("id").as_number().to_int64());
                        strcpy(rcmd.body.orderResponse.orderId, orderId.c_str());
                        strcpy(rcmd.body.orderResponse.clientOrderId, v.at("text").as_string().c_str());

                        bool is_close = v.at("is_close").as_bool();
                        if(!is_close){
                            rcmd.body.orderResponse.offsetFlag = OffsetFlag_OPEN;
                        }
                        else{
                            rcmd.body.orderResponse.offsetFlag = OffsetFlag_CLOSE;
                        }

                        double size  = stod(v.at("size").serialize().c_str());//v.at("size").as_integer();

                        rcmd.body.orderResponse.direction = size > 0 ? Direction_LONG : Direction_SHORT ;
                        rcmd.body.orderResponse.volumeTotal  = size > 0 ? size : -size ;
                        rcmd.body.orderResponse.limitPrice   = stod(v.at("price").as_string().c_str());
                        int left = stod(v.at("left").serialize().c_str());//v.at("left").as_integer();//
                        left = left > 0 ? left : -left;
                        rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
                        rcmd.body.orderResponse.tradePrice   = stod(v.at("fill_price").as_string().c_str() );
                        rcmd.body.orderResponse.reduceOnly = v.at("is_reduce_only").as_bool();
                        if(crypto::str_cmp(v.at("status").as_string().c_str() ,"open")){
                            //成交数量<挂单数量 && 成交数量大于0 --> 部分成交
                            if(rcmd.body.orderResponse.volumeTotal > rcmd.body.orderResponse.volumeTraded
                               && rcmd.body.orderResponse.volumeTraded > 0 ){
                                rcmd.body.orderResponse.orderStatus = OrderStatus_PARTFILLED;
                            }
                            else{
                                rcmd.body.orderResponse.orderStatus = OrderStatus_NEW;
                            }
                        }
                        else{
                            rcmd.body.orderResponse.orderStatus = OrderStatus_UNKNOWN;
                        }

                        string tif = v.at("tif").as_string();
                        GET_ORDERTYPE(rcmd)
                        rcmd.body.orderResponse.insertTime = stol(v.at("create_time").serialize().c_str()) * 1000*1000; //v.at("create_time").as_double() * 1000*1000;
                        if(v.has_field("finish_time")){
                            rcmd.body.orderResponse.updateTime = stol(v.at("finish_time").serialize().c_str()) * 1000*1000; //v.at("finish_time").as_double() * 1000*1000;
                        }
                        else{
                            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                        }
                        rcmd.body.orderResponse.tsParse = crypto::getCurrentTime();
                        g_rptInnerQueue.push(rcmd);
                    }
                }
            })
            .wait();
        }
        else{
            LOG_ERROR("not found GATEIO.SWAP.%s smc info", cmd.body.queryOrder.instId);
        }
    }
    catch(exception &e) {
        LOG_ERROR("%s", e.what());
    }
}

void GateioUSwapTradingClient::query_one_order(pubsub::TCommand &cmd){
    pubsub::RCommand rcmd;
    memset(&rcmd,0,sizeof(pubsub::RCommand));
    strcpy(rcmd.header.accountId, cmd.header.accountId);
    rcmd.header.instTypeEnum = cmd.header.instTypeEnum;
    rcmd.header.exchangeTypeEnum = cmd.header.exchangeTypeEnum;
    rcmd.header.insertTime = crypto::getCurrentTime();
    rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;

    strcpy(rcmd.body.orderResponse.instId, cmd.body.queryOrder.instId);
    strcpy(rcmd.body.orderResponse.orderId, cmd.body.queryOrder.orderId);
    strcpy(rcmd.body.orderResponse.clientOrderId, cmd.body.queryOrder.clientOrderId);
    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info("GATEIO", "SWAP", cmd.body.queryOrder.instId, info)){
            http_client restclient(m_curcfg.restBaseUrl);//bUrl
            http_request request(methods::GET);
            request.headers().add("Accept","application/json");
            request.headers().add("Content-Type","application/json");
            string queryStr{""};
            string time = to_string(crypto::getCurrentTimeSeconds());
            string queryOrderUrl = m_orderUrl.to_string();
            if(!crypto::str_cmp(cmd.body.queryOrder.orderId,"")){
                queryOrderUrl.append("/").append(cmd.body.queryOrder.orderId);
            }
            else if(!crypto::str_cmp(cmd.body.queryOrder.clientOrderId,"")){
                queryOrderUrl.append("/").append(cmd.body.queryOrder.clientOrderId);
            }
            else{
                LOG_ERROR("query_order need orderId or clientOrderId");
                return;
            }
            string sign = get_signature_rest("GET", queryOrderUrl.c_str(), time.c_str(),
                                             queryStr.c_str(), "");
//            queryOrderUrl.append("?").append(queryStr);
            web::uri m_queryOrderUrl  = queryOrderUrl;
            uri_builder builder(m_queryOrderUrl);
            LOG_DEBUG("%s url:%s,params:%s,",__FUNCTION__, m_queryOrderUrl.to_string().c_str(),queryStr.c_str());
            request.headers().add("KEY",m_curcfg.apiKey);
            request.headers().add("Timestamp",time);
            request.headers().add("SIGN",sign);
            request.set_request_uri(builder.to_string());

            restclient.request(request)
            .then([&](http_response response) -> pplx::task<json::value> { auto code = response.status_code();
//                if(code == status_codes::Created || code == status_codes::OK || code == status_codes::BadRequest)
                if(code == status_codes::Created || code == status_codes::OK
                       || code == status_codes::Unauthorized || code == status_codes::NotFound
                       || code == status_codes::BadRequest) {
                    return response.extract_json();
                }
                else{
                    auto s = response.extract_string().get();
                    rcmd.body.orderResponse.ErrorID = code;
                    rcmd.body.orderResponse.orderStatus = OrderStatus_REJECTED;
                    strncpy(rcmd.body.orderResponse.originMsg, s.c_str(), sizeof(rcmd.body.orderResponse.originMsg));
                    LOG_ERROR("query_one_order:code:%d,response:%s",code, s.c_str());
                    g_rptInnerQueue.push(rcmd);
                    return pplx::task_from_result(json::value());
                }
//                string msg{"response code is not 2xx , but:"};
//                msg.append(to_string(code));
//                throw crypto_exception(msg.c_str());
//                return pplx::task_from_result(json::value());
            })
            .then([&](pplx::task<json::value> previousTask) {
                json::value const &v = previousTask.get();
                LOG_DEBUG("query_one_order response:%s", v.serialize().c_str());
                if(v.has_field("id") && v.has_field("status")){
                    strcpy(rcmd.body.orderResponse.instId, info.instId);
                    string orderId = std::to_string(v.at("id").as_number().to_int64());
                    strcpy(rcmd.body.orderResponse.orderId, orderId.c_str());
                    strcpy(rcmd.body.orderResponse.clientOrderId, v.at("text").as_string().c_str());

                    bool is_close = v.at("is_close").as_bool();
                    if(!is_close){
                        rcmd.body.orderResponse.offsetFlag = OffsetFlag_OPEN;
                    }
                    else{
                        rcmd.body.orderResponse.offsetFlag = OffsetFlag_CLOSE;
                    }

                    double size  = v.at("size").as_integer();

                    rcmd.body.orderResponse.direction = size > 0 ? Direction_LONG : Direction_SHORT ;
                    rcmd.body.orderResponse.volumeTotal  = size > 0 ? size : -size ;
                    rcmd.body.orderResponse.limitPrice   = stod(v.at("price").as_string().c_str());
//                    int left = v.at("left").as_integer();//stod(v.at("left").serialize().c_str());
//                    left = left > 0 ? left : -left;
                    double left = stod(v.at("left").serialize().c_str());
                    left = left > 0 ? left : -left;
                    rcmd.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTotal - left;
                    rcmd.body.orderResponse.tradePrice   = stod(v.at("fill_price").as_string().c_str() );
                    rcmd.body.orderResponse.reduceOnly = v.at("is_reduce_only").as_bool();
                    if(crypto::str_cmp(v.at("status").as_string().c_str() ,"open")){
                        //成交数量<挂单数量 && 成交数量大于0 --> 部分成交
                        if(rcmd.body.orderResponse.volumeTotal > rcmd.body.orderResponse.volumeTraded
                           && rcmd.body.orderResponse.volumeTraded > 0 ){
                            rcmd.body.orderResponse.orderStatus = OrderStatus_PARTFILLED;
                        }
                        else{
                            rcmd.body.orderResponse.orderStatus = OrderStatus_NEW;
                        }
                    }
                    else{
                        if(v.has_field("finish_as")){
                            string finish_as = v.at("finish_as").as_string();
                            if(crypto::str_cmp(finish_as.c_str(),"filled")){
                                rcmd.body.orderResponse.orderStatus = OrderStatus_FILLED;
                            }
                            else if(crypto::str_cmp(finish_as.c_str(),"cancelled")
                                    || crypto::str_cmp(finish_as.c_str(),"liquidated")//- 强制平仓撤销
                                    || crypto::str_cmp(finish_as.c_str(),"ioc")//未立即完全成交，因为tif设置为ioc
                                    || crypto::str_cmp(finish_as.c_str(),"auto_deleveraged")//自动减仓撤销
                                    || crypto::str_cmp(finish_as.c_str(),"reduce_only")//: 增持仓位撤销，因为设置reduce_only或平仓
                                    ){
                                rcmd.body.orderResponse.orderStatus = OrderStatus_CANCELED;
                            }
                        }
                        else{
                            rcmd.body.orderResponse.orderStatus = OrderStatus_UNKNOWN;
                        }
                    }

                    string tif = v.at("tif").as_string();
                    GET_ORDERTYPE(rcmd)
                    rcmd.body.orderResponse.insertTime = stol(v.at("create_time").serialize().c_str()) * 1000*1000; //v.at("create_time").as_double() * 1000*1000;
                    if(v.has_field("finish_time")){
                        rcmd.body.orderResponse.updateTime = stol(v.at("finish_time").serialize().c_str()) * 1000*1000; //v.at("finish_time").as_double() * 1000*1000;
                    }
                    else{
                        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    }
                    rcmd.body.orderResponse.tsParse = crypto::getCurrentTime();
                    g_rptInnerQueue.push(rcmd);
                }
                else if(v.has_field("label")) {
                    rcmd.body.orderResponse.ErrorID = crypto::get_gateio_errorid(v.serialize().c_str());
                    rcmd.body.orderResponse.orderStatus = OrderStatus_REJECTED;
                    strncpy(rcmd.body.orderResponse.originMsg, v.serialize().c_str(), sizeof(rcmd.body.orderResponse.originMsg));
                    LOG_ERROR("query_one_order:%s",v.serialize().c_str());
                    g_rptInnerQueue.push(rcmd);
                }
            })
            .wait();
        }
        else{
            rcmd.body.orderResponse.ErrorID = ERROR_SMCInstrumentNotExistError;
            rcmd.body.orderResponse.orderStatus = OrderStatus_UNKNOWN;
            LOG_ERROR("not found GATEIO.SWAP.%s smc info", cmd.body.queryOrder.instId);
            g_rptInnerQueue.push(rcmd);
        }
    }
    catch(exception &e) {
        rcmd.body.orderResponse.ErrorID = ERROR_NetworkError;
        rcmd.body.orderResponse.orderStatus = OrderStatus_UNKNOWN;
        strncpy(rcmd.body.orderResponse.originMsg, e.what(), sizeof(rcmd.body.orderResponse.originMsg));
        LOG_ERROR("%s", e.what());
        g_rptInnerQueue.push(rcmd);
    }
}

void GateioUSwapTradingClient::query_order(pubsub::TCommand &cmd){
    pubsub::RCommand rcmd;
    memset(&rcmd, 0, sizeof(pubsub::RCommand));
    strcpy(rcmd.header.accountId, cmd.header.accountId);
    rcmd.header.instTypeEnum = cmd.header.instTypeEnum;
    rcmd.header.exchangeTypeEnum = cmd.header.exchangeTypeEnum;
    rcmd.header.insertTime = crypto::getCurrentTime();
    rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
    if(cmd.body.queryOrder.queryOrderTypeEnum == QOT_ONE_INST){
        query_one_order(cmd);
    }
    else if(cmd.body.queryOrder.queryOrderTypeEnum == QOT_MULTI_INST){
        query_multi_orders(cmd);
    }
    else{
        rcmd.body.orderResponse.ErrorID = ERROR_CancelOrQueryTypeError;
        LOG_ERROR("query_order got unknown queryOrderType:%d", cmd.body.queryOrder.queryOrderTypeEnum);
        g_rptInnerQueue.push(rcmd);
    }
}