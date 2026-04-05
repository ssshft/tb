#include "api/OKX/OKXSpotSwapFuturesTradingClient.h"


#define HANDLE_InstType(rcmd) \
    md::InstrumentInfo info;\
    if(this->smc->get_instrument_info("OKX", instType.c_str(), originInstId.c_str(), info)){\
        strcpy(rcmd.body.orderResponse.instId, info.instId);\
    }\
    else{\
        LOG_ERROR("not found OKX.%s.%s smc info", instType.c_str(), originInstId.c_str());\
        continue;\
    }\
    rcmd.header.instTypeEnum = info.instTypeEnum;

static std::string const base64_chars =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789+/";

OKXSpotSwapFuturesTradingClient::OKXSpotSwapFuturesTradingClient(){

}

OKXSpotSwapFuturesTradingClient::~OKXSpotSwapFuturesTradingClient(){
//    delete smc;
}

bool OKXSpotSwapFuturesTradingClient::Initialize(AccountCfg& cfg, sm::SecurityManager *smc){
    this->smc = smc;
    m_balanceUrl = "/api/v5/account/balance";
    m_positionUrl = "/api/v5/account/positions";
    m_orderUrl = "/api/v5/trade/order";
    m_cancelOrderUrl = "/api/v5/trade/cancel-order";

#ifdef DEBUG_OKX
    m_wssb_url = "/ws/v5/private?brokerId=9999";
#else
    m_wssb_url = "/ws/v5/private";
#endif

    m_curcfg = cfg;
    // addNewOrderRestClient = new http_client(m_curcfg.restBaseUrl);
    // cancelOrderRestClient = new http_client(m_curcfg.restBaseUrl);
    hotHttpClient = new http_client(m_curcfg.restBaseUrl);
    return true;
}

void OKXSpotSwapFuturesTradingClient::Run(){
    std::thread monitorThread(&OKXSpotSwapFuturesTradingClient::monitor, this);
    monitorThread.detach();
}

void OKXSpotSwapFuturesTradingClient::sub_websocket()//{// websocket_client websocket_callback_client &wsclient)
try{
    m_IsConnected = false;
    uri_builder builder(m_curcfg.wsBaseUrl);
    builder.append_path(m_wssb_url.to_string());
    wsClient.close();
    wsClient.connect(builder.to_string())
    .then([&](){
        std::function<void (const websocket_incoming_message &msg)> f;
        f = std::bind(&OKXSpotSwapFuturesTradingClient::on_websocket_msg, this, placeholders::_1);
        wsClient.set_message_handler(f);
        std::function<void (websocket_close_status close_status,
            const utility::string_t& reason, const std::error_code& error)> c;
        c =  std::bind(&OKXSpotSwapFuturesTradingClient::on_close_msg, this,
            placeholders::_1, placeholders::_2, placeholders::_3);
        wsClient.set_close_handler(c);
    }).wait();
    login();
    int count = 0;
    while(1){
        sleep(1);
        if(m_IsConnected){
	    // sub_balances_positions_channel();
            sub_account_channel();
            sub_positions_channel();
            sub_orders_channel();
            break;
        }
        else{
            count++;
        }
        if(count > 10){
            m_IsConnected = false;
            break;
        }
    }
}
catch(exception &e) {
    m_IsConnected = false;
    LOG_ERROR("%s", e.what());
}

void OKXSpotSwapFuturesTradingClient::login(){
    json::value subValue;
    subValue["op"] = json::value::string("login");
    subValue["args"][0]["apiKey"] = json::value::string(m_curcfg.apiKey);
    subValue["args"][0]["passphrase"] = json::value::string(m_curcfg.userId);
    LOG_INFO("apiKey: %s secretKey:%s passphrase: %s", m_curcfg.apiKey, m_curcfg.apiSecret, m_curcfg.userId);
    string timestamp = to_string(crypto::getCurrentTimeSeconds());
    string sigStr = timestamp + "GET/users/self/verify";
    // string sign = crypto::encryptWithHMACForOKX(m_curcfg.apiSecret, sigStr);//crypto::getSignOKX(m_curcfg.apiSecret, timestamp, "GET", "/users/self/verify", "");//
    string sign = get_signature_rest(timestamp, "GET", "/users/self/verify", "");
    subValue["args"][0]["timestamp"] = json::value::string(timestamp);
    subValue["args"][0]["sign"] = json::value::string(sign);
    websocket_outgoing_message outMsg;
    outMsg.set_utf8_message(subValue.serialize().c_str());
    wsClient.send(outMsg).wait();

    // orderWSClient.send(outMsg).then([](){ /* Successfully sent the message. */ });
}

void OKXSpotSwapFuturesTradingClient::sub_orders_channel(){
    json::value subValue;
    subValue["op"] = json::value::string("subscribe");
    subValue["args"][0]["channel"] = json::value::string("orders");
    subValue["args"][0]["instType"] = json::value::string("ANY");
    websocket_outgoing_message outMsg;
    outMsg.set_utf8_message(subValue.serialize().c_str());
    wsClient.send(outMsg).wait();
}

void OKXSpotSwapFuturesTradingClient::sub_balances_positions_channel(){
    json::value subValue;
    subValue["op"] = json::value::string("subscribe");
    subValue["args"][0]["channel"] = json::value::string("balance_and_position");
    subValue["args"][0]["instType"] = json::value::string("ANY");
    websocket_outgoing_message outMsg;
    outMsg.set_utf8_message(subValue.serialize().c_str());
    wsClient.send(outMsg).wait();
}

void OKXSpotSwapFuturesTradingClient::sub_account_channel() {
    json::value subValue;
    subValue["op"] = json::value::string("subscribe");
    subValue["args"][0]["channel"] = json::value::string("account");
    subValue["args"][0]["extraParams"] = json::value::string("{\"updateInterval\": 0}");
    websocket_outgoing_message outMsg;
    outMsg.set_utf8_message(subValue.serialize().c_str());
    wsClient.send(outMsg).wait();
}

void OKXSpotSwapFuturesTradingClient::sub_positions_channel() {
    json::value subValue;
    subValue["op"] = json::value::string("subscribe");
    subValue["args"][0]["channel"] = json::value::string("positions");
    subValue["args"][0]["extraParams"] = json::value::string("{\"updateInterval\": 0}");
    subValue["args"][0]["instType"] = json::value::string("ANY");
    websocket_outgoing_message outMsg;
    outMsg.set_utf8_message(subValue.serialize().c_str());
    wsClient.send(outMsg).wait();
}

void OKXSpotSwapFuturesTradingClient::monitor() {
    while(1){
        try{
            LOG_INFO("start to connect with okx ws api:%s", m_curcfg.wsBaseUrl.c_str());
            sub_websocket();
            sleep(2);
            while(m_IsConnected){
                sleep(10);
                if(!m_IsConnected){
                    LOG_ERROR("okx ws disconnected, will reconnect it now");
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
    }
}

void OKXSpotSwapFuturesTradingClient::on_websocket_msg(const websocket_incoming_message& msg)
try{
    // text_message, binary_message, close, ping, pong
    if (msg.message_type() == websocket_message_type::text_message){
        msg.extract_string().then([&](const string s){
	    LOG_INFO("on_websocket_msg: %s", s.c_str());
            rapidjson::Document d;
            rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(s.c_str());
            if(d.HasParseError() || !rawData.IsObject()){
                return;
            }
            if(rawData.HasMember("event")){
                string event = rawData["event"].GetString();
                if(event[0] == 'l' && rawData.HasMember("code")){
                    string code = rawData["code"].GetString();
                    if(code[0] == '0'){
                        //登录成功
                        m_IsConnected = true;
                        LOG_INFO("OKX ws login successfully");
                    }
                    else{
                        m_IsConnected = false;
                        LOG_ERROR("OKX ws login failed, msg:%s", s.c_str());
                    }
                    return;
                }
            }
            else if(rawData.HasMember("arg") && rawData.HasMember("data") ){
                string channel = rawData["arg"]["channel"].GetString();
                // const rapidjson::Value &data = rawData["data"];
                if(channel[0] == 'b'){//balance_and_position
                    const rapidjson::Value &bData = rawData["data"][0]["balData"];
                    for(rapidjson::SizeType i = 0; i < bData.Size(); i++){
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.header.cmdTime = crypto::getCurrentTime();
                        rcmd.header.exchangeTypeEnum = ExchangeType_OKX;
                        rcmd.header.instTypeEnum = InstType_SPOT;
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        strcpy(rcmd.body.balance.currency, bData[i]["ccy"].GetString());
                        if(crypto::str_cmp(bData[i]["cashBal"].GetString(),"") == false){
                            rcmd.body.balance.available = stod(bData[i]["cashBal"].GetString());
                        }
                        rcmd.body.balance.apiSourceEnum = ApiSource_WEBSOCKET;
                        PUSH_RCMD(rcmd)
                        // string ccy = rcmd.body.balance.currency;
                        // auto found = g_filterSymbolsMap.find(ccy);
                        // if (found != g_filterSymbolsMap.end()){
                        //     PUSH_RCMD(rcmd)
                        // }
                    }
                    const rapidjson::Value &pData = rawData["data"][0]["posData"];
                    for(rapidjson::SizeType i = 0; i < pData.Size(); i++){
                        // cout << pData[i]. << endl;
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.header.exchangeTypeEnum = ExchangeType_OKX;
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        //持仓数量，逐仓自主划转模式下，转入保证金后会产生pos为0的仓位
                        double positionAmt = 0;
                        if(crypto::str_cmp(pData[i]["pos"].GetString(),"") == false){
                            positionAmt = stod(pData[i]["pos"].GetString());
                        }
                        // double positionAmt = stod(pData[i]["pos"].GetString());
                        //交易产品ID，如 BTC-USD-180213
                        string originInstId = pData[i]["instId"].GetString();
                        //持仓方向：long, short, net
                        string positionSide = pData[i]["posSide"].GetString();
                        rcmd.body.position.direction = positionAmt >= 0 ? Direction_LONG : Direction_SHORT;

                        //保证金模式， isolated, cross
                        // string mgnMode = pData[i]["mgnMode"].GetString();
                        //占用保证金的币种
                        // string ccy = pData[i]["ccy"].GetString();
                        //交易产品类型， MARGIN：币币杠杆 SWAP：永续合约 FUTURES：交割合约 OPTION：期权
                        string instType = pData[i]["instType"].GetString();
                        HANDLE_InstType(rcmd)

                        positionAmt = positionAmt > 0 ? positionAmt : -positionAmt;
                        rcmd.body.position.volume = positionAmt;
                        rcmd.body.position.avgPrice = stod(pData[i]["avgPx"].GetString());
                        // rcmd.body.position.maintMargin = stod(pData[i]["mmr"].GetString());
                        // rcmd.body.position.unrealizedPnl = stod(pData[i]["upl"].GetString());
                        // rcmd.body.position.markPrice = stod(pData[i]["markPx"].GetString());
                        // rcmd.body.position.liquidPrice = stod(pData[i]["liqPx"].GetString());
                        // rcmd.body.position.adlQuantile = stod(pData[i]["adl"].GetString());

                        rcmd.body.position.apiSourceEnum = ApiSource_WEBSOCKET;
                        PUSH_RCMD(rcmd)
                        // string instId = rcmd.body.position.instId;
                        // auto found = g_filterSymbolsMap.find(instId);
                        // if(found != g_filterSymbolsMap.end()){
                        //     PUSH_RCMD(rcmd)
                        // }
                    }
                }
                else if(channel[0] == 'o'){//orders
                    const rapidjson::Value &pData = rawData["data"];
                    for(rapidjson::SizeType i = 0; i < pData.Size(); i++){
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.header.cmdTime = crypto::getCurrentTime();
                        rcmd.header.exchangeTypeEnum = ExchangeType_OKX;
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        strcpy(rcmd.body.orderResponse.orderId, pData[i]["ordId"].GetString());
                        strcpy(rcmd.body.orderResponse.orderSysId, pData[i]["clOrdId"].GetString());

                        //已经成交量
                        if(crypto::str_cmp(pData[i]["accFillSz"].GetString(), "") == false){
                            rcmd.body.orderResponse.volumeTraded = stod(pData[i]["accFillSz"].GetString());
                        }
                        //成交均价
                        if(crypto::str_cmp(pData[i]["avgPx"].GetString(), "") == false){
                            rcmd.body.orderResponse.tradePrice = stod(pData[i]["avgPx"].GetString());
                        }
                        //原始订单数量
                        if(crypto::str_cmp(pData[i]["sz"].GetString(), "") == false){
                            rcmd.body.orderResponse.volumeTotal = stod(pData[i]["sz"].GetString());
                        }
                        //原始订单价格
                        if(crypto::str_cmp(pData[i]["px"].GetString(), "") == false){
                            rcmd.body.orderResponse.limitPrice = stod(pData[i]["px"].GetString());
                        }
                        string orderStatus = pData[i]["state"].GetString();
                        if(orderStatus[0] == 'l'){//新订单
                            rcmd.body.orderResponse.orderStatus = OrderStatus_NEW;
                        }
                        else if(orderStatus[0] == 'c'){//CANCELED 订单被取消
                            rcmd.body.orderResponse.orderStatus = OrderStatus_CANCELED;
                        }
                        else if(orderStatus[0] == 'p'){
                            rcmd.body.orderResponse.orderStatus = OrderStatus_PARTFILLED;
                        }
                        else if(orderStatus[0] == 'f'){
                            rcmd.body.orderResponse.orderStatus = OrderStatus_FILLED;
                        }
                        else {
                            rcmd.body.orderResponse.orderStatus = OrderStatus_UNKNOWN;
                        }
                        //交易产品ID，如 BTC-USD-180213
                        string originInstId = pData[i]["instId"].GetString();
                        string instType = pData[i]["instType"].GetString();
                        md::InstrumentInfo info;
                        if(this->smc->get_instrument_info("OKX", "InstType_USDT_SWAP", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                            strcpy(rcmd.body.orderResponse.instId, info.instId);
                        }
                        else if(this->smc->get_instrument_info("OKX", "InstType_USDT_FUTURES", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_USDT_FUTURES;
                            strcpy(rcmd.body.orderResponse.instId, info.instId);
                        } 
                        else if(this->smc->get_instrument_info("OKX", "InstType_C_SWAP", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_C_SWAP;
                            strcpy(rcmd.body.orderResponse.instId, info.instId);
                        }
                        else if(this->smc->get_instrument_info("OKX", "InstType_C_FUTURES", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_C_FUTURES;
                            strcpy(rcmd.body.orderResponse.instId, info.instId);
                        }
                        else if(this->smc->get_instrument_info("OKX", "InstType_SPOT", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_SPOT;
                            strcpy(rcmd.body.orderResponse.instId, info.instId);
                        }
                        else{
                            LOG_ERROR("not found OKX.%s.%s smc info", instType.c_str(), originInstId.c_str());
                            continue;
                        }
                        
                        rcmd.body.orderResponse.offsetFlag = OffsetFlag_OPEN;
                        string side = pData[i]["side"].GetString();
                        rcmd.body.orderResponse.direction = side[0] == 'b' ? Direction_LONG : Direction_SHORT;
                        string orderType = pData[i]["ordType"].GetString();
                        if(orderType[0] == 'm'){
                            rcmd.body.orderResponse.orderType = OrderType_MARKET;
                        }
                        else if(orderType[0] == 'l'){
                            rcmd.body.orderResponse.orderType = OrderType_LIMIT;
                        }
                        else if(orderType[0] == 'p'){
                            rcmd.body.orderResponse.orderType = OrderType_POST_ONLY;
                        }
                        else if(orderType[0] == 'f'){
                            rcmd.body.orderResponse.orderType = OrderType_FOK;
                        }
                        else if(orderType[0] == 'i'){
                            rcmd.body.orderResponse.orderType = OrderType_IOC;
                        }
                        else if(orderType[0] == 'o'){// TODO optimal_limit_ioc：市价委托立即成交并取消剩余（仅适用交割、永续）
                            rcmd.body.orderResponse.orderType = OrderType_MARKET;
                        }
                        else{
                            rcmd.body.orderResponse.orderType = OrderType_UNKNOWN;
                        }
                        rcmd.body.orderResponse.apiSourceEnum = ApiSource_WEBSOCKET;
                        PUSH_RCMD(rcmd)
                    }
                } else if (channel[0] == 'a'){//account
                    const rapidjson::Value &pData = rawData["data"][0]["details"];
                    for(rapidjson::SizeType i = 0; i < pData.Size(); i++){
                        pubsub::RCommand rcmd;
                        rcmd.body.balance.total = stod(pData[i]["eq"].GetString());
                        rcmd.header.cmdTime = crypto::getCurrentTime();
                        rcmd.header.exchangeTypeEnum = ExchangeType_OKX;
                        rcmd.header.instTypeEnum = InstType_SPOT;
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        strcpy(rcmd.body.balance.currency, pData[i]["ccy"].GetString());
                        rcmd.body.balance.available = stod(pData[i]["cashBal"].GetString());
                        rcmd.body.balance.frozen = stod(pData[i]["frozenBal"].GetString());
                        rcmd.body.balance.unrealizedPnl = stod(pData[i]["upl"].GetString());
                        rcmd.body.balance.apiSourceEnum = ApiSource_WEBSOCKET;
                        PUSH_RCMD(rcmd)
                    }

                    const rapidjson::Value &data = rawData["data"][0];
                    pubsub::RCommand rcmd;
                    rcmd.header.cmdTime = crypto::getCurrentTime();
                    rcmd.header.exchangeTypeEnum = ExchangeType_OKX;
                    rcmd.header.instTypeEnum = InstType_SPOT;
                    rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
                    strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                    strcpy(rcmd.header.accountId, m_curcfg.accountId);
                    rcmd.body.totalAccount.totalEquity = stod(data["totalEq"].GetString());
                    rcmd.body.totalAccount.adjEquity = stod(data["adjEq"].GetString());
                    rcmd.body.totalAccount.mmr = stod(data["mmr"].GetString());
		    string mgnStr = data["mgnRatio"].GetString();
		    if (mgnStr != "") {
                    	rcmd.body.totalAccount.mgnRatio = stod(mgnStr);
            	    } else {
                	rcmd.body.totalAccount.mgnRatio = 100;
	    	    }
                    rcmd.body.totalAccount.apiSourceEnum = ApiSource_WEBSOCKET;
                    PUSH_RCMD(rcmd)
                } else if (channel[0] == 'p') {//positions
                    const rapidjson::Value &pData = rawData["data"];
                    for(rapidjson::SizeType i = 0; i < pData.Size(); i++){
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.header.cmdTime = crypto::getCurrentTime();
                        rcmd.header.exchangeTypeEnum = ExchangeType_OKX;
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        //持仓数量，逐仓自主划转模式下，转入保证金后会产生pos为0的仓位
                        double positionAmt = stod(pData[i]["pos"].GetString());
                        //交易产品ID，如 BTC-USD-180213
                        string originInstId = pData[i]["instId"].GetString();
                        //持仓方向：long, short, net
                        // string positionSide = it.at("posSide").as_string();
                        rcmd.body.position.direction = positionAmt > 0 ? Direction_LONG : Direction_SHORT;
                        // rcmd.body.position.direction = positionSide[0] == 'l' ? Direction_LONG :
                        //                                 positionSide[0] == 's' ? Direction_SHORT : Direction_NET;
                        //保证金模式， isolated, cross
                        // string mgnMode = it.at("mgnMode").as_string();
                        //占用保证金的币种
                        string ccy = pData[i]["ccy"].GetString();
                        //交易产品类型， MARGIN：币币杠杆 SWAP：永续合约 FUTURES：交割合约 OPTION：期权
                        string instType = pData[i]["instType"].GetString();
                        // HANDLE_InstType(rcmd)
                        md::InstrumentInfo info;
                        if (this->smc->get_instrument_info("OKX", "InstType_USDT_SWAP", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                            strcpy(rcmd.body.position.instId, info.instId);
                        }
                        else if (this->smc->get_instrument_info("OKX", "InstType_USDT_FUTURES", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_USDT_FUTURES;
                            strcpy(rcmd.body.position.instId, info.instId);
                        }
                        else if (this->smc->get_instrument_info("OKX", "InstType_C_SWAP", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_C_SWAP;
                            strcpy(rcmd.body.position.instId, info.instId);
                        }
                        else if (this->smc->get_instrument_info("OKX", "InstType_C_FUTURES", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_C_FUTURES;
                            strcpy(rcmd.body.position.instId, info.instId);
                        }
                        else{
                            LOG_ERROR("not found OKX.%s.%s smc info", instType.c_str(), originInstId.c_str());
                            continue;
                        }

                        positionAmt = positionAmt > 0 ? positionAmt : -positionAmt;
                        rcmd.body.position.volume = positionAmt;
                        rcmd.body.position.maintMargin = stod(pData[i]["mmr"].GetString());
                        rcmd.body.position.avgPrice = stod(pData[i]["avgPx"].GetString());
                        rcmd.body.position.unrealizedPnl = stod(pData[i]["upl"].GetString());
                        rcmd.body.position.markPrice = stod(pData[i]["markPx"].GetString());
                        string liqPx = pData[i]["liqPx"].GetString();
                        if(crypto::str_cmp(liqPx.c_str(), "") == false){
                            rcmd.body.position.liquidPrice = stod(liqPx.c_str());
                        }

                        rcmd.body.position.adlQuantile = stod(pData[i]["adl"].GetString());
                        rcmd.body.position.apiSourceEnum = ApiSource_WEBSOCKET;
                        PUSH_RCMD(rcmd)
                    }
                }
                else{
                    // TODO
                    DEBUGLINE
                }
            }
            else if(rawData.HasMember("id") && rawData.HasMember("op")){
                //{"id":"OKXCPP2023846078941918","op":"order","code":"0","msg":"","data":[{"tag":"","ordId":"523559255786782722","clOrdId":"OKXCPP2023846078941918","sCode":"0","sMsg":"Order successfully placed."}]}

            }
            else{
                // TODO
                DEBUGLINE
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
    m_IsConnected = false;
    LOG_ERROR("%s", e.what());
}


void OKXSpotSwapFuturesTradingClient::on_close_msg(websocket_close_status close_status,
                  const utility::string_t& reason, const std::error_code& error)
try{
    m_IsConnected = false;
    LOG_ERROR("okx recv CloseMsg, reason:%s ",reason.c_str() );
}
catch (exception &e){
    LOG_ERROR("%s", e.what());
}



std::string OKXSpotSwapFuturesTradingClient::base64_encode(unsigned char const * input, size_t len) {
    std::string ret;
    int i = 0;
    int j = 0;
    unsigned char char_array_3[3];
    unsigned char char_array_4[4];

    while (len--) {
        char_array_3[i++] = *(input++);
        if (i == 3) {
            char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
            char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
            char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
            char_array_4[3] = char_array_3[2] & 0x3f;

            for(i = 0; (i <4) ; i++) {
                ret += base64_chars[char_array_4[i]];
            }
            i = 0;
        }
    }

    if (i) {
        for(j = i; j < 3; j++) {
            char_array_3[j] = '\0';
        }

        char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
        char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
        char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
        char_array_4[3] = char_array_3[2] & 0x3f;

        for (j = 0; (j < i + 1); j++) {
            ret += base64_chars[char_array_4[j]];
        }

        while((i++ < 3)) {
            ret += '=';
        }
    }

    return ret;
}

string OKXSpotSwapFuturesTradingClient::base64_encode(std::string const & input) {
    return base64_encode(
            reinterpret_cast<const unsigned char *>(input.data()),
            input.size()
    );
}

int OKXSpotSwapFuturesTradingClient::HmacEncode(const char * algo,
               const char * key, unsigned int key_length,
               const char * input, unsigned int input_length,
               unsigned char * &output, unsigned int &output_length) {
    const EVP_MD * engine = NULL;
    if(strcasecmp("sha512", algo) == 0) {
        engine = EVP_sha512();
    }
    else if(strcasecmp("sha256", algo) == 0) {
        engine = EVP_sha256();
    }
    else if(strcasecmp("sha1", algo) == 0) {
        engine = EVP_sha1();
    }
    else if(strcasecmp("md5", algo) == 0) {
        engine = EVP_md5();
    }
    else if(strcasecmp("sha224", algo) == 0) {
        engine = EVP_sha224();
    }
    else if(strcasecmp("sha384", algo) == 0) {
        engine = EVP_sha384();
    }
    else if(strcasecmp("sha", algo) == 0) {
        //engine = EVP_sha();
        assert(0);
    }
    else {
        cout << "Algorithm " << algo << " is not supported by this program!" << endl;
        return -1;
    }

    output = (unsigned char*)malloc(EVP_MAX_MD_SIZE);

    /*
    HMAC_CTX ctx;
    HMAC_CTX_init(&ctx);
    HMAC_Init_ex(&ctx, key, strlen(key), engine, NULL);
    HMAC_Update(&ctx, (unsigned char*)input, strlen(input));        // input is OK; &input is WRONG !!!
    HMAC_Final(&ctx, output, &output_length);
    HMAC_CTX_cleanup(&ctx);

    */
/*
    HMAC_CTX *ctx = HMAC_CTX_new();
    HMAC_CTX_reset(ctx);
    HMAC_Init_ex(ctx, key, strlen(key), engine, NULL);
    HMAC_Update(ctx, (unsigned char*)input, strlen(input));        // input is OK; &input is WRONG !!!
    HMAC_Final(ctx, output, &output_length);
    HMAC_CTX_free(ctx);
*/

    return 0;
}

string OKXSpotSwapFuturesTradingClient::getSignature(const string &query,const string &apiSecret) {
    unsigned char * mac = NULL;
    unsigned int mac_length = 0;
    int ret = HmacEncode("sha256", apiSecret.c_str(), apiSecret.length(), query.c_str(), query.length(), mac, mac_length);
    string signature = base64_encode(mac, mac_length);
    return signature;
}

string OKXSpotSwapFuturesTradingClient::get_signature_rest(const string &timestamp, const string &method,
            const string &requestPath,const string &body){

    unsigned char * mac = NULL;
    unsigned int mac_length = 0;
    string data = timestamp + method + requestPath + body;
    // cout << data << endl;
    string key = m_curcfg.apiSecret;
    // crypto::HmacEncodeSHA256(key.c_str(), key.length(), data.c_str(), data.length(), mac, mac_length);
    //string sign = crypto::HmacEncodeOKX(key, data);
    //sign = websocketpp::base64_encode(mac, mac_length);
   
    string sign = getSignature(data, key);
    //
    //string sign = crypto::HmacEncodeOKX(key.c_str(), data.c_str());

    return sign;
}

void OKXSpotSwapFuturesTradingClient::ping(){
    try{
        if(m_IsConnected){
            websocket_outgoing_message outMsg;
            outMsg.set_utf8_message("ping");
            wsClient.send(outMsg).wait();
        }
    }
    catch(exception &e){
        m_IsConnected = false;
        LOG_ERROR("%s,%s",__FUNCTION__, e.what());
    }
}

bool OKXSpotSwapFuturesTradingClient::get_balances()
try {
    // http_client restclient(m_curcfg.restBaseUrl);
    http_request request(methods::GET);
    // request.headers().add("Accept","application/json");
    // request.headers().add("Content-Type","application/json");
#ifdef DEBUG_OKX
    request.headers().add("x-simulated-trading", "1");
#endif
    request.headers().add("Connection", "Keep-Alive");
    request.headers().add("Keep-Alive", "timeout=60, max=100000");
    string time = crypto::getTimestamp();
    string sign = get_signature_rest(time, "GET", m_balanceUrl.to_string(), "");
    request.headers().add("OK-ACCESS-KEY",m_curcfg.apiKey);
    request.headers().add("OK-ACCESS-TIMESTAMP",time);
    request.headers().add("OK-ACCESS-SIGN",sign);
    request.headers().add("OK-ACCESS-PASSPHRASE", m_curcfg.userId);
    uri_builder builder(m_balanceUrl);

    request.set_request_uri(builder.to_string());
    // restclient.request(request)
    hotHttpClient->request(request)
    .then([&](http_response response) -> pplx::task<json::value> {
        auto code = response.status_code();
        if(code == status_codes::OK || code == status_codes::BadRequest
        || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
            return response.extract_json();
        }
        else{
            LOG_ERROR("get_balances, code:%d",code);
            return pplx::task_from_result(json::value());
        }
    })
    .then([&](pplx::task<json::value> previousTask){
        json::value const &v = previousTask.get();
        LOG_INFO("get_balance: %s", v.serialize().c_str());
        if(v.has_field("data") && v.at("code").as_string()[0] == '0'){
            auto array = v.at("data").at(0).at("details").as_array();
            for(auto &it : array){
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.body.balance.total = stod(it.at("eq").as_string().c_str());
                if(crypto::is_zeronum(rcmd.body.balance.total)){
                    continue;
                }
                rcmd.header.cmdTime = crypto::getCurrentTime();
                rcmd.header.exchangeTypeEnum = ExchangeType_OKX;
                rcmd.header.instTypeEnum = InstType_SPOT;
                rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                strcpy(rcmd.header.accountId, m_curcfg.accountId);
                strcpy(rcmd.body.balance.currency, it.at("ccy").as_string().c_str());
                rcmd.body.balance.available = stod(it.at("availEq").as_string().c_str());
                rcmd.body.balance.frozen    = stod(it.at("frozenBal").as_string().c_str());

                rcmd.body.balance.unrealizedPnl = stod(it.at("upl").as_string().c_str());
                rcmd.body.balance.apiSourceEnum = ApiSource_REST;
                PUSH_RCMD(rcmd)

                if(crypto::str_cmp(rcmd.body.balance.currency, "USDT")){
                    // cout << it.serialize() << endl;
                    LOG_INFO("exchId:%s,instType:%s,accountId:%s,strategyId:%s,usdt available:%.2f,frozen:%.2f",
                        ExchangeTypeEnum2StrMap[rcmd.header.exchangeTypeEnum].c_str(),
                        InstTypeEnum2StrMap[rcmd.header.instTypeEnum].c_str(),
                        rcmd.header.accountId, rcmd.header.strategyId,
                        rcmd.body.balance.available, rcmd.body.balance.frozen);
                }
            }

	    auto& res = v.at("data").at(0);
            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.header.cmdTime = crypto::getCurrentTime();
            rcmd.header.exchangeTypeEnum = ExchangeType_OKX;
            rcmd.header.instTypeEnum = InstType_SPOT;
            rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
            strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
            strcpy(rcmd.header.accountId, m_curcfg.accountId);
            rcmd.body.totalAccount.totalEquity = stod(res.at("totalEq").as_string().c_str());
            rcmd.body.totalAccount.adjEquity = stod(res.at("adjEq").as_string().c_str());
            rcmd.body.totalAccount.mmr = stod(res.at("mmr").as_string().c_str());
	    string mgnStr = res.at("mgnRatio").as_string();
	    if (mgnStr != "") {
                rcmd.body.totalAccount.mgnRatio = stod(mgnStr);
            } else {
                rcmd.body.totalAccount.mgnRatio = 100;
	    }
            rcmd.body.totalAccount.apiSourceEnum = ApiSource_REST;
	    LOG_INFO("rcmd: %s", rcmd.getString().c_str());
            PUSH_RCMD(rcmd)
        }
        else{
            LOG_ERROR("%s", v.serialize().c_str());
        }
        return true;
    }) ;
    return true;
}
catch(exception &e){
    LOG_ERROR("%s", e.what());
    return false;
}
bool OKXSpotSwapFuturesTradingClient::get_positions()
try {
    // http_client restclient(m_curcfg.restBaseUrl);
    http_request request(methods::GET);
    // request.headers().add("Accept","application/json");
    // request.headers().add("Content-Type","application/json");
#ifdef DEBUG_OKX
    request.headers().add("x-simulated-trading", "1");
#endif
    request.headers().add("Connection", "Keep-Alive");
    request.headers().add("Keep-Alive", "timeout=60, max=100000");
    string time = crypto::getTimestamp();
    string sign = get_signature_rest(time, "GET", m_positionUrl.to_string(), "");
    request.headers().add("OK-ACCESS-KEY",m_curcfg.apiKey);
    request.headers().add("OK-ACCESS-TIMESTAMP",time);
    request.headers().add("OK-ACCESS-SIGN",sign);
    request.headers().add("OK-ACCESS-PASSPHRASE", m_curcfg.userId);
    uri_builder builder(m_positionUrl);

    request.set_request_uri(builder.to_string());
    // restclient.request(request)
    hotHttpClient->request(request)
    .then([&](http_response response) -> pplx::task<json::value> {
        auto code = response.status_code();
        if(code == status_codes::OK || code == status_codes::BadRequest
        || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
            return response.extract_json();
        }
        else{
            LOG_ERROR("get_positions, code:%d",code);
            return pplx::task_from_result(json::value());
        }
    })
    .then([&](pplx::task<json::value> previousTask){
        unordered_map<string, pubsub::RCommand> mCurrentPositions;
        json::value const &v = previousTask.get();
        if(v.has_field("data") && v.at("code").as_string()[0] == '0'){
            auto array = v.at("data").as_array();
            int size = array.size();
            int i = 0;
            for(auto &it : array){
                i++;
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.header.cmdTime = crypto::getCurrentTime();
                rcmd.header.exchangeTypeEnum = ExchangeType_OKX;
                rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                strcpy(rcmd.header.accountId, m_curcfg.accountId);
                //持仓数量，逐仓自主划转模式下，转入保证金后会产生pos为0的仓位
                double positionAmt = stod(it.at("pos").as_string().c_str());
                //交易产品ID，如 BTC-USD-180213
                string originInstId = it.at("instId").as_string();
                //持仓方向：long, short, net
                // string positionSide = it.at("posSide").as_string();
                rcmd.body.position.direction = positionAmt > 0 ? Direction_LONG : Direction_SHORT;
                // rcmd.body.position.direction = positionSide[0] == 'l' ? Direction_LONG :
                //                                 positionSide[0] == 's' ? Direction_SHORT : Direction_NET;
                //保证金模式， isolated, cross
                // string mgnMode = it.at("mgnMode").as_string();
                //占用保证金的币种
                string ccy = it.at("ccy").as_string();
                //交易产品类型， MARGIN：币币杠杆 SWAP：永续合约 FUTURES：交割合约 OPTION：期权
                string instType = it.at("instType").as_string();
                //HANDLE_InstType(rcmd)
                md::InstrumentInfo info;

                if (this->smc->get_instrument_info("OKX", "InstType_USDT_SWAP", originInstId.c_str(), info)){
                    rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                    strcpy(rcmd.body.position.instId, info.instId);
                }
                else if (this->smc->get_instrument_info("OKX", "InstType_USDT_FUTURES", originInstId.c_str(), info)){
                    rcmd.header.instTypeEnum = InstType_USDT_FUTURES;
                    strcpy(rcmd.body.position.instId, info.instId);
                }
                else if (this->smc->get_instrument_info("OKX", "InstType_C_SWAP", originInstId.c_str(), info)){
                    rcmd.header.instTypeEnum = InstType_C_SWAP;
                    strcpy(rcmd.body.position.instId, info.instId);
                }
                else if (this->smc->get_instrument_info("OKX", "InstType_C_FUTURES", originInstId.c_str(), info)){
                    rcmd.header.instTypeEnum = InstType_C_FUTURES;
                    strcpy(rcmd.body.position.instId, info.instId);
                }
                else{
                    LOG_ERROR("not found OKX.%s.%s smc info", instType.c_str(), originInstId.c_str());
                    continue;
                }

                positionAmt = positionAmt > 0 ? positionAmt : -positionAmt;
                rcmd.body.position.volume = positionAmt;
                rcmd.body.position.maintMargin = stod(it.at("mmr").as_string().c_str());
                rcmd.body.position.avgPrice = stod(it.at("avgPx").as_string().c_str());
                rcmd.body.position.unrealizedPnl = stod(it.at("upl").as_string().c_str());
                rcmd.body.position.markPrice = stod(it.at("markPx").as_string().c_str());
                string liqPx = it.at("liqPx").as_string();
                if(crypto::str_cmp(liqPx.c_str(), "") == false){
                    rcmd.body.position.liquidPrice = stod(liqPx.c_str());
                }

                rcmd.body.position.adlQuantile = stod(it.at("adl").as_string().c_str());
                rcmd.body.position.apiSourceEnum = ApiSource_REST;
                
                if (i == size) {
                    rcmd.body.position.isLast = 1;
                } else {
                    rcmd.body.position.isLast = 0;
                }
                
                PUSH_RCMD(rcmd)
            }
        }
        else{
            LOG_ERROR("%s", v.serialize().c_str());
            return false;
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

void OKXSpotSwapFuturesTradingClient::add_new_order(pubsub::TCommand &tcmd){
    //add_new_order_ws(tcmd);
    //return;
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)
    if(!m_IsConnected){
        rcmd.body.newOrder.ErrorID = ERROR_TBDisconnectError;
        PUSH_RCMD(rcmd)
        return;
    }
    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.newOrder.instId, info)) {
            // http_client restclient(m_curcfg.restBaseUrl);
            http_request request(methods::POST);
            // request.headers().add("Accept","application/json");
            // request.headers().add("Content-Type","application/json");
#ifdef DEBUG_OKX
            request.headers().add("x-simulated-trading","1");
#endif
            // request.headers().add("Connection", "Keep-Alive");
            // request.headers().add("Keep-Alive", "timeout=60, max=100000");
            string time = crypto::getTimestamp();
            request.headers().add("OK-ACCESS-KEY",m_curcfg.apiKey);
            request.headers().add("OK-ACCESS-TIMESTAMP",time);
            request.headers().add("OK-ACCESS-PASSPHRASE", m_curcfg.userId);
            uri_builder builder(m_orderUrl);
            char price[32];
            char amount[32];
            json::value obj;
            if(tcmd.body.newOrder.reduceOnly == true){
                sprintf(price,"%.12f", tcmd.body.newOrder.limitPrice);
                sprintf(amount,"%.12f", tcmd.body.newOrder.volumeTotal);
            }
            else{
                strcpy(price, crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice, info.tickSize).c_str());
                strcpy(amount, crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal, info.lotSize).c_str());
            }
            rcmd.body.newOrder.volumeTotal = stod(amount);
            rcmd.body.newOrder.limitPrice = stod(price);
            obj["reduceOnly"] = json::value::string(tcmd.body.newOrder.reduceOnly ? "true" : "false");
            obj["clOrdId"] = json::value::string(tcmd.body.newOrder.orderSysId);
            obj["tdMode"] = json::value::string("cross");
            obj["instId"] = json::value::string(info.originInstId);
            if(tcmd.body.newOrder.offsetFlag == OffsetFlag_OPEN){
                if (tcmd.body.newOrder.direction == Direction_LONG) {
                    obj["side"] = json::value::string("buy");
                } else if (tcmd.body.newOrder.direction == Direction_SHORT) {
                    obj["side"] = json::value::string("sell");
                } else {
                    rcmd.body.newOrder.ErrorID = ERROR_DirectionError;
                    PUSH_RCMD(rcmd)
                    return;
                }
            }
            else if(tcmd.body.newOrder.offsetFlag == OffsetFlag_CLOSE){
                if (tcmd.body.newOrder.direction == Direction_LONG) {
                    obj["side"] = json::value::string("sell");
                } else if (tcmd.body.newOrder.direction == Direction_SHORT) {
                    obj["side"] = json::value::string("buy");
                } else {
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
            if (tcmd.body.newOrder.orderType == OrderType_LIMIT) {
                obj["ordType"] = json::value::string("limit");
                obj["px"] = json::value::string(price);
                obj["sz"] = json::value::string(amount);
            } else if (tcmd.body.newOrder.orderType == OrderType_MARKET) {
                obj["ordType"] = json::value::string("market");
                obj["sz"] = json::value::string(amount);
            } else if (tcmd.body.newOrder.orderType == OrderType_POST_ONLY) {
                obj["ordType"] = json::value::string("post_only");
                obj["px"] = json::value::string(price);
                obj["sz"] = json::value::string(amount);
            } else if (tcmd.body.newOrder.orderType == OrderType_IOC) {
                obj["ordType"] = json::value::string("ioc");
                obj["px"] = json::value::string(price);
                obj["sz"] = json::value::string(amount);
            } else if (tcmd.body.newOrder.orderType == OrderType_FOK) {
                obj["ordType"] = json::value::string("fok");
                obj["px"] = json::value::string(price);
                obj["sz"] = json::value::string(amount);
            } else {
                rcmd.body.newOrder.ErrorID = ERROR_OrderTypeError;
                PUSH_RCMD(rcmd)
                return;
            }
            string params = obj.serialize();
            string sign = get_signature_rest(time, "POST", m_orderUrl.to_string().c_str(), params);
            request.headers().add("OK-ACCESS-SIGN", sign);
            request.set_body(params,"application/json; charset=UTF-8");
            request.set_request_uri(builder.to_string());
            const http_response &response = hotHttpClient->request(request).get();
            rcmd.body.newOrder.tsNet = crypto::getCurrentTime();
            LOG_DEBUG("OKX,%s,internetDelay,%ld", __FUNCTION__, rcmd.body.newOrder.tsNet - rcmd.body.newOrder.tsSent);
            auto code = response.status_code();
            // cout << response.to_string() << endl;
            if(code == status_codes::Created || code == status_codes::OK
                    || code == status_codes::Unauthorized || code == status_codes::NotFound
                    || code == status_codes::BadRequest){
                const string &v = response.extract_string().get();
                LOG_DEBUG("add_new_order reponse: %s", v.c_str());
                rapidjson::Document d;
                rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(v.c_str());
                if(rawData["code"].GetString()[0] == '0'){
                    strcpy(rcmd.body.newOrder.orderId, rawData["data"][0]["ordId"].GetString());
                    rcmd.body.newOrder.orderStatus =  OrderStatus_REST_NEW;
                    PUSH_RCMD(rcmd)
                }
                else {
                    int code = 0;
                    if(rawData.HasMember("data") && rawData["data"].Size() > 0 && rawData["data"][0].HasMember("sCode")){
                        code = stoi(rawData["data"][0]["sCode"].GetString());
                        strncpy(rcmd.body.newOrder.originMsg, rawData["data"][0]["sMsg"].GetString(), sizeof(rcmd.body.newOrder.originMsg));
                    }
                    else{
                        code = stoi(rawData["code"].GetString());
                        strncpy(rcmd.body.newOrder.originMsg, rawData["msg"].GetString(), sizeof(rcmd.body.newOrder.originMsg));
                    }
                    rcmd.body.newOrder.ErrorID = crypto::get_okx_errorid(code);
                    rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED;
                    PUSH_RCMD(rcmd)
                }
            }
            else if(code == status_codes::ServiceUnavailable //503
            || code == status_codes::InternalError ){//500
                auto s = response.extract_string().get();
                rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN;
                rcmd.body.newOrder.ErrorID = ERROR_NetworkUnknownError;
                LOG_ERROR("%s", s.c_str());
                PUSH_RCMD(rcmd)
            }
            else{
                auto s = response.extract_string().get();
                rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED;
                rcmd.body.newOrder.ErrorID = code;
                // strncpy(rcmd.body.newOrder.originMsg, s.c_str(), sizeof(rcmd.body.newOrder.originMsg));
                LOG_ERROR("%s", s.c_str());
                PUSH_RCMD(rcmd)
            }
        }
        else{
            rcmd.body.newOrder.ErrorID = ERROR_SMCInstrumentNotExistError;
            LOG_ERROR("not found OKX.%s.%s smc info", InstTypeEnum2StrMap[tcmd.header.instTypeEnum].c_str(), tcmd.body.newOrder.instId);
            PUSH_RCMD(rcmd)
            return;
        }
    }
    catch(exception &e){
        rcmd.body.newOrder.ErrorID = ERROR_NetworkError;
        rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN;
        strncpy(rcmd.body.newOrder.originMsg, e.what(), sizeof(rcmd.body.newOrder.originMsg));
        LOG_ERROR("%s", e.what());
        PUSH_RCMD(rcmd)
    }
}

void OKXSpotSwapFuturesTradingClient::add_new_order_ws(pubsub::TCommand &tcmd){
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)
    if(!m_IsConnected){
        rcmd.body.newOrder.ErrorID = ERROR_TBDisconnectError;
        PUSH_RCMD(rcmd)
        return;
    }
    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.newOrder.instId, info)) {
            json::value subValue;
            subValue["id"] = json::value::string(tcmd.body.newOrder.orderSysId);
            subValue["op"] = json::value::string("order");
            char price[32];
            char amount[32];
            json::value obj;
            if(tcmd.body.newOrder.reduceOnly == true){
                sprintf(price,"%.12f", tcmd.body.newOrder.limitPrice);
                sprintf(amount,"%.12f", tcmd.body.newOrder.volumeTotal);
            }
            else{
                strcpy(price, crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice, info.tickSize).c_str());
                strcpy(amount, crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal, info.lotSize).c_str());
            }
            rcmd.body.newOrder.volumeTotal = stod(amount);
            rcmd.body.newOrder.limitPrice = stod(price);
            obj["reduceOnly"] = json::value::string(tcmd.body.newOrder.reduceOnly ? "true" : "false");
            obj["clOrdId"] = json::value::string(tcmd.body.newOrder.orderSysId);
            obj["tdMode"] = json::value::string("cross");
            obj["instId"] = json::value::string(info.originInstId);
            if(tcmd.body.newOrder.offsetFlag == OffsetFlag_OPEN){
                if (tcmd.body.newOrder.direction == Direction_LONG) {
                    obj["side"] = json::value::string("buy");
                } else if (tcmd.body.newOrder.direction == Direction_SHORT) {
                    obj["side"] = json::value::string("sell");
                } else {
                    rcmd.body.newOrder.ErrorID = ERROR_DirectionError;
                    PUSH_RCMD(rcmd)
                    return;
                }
            }
            else if(tcmd.body.newOrder.offsetFlag == OffsetFlag_CLOSE){
                if (tcmd.body.newOrder.direction == Direction_LONG) {
                    obj["side"] = json::value::string("sell");
                } else if (tcmd.body.newOrder.direction == Direction_SHORT) {
                    obj["side"] = json::value::string("buy");
                } else {
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
            if (tcmd.body.newOrder.orderType == OrderType_LIMIT) {
                obj["ordType"] = json::value::string("limit");
                obj["px"] = json::value::string(price);
                obj["sz"] = json::value::string(amount);
            } else if (tcmd.body.newOrder.orderType == OrderType_MARKET) {
                obj["ordType"] = json::value::string("market");
                obj["sz"] = json::value::string(amount);
            } else if (tcmd.body.newOrder.orderType == OrderType_POST_ONLY) {
                obj["ordType"] = json::value::string("post_only");
                obj["px"] = json::value::string(price);
                obj["sz"] = json::value::string(amount);
            } else if (tcmd.body.newOrder.orderType == OrderType_IOC) {
                obj["ordType"] = json::value::string("ioc");
                obj["px"] = json::value::string(price);
                obj["sz"] = json::value::string(amount);
            } else if (tcmd.body.newOrder.orderType == OrderType_FOK) {
                obj["ordType"] = json::value::string("fok");
                obj["px"] = json::value::string(price);
                obj["sz"] = json::value::string(amount);
            } else {
                rcmd.body.newOrder.ErrorID = ERROR_OrderTypeError;
                PUSH_RCMD(rcmd)
                return;
            }
            subValue["args"][0] = obj;
            websocket_outgoing_message outMsg;
            outMsg.set_utf8_message(subValue.serialize().c_str());
            wsClient.send(outMsg).then([&](){ });
            // wsClient.receive().then([](websocket_incoming_message msg) {
            //     return msg.extract_string();
            // }).then([](std::string body) {
            //     std::cout << body << std::endl;
            // });
            // auto response = wsClient.send(outMsg).get();//.then([](){ /* Successfully sent the message. */ });;
            // orderWSClient.send(outMsg);
            return;
        }
        else{
            rcmd.body.newOrder.ErrorID = ERROR_SMCInstrumentNotExistError;
            LOG_ERROR("not found OKX.%s.%s smc info", InstTypeEnum2StrMap[tcmd.header.instTypeEnum].c_str(), tcmd.body.newOrder.instId);
            PUSH_RCMD(rcmd)
            return;
        }
    }
    catch(exception &e){
        rcmd.body.newOrder.ErrorID = ERROR_NetworkError;
        rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN;
        strncpy(rcmd.body.newOrder.originMsg, e.what(), sizeof(rcmd.body.newOrder.originMsg));
        LOG_ERROR("%s", e.what());
        PUSH_RCMD(rcmd)
    }
}

void OKXSpotSwapFuturesTradingClient::cancel_order(pubsub::TCommand &tcmd){
    // cancel_order_ws(tcmd);
    // return;
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    try {
        md::InstrumentInfo info;
        if (smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.cancelOrder.instId, info)){
            // http_client restclient(m_curcfg.restBaseUrl);
            http_request request(methods::POST);
#ifdef DEBUG_OKX
            request.headers().add("x-simulated-trading","1");
#endif
            request.headers().add("Connection", "Keep-Alive");
            request.headers().add("Keep-Alive", "timeout=60, max=100000");
            string time = crypto::getTimestamp();
            request.headers().add("OK-ACCESS-KEY", m_curcfg.apiKey);
            request.headers().add("OK-ACCESS-TIMESTAMP",time);
            request.headers().add("OK-ACCESS-PASSPHRASE", m_curcfg.userId);
            json::value obj;
            if(tcmd.body.cancelOrder.cancelOrderTypeEnum == pubsub::COT_ONE_INST){
                uri_builder builder(m_cancelOrderUrl);
                obj["instId"] = json::value::string(info.originInstId);

                if(tcmd.body.cancelOrder.clientOrderId != 0){
                    obj["clOrdId"] = json::value::string(tcmd.body.cancelOrder.orderSysId);
                }
                else if(crypto::str_cmp(tcmd.body.cancelOrder.orderId, "") == false){
                    obj["ordId"] = json::value::string(tcmd.body.cancelOrder.orderId);
                }
                else{
                    LOG_ERROR("cancel order need orderId or clientOrderId");
                    strcpy(rcmd.body.cancelOrder.originMsg, "cancel order need orderId or clientOrderId");
                    rcmd.body.cancelOrder.ErrorID = ERROR_NoOrderIdError;
                    rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                    PUSH_RCMD(rcmd)
                    return;
                }
                string params = obj.serialize();
                string sign = get_signature_rest(time, "POST", m_cancelOrderUrl.to_string().c_str(), params);
                request.headers().add("OK-ACCESS-SIGN", sign);
                request.set_body(params, "application/json; charset=UTF-8");
                request.set_request_uri(builder.to_string());
                LOG_DEBUG("%s:%s", __FUNCTION__, builder.to_string().c_str());
                // rcmd.body.cancelOrder.tsSent = crypto::getCurrentTime();
                // restclient.request(request)
                // cancelOrderRestClient->request(request)
                const http_response &response = hotHttpClient->request(request).get();
                rcmd.body.cancelOrder.tsNet = crypto::getCurrentTime();
                LOG_DEBUG("OKX,%s,internetDelay,%ld", __FUNCTION__, rcmd.body.cancelOrder.tsNet - rcmd.body.cancelOrder.tsSent);
                auto code = response.status_code();
                // cout << response.to_string() << endl;
                if (code == status_codes::OK || code == status_codes::BadRequest
                        || code == status_codes::TooManyRequests || code == status_codes::Unauthorized) {
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
                    if(rawData["code"].GetString()[0] == '0'){
                        // rcmd.body.cancelOrder.orderStatus = OrderStatus_CANCELED;
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_CANCELLING;
                        PUSH_RCMD(rcmd)
                    }
                    else {
                        int code = 0;
                        if(rawData.HasMember("data") && rawData["data"].Size() > 0 && rawData["data"][0].HasMember("sCode")){
                            code = stoi(rawData["data"][0]["sCode"].GetString());
                            strncpy(rcmd.body.cancelOrder.originMsg, rawData["data"][0]["sMsg"].GetString(), sizeof(rcmd.body.cancelOrder.originMsg));
                        }
                        else{
                            code = stoi(rawData["code"].GetString());
                            strncpy(rcmd.body.cancelOrder.originMsg, rawData["msg"].GetString(), sizeof(rcmd.body.cancelOrder.originMsg));
                        }

                        rcmd.body.cancelOrder.ErrorID = crypto::get_okx_errorid(code);

                        if(rcmd.body.cancelOrder.ErrorID == ERROR_OrderNotFoundError){
                            rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                        }
                        else if(rcmd.body.cancelOrder.ErrorID == ERROR_OrderAlreadyFinishedError){
                            rcmd.body.cancelOrder.orderStatus = OrderStatus_CANCELED;
                        }
                        else{
                            rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                        }
                        PUSH_RCMD(rcmd)
                    }
                }
                else{
                    auto s = response.extract_string().get();
                    LOG_ERROR("%s", s.c_str());
                    rcmd.body.cancelOrder.ErrorID = code;
                    // strncpy(rcmd.body.cancelOrder.originMsg, s.c_str(), sizeof(rcmd.body.cancelOrder.originMsg));
                    rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                    PUSH_RCMD(rcmd)
                }
            }
            else if(tcmd.body.cancelOrder.cancelOrderTypeEnum == pubsub::COT_MULTI_INST){

            }
            else{
                rcmd.body.cancelOrder.ErrorID = ERROR_OrderTypeError;
                rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                PUSH_RCMD(rcmd)
            }
        }
        else{
            rcmd.body.cancelOrder.ErrorID = ERROR_SMCInstrumentNotExistError;
            rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
            PUSH_RCMD(rcmd)
        }
    }
    catch(exception &e) {
        rcmd.body.cancelOrder.ErrorID = ERROR_NetworkError;
        rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
        strncpy(rcmd.body.cancelOrder.originMsg, e.what(), sizeof(rcmd.body.cancelOrder.originMsg));
        LOG_ERROR("%s", e.what());
        PUSH_RCMD(rcmd)
    }
}

void OKXSpotSwapFuturesTradingClient::cancel_order_ws(pubsub::TCommand &tcmd){
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    try{
        md::InstrumentInfo info;
        if (smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.cancelOrder.instId, info)){
            json::value subValue;
            subValue["id"] = json::value::string(tcmd.body.newOrder.orderSysId);
            subValue["op"] = json::value::string("cancel-order");
            json::value obj;
            if(tcmd.body.cancelOrder.cancelOrderTypeEnum == pubsub::COT_ONE_INST){
                uri_builder builder(m_cancelOrderUrl);
                obj["instId"] = json::value::string(info.originInstId);

                if(tcmd.body.cancelOrder.clientOrderId != 0){
                    obj["clOrdId"] = json::value::string(tcmd.body.cancelOrder.orderSysId);
                }
                else if(crypto::str_cmp(tcmd.body.cancelOrder.orderId, "") == false){
                    obj["ordId"] = json::value::string(tcmd.body.cancelOrder.orderId);
                }
                else{
                    LOG_ERROR("cancel order need orderId or clientOrderId");
                    strcpy(rcmd.body.cancelOrder.originMsg, "cancel order need orderId or clientOrderId");
                    rcmd.body.cancelOrder.ErrorID = ERROR_NoOrderIdError;
                    rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                    PUSH_RCMD(rcmd)
                    return;
                }
                subValue["args"][0] = obj;
                websocket_outgoing_message outMsg;
                outMsg.set_utf8_message(subValue.serialize().c_str());
                wsClient.send(outMsg);
                return;
            }
            else if(tcmd.body.cancelOrder.cancelOrderTypeEnum == pubsub::COT_MULTI_INST){

            }
            else{
                rcmd.body.cancelOrder.ErrorID = ERROR_OrderTypeError;
                rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                PUSH_RCMD(rcmd)
            }
        }
        else{
            rcmd.body.cancelOrder.ErrorID = ERROR_SMCInstrumentNotExistError;
            rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
            PUSH_RCMD(rcmd)
        }
    }
    catch(exception &e) {
        rcmd.body.cancelOrder.ErrorID = ERROR_NetworkError;
        rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
        strncpy(rcmd.body.cancelOrder.originMsg, e.what(), sizeof(rcmd.body.cancelOrder.originMsg));
        LOG_ERROR("%s", e.what());
        PUSH_RCMD(rcmd)
    }
}


void OKXSpotSwapFuturesTradingClient::query_order(pubsub::TCommand &tcmd){
    if(tcmd.body.queryOrder.queryOrderTypeEnum == QOT_ONE_INST){
        query_one_order(tcmd);
    }
    else if(tcmd.body.queryOrder.queryOrderTypeEnum == QOT_MULTI_INST){
        // query_multi_orders(cmd);
    }
    else{

    }
}


void OKXSpotSwapFuturesTradingClient::query_one_order(pubsub::TCommand &tcmd){
    QUERY_ORDER_TCMD_2_RCMD(tcmd)
    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.queryOrder.instId, info)){
            http_client restclient(m_curcfg.restBaseUrl);
            http_request request(methods::GET);
            // request.headers().add(U("Accept"), U("application/json"));
            // request.headers().set_content_type(U("application/json; charset=UTF-8"));
#ifdef DEBUG_OKX
            request.headers().add("x-simulated-trading", "1");
#endif
            string time = crypto::getTimestamp();
            request.headers().add("OK-ACCESS-KEY",m_curcfg.apiKey);
            request.headers().add("OK-ACCESS-TIMESTAMP",time);
            request.headers().add("OK-ACCESS-PASSPHRASE", m_curcfg.userId);
            string queryStr{"?instId="};
            queryStr.append(info.originInstId).append("&");
            if(crypto::str_cmp(tcmd.body.queryOrder.orderId, "") == false){
                queryStr.append("ordId=").append(tcmd.body.queryOrder.orderId);
            }
            else if(tcmd.body.queryOrder.clientOrderId != 0){
                queryStr.append("clOrdId=").append(tcmd.body.queryOrder.orderSysId);
            }
            else{
                LOG_ERROR("query_one_order need orderId or clientOrderId");
                return;
            }
            string path = m_orderUrl.to_string()+queryStr;
            uri_builder builder(path.c_str());
            string sign = get_signature_rest(time, "GET", builder.to_string(), "");
            request.headers().add("OK-ACCESS-SIGN", sign);

            LOG_DEBUG("query_one_order,path:%s", builder.to_string().c_str() );// params.c_str()
            request.set_request_uri(builder.to_string());
            // restclient.request(request)
            hotHttpClient->request(request)
            .then([&](http_response response) -> pplx::task<json::value> {
                auto code = response.status_code();
                if(code == status_codes::OK || code == status_codes::BadRequest
                || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
                    return response.extract_json();
                }
                else{
                    auto s = response.extract_string().get();
                    rcmd.body.queryOrder.ErrorID = code;
                    rcmd.body.queryOrder.orderStatus = OrderStatus_REJECTED;
                    // strncpy(rcmd.body.queryOrder.originMsg, s.c_str(), sizeof(rcmd.body.queryOrder.originMsg));
                    LOG_ERROR("query_one_order:code:%d,response:%s", code, s.c_str());
                    PUSH_RCMD(rcmd)
                    return pplx::task_from_result(json::value());
                }
            })
            .then([&](pplx::task<json::value> previousTask) {
                json::value const & v = previousTask.get();
                LOG_DEBUG("query_one_order reponse:%s", v.serialize().c_str());

                if(v.at("code").as_string()[0] == '0'){
                    auto data = v.at("data").at(0);
                    strcpy(rcmd.body.queryOrder.orderId, data.at("ordId").as_string().c_str());
                    strcpy(rcmd.body.queryOrder.orderSysId, data.at("clOrdId").as_string().c_str());
                    if(crypto::str_cmp(data.at("sz").as_string().c_str(), "") == false){
                        rcmd.body.queryOrder.volumeTotal  = stod(data.at("sz").as_string().c_str());
                    }
                    if(crypto::str_cmp(data.at("px").as_string().c_str(), "") == false){
                        rcmd.body.queryOrder.limitPrice   = stod(data.at("px").as_string().c_str());
                    }
                    if(crypto::str_cmp(data.at("accFillSz").as_string().c_str(), "") == false){
                        rcmd.body.queryOrder.volumeTraded = stod(data.at("accFillSz").as_string().c_str()) ;
                    }
                    if(crypto::str_cmp(data.at("avgPx").as_string().c_str(), "") == false){
                        rcmd.body.queryOrder.tradePrice   = stod(data.at("avgPx").as_string().c_str() );
                    }
                    string orderStatus = data.at("state").as_string();
                    if(orderStatus[0] == 'l'){
                        rcmd.body.queryOrder.orderStatus = OrderStatus_NEW;
                    }
                    else if(orderStatus[0] == 'c'){
                        rcmd.body.queryOrder.orderStatus = OrderStatus_CANCELED;
                    }
                    else if(orderStatus[0] == 'p'){
                        rcmd.body.queryOrder.orderStatus = OrderStatus_PARTFILLED;
                    }
                    else if(orderStatus[0] == 'f'){
                        rcmd.body.queryOrder.orderStatus = OrderStatus_FILLED;
                    }
                    else {
                        rcmd.body.queryOrder.orderStatus = OrderStatus_UNKNOWN;
                    }
                    PUSH_RCMD(rcmd)
                }
                else {
                    int code = 0;
                    if(v.has_field("data") && v.at("data").as_array().size() > 0 && v.at("data").at(0).has_field("sCode")){
                        code = stoi(v.at("data").at(0).at("sCode").as_string().c_str());
                    }
                    else{
                        code = stoi(v.at("code").as_string().c_str());
                    }
                    rcmd.body.queryOrder.ErrorID = crypto::get_okx_errorid(code);
                    rcmd.body.queryOrder.orderStatus = OrderStatus_REJECTED;
                    strncpy(rcmd.body.queryOrder.originMsg, v.serialize().c_str(), sizeof(rcmd.body.queryOrder.originMsg));
                    PUSH_RCMD(rcmd)
                }
            })
            .wait();
        }
        else{
            LOG_ERROR("not found OKX %s smc info", tcmd.body.queryOrder.instId);
        }
    }
    catch(exception &e) {
        LOG_ERROR("%s", e.what());
    }
}

void OKXSpotSwapFuturesTradingClient::query_multi_orders(pubsub::TCommand &tcmd){

}
