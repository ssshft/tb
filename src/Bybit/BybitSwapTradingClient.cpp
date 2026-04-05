#include "api/Bybit/BybitSwapTradingClient.h"
#define HANDLE_InstType(rcmd) \
    md::InstrumentInfo info;\
    if(this->smc->get_instrument_info("BYBIT", instType.c_str(), originInstId.c_str(), info)){\
        strcpy(rcmd.body.orderResponse.instId, info.instId);\
    }\
    else{\
        LOG_ERROR("not found BYBIT.%s.%s smc info", instType.c_str(), originInstId.c_str());\
        continue;\
    }\
    rcmd.header.instTypeEnum = info.instTypeEnum;

BybitSwapTradingClient::BybitSwapTradingClient(){

}

BybitSwapTradingClient::~BybitSwapTradingClient(){
//    delete smc;
}

bool BybitSwapTradingClient::Initialize(AccountCfg& cfg, sm::SecurityManager *smc){
    this->smc = smc;
    m_balanceUrl = "/v5/account/wallet-balance";
    m_positionUrl = "/v5/position/list";
    m_orderUrl = "/v5/order/create";
    m_cancelOrderUrl = "/v5/order/cancel";
    m_queryOrderUrl = "/v5/order/realtime";
    m_queryOrderHistoryUrl = "/v5/order/history";
    m_curcfg = cfg;
    // hotHttpClient = new http_client(m_curcfg.restBaseUrl);
    hotHttpClient = new crypto::RestClientCPP(m_curcfg.restBaseUrl.c_str());
    // addHotHttpClient = new crypto::RestClientCPP(m_curcfg.restBaseUrl.c_str());
    // hotHttpClient = make_shared<crypto::RestClientCPP>(m_curcfg.restBaseUrl.c_str());
    // addHotHttpClient = make_shared<crypto::RestClientCPP>(m_curcfg.restBaseUrl.c_str());
    return true;
}

void BybitSwapTradingClient::Run(){
    std::thread monitorThread(&BybitSwapTradingClient::monitor, this);
    monitorThread.detach();
}

void BybitSwapTradingClient::sub_websocket()
try{
    m_IsConnected = false;
    uri_builder builder(m_curcfg.wsBaseUrl);
    wsClient.close();
    wsClient.connect(builder.to_string())
    .then([&](){
        std::function<void (const websocket_incoming_message &msg)> f;
        f = std::bind(&BybitSwapTradingClient::on_websocket_msg, this, placeholders::_1);
        wsClient.set_message_handler(f);
        std::function<void (websocket_close_status close_status,
            const utility::string_t& reason, const std::error_code& error)> c;
        c =  std::bind(&BybitSwapTradingClient::on_close_msg, this,
            placeholders::_1, placeholders::_2, placeholders::_3);
        wsClient.set_close_handler(c);
    }).wait();
    login();
    sub_channels();
    m_IsConnected = true;
}
catch(exception &e) {
    m_IsConnected = false;
    LOG_ERROR("%s", e.what());
}

void BybitSwapTradingClient::login(){
    json::value subValue;
    subValue["op"] = json::value::string("auth");
    subValue["args"][0]  = json::value::string(m_curcfg.apiKey);
    string timestamp = fmt::format("{}", crypto::getCurrentTimeMilli()+1000);
    string data = fmt::format("GET/realtime{}", timestamp);
    string sign = get_signature_ws(timestamp, data);

    subValue["args"][1]  = json::value::string(timestamp);
    subValue["args"][2]  = json::value::string(sign);
    websocket_outgoing_message outMsg;
    // cout << subValue.serialize() << endl;
    outMsg.set_utf8_message(subValue.serialize().c_str());
    wsClient.send(outMsg).wait();
}

void BybitSwapTradingClient::sub_channels(){
    json::value subValue;
    subValue["op"] = json::value::string("subscribe");
    subValue["args"][0] = json::value::string("position");
    subValue["args"][1] = json::value::string("wallet");
    subValue["args"][2] = json::value::string("order");
    // subValue["args"][3] = json::value::string("execution");
    websocket_outgoing_message outMsg;
    outMsg.set_utf8_message(subValue.serialize().c_str());
    wsClient.send(outMsg).wait();
}

void BybitSwapTradingClient::monitor() {
    while(1){
        try{
            LOG_INFO("start to connect with bybit ws api:%s", m_curcfg.wsBaseUrl.c_str());
            sub_websocket();
            sleep(2);
            while(m_IsConnected){
                sleep(10);
                if(!m_IsConnected){
                    LOG_ERROR("bybit ws disconnected, will reconnect it now");
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

void BybitSwapTradingClient::on_websocket_msg(const websocket_incoming_message& msg)
try{
    // text_message, binary_message, close, ping, pong
    if (msg.message_type() == websocket_message_type::text_message){
        msg.extract_string().then([&](const string s){
            rapidjson::Document d;
            rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(s.c_str());
            if(d.HasParseError() || !rawData.IsObject()){
                return;
            }
            LOG_DEBUG("on_websocket_msg:%s", s.c_str());
            // return;
            if(rawData.HasMember("topic") && rawData.HasMember("data") ){
                const string &topic = rawData["topic"].GetString();
                // const rapidjson::Value &data = rawData["data"];
                if(topic[0] == 'w'){
                    const rapidjson::Value &bData = rawData["data"][0]["coin"];
                    for(rapidjson::SizeType i = 0; i < bData.Size(); i++){
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.header.cmdTime = crypto::getCurrentTime();
                        rcmd.header.exchangeTypeEnum = ExchangeType_BYBIT;
                        rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        strcpy(rcmd.body.balance.currency, bData[i]["coin"].GetString());
                        rcmd.body.balance.total = stod(bData[i]["walletBalance"].GetString());
                        rcmd.body.balance.available = stod(bData[i]["walletBalance"].GetString());
                        rcmd.body.balance.frozen = 0;
                        if(crypto::str_cmp(bData[i]["totalPositionMM"].GetString(), "") == false){
                            rcmd.body.balance.frozen += stod(bData[i]["totalPositionMM"].GetString());
                        }
                        else{
                            rcmd.body.balance.frozen = stod(bData[i]["totalOrderIM"].GetString()) +
                                                    stod(bData[i]["totalPositionIM"].GetString());
                        }
                        rcmd.body.balance.unrealizedPnl = stod(bData[i]["unrealisedPnl"].GetString());
                        rcmd.body.balance.apiSourceEnum = ApiSource_WEBSOCKET;
                        PUSH_RCMD(rcmd)
                    }

                    const rapidjson::Value &totalAccountInfo = rawData["data"][0];
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.header.cmdTime = crypto::getCurrentTime();
                    rcmd.header.exchangeTypeEnum = ExchangeType_BYBIT;
                    rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                    rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
                    strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                    strcpy(rcmd.header.accountId, m_curcfg.accountId);

                    string totalStr = totalAccountInfo["totalEquity"].GetString();
                    if (totalStr != "") {
                        rcmd.body.totalAccount.totalEquity = stod(totalStr);
                    }
                    
                    string mmrStr = totalAccountInfo["totalMaintenanceMargin"].GetString();
                    if (mmrStr != "") {
                        rcmd.body.totalAccount.mmr = stod(mmrStr);
                    }

                    string mgnStr = totalAccountInfo["accountMMRate"].GetString();
                    if (mgnStr != "") {
                            rcmd.body.totalAccount.mgnRatio = stod(mgnStr);
                        } else {
                            rcmd.body.totalAccount.mgnRatio = 100;
                    }
                        rcmd.body.totalAccount.apiSourceEnum = ApiSource_REST;
                    LOG_INFO("rcmd: %s", rcmd.getString().c_str());
                    PUSH_RCMD(rcmd)
                }
                else if(topic[0] == 'p'){
                    // return;
                    const rapidjson::Value &pData = rawData["data"];
                    for(rapidjson::SizeType i = 0; i < pData.Size(); i++){
                        // cout << pData[i]. << endl;
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.header.exchangeTypeEnum = ExchangeType_BYBIT;
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        double positionAmt = stod(pData[i]["size"].GetString());
                        string originInstId = pData[i]["symbol"].GetString();
                        //持仓方向Buy Sell None
                        string positionSide = pData[i]["side"].GetString();
                        rcmd.body.position.direction = (positionSide[0] == 'B' || positionSide[0] == 'N') ? Direction_LONG : Direction_SHORT;
                        // positionSide[0] == 'B' ? Direction_LONG : Direction_SHORT;

                        md::InstrumentInfo info;
                        if(this->smc->get_instrument_info("BYBIT", "InstType_USDT_SWAP", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                            strcpy(rcmd.body.position.instId, info.instId);
                        }
                        else if(this->smc->get_instrument_info("BYBIT", "InstType_USDT_FUTURES", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_USDT_FUTURES;
                            strcpy(rcmd.body.position.instId, info.instId);
                        }
                        else if(this->smc->get_instrument_info("BYBIT", "InstType_C_SWAP", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_C_SWAP;
                            strcpy(rcmd.body.position.instId, info.instId);
                        }
                        else if(this->smc->get_instrument_info("BYBIT", "InstType_C_FUTURES", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_C_FUTURES;
                            strcpy(rcmd.body.position.instId, info.instId);
                        }
                        else{
                            LOG_ERROR("not found BYBIT.SWAP.%s smc info" , originInstId.c_str());
                            continue;
                        }
                       
                        // rcmd.body.position.direction = positionAmt >= 0 ? Direction_LONG : Direction_SHORT; //positionSide[0] == 'B' ? Direction_LONG : positionSide[0] == 'S' ? Direction_SHORT : Direction_NET;

                        positionAmt = positionAmt >= 0 ? positionAmt : -positionAmt;

                        rcmd.body.position.volume = positionAmt*info.magnifyNumber;
                        rcmd.body.position.avgPrice = stod(pData[i]["entryPrice"].GetString())*info.reduceNumber;
                        rcmd.body.position.unrealizedPnl = stod(pData[i]["unrealisedPnl"].GetString());
                        rcmd.body.position.markPrice = stod(pData[i]["markPrice"].GetString())*info.reduceNumber;
                        if(crypto::str_cmp(pData[i]["liqPrice"].GetString(), "") == false){
                            rcmd.body.position.liquidPrice = stod(pData[i]["liqPrice"].GetString())*info.reduceNumber;
                        }
                        double adl = stod(pData[i]["adlRankIndicator"].GetString());
                        if(adl >= 2 ){
                            adl -= 1;
                        }
                        else {
                            adl = 1;
                        }
                        rcmd.body.position.adlQuantile = adl;
                        rcmd.body.position.apiSourceEnum = ApiSource_WEBSOCKET;
                        PUSH_RCMD(rcmd)
                    }
                }
                else if(topic[0] == 'o'){//order
                    // cout << s << endl;
                    const rapidjson::Value &pData = rawData["data"];
                    for(rapidjson::SizeType i = 0; i < pData.Size(); i++){
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.header.cmdTime = crypto::getCurrentTime();
                        rcmd.header.exchangeTypeEnum = ExchangeType_BYBIT;
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        strcpy(rcmd.body.orderResponse.orderId, pData[i]["orderId"].GetString());
                        strcpy(rcmd.body.orderResponse.orderSysId, pData[i]["orderLinkId"].GetString());


                        string orderStatus = pData[i]["orderStatus"].GetString();
                        if((orderStatus[0] == 'C' && orderStatus[2] == 'e') || orderStatus[0] == 'N'){//Created New新订单
                            rcmd.body.orderResponse.orderStatus = OrderStatus_NEW;
                        }
                        else if(orderStatus[0] == 'C' && orderStatus[2] == 'n'){//Cancelled 订单被取消
                            rcmd.body.orderResponse.orderStatus = OrderStatus_CANCELED;
                        }
                        else if(orderStatus[0] == 'P'){//PartiallyFilled
                            rcmd.body.orderResponse.orderStatus = OrderStatus_PARTFILLED;
                        }
                        else if(orderStatus[0] == 'F'){//Filled
                            rcmd.body.orderResponse.orderStatus = OrderStatus_FILLED;
                        }
                        else {
                            rcmd.body.orderResponse.orderStatus = OrderStatus_UNKNOWN;
                        }
                        //合約名稱，如 BTC-USDT
                        string originInstId = pData[i]["symbol"].GetString();
                        // string instType = pData[i]["instType"].GetString();
                        md::InstrumentInfo info;
                        if(this->smc->get_instrument_info("BYBIT", "InstType_USDT_SWAP", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                            strcpy(rcmd.body.orderResponse.instId, info.instId);
                        }
                        else if(this->smc->get_instrument_info("BYBIT", "InstType_USDT_FUTURES", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_USDT_FUTURES;
                            strcpy(rcmd.body.orderResponse.instId, info.instId);
                        }
                        else if(this->smc->get_instrument_info("BYBIT", "InstType_C_SWAP", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_C_SWAP;
                            strcpy(rcmd.body.orderResponse.instId, info.instId);
                        }
                        else if(this->smc->get_instrument_info("BYBIT", "InstType_C_FUTURES", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_C_FUTURES;
                            strcpy(rcmd.body.orderResponse.instId, info.instId);
                        }
                        else if(this->smc->get_instrument_info("BYBIT", "InstType_SPOT", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_SPOT;
                            strcpy(rcmd.body.orderResponse.instId, info.instId);
                        }
                        else{
                            LOG_ERROR("not found BYBIT.SWAP.%s smc info", originInstId.c_str());
                            continue;
                        }
                        //已经成交量
                        if(crypto::str_cmp(pData[i]["cumExecQty"].GetString(), "") == false){
                            rcmd.body.orderResponse.volumeTraded = stod(pData[i]["cumExecQty"].GetString())*info.magnifyNumber;
                        }
                        //成交均价
                        if(crypto::str_cmp(pData[i]["cumExecValue"].GetString(), "") == false && rcmd.body.orderResponse.volumeTraded > ZERO_NUM){
                            rcmd.body.orderResponse.tradePrice = (stod(pData[i]["cumExecValue"].GetString())
                                                                    / stod(pData[i]["cumExecQty"].GetString()))*info.reduceNumber;
                        }
                        //原始订单数量
                        if(crypto::str_cmp(pData[i]["qty"].GetString(), "") == false){
                            rcmd.body.orderResponse.volumeTotal = stod(pData[i]["qty"].GetString())*info.magnifyNumber;
                        }
                        //原始订单价格
                        if(crypto::str_cmp(pData[i]["price"].GetString(), "") == false){
                            rcmd.body.orderResponse.limitPrice = stod(pData[i]["price"].GetString())*info.reduceNumber;
                        }

                        
                        rcmd.body.orderResponse.offsetFlag = OffsetFlag_OPEN;
                        string side = pData[i]["side"].GetString();
                        rcmd.body.orderResponse.direction = side[0] == 'S' ?Direction_SHORT : Direction_LONG ;
                        const string &orderType = pData[i]["orderType"].GetString();
                        const string &timeInForce = pData[i]["timeInForce"].GetString();
                        rcmd.body.orderResponse.orderType = crypto::get_bybit_ordertype(timeInForce.c_str(), orderType.c_str());

                        rcmd.body.orderResponse.apiSourceEnum = ApiSource_WEBSOCKET;
                        PUSH_RCMD(rcmd)
                    }
                }
                else{
                    LOG_INFO("%s", s.c_str());
                }
            }
            else{
                // LOG_INFO("%s", s.c_str());
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


void BybitSwapTradingClient::on_close_msg(websocket_close_status close_status,
                  const utility::string_t& reason, const std::error_code& error)
try{
    m_IsConnected = false;
    LOG_ERROR("bybit recv CloseMsg, reason:%s ",reason.c_str() );
}
catch (exception &e){
    LOG_ERROR("%s", e.what());
}

std::string BybitSwapTradingClient::params_string(std::map<std::string, std::string> const &params, int type=0){
    if(type == 0){
        if(params.empty())
            return "";
        std::map<std::string, std::string>::const_iterator pb= params.cbegin(), pe= params.cend();
        std::string data= pb -> first + "=" + pb-> second;
        ++pb;
        if(pb == pe)
            return data;
        for( ; pb!= pe; ++ pb)
            data+= "&"+ pb-> first+ "="+ pb-> second;
        return data;
    }
    else{
        if(params.empty())
            return "";
        std::string data{"{\n"};
        for(auto it=params.begin(); it != params.end(); ++it){
            data += "\"" +it->first + "\":\"" + it->second+"\",\n";
        }
        data  = data.substr(0, data.length()-2);
        data += "\n}";
        return data;
    }

}

string BybitSwapTradingClient::get_signature_rest(long timestamp, int recv_window, std::map<std::string, std::string> const &params, int type=0){//0 rest 1 post
    if(type ==0){
        std::string input{""};
        for(auto it=params.begin(); it != params.end(); ++it){
            input += it->first + "=" + it->second+"&";
        }
        string s = fmt::format("{}{}{}{}", timestamp, m_curcfg.apiKey, recv_window, input.c_str());
        // cout << s << endl;
        return crypto::HmacEncodeBybit(m_curcfg.apiSecret, s.substr(0, s.length()-1).c_str());
    }
    else{
        std::string input{"{\n"};
        for(auto it=params.begin(); it != params.end(); ++it){
            input += "\"" +it->first + "\":\"" + it->second+"\",\n";
        }
        input  = input.substr(0, input.length()-2);
        input += "\n}";
        string s = fmt::format("{}{}{}{}", timestamp, m_curcfg.apiKey, recv_window, input.c_str());
        // cout << s << endl;
        return crypto::HmacEncodeBybit(m_curcfg.apiSecret, s.c_str());
    }
}

string BybitSwapTradingClient::get_signature_ws(const string &timestamp, const string &data){
    string sign = crypto::HmacEncodeBybit(m_curcfg.apiSecret, data.c_str());
    // cout << data << endl;
    // cout << sign << endl;
    return sign;
}


void BybitSwapTradingClient::ping(){
    try{
        if(m_IsConnected){
            websocket_outgoing_message outMsg;
            json::value pingSubValue;
            pingSubValue["op"] = json::value::string("ping");
            outMsg.set_utf8_message(pingSubValue.serialize().c_str());
            wsClient.send(outMsg).wait();
        }
    }
    catch(exception &e){
        m_IsConnected = false;
        LOG_ERROR("%s,%s", __FUNCTION__, e.what());
    }
}

bool BybitSwapTradingClient::get_balances(pubsub::TCommand &tcmd)
try {
    http_request request(methods::GET);
    int rec_window = 5000;
    #ifdef USE_UNIFIED
    std::map<std::string, std::string> param{
        {"accountType","UNIFIED" }
    };
    #else
    std::map<std::string, std::string> param{
        {"accountType","CONTRACT" }
    };
    #endif
    if(tcmd.header.cmdTypeEnum == CMD_QUERY_BALANCE){
        param["coin"] = tcmd.body.queryBalance.currency;
    }
    long time = crypto::getCurrentTimeMilli();
    string sign  = get_signature_rest(time, rec_window, param);
    FORMAT_BYBIT_REQUEST(request)
    std::string url = fmt::format("{}?", m_balanceUrl.to_string());
    url += params_string(param);
    uri_builder builder(url);
    request.set_request_uri(builder.to_string());
    const http_response &response = hotHttpClient->request(request);
    // cout << response.to_string() << endl;
    auto code = response.status_code();
    if(code == status_codes::OK || code == status_codes::BadRequest
    || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
        json::value const &v = response.extract_json().get();
        if(v.has_field("result") && v.at("retCode") == 0){
            auto array = v.at("result").at("list").at(0).at("coin").as_array();
            for(auto &it : array){
                pubsub::RCommand rcmd;
                // rcmd.body.balance.total = stod(it.at("equity").as_string().c_str());
                rcmd.header.cmdTime = crypto::getCurrentTime();
                rcmd.header.exchangeTypeEnum = ExchangeType_BYBIT;
                rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                strcpy(rcmd.header.accountId, m_curcfg.accountId);
                string ccy = crypto::to_upper(it.at("coin").as_string().c_str());
                if(tcmd.header.cmdTypeEnum == CMD_QUERY_BALANCE){
                    if(crypto::str_cmp(ccy.c_str(), tcmd.body.queryBalance.currency) == false){
                        continue;
                    }
                }
                strcpy(rcmd.body.balance.currency, ccy.c_str());
                rcmd.body.balance.total = stod(it.at("walletBalance").as_string().c_str());
                rcmd.body.balance.available = stod(it.at("walletBalance").as_string().c_str());
                if(crypto::str_cmp(it.at("totalPositionMM").as_string().c_str(), "") == false){
                    rcmd.body.balance.frozen = stod(it.at("totalPositionMM").as_string().c_str());
                }
                else{
                    rcmd.body.balance.frozen = stod(it.at("totalOrderIM").as_string().c_str()) +
                                            stod( it.at("totalPositionIM").as_string().c_str());
                }

                if (crypto::str_cmp(it.at("unrealisedPnl").as_string().c_str(), "") == false) {
                    rcmd.body.balance.unrealizedPnl = stod(it.at("unrealisedPnl").as_string().c_str());
                }  
                
                rcmd.body.balance.apiSourceEnum = ApiSource_REST;
                PUSH_RCMD(rcmd)
                if(crypto::str_cmp(rcmd.body.balance.currency, "USDT")){
                    LOG_INFO("exchId:%s,instType:%s,accountId:%s,strategyId:%s,usdt available:%.2f,frozen:%.2f",
                        ExchangeTypeEnum2StrMap[rcmd.header.exchangeTypeEnum].c_str(),
                        InstTypeEnum2StrMap[rcmd.header.instTypeEnum].c_str(),
                        rcmd.header.accountId, rcmd.header.strategyId,
                        rcmd.body.balance.available, rcmd.body.balance.frozen);
                }
            }

            auto totalAccountInfo = v.at("result").at("list").at(0);
            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.header.cmdTime = crypto::getCurrentTime();
            rcmd.header.exchangeTypeEnum = ExchangeType_BYBIT;
            rcmd.header.instTypeEnum = InstType_USDT_SWAP;
            rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
            strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
            strcpy(rcmd.header.accountId, m_curcfg.accountId);
            string totalStr = totalAccountInfo.at("totalEquity").as_string();
            if (totalStr != "") {
                rcmd.body.totalAccount.totalEquity = stod(totalStr);
            }
            
            string mmrStr = totalAccountInfo.at("totalMaintenanceMargin").as_string();
            if (mmrStr != "") {
                rcmd.body.totalAccount.mmr = stod(mmrStr);
            }

            string mgnStr = totalAccountInfo.at("accountMMRate").as_string();
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
    }
    else{
        LOG_ERROR("get_balances, code:%d",code);
        // return pplx::task_from_result(json::value());
    }
    return true;
}
catch(exception &e){
    LOG_ERROR("%s", e.what());
    return false;
}

bool BybitSwapTradingClient::get_positions(pubsub::TCommand &tcmd)
try {
#if 1
    http_request request(methods::GET);
    int rec_window = 5000;
    std::map<std::string, std::string> param{
        {"category", "linear"},
        {"settleCoin", "USDT"}
    };
    if(tcmd.header.cmdTypeEnum == CMD_QUERY_POSITION){
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.queryPosition.instId, info)) {
            param["symbol"] = info.originInstId;
        }
    }
    long time = crypto::getCurrentTimeMilli();
    string sign  = get_signature_rest(time, rec_window, param);
    FORMAT_BYBIT_REQUEST(request)

    std::string url = fmt::format("{}?", m_positionUrl.to_string());
    // param["sign"] = get_signature_rest(param);
    url += params_string(param);
    uri_builder builder(url);
    request.set_request_uri(builder.to_string());
    const http_response &response = hotHttpClient->request(request);
    // cout << response.to_string() << endl;
    auto code = response.status_code();
    if(code == status_codes::OK || code == status_codes::BadRequest
    || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
        unordered_map<string, pubsub::RCommand> mCurrentPositions;
        json::value const &v = response.extract_json().get();
        // LOG_DEBUG("get_positions:%s", v.to_string().c_str());
        if(v.has_field("result") && v.at("retCode") == 0){
            auto array = v.at("result").at("list").as_array();
            int size = array.size();
            int i = 0;
            for(auto &it : array){
                i++;
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.header.cmdTime = crypto::getCurrentTime();
                rcmd.header.exchangeTypeEnum = ExchangeType_BYBIT;
                rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                strcpy(rcmd.header.accountId, m_curcfg.accountId);
                double positionAmt = stod(it.at("size").as_string().c_str());
                string originInstId = it.at("symbol").as_string();
                md::InstrumentInfo info;
                if(this->smc->get_instrument_info("BYBIT", "InstType_USDT_SWAP", originInstId.c_str(), info)){
                    rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                    strcpy(rcmd.body.position.instId, info.instId);
                }
                else if(this->smc->get_instrument_info("BYBIT", "InstType_USDT_FUTURES", originInstId.c_str(), info)){
                    rcmd.header.instTypeEnum = InstType_USDT_FUTURES;
                    strcpy(rcmd.body.position.instId, info.instId);
                }
                else if(this->smc->get_instrument_info("BYBIT", "InstType_C_SWAP", originInstId.c_str(), info)){
                    rcmd.header.instTypeEnum = InstType_C_SWAP;
                    strcpy(rcmd.body.position.instId, info.instId);
                }
                else if(this->smc->get_instrument_info("BYBIT", "InstType_C_FUTURES", originInstId.c_str(), info)){
                    rcmd.header.instTypeEnum = InstType_C_FUTURES;
                    strcpy(rcmd.body.position.instId, info.instId);
                }
                else{
                    LOG_ERROR("not found BYBIT.SWAP.%s smc info", originInstId.c_str());
                    continue;
                }
                
                const string &positionSide = it.at("side").as_string();
                rcmd.body.position.direction = (positionSide[0] == 'B' || positionSide[0] == 'N') ? Direction_LONG : Direction_SHORT;

                rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                rcmd.body.position.volume = positionAmt*info.magnifyNumber;

                rcmd.body.position.avgPrice = stod(it.at("avgPrice").as_string().c_str())*info.reduceNumber;
                rcmd.body.position.unrealizedPnl = stod(it.at("unrealisedPnl").as_string().c_str());
                rcmd.body.position.markPrice = stod(it.at("markPrice").as_string().c_str())*info.reduceNumber;
                string liqPx = it.at("liqPrice").as_string();
                if(crypto::str_cmp(liqPx.c_str(), "") == false){
                    rcmd.body.position.liquidPrice = stod(liqPx.c_str())*info.reduceNumber;
                }
                // rcmd.body.position.adlQuantile = it.at("adlRankIndicator").as_integer();
                double adl = it.at("adlRankIndicator").as_integer();
                if(adl >= 2){
                    adl -= 1;
                }
                rcmd.body.position.adlQuantile = adl;
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
        }
    }
    else{
        LOG_ERROR("get_positions, code:%d",code);
    }
#endif
    return true;
}
catch(exception &e){
    LOG_ERROR("%s", e.what());
    return false;
}

void BybitSwapTradingClient::add_new_order(pubsub::TCommand &tcmd){
    #if 1
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)
    if(!m_IsConnected){
        rcmd.body.newOrder.ErrorID = ERROR_TBDisconnectError;
        PUSH_RCMD(rcmd)
        return;
    }
    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.newOrder.instId, info)) {
            http_request request(methods::POST);
            int rec_window = 5000;
            std::map<std::string, std::string> param;

            if (tcmd.header.instTypeEnum == InstType_SPOT) {
                param["category"] = "spot";
            } else if (tcmd.header.instTypeEnum == InstType_USDT_SWAP || tcmd.header.instTypeEnum == InstType_USDT_FUTURES) {
                param["category"] = "linear";
            } else if (tcmd.header.instTypeEnum == InstType_C_SWAP || tcmd.header.instTypeEnum == InstType_C_FUTURES) {
                param["category"] = "inverse";
            }

            long time = crypto::getCurrentTimeMilli();
            std::string url = fmt::format("{}?", m_orderUrl.to_string());
            string price;
            string amount;
            if(tcmd.body.newOrder.reduceOnly == true){
                price = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice*info.magnifyNumber, info.tickSize*info.magnifyNumber);
                amount = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal*info.reduceNumber, info.lotSize*info.reduceNumber);
            }
            else{
                price = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice*info.magnifyNumber, info.tickSize*info.magnifyNumber);
                amount = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal*info.reduceNumber, info.lotSize*info.reduceNumber);
                // strcpy(price, crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice, info.tickSize).c_str());
                // strcpy(amount, crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal, info.lotSize).c_str());
            }
            rcmd.body.newOrder.limitPrice = stod(price.c_str())*info.reduceNumber;
            rcmd.body.newOrder.volumeTotal = stod(amount.c_str())*info.magnifyNumber;

            param["symbol"] = info.originInstId;
            param["reduceOnly"] = tcmd.body.newOrder.reduceOnly ? "true" : "false";
            param["orderLinkId"] = tcmd.body.newOrder.orderSysId;

            //open
            if(tcmd.body.newOrder.offsetFlag == OffsetFlag_OPEN){
                if (tcmd.body.newOrder.direction == Direction_LONG) {
                    param["side"] = "Buy";
                } else if (tcmd.body.newOrder.direction == Direction_SHORT) {
                    param["side"] = "Sell";
                } else {
                    rcmd.body.newOrder.ErrorID = ERROR_DirectionError;
                    PUSH_RCMD(rcmd)
                    return;
                }
            }//close
            else if(tcmd.body.newOrder.offsetFlag == OffsetFlag_CLOSE){
                if (tcmd.body.newOrder.direction == Direction_LONG){
                    param["side"] = "Sell";
                } else if (tcmd.body.newOrder.direction == Direction_SHORT){
                    param["side"] = "Buy";
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
                param["orderType"] = "Limit";
                param["timeInForce"] = "GTC";
                param["price"] = price;
                param["qty"] = amount;
            } else if (tcmd.body.newOrder.orderType == OrderType_MARKET) {
                param["orderType"] = "Market";
                param["qty"] = amount;
            } else if (tcmd.body.newOrder.orderType == OrderType_POST_ONLY) {
                param["orderType"] = "Limit";
                param["timeInForce"] = "PostOnly";
                param["price"] = price;
                param["qty"] = amount;
            } else if (tcmd.body.newOrder.orderType == OrderType_IOC) {
                param["orderType"] = "Limit";
                param["timeInForce"] = "IOC";
                param["price"] = price;
                param["qty"] = amount;
            } else if (tcmd.body.newOrder.orderType == OrderType_FOK) {
                param["orderType"] = "Limit";
                param["timeInForce"] = "FOK";
                param["price"] = price;
                param["qty"] = amount;
            } else {
                rcmd.body.newOrder.ErrorID = ERROR_OrderTypeError;
                PUSH_RCMD(rcmd)
                return;
            }
            string sign  = get_signature_rest(time, rec_window, param, 1);
            FORMAT_BYBIT_REQUEST(request)
            // param["sign"] = get_signature_rest(param);

            // url += params_string(param);
            request.set_body(params_string(param, 1), "application/json; charset=UTF-8");
            // cout << url << endl;
            uri_builder builder(url);
            request.set_request_uri(builder.to_string());
            LOG_DEBUG("%s:%s", __FUNCTION__, builder.to_string().c_str());
#if 1
            const http_response &response = hotHttpClient->request(request);
            if(tcmd.body.newOrder.clientOrderId == TESTCLIENTORDERID){
                return;
            }
            auto code = response.status_code();
            rcmd.body.newOrder.tsNet = crypto::getCurrentTime();

            LOG_DEBUG("BYBIT,%s,internetDelay,%ld", __FUNCTION__, rcmd.body.newOrder.tsNet - rcmd.body.newOrder.tsSent);

            //{"retCode":0,"retMsg":"OK","result":{"orderId":"9760a032-cabc-400f-9537-a3e7636a1657","orderLinkId":"BYBITCPP5715675975412053"},"retExtInfo":{},"time":1673238273699}
            //200 400 429 401
            if(code == status_codes::OK || code == status_codes::BadRequest
                || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
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
                if(rawData.HasMember("retCode") && rawData.HasMember("retMsg")){
                    int retCode = stoi(rawData["retCode"].GetString());
                    string retMsg = rawData["retMsg"].GetString();
                    if(retCode == 0){
                        strcpy(rcmd.body.newOrder.orderId, rawData["result"]["orderId"].GetString());
                        rcmd.body.newOrder.orderStatus = OrderStatus_NEW;
                        PUSH_RCMD(rcmd)
                        return;
                    }
                    else{
                        rcmd.body.newOrder.ErrorID = crypto::get_bybit_errorid(retCode); 
                        if (rcmd.body.newOrder.ErrorID == 10016) {   // 10016 报单状态未知
                            rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN;
                        } else {
                            rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED;
                        }
                        strncpy(rcmd.body.newOrder.originMsg, rawData["retMsg"].GetString(), sizeof(rcmd.body.newOrder.originMsg));
                        // LOG_ERROR("%s", s.c_str());
                        PUSH_RCMD(rcmd)
                        return;
                    }
                }
                else{
                    rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN;
                    PUSH_RCMD(rcmd)
                    return;
                }
                // if(rawData.HasMember("result") && rawData["result"].HasMember("orderId")){
                //     strcpy(rcmd.body.newOrder.orderId, rawData["result"]["orderId"].GetString());
                //     rcmd.body.newOrder.orderStatus = OrderStatus_NEW;
                //     PUSH_RCMD(rcmd)
                // }
                // else{
                //     rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED;
                //     strncpy(rcmd.body.newOrder.originMsg, rawData["retMsg"].GetString(), sizeof(rcmd.body.newOrder.originMsg));
                //     // LOG_ERROR("%s", s.c_str());
                //     PUSH_RCMD(rcmd)
                // }
            }
            else if(code == status_codes::ServiceUnavailable){//503
                auto v = response.extract_json().get();
                rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED ;
                rcmd.body.newOrder.ErrorID = ERROR_NetworkInternalError;
                if(v.has_field("retMsg"))
                    strncpy(rcmd.body.newOrder.originMsg, v.at("retMsg").as_string().c_str(), sizeof(rcmd.body.newOrder.originMsg));
                // LOG_ERROR("%s", s.c_str());
                PUSH_RCMD(rcmd)
                return;
            }
            else{
                auto v = response.extract_json().get();
                rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED;
                rcmd.body.newOrder.ErrorID = code;
                if(v.has_field("retMsg"))
                    strncpy(rcmd.body.newOrder.originMsg, v.at("retMsg").as_string().c_str(), sizeof(rcmd.body.newOrder.originMsg));
                // LOG_ERROR("%s", s.c_str());
                PUSH_RCMD(rcmd)
                return;
            }
#endif
#if 0
            addHotHttpClient->hotHttpClient->request(request).then([&](http_response response)->pplx::task<string>{
                //handle response
                auto code = response.status_code();
                rcmd.body.newOrder.tsNet = crypto::getCurrentTime();
                LOG_DEBUG("BYBIT,add_new_order,internetDelay,%ld", rcmd.body.newOrder.tsNet - rcmd.body.newOrder.tsSent);
                //{"retCode":0,"retMsg":"OK","result":{"orderId":"9760a032-cabc-400f-9537-a3e7636a1657","orderLinkId":"BYBITCPP5715675975412053"},"retExtInfo":{},"time":1673238273699}
                //200 400 429 401
                if(code == status_codes::OK || code == status_codes::BadRequest
                    || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
                    const string &v = response.extract_string().get();
                    LOG_DEBUG("add_new_order reponse: %s", v.c_str());
                    rapidjson::Document d;
                    rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(v.c_str());
                    if(d.HasParseError()){
                        rcmd.body.newOrder.ErrorID = ERROR_UnknownError;
                        rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN;
                        PUSH_RCMD(rcmd)
                        // return;
                    }
                    if(rawData.HasMember("result") && rawData["result"].HasMember("orderId")){
                        strcpy(rcmd.body.newOrder.orderId, rawData["result"]["orderId"].GetString());
                        rcmd.body.newOrder.orderStatus = OrderStatus_NEW;
                        PUSH_RCMD(rcmd)
                    }
                    else{
                        rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED;
                        strncpy(rcmd.body.newOrder.originMsg, rawData["retMsg"].GetString(), sizeof(rcmd.body.newOrder.originMsg));
                        // LOG_ERROR("%s", s.c_str());
                        PUSH_RCMD(rcmd)
                    }
                }
                else if(code == status_codes::ServiceUnavailable){//503
                    auto v = response.extract_json().get();
                    rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED ;
                    rcmd.body.newOrder.ErrorID = ERROR_NetworkInternalError;
                    if(v.has_field("retMsg"))
                        strncpy(rcmd.body.newOrder.originMsg, v.at("retMsg").as_string().c_str(), sizeof(rcmd.body.newOrder.originMsg));
                    // LOG_ERROR("%s", s.c_str());
                    PUSH_RCMD(rcmd)
                }
                else{
                    auto v = response.extract_json().get();
                    rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED;
                    rcmd.body.newOrder.ErrorID = code;
                    if(v.has_field("retMsg"))
                        strncpy(rcmd.body.newOrder.originMsg, v.at("retMsg").as_string().c_str(), sizeof(rcmd.body.newOrder.originMsg));
                    // LOG_ERROR("%s", s.c_str());
                    PUSH_RCMD(rcmd)
                }
                return response.extract_string();
            })
            .then([](pplx::task<string> previous_task) mutable {//mutable
                // if (previous_task._GetImpl()->_HasUserException()) {
                // }
            });
#endif
            // cout << response.to_string() << endl;
        }
        else{
            rcmd.body.newOrder.ErrorID = ERROR_SMCInstrumentNotExistError;
            LOG_ERROR("not found BYBIT.%s.%s smc info", InstTypeEnum2StrMap[tcmd.header.instTypeEnum].c_str(), tcmd.body.newOrder.instId);
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
    #endif
}

void BybitSwapTradingClient::cancel_order(pubsub::TCommand &tcmd){
    #if 1
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    try {
        md::InstrumentInfo info;
        if (smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.cancelOrder.instId, info)){
            if(tcmd.body.cancelOrder.cancelOrderTypeEnum == pubsub::COT_ONE_INST){
                http_request request(methods::POST);
                int rec_window = 5000;
                std::map<std::string, std::string> param;
                
                if (tcmd.header.instTypeEnum == InstType_SPOT) {
                    param["category"] = "spot";
                } else if (tcmd.header.instTypeEnum == InstType_USDT_SWAP || tcmd.header.instTypeEnum == InstType_USDT_FUTURES) {
                    param["category"] = "linear";
                } else if (tcmd.header.instTypeEnum == InstType_C_SWAP || tcmd.header.instTypeEnum == InstType_C_FUTURES) {
                    param["category"] = "inverse";
                }
                long time = crypto::getCurrentTimeMilli();

                // std::string url = fmt::format("{}?", m_cancelOrderUrl.to_string());
                param["symbol"] = info.originInstId;
                if(crypto::str_cmp(tcmd.body.cancelOrder.orderId, "") == false){
                    param["orderId"] = tcmd.body.cancelOrder.orderId;
                }
                else  if(tcmd.body.cancelOrder.clientOrderId != 0){
                    param["orderLinkId"] = tcmd.body.cancelOrder.orderSysId;
                }
                else{
                    LOG_ERROR("cancel order need orderId or clientOrderId");
                    strcpy(rcmd.body.cancelOrder.originMsg, "cancel order need orderId or clientOrderId");
                    rcmd.body.cancelOrder.ErrorID = ERROR_NoOrderIdError;
                    rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                    PUSH_RCMD(rcmd)
                    return;
                }
                // param["sign"] = get_signature_rest(param);
                string sign  = get_signature_rest(time, rec_window, param, 1);
                FORMAT_BYBIT_REQUEST(request)

                request.set_body(params_string(param, 1), "application/json; charset=UTF-8");

                uri_builder builder(m_cancelOrderUrl.to_string());
                request.set_request_uri(builder.to_string());
                LOG_DEBUG("cancel order: %s ", builder.to_string().c_str());
#if 1
                const http_response &response = hotHttpClient->request(request);
                rcmd.body.cancelOrder.tsNet = crypto::getCurrentTime();
                LOG_DEBUG("BYBIT,cancel_order,internetDelay,%ld", rcmd.body.cancelOrder.tsNet - rcmd.body.cancelOrder.tsSent);

                auto code = response.status_code();
                if (code == status_codes::OK || code == status_codes::BadRequest
                    || code == status_codes::TooManyRequests || code == status_codes::Unauthorized) {
                    const string &v = response.extract_string().get();
                    LOG_DEBUG("cancel order response:%s", v.c_str());
                    rapidjson::Document d;
                    rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(v.c_str());
                    if(d.HasParseError()){
                        rcmd.body.cancelOrder.ErrorID = ERROR_UnknownError;
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                        PUSH_RCMD(rcmd)
                        return;
                    }
                    if(rawData.HasMember("retCode") && rawData.HasMember("retMsg")){
                        int retCode = stoi(rawData["retCode"].GetString());
                        string retMsg = rawData["retMsg"].GetString();
                        if(retCode != 0 ) {
                            rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                            rcmd.body.cancelOrder.ErrorID = crypto::get_bybit_errorid(retCode);
                            strncpy(rcmd.body.cancelOrder.originMsg, retMsg.c_str(), sizeof(rcmd.body.cancelOrder.originMsg));
                            PUSH_RCMD(rcmd)
                            return;
                        }
                        // if(retCode == 0 ){
                        //     rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                        //     PUSH_RCMD(rcmd)
                        // }
                        // else{
                        //     rcmd.body.cancelOrder.ErrorID = crypto::get_bybit_errorid(retCode);
                        //     strncpy(rcmd.body.cancelOrder.originMsg, retMsg.c_str(), sizeof(rcmd.body.cancelOrder.originMsg));
                        //     PUSH_RCMD(rcmd)
                        // }
                    }
                    else{
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                        PUSH_RCMD(rcmd)
                        return;
                    }
                    return;
                }
                else{
                    auto v = response.extract_json().get();
                    LOG_ERROR("%s", v.serialize().c_str());
                    rcmd.body.cancelOrder.ErrorID = code;
                    if(v.has_field("retMsg")){
                        strncpy(rcmd.body.cancelOrder.originMsg, v.at("retMsg").as_string().c_str(), sizeof(rcmd.body.cancelOrder.originMsg));
                    }
                    rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                    PUSH_RCMD(rcmd)
                    return;
                }
#endif
#if 0
                hotHttpClient->hotHttpClient->request(request).then([&](http_response response)->pplx::task<string>{
                    rcmd.body.cancelOrder.tsNet = crypto::getCurrentTime();
                    LOG_DEBUG("BYBIT,cancel_order,internetDelay,%ld", rcmd.body.cancelOrder.tsNet - rcmd.body.cancelOrder.tsSent);

                    auto code = response.status_code();
                    if (code == status_codes::OK || code == status_codes::BadRequest
                        || code == status_codes::TooManyRequests || code == status_codes::Unauthorized) {
                        const string &v = response.extract_string().get();
                        LOG_DEBUG("cancel order response:%s", v.c_str());
                        rapidjson::Document d;
                        rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(v.c_str());
                        if(d.HasParseError()){
                            rcmd.body.cancelOrder.ErrorID = ERROR_UnknownError;
                            rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                            PUSH_RCMD(rcmd)
                        }
                        if(rawData.HasMember("result") && rawData["result"].HasMember("orderId")){
                            strcpy(rcmd.body.cancelOrder.orderId, rawData["result"]["orderId"].GetString() );
                            strcpy(rcmd.body.cancelOrder.orderSysId, rawData["result"]["orderLinkId"].GetString());
                            rcmd.body.cancelOrder.orderStatus = OrderStatus_CANCELED;
                        }
                        else{
                            if(rawData.HasMember("retMsg")){
                                strncpy(rcmd.body.cancelOrder.originMsg, rawData["retMsg"].GetString(), sizeof(rcmd.body.cancelOrder.originMsg));
                            }
                            if(rawData.HasMember("retCode")){
                                rcmd.body.cancelOrder.ErrorID = stoi(rawData["retCode"].GetString());
                            }

                            rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                        }
                        PUSH_RCMD(rcmd)
                    }
                    else{
                        auto v = response.extract_json().get();
                        LOG_ERROR("%s", v.serialize().c_str());
                        rcmd.body.cancelOrder.ErrorID = code;
                        if(v.has_field("retMsg")){
                            strncpy(rcmd.body.cancelOrder.originMsg, v.at("retMsg").as_string().c_str(), sizeof(rcmd.body.cancelOrder.originMsg));
                        }

                        rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                        PUSH_RCMD(rcmd)
                    }
                    return response.extract_string();
                })
                .then([](pplx::task<string> previous_task) mutable {
                    if (previous_task._GetImpl()->_HasUserException()) {
                        cout << "_HasUserException" <<endl;
                    }
                });
#endif
            }
            else if(tcmd.body.cancelOrder.cancelOrderTypeEnum == pubsub::COT_MULTI_INST){

            }
            else{
                rcmd.body.cancelOrder.ErrorID = ERROR_OrderTypeError;
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
        rcmd.body.cancelOrder.ErrorID = ERROR_NetworkError;
        rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
        strncpy(rcmd.body.cancelOrder.originMsg, e.what(), sizeof(rcmd.body.cancelOrder.originMsg));
        LOG_ERROR("%s", e.what());
        PUSH_RCMD(rcmd)
        return;
    }
    #endif
}

void BybitSwapTradingClient::query_order(pubsub::TCommand &tcmd){
    #if 1
    if(tcmd.body.queryOrder.queryOrderTypeEnum == QOT_ONE_INST){
        query_one_order(tcmd);
    }
    else if(tcmd.body.queryOrder.queryOrderTypeEnum == QOT_MULTI_INST){
        // query_multi_orders(cmd);
    }
    else{

    }
    #endif
}


void BybitSwapTradingClient::query_one_order(pubsub::TCommand &tcmd){
    #if 1
    QUERY_ORDER_TCMD_2_RCMD(tcmd)
    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.queryOrder.instId, info)){
            // http_client restclient(m_curcfg.restBaseUrl);
            http_request request(methods::GET);
            int rec_window = 5000;
            std::map<std::string, std::string> param;
            
            if (tcmd.header.instTypeEnum == InstType_SPOT) {
                param["category"] = "spot";
            } else if (tcmd.header.instTypeEnum == InstType_USDT_SWAP || tcmd.header.instTypeEnum == InstType_USDT_FUTURES) {
                param["category"] = "linear";
            } else if (tcmd.header.instTypeEnum == InstType_C_SWAP || tcmd.header.instTypeEnum == InstType_C_FUTURES) {
                param["category"] = "inverse";
            }

            long time = crypto::getCurrentTimeMilli();
            std::string url = fmt::format("{}?", m_queryOrderUrl.to_string());
            param["symbol"] = info.originInstId;

            if(crypto::str_cmp(tcmd.body.queryOrder.orderId, "") == false){
                param["orderId"] = tcmd.body.queryOrder.orderId;
            }
            else if(tcmd.body.queryOrder.clientOrderId != 0){
                param["orderLinkId"] = tcmd.body.queryOrder.orderSysId;
            }
            else{
                LOG_ERROR("query_one_order need orderId or clientOrderId");
                return;
            }
            string sign  = get_signature_rest(time, rec_window, param);
            FORMAT_BYBIT_REQUEST(request)

            // string path = m_orderUrl.to_string()+queryStr;
            // param["sign"] = get_signature_rest(param);

            url += params_string(param);
            // cout << url << endl;
            uri_builder builder(url);
            LOG_DEBUG("query_order:%s", builder.to_string().c_str());
            request.set_request_uri(builder.to_string());
            const http_response &response = hotHttpClient->request(request);
            auto code = response.status_code();
            if(code == status_codes::OK || code == status_codes::BadRequest
            || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
                // return response.extract_json();
                const string &v = response.extract_string().get();
                LOG_DEBUG("query one order response:%s", v.c_str());
                rapidjson::Document d;
                rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(v.c_str());
                if(d.HasParseError()){
                    rcmd.body.queryOrder.ErrorID = ERROR_UnknownError;
                    rcmd.body.queryOrder.orderStatus = OrderStatus_UNKNOWN;
                    PUSH_RCMD(rcmd)
                    return;
                }
                if(rawData.HasMember("result") && rawData["result"].HasMember("list")){
                    const rapidjson::Value &pData = rawData["result"]["list"];
                    // memset(&rcmd.body, 0, sizeof(rcmd.body));
                    if(pData.Size() == 0){
                        rcmd.body.queryOrder.ErrorID = ERROR_OrderNotFoundError;
                        rcmd.body.queryOrder.orderStatus = OrderStatus_REJECTED;
                        PUSH_RCMD(rcmd)
                        return;
                    }
                    else{
                        for(rapidjson::SizeType i = 0; i < pData.Size(); i++){
                            strcpy(rcmd.body.queryOrder.orderId, pData[i]["orderId"].GetString());
                            strcpy(rcmd.body.queryOrder.orderSysId, pData[i]["orderLinkId"].GetString());

                            string orderStatus = pData[i]["orderStatus"].GetString();
                            if((orderStatus[0] == 'C' && orderStatus[2] == 'e') || orderStatus[0] == 'N'){//Created New新订单
                                rcmd.body.queryOrder.orderStatus = OrderStatus_NEW;
                            }
                            else if(orderStatus[0] == 'C' && orderStatus[2] == 'n'){//Cancelled 订单被取消
                                rcmd.body.queryOrder.orderStatus = OrderStatus_CANCELED;
                            }
                            else if(orderStatus[0] == 'P'){//PartiallyFilled
                                rcmd.body.queryOrder.orderStatus = OrderStatus_PARTFILLED;
                            }
                            else if(orderStatus[0] == 'F'){//Filled
                                rcmd.body.queryOrder.orderStatus = OrderStatus_FILLED;
                            }
                            else {
                                rcmd.body.queryOrder.orderStatus = OrderStatus_UNKNOWN;
                            }
                            //合約名稱，如 BTC-USDT
                            string originInstId = pData[i]["symbol"].GetString();
                            // string instType = pData[i]["instType"].GetString();
                            md::InstrumentInfo info;
                            if(this->smc->get_instrument_info("BYBIT", "SWAP", originInstId.c_str(), info)){
                                strcpy(rcmd.body.queryOrder.instId, info.instId);
                            }
                            else{
                                LOG_ERROR("not found BYBIT.SWAP.%s smc info", originInstId.c_str());
                                continue;
                            }
                            //已经成交量
                            if(crypto::str_cmp(pData[i]["cumExecQty"].GetString(), "") == false){
                                rcmd.body.queryOrder.volumeTraded = stod(pData[i]["cumExecQty"].GetString())*info.magnifyNumber;
                            }
                            //成交均价
                            if(crypto::str_cmp(pData[i]["avgPrice"].GetString(), "") == false){
                                rcmd.body.queryOrder.tradePrice = stod(pData[i]["avgPrice"].GetString())*info.reduceNumber;
                            }
                            //原始订单数量
                            if(crypto::str_cmp(pData[i]["qty"].GetString(), "") == false){
                                rcmd.body.queryOrder.volumeTotal = stod(pData[i]["qty"].GetString())*info.magnifyNumber;
                            }
                            //原始订单价格
                            if(crypto::str_cmp(pData[i]["price"].GetString(), "") == false){
                                rcmd.body.queryOrder.limitPrice = stod(pData[i]["price"].GetString())*info.reduceNumber;
                            }
                            rcmd.header.instTypeEnum = info.instTypeEnum;
                            // rcmd.body.queryOrder.offsetFlag = OffsetFlag_OPEN;
                            // string side = pData[i]["side"].GetString();
                            // rcmd.body.queryOrder.direction = side[0] == 'B' ? Direction_LONG : Direction_SHORT;
                            const string &orderType = pData[i]["orderType"].GetString();
                            const string &timeInForce = pData[i]["timeInForce"].GetString();
                            // rcmd.body.queryOrder.orderType = crypto::get_bybit_ordertype(timeInForce.c_str(), orderType.c_str());

                            // rcmd.body.queryOrder.apiSourceEnum = ApiSource_REST;
                            PUSH_RCMD(rcmd)
                            return;
                        }
                    }
                }
                else{
                    rcmd.body.queryOrder.orderStatus = OrderStatus_UNKNOWN;
                }
                PUSH_RCMD(rcmd)
            }
            else{
                auto s = response.extract_string().get();
                rcmd.body.queryOrder.ErrorID = code;
                rcmd.body.queryOrder.orderStatus = OrderStatus_REJECTED;
                strncpy(rcmd.body.queryOrder.originMsg, s.c_str(), sizeof(rcmd.body.queryOrder.originMsg));
                LOG_ERROR("query_one_order:code:%d, response:%s", code, s.c_str());
                PUSH_RCMD(rcmd)
                return;
                // return pplx::task_from_result(json::value());
            }
        }
        else{
            LOG_ERROR("not found BYBIT %s smc info", tcmd.body.queryOrder.instId);
        }
    }
    catch(exception &e){
        LOG_ERROR("%s", e.what());
    }
    #endif
}

void BybitSwapTradingClient::query_multi_orders(pubsub::TCommand &tcmd){

}