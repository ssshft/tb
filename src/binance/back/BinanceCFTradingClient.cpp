#include "binance/BinanceCFTradingClient.h"

BinanceCFTradingClient::BinanceCFTradingClient(){

}

BinanceCFTradingClient::~BinanceCFTradingClient(){
//    delete smc;
}

bool BinanceCFTradingClient::Initialize(AccountCfg& cfg, sm::SecurityManager *smc){
    this->smc = smc;
    //static values
    m_wssb_url = "/ws/";
//    m_ord_url = "/api/v3/order";
    m_ord_url = "/dapi/v1/order";
    //m_cxt_url = "/api/v3/order";
//    m_trans_url = "/sapi/v1/asset/transfer";
//    m_wsLiskey_url = "/api/v3/userDataStream";
    m_wsLiskey_url = "/dapi/v1/listenKey";
    m_queryord_url[0] = "/dapi/v1/order";
    m_queryord_url[1] = "/dapi/v1/openOrders";
    m_queryord_url[2] = "/dapi/v1/allOrders";
//
    m_balanceUrl = "/dapi/v1/account";
    m_positionUrl = "/dapi/v2/positionRisk";
    m_adlquantileUrl = "/dapi/v1/adlQuantile";
//    m_orderUrl = "/api/v4/spot/orders";
//    m_cancelOrderUrl = "/spot/orders/";
    m_curcfg = cfg;
    // addNewOrderRestClient = new http_client(m_curcfg.restBaseUrl);
    // cancelOrderRestClient = new http_client(m_curcfg.restBaseUrl);
    // hotHttpClient = new http_client(m_curcfg.restBaseUrl);
    hotHttpClient = new crypto::RestClientCPP(m_curcfg.restBaseUrl.c_str());
    return true;
}

void BinanceCFTradingClient::Run() {
    std::thread monitorThread(&BinanceCFTradingClient::monitor, this);
    monitorThread.detach();
}

void BinanceCFTradingClient::sub_websocket(){
    try{
        uri_builder builder(m_curcfg.wsBaseUrl);
        builder.append_path(m_wssb_url.to_string());
        builder.append_path(m_listenkey);//m_listenkey
//        cout << builder.to_string() << endl;
        // wsClient = new websocket_callback_client();
        wsClient.close();
        wsClient.connect(builder.to_string())
        .then([&]() {
            std::function<void (const websocket_incoming_message &msg)> f;
            f = std::bind(&BinanceCFTradingClient::on_websocket_msg, this, placeholders::_1);
            wsClient.set_message_handler(f);
            std::function<void (websocket_close_status close_status,
                                const utility::string_t& reason,
                                const std::error_code& error)> c;
            c =  std::bind(&BinanceCFTradingClient::on_close_msg,this
                    ,placeholders::_1,placeholders::_2,placeholders::_3);
            wsClient.set_close_handler(c);
        }).wait();
        m_IsConnected = true;
        LOG_INFO("connected with binance coin futures api");
    }
    catch(exception &e){
        m_IsConnected = false;
        LOG_ERROR("%s", e.what());
    }
}

void BinanceCFTradingClient::monitor() {
    while(1){
        try{
            static auto tk = crypto::getCurrentTimeSeconds();
            LOG_INFO("start to connect with binance coin futures api");
            gen_listen_key();
            sub_websocket();

            while(m_IsConnected){
                sleep(10);
                if(!m_IsConnected){
                    LOG_ERROR("binance coin futures ws disconnected, will reconnect it now");
                    break;
                }
                else{
                    pong();
                }
                auto now = crypto::getCurrentTimeSeconds();
                if(now - tk > 5 * 60){//5 minutes
                    keep_listen_key();
                    tk = crypto::getCurrentTimeSeconds();
                }
            }
        }
        catch(exception &e){
            m_IsConnected = false;
            LOG_ERROR("%s", e.what());
        }
        sleep(5);
    }
}

void BinanceCFTradingClient::on_websocket_msg(const websocket_incoming_message& msg){
    try{
        // text_message, binary_message, close, ping, pong
        if (msg.message_type() == websocket_message_type::text_message){
            msg.extract_string().then([&](const string s){
                rapidjson::Document d;
                rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(s.c_str());
                if(d.HasParseError() || !rawData.IsObject() || !rawData.HasMember("e")){
                    return;
                }
                LOG_DEBUG("on_websocket_msg:%s", s.c_str());
                string e = rawData["e"].GetString();
                if(crypto::str_cmp(e.c_str(), "ACCOUNT_UPDATE")){
                    //https://binance-docs.github.io/apidocs/futures/cn/#balance-position
                    const rapidjson::Value &data = rawData["a"]["B"];
                    for(rapidjson::SizeType i = 0; i < data.Size(); i++){
                        pubsub::RCommand rcmd;
                        memset(&rcmd,0,sizeof(pubsub::RCommand));
                        rcmd.header.cmdTime = crypto::getCurrentTime();
                        rcmd.header.exchangeTypeEnum = ExchangeType_BINANCE;
                        rcmd.header.instTypeEnum = InstType_C_SWAP;
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        // cmd.body.balance.updateTime = stoll(rawData["E"].GetString())*1000;
                        // strcpy(cmd.body.balance.currency, data[i]["a"].GetString());
                        // cmd.body.balance.available = stod(data[i]["cw"].GetString());
                        // cmd.body.balance.frozen = stod(data[i]["wb"].GetString()) - cmd.body.balance.available;
                        strcpy(rcmd.body.balance.currency, data[i]["a"].GetString());
                        rcmd.body.balance.available = stod(data[i]["cw"].GetString());
                        rcmd.body.balance.frozen = stod(data[i]["wb"].GetString()) - rcmd.body.balance.available;
                        rcmd.body.balance.total = rcmd.body.balance.available + rcmd.body.balance.frozen;
                        rcmd.body.balance.apiSourceEnum = ApiSource_WEBSOCKET;
                        PUSH_RCMD(rcmd)
                        // string ccy = rcmd.body.balance.currency;
                        // auto found = g_filterSymbolsMap.find(ccy);
                        // if (found != g_filterSymbolsMap.end()){
                        //     PUSH_RCMD(rcmd)
                        // }
                    }
                    const rapidjson::Value &pData = rawData["a"]["P"];
                    for(rapidjson::SizeType i = 0; i < pData.Size(); i++){
//                        auto it = pData[i];
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.header.cmdTime = crypto::getCurrentTime();
                        rcmd.header.exchangeTypeEnum = ExchangeType_BINANCE;
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        double positionAmt = stod(pData[i]["pa"].GetString());
                        string originInstId = pData[i]["s"].GetString();
                        string positionSide = pData[i]["ps"].GetString();
                        if(positionSide[0] == 'B'){//单向模式
                            rcmd.body.position.direction = positionAmt > 0 ? Direction_LONG : Direction_SHORT;
                            md::InstrumentInfo info;
                            if(smc->get_instrument_info("BINANCE", "InstType_C_SWAP",originInstId.c_str(),info)) {
                                rcmd.header.instTypeEnum = InstType_C_SWAP;
                                strcpy(rcmd.body.position.instId, info.instId);
                            }
                            else if(smc->get_instrument_info("BINANCE", "InstType_C_FUTURES",originInstId.c_str(),info)) {
                                rcmd.header.instTypeEnum = InstType_C_FUTURES;
                                strcpy(rcmd.body.position.instId, info.instId);
                            }
                            else{
                                LOG_ERROR("not found binance %s smc info", originInstId.c_str());
                                continue;
                            }
                            positionAmt = positionAmt > 0 ? positionAmt : -positionAmt;
                            rcmd.body.position.volume = positionAmt;
                            rcmd.body.position.maintMargin = stod(pData[i]["iw"].GetString());
                            rcmd.body.position.avgPrice = stod(pData[i]["ep"].GetString());
                            rcmd.body.position.unrealizedPnl = stod(pData[i]["up"].GetString());

                            rcmd.body.position.apiSourceEnum = ApiSource_WEBSOCKET;
                            PUSH_RCMD(rcmd)
                            // string instId = rcmd.body.position.instId;
                            // auto found = g_filterSymbolsMap.find(instId);
                            // if(found != g_filterSymbolsMap.end()){
                            //     PUSH_RCMD(rcmd)
                            // }
                        }
                        else{//双向模式

                        }
                    }
                }
                else if(crypto::str_cmp(e.c_str(), "ORDER_TRADE_UPDATE")){
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.header.cmdTime = crypto::getCurrentTime();
                    rcmd.header.exchangeTypeEnum = ExchangeType_BINANCE;
                    rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                    strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                    strcpy(rcmd.header.accountId, m_curcfg.accountId);
                    strcpy(rcmd.body.orderResponse.orderId, rawData["o"]["i"].GetString());
                    //TODO
                    strcpy(rcmd.body.orderResponse.orderSysId, rawData["o"]["c"].GetString());
                    rcmd.body.orderResponse.offsetFlag = OffsetFlag_OPEN;//crypto::str_cmp(data[i][""].GetString())
                    string side = rawData["o"]["S"].GetString();
                    rcmd.body.orderResponse.direction = side[0] == 'B' ? Direction_LONG : Direction_SHORT ;
//                    string orderType = rawData["o"]["o"].GetString();
                    string timeInForce = rawData["o"]["f"].GetString();
                    string orderType = rawData["o"]["o"].GetString(); // 订单类型
                    rcmd.body.orderResponse.orderType = crypto::get_binance_ordertype(timeInForce.c_str(), orderType.c_str());

                    //已经成交量
                    rcmd.body.orderResponse.volumeTraded = stod(rawData["o"]["z"].GetString());
                    //成交均价
                    rcmd.body.orderResponse.tradePrice = stod(rawData["o"]["ap"].GetString());
                    //原始订单数量
                    rcmd.body.orderResponse.volumeTotal  = stod(rawData["o"]["q"].GetString());
                    //原始订单价格
                    rcmd.body.orderResponse.limitPrice   = stod(rawData["o"]["p"].GetString());
                    string status = rawData["o"]["X"].GetString();
                    rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(status);

                    string originInstId = rawData["o"]["s"].GetString();
                    md::InstrumentInfo info;
                    if(this->smc->get_instrument_info("BINANCE", "InstType_C_SWAP", originInstId.c_str(), info)) {
                        rcmd.header.instTypeEnum = InstType_C_SWAP;
                        strcpy(rcmd.body.orderResponse.instId, info.instId);
                    }
                    else if(this->smc->get_instrument_info("BINANCE", "InstType_C_FUTURES", originInstId.c_str(), info)) {
                        rcmd.header.instTypeEnum = InstType_C_FUTURES;
                        strcpy(rcmd.body.orderResponse.instId, info.instId);
                    }
                    else{
                        LOG_ERROR("not found binance %s smc info", originInstId.c_str());
                    }
                    rcmd.body.orderResponse.apiSourceEnum = ApiSource_WEBSOCKET;
                    PUSH_RCMD(rcmd)
                }
                else{

                }
            });
        }
        else if(msg.message_type() == websocket_message_type::ping){
            pong();
        }
        else if(msg.message_type() == websocket_message_type::close){
            m_IsConnected = false;
        }
    }
    catch(exception &e)
    {
        m_IsConnected = false;
        LOG_ERROR("%s", e.what());
    }
}


bool BinanceCFTradingClient::get_balances()//vector<Balance> &balanceVec
try {
    // http_client restclient(m_curcfg.restBaseUrl);
    http_request request(methods::GET);
    request.headers().add("X-MBX-APIKEY",m_curcfg.apiKey);
    uri_builder builder(m_balanceUrl);

    builder.append_query("recvWindow",5000);
    builder.append_query("timestamp",crypto::getCurrentTimeMilli() );//
    auto signature = get_signature_rest(m_curcfg.apiSecret, builder.query());
    builder.append_query("signature",signature);
    request.set_request_uri(builder.to_string());
//    LOG_DEBUG("%s,url:%s%s",__FUNCTION__ , m_curcfg.restBaseUrl.c_str(), builder.to_string().c_str());
    // restclient.request(request)
    http_response response = hotHttpClient->request(request);
    auto code = response.status_code();
    if(code == status_codes::OK || code == status_codes::BadRequest
        || code == status_codes::TooManyRequests || code == status_codes::Unauthorized) {
        json::value const & v = response.extract_json().get();
        if(v.has_field("code")){
            LOG_ERROR("%s", v.serialize().c_str());
        }
        else{
            if (v.has_field("assets")) {
                auto array = v.at("assets").as_array();
                for(auto it : array){
                    double total = stod(it.at("walletBalance").as_string().c_str());
                    if(crypto::is_zeronum(total)){
                        continue;
                    }
                    //{"assets":[{"asset":"AAVE","availableBalance":"0.00000000","crossUnPnl":"0.00000000","crossWalletBalance":"0.00000000","initialMargin":"0.00000000","maintMargin":"0.00000000","marginBalance":"0.00000000","maxWithdrawAmount":"0.00000000","openOrderInitialMargin":"0.00000000","positionInitialMargin":"0.00000000","unrealizedProfit":"0.00000000","walletBalance":"0.00000000"}
                    pubsub::RCommand rcmd;
                    memset(&rcmd,0,sizeof(pubsub::RCommand));
                    rcmd.header.cmdTime = crypto::getCurrentTime();
                    rcmd.header.exchangeTypeEnum = ExchangeType_BINANCE;
                    string symbol = it.at("asset").as_string();
                    symbol = crypto::to_upper(symbol.c_str());
                    rcmd.header.instTypeEnum = InstType_C_SWAP;
                    rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                    strcpy(rcmd.header.accountId, m_curcfg.accountId);
                    strcpy(rcmd.body.balance.currency, symbol.c_str());
                    rcmd.body.balance.total = total;
                    rcmd.body.balance.unrealizedPnl = stod(it.at("unrealizedProfit").as_string().c_str());
                    rcmd.body.balance.available = stod(it.at("availableBalance").as_string().c_str());
                    rcmd.body.balance.frozen = rcmd.body.balance.total - rcmd.body.balance.available;
                    rcmd.body.balance.apiSourceEnum = ApiSource_REST;
                    PUSH_RCMD(rcmd)
                }
            }
        }
    }
    else{
         LOG_ERROR("%s", response.extract_string().get().c_str());
    }
    return true;
}
catch(exception &e)
{
    LOG_ERROR("%s", e.what());
    return false;
}

bool BinanceCFTradingClient::get_positions()
try {
    // http_client restclient(m_curcfg.restBaseUrl);
    http_request request(methods::GET);
    request.headers().add("X-MBX-APIKEY",m_curcfg.apiKey);
    uri_builder builder(m_positionUrl);

    builder.append_query("recvWindow",5000);
    // builder.append_query("symbol",originInstId.c_str());
    builder.append_query("timestamp",crypto::getCurrentTimeMilli() );//
    auto signature = get_signature_rest(m_curcfg.apiSecret, builder.query());
    builder.append_query("signature",signature);
    request.set_request_uri(builder.to_string());
    // restclient.request(request)
    http_response response = hotHttpClient->request(request);
    auto code = response.status_code();
    if(code == status_codes::OK || code == status_codes::BadRequest
        || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
        json::value const & v = response.extract_json().get();
        LOG_DEBUG("%s", v.serialize().c_str());
        // auto array = v.at("positions").as_array();

        for(auto it : v.as_array()){
            double positionAmt = stod(it.at("positionAmt").as_string().c_str()) ;
            if(positionAmt > -ZERO_NUM && positionAmt < ZERO_NUM){
                continue;
            }
            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.header.exchangeTypeEnum = ExchangeType_BINANCE;
            rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
            strcpy(rcmd.header.accountId, m_curcfg.accountId);
            strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
            string originInstId = it.at("symbol").as_string();
            string positionSide = it.at("positionSide").as_string();
            if(positionSide[0] == 'B'){//单向模式
                rcmd.body.position.direction = positionAmt > 0 ? Direction_LONG : Direction_SHORT;
                md::InstrumentInfo info;
                if(smc->get_instrument_info("BINANCE", "InstType_C_SWAP",originInstId.c_str(),info)) {
                    rcmd.header.instTypeEnum = InstType_C_SWAP;
                    strcpy(rcmd.body.position.instId, info.instId);
                }
                else if(smc->get_instrument_info("BINANCE", "InstType_C_FUTURES",originInstId.c_str(),info)) {
                    rcmd.header.instTypeEnum = InstType_C_FUTURES;
                    strcpy(rcmd.body.position.instId, info.instId);
                }
                else{
                    //LOG_ERROR("not found binance %s smc info", originInstId.c_str());
                    continue;
                }
                positionAmt = positionAmt > 0 ? positionAmt : -positionAmt;
                rcmd.body.position.volume = positionAmt;
                rcmd.body.position.maintMargin = stod(it.at("isolatedWallet").as_string().c_str());
                rcmd.body.position.avgPrice = stod(it.at("entryPrice").as_string().c_str());
                rcmd.body.position.unrealizedPnl = stod(it.at("unRealizedProfit").as_string().c_str());
                rcmd.body.position.markPrice = stod(it.at("markPrice").as_string().c_str());
                rcmd.body.position.liquidPrice = stod(it.at("liquidationPrice").as_string().c_str());
                // rcmd.body.position.adlQuantile = -1;
                rcmd.body.position.apiSourceEnum = ApiSource_REST;
                PUSH_RCMD(rcmd)
            }
        }
    }
    else{
         LOG_ERROR("%s", response.extract_string().get().c_str());
    }
    return true;
}
catch(exception &e){
    LOG_ERROR("%s", e.what());
    return false;
}

bool BinanceCFTradingClient::get_adlquantile()
try {
    // http_client restclient(m_curcfg.restBaseUrl);
    http_request request(methods::GET);
    request.headers().add("X-MBX-APIKEY",m_curcfg.apiKey);
    uri_builder builder(m_adlquantileUrl);

    builder.append_query("recvWindow",5000);
    // builder.append_query("symbol",originInstId.c_str());
    builder.append_query("timestamp",crypto::getCurrentTimeMilli() );//
    auto signature = get_signature_rest(m_curcfg.apiSecret, builder.query());
    builder.append_query("signature",signature);
    request.set_request_uri(builder.to_string());
    // restclient.request(request)
    http_response response = hotHttpClient->request(request);
    auto code = response.status_code();
    if(code == status_codes::OK || code == status_codes::BadRequest
        || code == status_codes::TooManyRequests || code == status_codes::Unauthorized)
    {
         json::value const & v = response.extract_json().get();
        // LOG_DEBUG("%s", v.serialize().c_str());
        for(auto it : v.as_array()){
            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.header.exchangeTypeEnum = ExchangeType_BINANCE;
            rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
            strcpy(rcmd.header.accountId, m_curcfg.accountId);
            strcpy(rcmd.header.strategyId, m_curcfg.strategyId);

            string originInstId = it.at("symbol").as_string();
            md::InstrumentInfo info;
            if(smc->get_instrument_info("BINANCE", "InstType_C_SWAP", originInstId.c_str(),info)) {
                rcmd.header.instTypeEnum = InstType_C_SWAP;
                strcpy(rcmd.body.position.instId, info.instId);
            }
            else if(smc->get_instrument_info("BINANCE", "InstType_C_FUTURES", originInstId.c_str(), info)) {
                rcmd.header.instTypeEnum = InstType_C_FUTURES;
                strcpy(rcmd.body.position.instId, info.instId);
            }
            else{
                //    LOG_ERROR("not found binance %s smc info", originInstId.c_str());
                continue;
            }
            if(it.at("adlQuantile").has_field("BOTH")){
                rcmd.body.position.adlQuantile = stod(it.at("adlQuantile").at("BOTH").serialize().c_str());;
                rcmd.body.position.apiSourceEnum = ApiSource_REST;
                PUSH_RCMD(rcmd)
                // string instId = rcmd.body.position.instId;
                // auto found = g_filterSymbolsMap.find(instId);
                // if(found != g_filterSymbolsMap.end()){
                //     PUSH_RCMD(rcmd)
                // }
            }
        }
    }
    else{
         LOG_ERROR("%s", response.extract_string().get().c_str());
    }

    return true;
}
catch(exception &e) {
    LOG_ERROR("%s", e.what());
    return false;
}

void BinanceCFTradingClient::add_new_order(pubsub::TCommand &tcmd){
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (!m_IsConnected){
        rcmd.body.newOrder.ErrorID = ERROR_TBDisconnectError;
        PUSH_RCMD(rcmd)
        return;
    }

    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.newOrder.instId,info)) {
            // http_client restclient(m_curcfg.restBaseUrl);
            http_request request(methods::POST);
            request.headers().add("X-MBX-APIKEY", m_curcfg.apiKey);
            uri_builder builder(m_ord_url);
            auto price = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice, info.tickSize);
            auto amount = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal, info.lotSize);
            rcmd.body.newOrder.volumeTotal = stod(amount);
            rcmd.body.newOrder.limitPrice = stod(price);
            builder.append_query("reduceOnly", tcmd.body.newOrder.reduceOnly ? "true" : "false");
            builder.append_query("recvWindow", 5000);
            builder.append_query("newClientOrderId", tcmd.body.newOrder.orderSysId);
            builder.append_query("symbol", info.originInstId);

            //open
            if(tcmd.body.newOrder.offsetFlag == OffsetFlag_OPEN){
                if (tcmd.body.newOrder.direction == Direction_LONG) {
                    builder.append_query("side", "BUY");
//                    builder.append_query("positionSide", "LONG");
                } else if (tcmd.body.newOrder.direction == Direction_SHORT) {
                    builder.append_query("side", "SELL");
//                    builder.append_query("positionSide", "SHORT");
                } else {
                    rcmd.body.newOrder.ErrorID = ERROR_DirectionError;
                    PUSH_RCMD(rcmd)
                    return;
                }
            }//close
            else if(tcmd.body.newOrder.offsetFlag == OffsetFlag_CLOSE){
                if (tcmd.body.newOrder.direction == Direction_LONG) {
                    builder.append_query("side", "SELL");
//                    builder.append_query("positionSide", "SHORT");
                } else if (tcmd.body.newOrder.direction == Direction_SHORT) {
                    builder.append_query("side", "BUY");
//                    builder.append_query("positionSide", "LONG");
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
                builder.append_query("type", "LIMIT");
                builder.append_query("timeInForce", "GTC");
                builder.append_query("price", price);
                builder.append_query("quantity", amount);
            }
            else if (tcmd.body.newOrder.orderType == OrderType_MARKET) {
                builder.append_query("type", "MARKET");
//                builder.append_query("timeInForce", "GTC");
//                builder.append_query("price", price);
                builder.append_query("quantity", amount);
            }
            else if (tcmd.body.newOrder.orderType == OrderType_POST_ONLY) {
                builder.append_query("type", "LIMIT");
                builder.append_query("timeInForce", "GTX");
                builder.append_query("price", price);
                builder.append_query("quantity", amount);
            }
            else if (tcmd.body.newOrder.orderType == OrderType_IOC) {
                builder.append_query("type", "LIMIT");
                builder.append_query("timeInForce", "IOC");
                builder.append_query("price", price);
                builder.append_query("quantity", amount);
            }
            else if (tcmd.body.newOrder.orderType == OrderType_FOK) {
                builder.append_query("type", "LIMIT");
                builder.append_query("timeInForce", "FOK");
                builder.append_query("price", price);
                builder.append_query("quantity", amount);
            }
            else {
                rcmd.body.newOrder.ErrorID = ERROR_OrderTypeError;
                PUSH_RCMD(rcmd)
                return;
            }

            //ack是最快，信息最少的
            builder.append_query("newOrderRespType", "ACK");
            builder.append_query("timestamp", crypto::getCurrentTimeMilli());
            auto signature = get_signature_rest(m_curcfg.apiSecret, builder.query());
            builder.append_query("signature", signature);
            request.set_request_uri(builder.to_string());
            LOG_DEBUG("%s:%s", __FUNCTION__, builder.to_string().c_str());
            rcmd.body.newOrder.tsSent = crypto::getCurrentTime();
            // restclient.request(request)
            http_response response = hotHttpClient->request(request);
            rcmd.body.newOrder.tsNet = crypto::getCurrentTime();
            auto code = response.status_code();
            LOG_DEBUG("add_new_order reponse status_code:%d", code);
            //200 400 429 401
            if(code == status_codes::OK || code == status_codes::BadRequest
                || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
                json::value const &v = response.extract_json().get();
                LOG_DEBUG("add_new_order reponse: %s", v.serialize().c_str());
                if (v.has_field("code")){
                    rcmd.body.newOrder.ErrorID = crypto::get_binance_errorid(v.at("code").as_integer());
                    strncpy(rcmd.body.newOrder.originMsg, v.at("msg").as_string().c_str(), sizeof(rcmd.body.newOrder.originMsg));
                    LOG_ERROR("%s", v.serialize().c_str());
                    PUSH_RCMD(rcmd)
                }
                else if(v.has_field("orderId")){
                    strcpy(rcmd.body.newOrder.orderId, v.at("orderId").serialize().c_str());
                    rcmd.body.newOrder.orderStatus =  OrderStatus_REST_NEW;
                    rcmd.body.newOrder.volumeTraded = stod(v.at("executedQty").as_string().c_str());
                    rcmd.body.newOrder.tradePrice = stod(v.at("avgPrice").as_string().c_str());

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
                }
                else{

                }
            }
            else if(code == status_codes::ServiceUnavailable){//503
                //"Service Unavailable."
                auto s = response.extract_string().get();
                if(crypto::has_str(s.c_str(),"Unavailable")){
                    rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED ;
                    rcmd.body.newOrder.ErrorID = ERROR_NetworkServiceUnavailableError;
                }
                //"Internal error; unable to process your request. Please try again."
                else if(crypto::has_str(s.c_str(),"Internal")){
                    rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED ;
                    rcmd.body.newOrder.ErrorID = ERROR_NetworkInternalError;
                } //"Unknown error, please check your request or try again later."
                else{//(crypto::has_str(s.c_str(),"Unknown"))
                    rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN ;
                    rcmd.body.newOrder.ErrorID = ERROR_NetworkUnknownError;
                }
                rapidjson::Document d;
                rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(s.c_str());
                if(rawData.HasMember("msg")){
                    strncpy(rcmd.body.newOrder.originMsg, rawData["msg"].GetString(), sizeof(rcmd.body.newOrder.originMsg));
                }
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
            LOG_ERROR("not found BINANCE.%s.%s smc info", InstTypeEnum2StrMap[tcmd.header.instTypeEnum].c_str(), tcmd.body.newOrder.instId);
            PUSH_RCMD(rcmd)
            return;
        }
    }
    catch(exception &e) {
        rcmd.body.newOrder.ErrorID = ERROR_NetworkError;
        rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN;
        strncpy(rcmd.body.newOrder.originMsg, e.what(), sizeof(rcmd.body.newOrder.originMsg));
        LOG_ERROR("%s", e.what());
        PUSH_RCMD(rcmd)
    }
}

void BinanceCFTradingClient::cancel_order(pubsub::TCommand &tcmd){
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)
    try {
        md::InstrumentInfo info;
        if (smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.cancelOrder.instId, info)){
            // http_client restclient(m_curcfg.restBaseUrl);
            http_request request(methods::DEL);
            request.headers().add("X-MBX-APIKEY", m_curcfg.apiKey);
            if(tcmd.body.cancelOrder.cancelOrderTypeEnum == pubsub::COT_ONE_INST){
                uri_builder builder(m_ord_url);
                builder.append_query("symbol", info.originInstId);
                if(crypto::str_cmp(tcmd.body.cancelOrder.orderId, "") == false){
                    builder.append_query("orderId", tcmd.body.cancelOrder.orderId);
                }
                else if(tcmd.body.cancelOrder.clientOrderId != 0){
                    builder.append_query("origClientOrderId", tcmd.body.cancelOrder.orderSysId);
                }
                else{
                    LOG_ERROR("cancel order need orderId or clientOrderId");
                    strcpy(rcmd.body.cancelOrder.originMsg, "cancel order need orderId or clientOrderId");
                    rcmd.body.cancelOrder.ErrorID = ERROR_NoOrderIdError;
                    rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                    PUSH_RCMD(rcmd)
                    return;
                }

                builder.append_query("recvWindow", 5000);
                builder.append_query("timestamp", crypto::getCurrentTimeMilli());
                auto signature = get_signature_rest(m_curcfg.apiSecret, builder.query());
                builder.append_query("signature", signature);
                //std::cout << builder.query() << "\n";
                LOG_DEBUG(" cancel order: '%s' ", builder.to_string().c_str());
                // rcmd.body.cancelOrder.tsSent = crypto::getCurrentTime();
                request.set_request_uri(builder.to_string());
                // restclient.request(request)
                http_response response = hotHttpClient->request(request);
                rcmd.body.cancelOrder.tsNet = crypto::getCurrentTime();
                LOG_DEBUG("BINANCE,%s,internetDelay,%ld", __FUNCTION__, rcmd.body.cancelOrder.tsNet - rcmd.body.cancelOrder.tsSent);

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
                    if (rawData.HasMember("code")){
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                        rcmd.body.cancelOrder.ErrorID = crypto::get_binance_errorid(stoi(rawData["code"].GetString()));
                        if(rcmd.body.cancelOrder.ErrorID == ERROR_OrderNotFoundError){
                            rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                        }
                        strncpy(rcmd.body.cancelOrder.originMsg, rawData["msg"].GetString(), ORIGINMSG_SIZE);
                    }
                    else{
                        strcpy(rcmd.body.cancelOrder.orderId, rawData["orderId"].GetString() );
                        // strcpy(rcmd.body.cancelOrder.orderSysId, v.at("clientOrderId").as_string().c_str());
                        rcmd.body.cancelOrder.volumeTraded = stod(rawData["executedQty"].GetString());
                        rcmd.body.cancelOrder.tradePrice   = stod(rawData["avgPrice"].GetString());
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_CANCELED;
                    }
                    PUSH_RCMD(rcmd)
                }
                else{
                    const string &s = response.extract_string().get();
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
        strncpy(rcmd.body.cancelOrder.originMsg, e.what(), sizeof(rcmd.body.newOrder.originMsg));
        LOG_ERROR("%s", e.what());
        PUSH_RCMD(rcmd)
    }
}

void BinanceCFTradingClient::query_multi_orders(pubsub::TCommand &cmd){
    #if 0
    try {
        string instType = "SWAP";
        if(cmd.header.instTypeEnum == InstType_C_FUTURES){
            instType = "FUTURES";
        }
        md::InstrumentInfo info;
        if(smc->get_instrument_info("BINANCE",instType.c_str(),cmd.body.queryOrder.instId, info)){
//            auto ord = cmd.body.queryOrder;
            auto url = m_queryord_url[1];
            http_client restclient(m_curcfg.restBaseUrl);
            http_request request(methods::GET);
            request.headers().add("X-MBX-APIKEY",m_curcfg.apiKey);
            uri_builder builder(url);
            builder.append_query("symbol",info.originInstId);

            builder.append_query("recvWindow",5000);
            builder.append_query("timestamp",crypto::getCurrentTimeMilli());
            auto signature = get_signature_rest(m_curcfg.apiSecret, builder.query());
            builder.append_query("signature",signature);
            LOG_DEBUG("query_multi_orders: '%s' ",builder.to_string().c_str());
            request.set_request_uri(builder.to_string());
            restclient.request(request)
            .then([](http_response response) -> pplx::task<json::value> {
                auto code = response.status_code();
                if(code == status_codes::OK || code == status_codes::BadRequest
                    /*|| code == status_codes::TooManyRequests || code == status_codes::Unauthorized*/)
                    return response.extract_json();
                return pplx::task_from_result(json::value());
            })
            .then([&](pplx::task<json::value> previousTask) {
                json::value const & data = previousTask.get();
                LOG_DEBUG("query_multi_orders reponse:%s", data.serialize().c_str());
                if(data.is_array()){
                    for(auto &v : data.as_array()){
                        pubsub::RCommand rcmd;
                        memset(&rcmd,0,sizeof(pubsub::RCommand));
                        rcmd.header.exchangeTypeEnum = cmd.header.exchangeTypeEnum;
                        rcmd.header.instTypeEnum = cmd.header.instTypeEnum;
                        rcmd.header.insertTime = crypto::getCurrentTime();
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        strcpy(rcmd.body.orderResponse.instId, info.instId);
                        rcmd.body.orderResponse.ErrorID = ERROR_NoError;
                        strcpy(rcmd.body.orderResponse.orderId, v.at("orderId").serialize().c_str());
                        strcpy(rcmd.body.orderResponse.clientOrderId, v.at("clientOrderId").as_string().c_str());

                        rcmd.body.orderResponse.offsetFlag = OffsetFlag_OPEN;

                        string side = v.at("side").as_string();
                        rcmd.body.orderResponse.direction = side[0] == 'B' ? Direction_LONG : Direction_SHORT ;

                        rcmd.body.orderResponse.volumeTotal  = stod(v.at("origQty").as_string().c_str());
                        rcmd.body.orderResponse.limitPrice   = stod(v.at("price").as_string().c_str());
                        rcmd.body.orderResponse.volumeTraded = stod(v.at("executedQty").as_string().c_str()) ;

                        string status = v.at("status").as_string();
                        rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(status);
                        rcmd.body.orderResponse.reduceOnly = v.at("reduceOnly").as_bool();
                        string timeInForce = v.at("timeInForce").as_string();
                        string orderType = v.at("type").as_string(); // 订单类型
                        rcmd.body.orderResponse.orderType = crypto::get_binance_ordertype(timeInForce.c_str(), orderType.c_str());

                        rcmd.body.orderResponse.insertTime = stol(v.at("time").serialize().c_str()) * 1000;
                        rcmd.body.orderResponse.updateTime = stol(v.at("updateTime").serialize().c_str()) * 1000;
                        rcmd.body.orderResponse.tsParse = crypto::getCurrentTime();
//                        LOG_INFO("%s", rcmd.getString().c_str());
                        PUSH_RCMD(rcmd)
                    }
                }
                else{
                    LOG_ERROR("%s", data.serialize().c_str());
                }
            })
            .wait();
        }
        else{
            LOG_ERROR("not found Binance %s smc info",cmd.body.queryOrder.instId);
        }
    }
    catch(exception &e) {
        LOG_ERROR("%s", e.what());
    }
    #endif
}

void BinanceCFTradingClient::query_one_order(pubsub::TCommand &tcmd){
    QUERY_ORDER_TCMD_2_RCMD(tcmd)
    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.queryOrder.instId, info)){
            auto ord = tcmd.body.queryOrder;
            auto url = m_queryord_url[0];
            http_client restclient(m_curcfg.restBaseUrl);
            http_request request(methods::GET);
            request.headers().add("X-MBX-APIKEY",m_curcfg.apiKey);
            uri_builder builder(url);
            builder.append_query("symbol",info.originInstId);

            if(crypto::str_cmp(tcmd.body.queryOrder.orderId, "") == false){
                builder.append_query("orderId", ord.orderId);
            }
            else if( tcmd.body.queryOrder.clientOrderId != 0){
                builder.append_query("origClientOrderId", ord.orderSysId);
            }
            else{
                LOG_ERROR("query_one_order need orderId or clientOrderId" );
                return;
            }

            builder.append_query("recvWindow",5000);
            builder.append_query("timestamp",crypto::getCurrentTimeMilli());
            auto signature = get_signature_rest(m_curcfg.apiSecret, builder.query());
            builder.append_query("signature",signature);
            LOG_DEBUG("query_one_order: '%s' ",builder.to_string().c_str());
            request.set_request_uri(builder.to_string());
            // restclient.request(request)
            http_response response = hotHttpClient->request(request);
            auto code = response.status_code();
            if(code == status_codes::OK || code == status_codes::BadRequest
                || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
                json::value const & v = response.extract_json().get();
                LOG_DEBUG("query_one_order reponse:%s", v.serialize().c_str());
                if (v.has_field("code")) {
                    rcmd.body.queryOrder.ErrorID = crypto::get_binance_errorid(v.at("code").as_integer());
                    rcmd.body.queryOrder.orderStatus = OrderStatus_REJECTED;
                    strncpy(rcmd.body.queryOrder.originMsg, v.serialize().c_str(), sizeof(rcmd.body.queryOrder.originMsg));
                    PUSH_RCMD(rcmd)
                }
                else if(v.has_field("orderId")){
                    rcmd.body.queryOrder.ErrorID = ERROR_NoError;
                    strcpy(rcmd.body.queryOrder.orderId, v.at("orderId").serialize().c_str());
                    strcpy(rcmd.body.queryOrder.orderSysId, v.at("clientOrderId").as_string().c_str());

                    // rcmd.body.orderResponse.offsetFlag = OffsetFlag_OPEN;
                    // string side = v.at("side").as_string();
                    // rcmd.body.orderResponse.direction = side[0] == 'B' ? Direction_LONG : Direction_SHORT ;
                    rcmd.body.queryOrder.volumeTotal = stod(v.at("origQty").as_string().c_str());
                    rcmd.body.queryOrder.limitPrice = stod(v.at("price").as_string().c_str());
                    rcmd.body.queryOrder.volumeTraded = stod(v.at("executedQty").as_string().c_str());
                    rcmd.body.queryOrder.tradePrice = stod(v.at("avgPrice").as_string().c_str());

                    string status = v.at("status").as_string();
                    rcmd.body.queryOrder.orderStatus = crypto::get_binance_orderstatus(status);
                    // rcmd.body.orderResponse.reduceOnly = v.at("reduceOnly").as_bool();
                    // string timeInForce = v.at("timeInForce").as_string();
                    // string orderType = v.at("type").as_string(); //订单类型
                    // rcmd.body.orderResponse.orderType = crypto::get_binance_ordertype(timeInForce.c_str(),
                    //                                                                   orderType.c_str());
                    // rcmd.body.orderResponse.insertTime = stol(v.at("time").serialize().c_str()) * 1000;
                    // rcmd.body.orderResponse.updateTime = stol(v.at("updateTime").serialize().c_str()) * 1000;
                    // rcmd.body.orderResponse.tsParse = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                }
            }
            else{
                auto s = response.extract_string().get();
                rcmd.body.queryOrder.ErrorID = code;
                rcmd.body.queryOrder.orderStatus = OrderStatus_UNKNOWN;
                // strncpy(rcmd.body.queryOrder.originMsg, s.c_str(), sizeof(rcmd.body.queryOrder.originMsg));
                LOG_ERROR("query_one_order:code:%d,response:%s", code, s.c_str());
                PUSH_RCMD(rcmd)
            }
        }
        else{
            LOG_ERROR("not found Binance %s smc info", tcmd.body.queryOrder.instId);
        }
    }
    catch(exception &e) {
        LOG_ERROR("%s", e.what());
    }
}

void BinanceCFTradingClient::query_order(pubsub::TCommand &tcmd){
    if (tcmd.body.queryOrder.queryOrderTypeEnum == QOT_ONE_INST){
        query_one_order(tcmd);
    }
    else if (tcmd.body.queryOrder.queryOrderTypeEnum == QOT_MULTI_INST){
        // query_multi_orders(tcmd);
    }
    else{
        // rcmd.body.orderResponse.ErrorID = ERROR_CancelOrQueryTypeError;
        // LOG_ERROR("query_order got unknown queryOrderType:%d", cmd.body.queryOrder.queryOrderTypeEnum);
        // g_rptInnerQueue.push(rcmd);
    }
}