#include "binance/BinanceUnifiedTradingClient.h"

BinanceUnifiedTradingClient::BinanceUnifiedTradingClient(){

}

BinanceUnifiedTradingClient::~BinanceUnifiedTradingClient(){
//    delete smc;
}

bool BinanceUnifiedTradingClient::Initialize(AccountCfg& cfg, sm::SecurityManager *smc){
    this->smc = smc;
    //static values
    m_wssb_url = "/ws/";
    m_ord_url = "";
    //m_cxt_url = "/api/v3/order";
//    m_trans_url = "/sapi/v1/asset/transfer";
//    m_wsLiskey_url = "/api/v3/userDataStream";
    m_wsLiskey_url = "/papi/v1/listenKey";
    m_queryord_url[0] = "/fapi/v1/order";
    m_queryord_url[1] = "/fapi/v1/openOrders";
    m_queryord_url[2] = "/fapi/v1/allOrders";
//
    m_balanceUrl = "/papi/v1/balance";
    m_accountUrl = "/papi/v1/account";
    m_positionUrl = "";
    m_adlquantileUrl = "";
//    m_orderUrl = "/api/v4/spot/orders";
//    m_cancelOrderUrl = "/spot/orders/";
    m_curcfg = cfg;
    // addNewOrderRestClient = new http_client(m_curcfg.restBaseUrl);
    // cancelOrderRestClient = new http_client(m_curcfg.restBaseUrl);
    // hotHttpClient = new http_client(m_curcfg.restBaseUrl);
    hotHttpClient = new crypto::RestClientCPP(m_curcfg.restBaseUrl.c_str());
    return true;
}

void BinanceUnifiedTradingClient::Run() {
    std::thread monitorThread(&BinanceUnifiedTradingClient::monitor, this);
    monitorThread.detach();
}

void BinanceUnifiedTradingClient::sub_websocket(){
    try{
        uri_builder builder(m_curcfg.wsBaseUrl);
        builder.append_path(m_wssb_url.to_string());
        builder.append_path(m_listenkey);
        wsClient.close();
        wsClient.connect(builder.to_string())
        .then([&]() {
            std::function<void (const websocket_incoming_message &msg)> f;
            f = std::bind(&BinanceUnifiedTradingClient::on_websocket_msg, this, placeholders::_1);
            wsClient.set_message_handler(f);
            std::function<void (websocket_close_status close_status,
                                const utility::string_t& reason,
                                const std::error_code& error)> c;
            c =  std::bind(&BinanceUnifiedTradingClient::on_close_msg,this
                    ,placeholders::_1,placeholders::_2,placeholders::_3);
            wsClient.set_close_handler(c);
        })
        .wait();
        m_IsConnected = true;
        LOG_INFO("connected with binance usdt futures api");
    }
    catch(exception &e){
        m_IsConnected = false;
        LOG_ERROR("%s", e.what());
    }
}

void BinanceUnifiedTradingClient::monitor() {
    while(1){
        try{
            static auto tk = crypto::getCurrentTimeSeconds();
            LOG_INFO("start to connect with binance usdt futures api");
            gen_listen_key();
            sub_websocket();
            sleep(3);
            while(m_IsConnected){
                sleep(10);
                if(!m_IsConnected){
                    LOG_ERROR("binance usdt futures ws disconnected, will reconnect it now");
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
        sleep(2);
    }
}

void BinanceUnifiedTradingClient::on_websocket_msg(const websocket_incoming_message& msg){
    try{
        // text_message, binary_message, close, ping, pong
        if (msg.message_type() == websocket_message_type::text_message){
            msg.extract_string().then([&](const string s){
                rapidjson::Document d;
                rapidjson::Value &rawData = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(s.c_str());
                if(d.HasParseError() || !rawData.IsObject() || !rawData.HasMember("e")){
                    return;
                }
                const string &e = rawData["e"].GetString();
                LOG_DEBUG("on_websocket_msg:%s", s.c_str());
                if(crypto::str_cmp(e.c_str(),"outboundAccountPosition") ){//e[0] == 'o' && e[4] == 'o' 杠杆账户更新
                    const rapidjson::Value &data = rawData["B"];
                    for(rapidjson::SizeType i = 0; i < data.Size(); i++){
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.header.cmdTime = crypto::getCurrentTime();
                        rcmd.header.exchangeTypeEnum = ExchangeType_BINANCE;
                        rcmd.header.instTypeEnum = InstType_SPOT;
                        rcmd.header.cmdTypeEnum  = pubsub::CMD_RPT_BALANCE;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        // cmd.body.balance.updateTime = stoll(rawData["E"].GetString())*1000;
                        strcpy(rcmd.body.balance.currency, data[i]["a"].GetString());
                        rcmd.body.balance.available = stod(data[i]["f"].GetString());
                        rcmd.body.balance.frozen = stod(data[i]["l"].GetString()) ;
                        rcmd.body.balance.total = rcmd.body.balance.available + rcmd.body.balance.frozen;
                        rcmd.body.balance.apiSourceEnum = ApiSource_WEBSOCKET;
                        PUSH_RCMD(rcmd)
                    }
                } else if(e[0] == 'A'){ // ACCOUNT_UPDATE 合约balance position
                    const string &fs = rawData["fs"].GetString();
                    const rapidjson::Value &data = rawData["a"]["B"];
                    for(rapidjson::SizeType i = 0; i < data.Size(); i++){
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.header.exchangeTypeEnum = ExchangeType_BINANCE;

                        if (crypto::str_cmp(fs, "UM")) {
                            rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                        } else if (crypto::str_cmp(fs, "CM")) {
                            rcmd.header.instTypeEnum = InstType_C_SWAP;
                        }
                        
                        rcmd.header.cmdTypeEnum  = pubsub::CMD_RPT_BALANCE;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        strcpy(rcmd.body.balance.currency, data[i]["a"].GetString());
                        rcmd.body.balance.available = stod(data[i]["cw"].GetString());
                        rcmd.body.balance.frozen = stod(data[i]["wb"].GetString()) - rcmd.body.balance.available;
                        rcmd.body.balance.total = rcmd.body.balance.available + rcmd.body.balance.frozen;
                        rcmd.body.balance.apiSourceEnum = ApiSource_WEBSOCKET;
                        PUSH_RCMD(rcmd)
                    }
                    const rapidjson::Value &pData = rawData["a"]["P"];
                    for(rapidjson::SizeType i = 0; i < pData.Size(); i++){
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.header.exchangeTypeEnum = ExchangeType_BINANCE;
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        // rcmd.body.position.updateTime = stoll(rawData["E"].GetString())*1000;//crypto::getCurrentTime();
                        double positionAmt = stod(pData[i]["pa"].GetString());
                        const string &originInstId = pData[i]["s"].GetString();
                        const string &positionSide = pData[i]["ps"].GetString();
                        if(positionSide[0] == 'B'){//单向模式
//                            cmd.body.position.direction = positionSide[0] == 'B' ? Direction_NET : (side[0] == 'L' ?  Direction_LONG : Direction_SHORT);
                            rcmd.body.position.direction = positionAmt >= 0 ? Direction_LONG : Direction_SHORT;
                            md::InstrumentInfo info;

                            if (crypto::str_cmp(fs, "UM")) {
                                if(smc->get_instrument_info("BINANCE", "InstType_USDT_SWAP", originInstId.c_str(), info)) {
                                    rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                                    strcpy(rcmd.body.position.instId, info.instId);
                                }
                                else if(smc->get_instrument_info("BINANCE", "InstType_USDT_FUTURES", originInstId.c_str(), info)) {
                                    rcmd.header.instTypeEnum = InstType_USDT_FUTURES;
                                    strcpy(rcmd.body.position.instId, info.instId);
                                }
                                else{
                                    LOG_ERROR("not found binance %s smc info", originInstId.c_str());
                                    continue;
                                }
                            } else if (crypto::str_cmp(fs, "CM")) {
                                if(smc->get_instrument_info("BINANCE", "InstType_C_SWAP", originInstId.c_str(), info)) {
                                    rcmd.header.instTypeEnum = InstType_C_SWAP;
                                    strcpy(rcmd.body.position.instId, info.instId);
                                }
                                else if(smc->get_instrument_info("BINANCE", "InstType_C_FUTURES", originInstId.c_str(), info)) {
                                    rcmd.header.instTypeEnum = InstType_C_FUTURES;
                                    strcpy(rcmd.body.position.instId, info.instId);
                                }
                                else{
                                    LOG_ERROR("not found binance %s smc info", originInstId.c_str());
                                    continue;
                                }
                            }

                            positionAmt = positionAmt >= 0 ? positionAmt : -positionAmt;
                            rcmd.body.position.volume = positionAmt*info.magnifyNumber;
                            rcmd.body.position.maintMargin = stod(pData[i]["iw"].GetString());
                            rcmd.body.position.avgPrice = stod(pData[i]["ep"].GetString())*info.reduceNumber;
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
                            LOG_ERROR("not support 双向模式 now:%s",s.c_str());
                        }
                    }
                }
                else if(crypto::str_cmp(e.c_str(),"executionReport")){ // 杠杆账户订单
                    string originInstId = rawData["s"].GetString();
                    md::InstrumentInfo info;
                    if(this->smc->get_instrument_info("BINANCE", "InstType_SPOT", originInstId.c_str(), info)) {
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.header.exchangeTypeEnum = ExchangeType_BINANCE;
                        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                        rcmd.header.instTypeEnum = InstType_SPOT;
                        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                        strcpy(rcmd.header.accountId, m_curcfg.accountId);
                        strcpy(rcmd.body.orderResponse.orderId, rawData["i"].GetString());
                        //TODO 大写C是撤单的clientOrderId，小写的报单的 clientOrderId
                        //报单的时候C 为空，撤单不为空，是用户自定义订单
                        if(crypto::str_cmp(rawData["C"].GetString(),"")){
                            strcpy(rcmd.body.orderResponse.orderSysId, rawData["c"].GetString());
                        }
                        else{// 原始订单自定义ID(原始订单，指撤单操作的对象。撤单本身被视为另一个订单)
                            strcpy(rcmd.body.orderResponse.orderSysId, rawData["C"].GetString());
                        }
                        //已经成交量
                        rcmd.body.orderResponse.volumeTraded = stod(rawData["z"].GetString());
                        //成交均价
                        if(rcmd.body.orderResponse.volumeTraded > 0){
                            rcmd.body.orderResponse.tradePrice = stod(rawData["Z"].GetString())
                                                                / rcmd.body.orderResponse.volumeTraded;
                        }
                        //原始订单数量
                        rcmd.body.orderResponse.volumeTotal  = stod(rawData["q"].GetString());
                        //原始订单价格
                        rcmd.body.orderResponse.limitPrice   = stod(rawData["p"].GetString());
                        string status = rawData["X"].GetString();
                        rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(status);
                        rcmd.body.orderResponse.apiSourceEnum = ApiSource_WEBSOCKET;
                        PUSH_RCMD(rcmd)
                    }
                }
                // else if(crypto::str_cmp(e.c_str(), "ORDER_TRADE_UPDATE")){//e[0] == 'A'
                else if(e[0] == 'O'){  // ORDER_TRADE_UPDATE 合约订单
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.header.cmdTime = crypto::getCurrentTime();
                    rcmd.header.exchangeTypeEnum = ExchangeType_BINANCE;
                    rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                    strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
                    strcpy(rcmd.header.accountId, m_curcfg.accountId);
                    strcpy(rcmd.body.orderResponse.orderId, rawData["o"]["i"].GetString());
                    strcpy(rcmd.body.orderResponse.orderSysId, rawData["o"]["c"].GetString());

                    const string &originInstId = rawData["o"]["s"].GetString();
                    md::InstrumentInfo info;

                    const string &fs = rawData["fs"].GetString();
                    if (crypto::str_cmp(fs, "UM")) {
                        if (this->smc->get_instrument_info("BINANCE", "InstType_USDT_SWAP", originInstId.c_str(), info)) {
                            rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                            strcpy(rcmd.body.orderResponse.instId, info.instId);
                        }
                        else if (this->smc->get_instrument_info("BINANCE", "InstType_USDT_FUTURES", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_USDT_FUTURES;
                            strcpy(rcmd.body.orderResponse.instId, info.instId);
                        }
                        else{
                            LOG_ERROR("not found binance %s smc info", originInstId.c_str());
                        }
                    } else if (crypto::str_cmp(fs, "CM")) {
                        if (this->smc->get_instrument_info("BINANCE", "InstType_C_SWAP", originInstId.c_str(), info)) {
                            rcmd.header.instTypeEnum = InstType_C_SWAP;
                            strcpy(rcmd.body.orderResponse.instId, info.instId);
                        }
                        else if (this->smc->get_instrument_info("BINANCE", "InstType_C_FUTURES", originInstId.c_str(), info)){
                            rcmd.header.instTypeEnum = InstType_C_FUTURES;
                            strcpy(rcmd.body.orderResponse.instId, info.instId);
                        }
                        else{
                            LOG_ERROR("not found binance %s smc info", originInstId.c_str());
                        }
                    } 

                    //已经成交量
                    rcmd.body.orderResponse.volumeTraded = stod(rawData["o"]["z"].GetString())*info.magnifyNumber;
                    //成交均价
                    rcmd.body.orderResponse.tradePrice = stod(rawData["o"]["ap"].GetString())*info.reduceNumber;
                    //原始订单数量
                    rcmd.body.orderResponse.volumeTotal  = stod(rawData["o"]["q"].GetString())*info.magnifyNumber;
                    //原始订单价格
                    rcmd.body.orderResponse.limitPrice   = stod(rawData["o"]["p"].GetString())*info.reduceNumber;

                    rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(rcmd.header.instTypeEnum, rawData);
                    rcmd.body.orderResponse.offsetFlag = OffsetFlag_OPEN;
                    const string &side = rawData["o"]["S"].GetString();
                    rcmd.body.orderResponse.direction = side[0] == 'B' ? Direction_LONG : Direction_SHORT;

                    const string &ps = rawData["o"]["ps"].GetString();
                    // STOP 止损限价单 STOP_MARKET 止损市价单
                    // TAKE_PROFIT 止盈限价单 TAKE_PROFIT_MARKET 止盈市价单 TRAILING_STOP_MARKET 跟踪止损单
                    const string &timeInForce = rawData["o"]["f"].GetString();
                    // string type = rawData["o"]["ot"].GetString();// 原始订单类型
                    const string &orderType = rawData["o"]["o"].GetString(); // 订单类型
                    rcmd.body.orderResponse.orderType = crypto::get_binance_ordertype(timeInForce.c_str(), orderType.c_str());
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
    catch(exception &e){
        m_IsConnected = false;
        LOG_ERROR("%s", e.what());
    }
}

bool BinanceUnifiedTradingClient::get_balances(pubsub::TCommand &tcmd)//vector<Balance> &balanceVec
try {
    // http_client restclient(m_curcfg.restBaseUrl);
    http_request request(methods::GET);
    // request.headers().add("Connection", "Keep-Alive");
    // request.headers().add("Keep-Alive", "timeout=60, max=100000");
    request.headers().add("X-MBX-APIKEY",m_curcfg.apiKey);
    uri_builder builder(m_balanceUrl);

    builder.append_query("recvWindow",5000);
    builder.append_query("timestamp",crypto::getCurrentTimeMilli());

    auto signature = get_signature_rest(m_curcfg.apiSecret, builder.query());
    builder.append_query("signature",signature);
    request.set_request_uri(builder.to_string());
    // restclient.request(request)
    const http_response &response = hotHttpClient->request(request);
    // const http_response &response = restclient.request(request).get();
    auto code = response.status_code();
    if(code == status_codes::OK || code == status_codes::BadRequest
        || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
        json::value const & v = response.extract_json().get();
        // LOG_DEBUG("%s,%s",__FUNCTION__, v.serialize().c_str());
        auto array = v.as_array();
        for(auto it : array){
            double total = stod(it.at("totalWalletBalance").as_string().c_str());

            if(total > -ZERO_NUM && total < ZERO_NUM){
                continue;
            }
            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            // cmd.header.cmdTime = crypto::getCurrentTime();
            rcmd.header.exchangeTypeEnum = ExchangeType_BINANCE;
            // TODO
            rcmd.header.instTypeEnum = InstType_SPOT;
            rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
            strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
            strcpy(rcmd.header.accountId, m_curcfg.accountId);
            string ccy = it.at("asset").as_string();
            ccy = crypto::to_upper(ccy.c_str());
            if(tcmd.header.cmdTypeEnum == CMD_QUERY_BALANCE){
                if(crypto::str_cmp(ccy.c_str(), tcmd.body.queryBalance.currency) == false){
                    continue;
                }
            }
            strcpy(rcmd.body.balance.currency, ccy.c_str());
            rcmd.body.balance.total = total;
            // rcmd.body.balance.unrealizedPnl = stod(it.at("unrealizedProfit").as_string().c_str());
            // rcmd.body.balance.available = stod(it.at("availableBalance").as_string().c_str());
            rcmd.body.balance.frozen = rcmd.body.balance.total - rcmd.body.balance.available;
            rcmd.body.balance.apiSourceEnum = ApiSource_REST;
            PUSH_RCMD(rcmd)
            if (crypto::str_cmp(rcmd.body.balance.currency, "USDT")){
                LOG_INFO("exchId:%s,instType:%s,accountId:%s,strategyId:%s,usdt available:%.2f,frozen:%.2f",
                    ExchangeTypeEnum2StrMap[rcmd.header.exchangeTypeEnum].c_str(),
                    InstTypeEnum2StrMap[rcmd.header.instTypeEnum].c_str(),
                    rcmd.header.accountId, rcmd.header.strategyId,
                    rcmd.body.balance.available, rcmd.body.balance.frozen);
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

bool BinanceUnifiedTradingClient::get_account(pubsub::TCommand &tcmd)//vector<Balance> &balanceVec
try {
    // http_client restclient(m_curcfg.restBaseUrl);
    http_request request(methods::GET);
    // request.headers().add("Connection", "Keep-Alive");
    // request.headers().add("Keep-Alive", "timeout=60, max=100000");
    request.headers().add("X-MBX-APIKEY",m_curcfg.apiKey);
    uri_builder builder(m_accountUrl);

    builder.append_query("recvWindow",5000);
    builder.append_query("timestamp",crypto::getCurrentTimeMilli());

    auto signature = get_signature_rest(m_curcfg.apiSecret, builder.query());
    builder.append_query("signature",signature);
    request.set_request_uri(builder.to_string());
    // restclient.request(request)
    const http_response &response = hotHttpClient->request(request);
    // const http_response &response = restclient.request(request).get();
    auto code = response.status_code();
    if(code == status_codes::OK || code == status_codes::BadRequest
        || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
        json::value const & v = response.extract_json().get();
        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.header.cmdTime = crypto::getCurrentTime();
        rcmd.header.exchangeTypeEnum = ExchangeType_BINANCE;
        rcmd.header.instTypeEnum = InstType_SPOT;
        rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
        strcpy(rcmd.header.strategyId, m_curcfg.strategyId);
        strcpy(rcmd.header.accountId, m_curcfg.accountId);
        rcmd.body.totalAccount.totalEquity = stod(v.at("accountEquity").as_string().c_str());
        rcmd.body.totalAccount.mmr = stod(v.at("accountMaintMargin").as_string().c_str());
        string mgnStr = v.at("uniMMR").as_string();
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
         LOG_ERROR("%s", response.extract_string().get().c_str());
    }
    return true;
}
catch(exception &e){
    LOG_ERROR("%s", e.what());
    return false;
}

bool BinanceUnifiedTradingClient::get_positions(pubsub::TCommand &tcmd)
try {
    if (tcmd.header.instTypeEnum == InstType_USDT_FUTURES || tcmd.header.instTypeEnum == InstType_USDT_SWAP) {
        m_positionUrl = "/papi/v1/um/positionRisk";
    } else if (tcmd.header.instTypeEnum == InstType_C_FUTURES || tcmd.header.instTypeEnum == InstType_C_SWAP) {
        m_positionUrl = "/papi/v1/cm/positionRisk";
    }
    // http_client restclient(m_curcfg.restBaseUrl);
    http_request request(methods::GET);
    // request.headers().add("Connection", "Keep-Alive");
    // request.headers().add("Keep-Alive", "timeout=60, max=100000");
    request.headers().add("X-MBX-APIKEY",m_curcfg.apiKey);
    uri_builder builder(m_positionUrl);

    builder.append_query("recvWindow", 5000);
    // builder.append_query("symbol",originInstId.c_str());
    if(tcmd.header.cmdTypeEnum == pubsub::CMD_QUERY_POSITION){
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.queryPosition.instId, info)) {
            builder.append_query("symbol", info.originInstId);//
        }
    }
    builder.append_query("timestamp",crypto::getCurrentTimeMilli());

    auto signature = get_signature_rest(m_curcfg.apiSecret, builder.query());
    builder.append_query("signature",signature);
    request.set_request_uri(builder.to_string());
    // restclient.request(request)
    const http_response &response = hotHttpClient->request(request);
    // const http_response &response = restclient.request(request).get();
    auto code = response.status_code();

    if(code == status_codes::OK || code == status_codes::BadRequest
        || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
        json::value const & v = response.extract_json().get();
        // LOG_DEBUG("get_positions:%s", v.to_string().c_str());
        for(auto &it : v.as_array()){
            double positionAmt = stod(it.at("positionAmt").as_string().c_str()) ;
            if(tcmd.header.cmdTypeEnum != pubsub::CMD_QUERY_POSITION){
                if(positionAmt > -ZERO_NUM && positionAmt < ZERO_NUM){
                    continue;
                }
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
                rcmd.body.position.direction = positionAmt >= 0 ? Direction_LONG : Direction_SHORT;
                md::InstrumentInfo info;
                if(smc->get_instrument_info("BINANCE", "InstType_USDT_SWAP", originInstId.c_str(), info)) {
                    rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                    strcpy(rcmd.body.position.instId, info.instId);
                }
                else if(smc->get_instrument_info("BINANCE", "InstType_USDT_FUTURES", originInstId.c_str(), info)) {
                    rcmd.header.instTypeEnum = InstType_USDT_FUTURES;
                    strcpy(rcmd.body.position.instId, info.instId);
                }
                else if(smc->get_instrument_info("BINANCE", "InstType_C_SWAP", originInstId.c_str(), info)) {
                    rcmd.header.instTypeEnum = InstType_C_SWAP;
                    strcpy(rcmd.body.position.instId, info.instId);
                }
                else if(smc->get_instrument_info("BINANCE", "InstType_C_FUTURES", originInstId.c_str(), info)) {
                    rcmd.header.instTypeEnum = InstType_C_FUTURES;
                    strcpy(rcmd.body.position.instId, info.instId);
                }
                else{
                    //LOG_ERROR("not found binance %s smc info", originInstId.c_str());
                    continue;
                }
                positionAmt = positionAmt >= 0 ? positionAmt : -positionAmt;
                rcmd.body.position.volume = positionAmt*info.magnifyNumber;
                // rcmd.body.position.maintMargin = stod(it.at("isolatedWallet").as_string().c_str());
                rcmd.body.position.avgPrice = stod(it.at("entryPrice").as_string().c_str())*info.reduceNumber;
                rcmd.body.position.unrealizedPnl = stod(it.at("unRealizedProfit").as_string().c_str());
                rcmd.body.position.markPrice = stod(it.at("markPrice").as_string().c_str())*info.reduceNumber;
                rcmd.body.position.liquidPrice = stod(it.at("liquidationPrice").as_string().c_str())*info.reduceNumber;
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
catch(exception &e) {
    LOG_ERROR("%s", e.what());
    return false;
}

bool BinanceUnifiedTradingClient::get_adlquantile(pubsub::TCommand &tcmd)
try {
    if (tcmd.header.instTypeEnum == InstType_USDT_FUTURES || tcmd.header.instTypeEnum == InstType_USDT_SWAP) {
        m_adlquantileUrl = "/papi/v1/um/adlQuantile";
    } else if (tcmd.header.instTypeEnum == InstType_C_FUTURES || tcmd.header.instTypeEnum == InstType_C_SWAP) {
        m_adlquantileUrl = "/papi/v1/cm/adlQuantile";
    }
    // http_client restclient(m_curcfg.restBaseUrl);
    http_request request(methods::GET);
    // request.headers().add("Connection", "Keep-Alive");
    // request.headers().add("Keep-Alive", "timeout=60, max=100000");
    request.headers().add("X-MBX-APIKEY", m_curcfg.apiKey);
    uri_builder builder(m_adlquantileUrl);

    builder.append_query("recvWindow", 5000);
    // builder.append_query("symbol",originInstId.c_str());
    builder.append_query("timestamp", crypto::getCurrentTimeMilli());

    auto signature = get_signature_rest(m_curcfg.apiSecret, builder.query());
    builder.append_query("signature",signature);
    request.set_request_uri(builder.to_string());
    // restclient.request(request)
    const http_response &response = hotHttpClient->request(request);
    // const http_response &response = restclient.request(request).get();
    auto code = response.status_code();
    if(code == status_codes::OK || code == status_codes::BadRequest
        || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
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
            if(smc->get_instrument_info("BINANCE", "InstType_USDT_SWAP", originInstId.c_str(),info)) {
                rcmd.header.instTypeEnum = InstType_USDT_SWAP;
                strcpy(rcmd.body.position.instId, info.instId);
            }
            else if(smc->get_instrument_info("BINANCE", "InstType_USDT_FUTURES", originInstId.c_str(),info)) {
                rcmd.header.instTypeEnum = InstType_USDT_FUTURES;
                strcpy(rcmd.body.position.instId, info.instId);
            }
            else if(smc->get_instrument_info("BINANCE", "InstType_C_SWAP", originInstId.c_str(),info)) {
                rcmd.header.instTypeEnum = InstType_C_SWAP;
                strcpy(rcmd.body.position.instId, info.instId);
            }
            else if(smc->get_instrument_info("BINANCE", "InstType_C_FUTURES", originInstId.c_str(),info)) {
                rcmd.header.instTypeEnum = InstType_C_FUTURES;
                strcpy(rcmd.body.position.instId, info.instId);
            }
            else{
                //    LOG_ERROR("not found binance %s smc info", originInstId.c_str());
                continue;
            }
            if(it.at("adlQuantile").has_field("BOTH")){
                rcmd.body.position.adlQuantile = stod(it.at("adlQuantile").at("BOTH").serialize().c_str()) + 1;;
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
catch(exception &e) {
    LOG_ERROR("%s", e.what());
    return false;
}

void BinanceUnifiedTradingClient::add_new_order(pubsub::TCommand &tcmd){
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (tcmd.header.instTypeEnum == InstType_SPOT) {
        m_ord_url = "/papi/v1/margin/order";
    }
    else if (tcmd.header.instTypeEnum == InstType_USDT_FUTURES || tcmd.header.instTypeEnum == InstType_USDT_SWAP) {
        m_ord_url = "/papi/v1/um/order";
    } else if (tcmd.header.instTypeEnum == InstType_C_FUTURES || tcmd.header.instTypeEnum == InstType_C_SWAP) {
        m_ord_url = "/papi/v1/cm/order";
    }

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
            // request.headers().add("Connection", "Keep-Alive");
            // request.headers().add("Keep-Alive", "timeout=60, max=100000");
            request.headers().add("X-MBX-APIKEY", m_curcfg.apiKey);
            uri_builder builder(m_ord_url);
            char price[32];
            char amount[32];
            if(tcmd.body.newOrder.reduceOnly == true){
                // sprintf(price,"%.12f", tcmd.body.newOrder.limitPrice);
                // sprintf(amount,"%.12f", tcmd.body.newOrder.volumeTotal);
                strcpy(price, crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice*info.magnifyNumber, info.tickSize*info.magnifyNumber).c_str());
                strcpy(amount, crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal*info.reduceNumber, info.lotSize*info.reduceNumber).c_str());
            }
            else{
                strcpy(price, crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice*info.magnifyNumber, info.tickSize*info.magnifyNumber).c_str());
                strcpy(amount, crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal*info.reduceNumber, info.lotSize*info.reduceNumber).c_str());
            }
            // auto price = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice, info.tickSize);
            // auto amount = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal, info.lotSize);

            rcmd.body.newOrder.volumeTotal = stod(amount)*info.magnifyNumber;
            rcmd.body.newOrder.limitPrice = stod(price)*info.reduceNumber;

            builder.append_query("reduceOnly", tcmd.body.newOrder.reduceOnly ? "true" : "false");
            builder.append_query("recvWindow", 5000);
            builder.append_query("newClientOrderId", tcmd.body.newOrder.orderSysId);
            builder.append_query("symbol", info.originInstId);

            //open
            // positionSide	ENUM NO	持仓方向，单向持仓模式下非必填，默认且仅可填BOTH;
            // 在双向持仓模式下必填,且仅可选择 LONG 或 SHORT
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
            //ack是最快，信息最少的,需要考虑ioc异步回复的问题
            if (tcmd.body.newOrder.orderType == OrderType_LIMIT) {
                builder.append_query("type", "LIMIT");
                builder.append_query("timeInForce", "GTC");
                builder.append_query("price", price);
                builder.append_query("quantity", amount);
                builder.append_query("newOrderRespType", "ACK");
            } else if (tcmd.body.newOrder.orderType == OrderType_MARKET) {
                builder.append_query("type", "MARKET");
                builder.append_query("quantity", amount);
                builder.append_query("newOrderRespType", "RESULT");
            } else if (tcmd.body.newOrder.orderType == OrderType_POST_ONLY) {
                builder.append_query("type", "LIMIT");
                builder.append_query("timeInForce", "GTX");
                builder.append_query("price", price);
                builder.append_query("quantity", amount);
                builder.append_query("newOrderRespType", "ACK");
            } else if (tcmd.body.newOrder.orderType == OrderType_IOC) {
                builder.append_query("type", "LIMIT");
                builder.append_query("timeInForce", "IOC");
                builder.append_query("price", price);
                builder.append_query("quantity", amount);
                builder.append_query("newOrderRespType", "RESULT");
            } else if (tcmd.body.newOrder.orderType == OrderType_FOK) {
                builder.append_query("type", "LIMIT");
                builder.append_query("timeInForce", "FOK");
                builder.append_query("price", price);
                builder.append_query("quantity", amount);
                builder.append_query("newOrderRespType", "RESULT");
            } else {
                rcmd.body.newOrder.ErrorID = ERROR_OrderTypeError;
                PUSH_RCMD(rcmd)
                return;
            }

            builder.append_query("timestamp", crypto::getCurrentTimeMilli());

            auto signature = get_signature_rest(m_curcfg.apiSecret, builder.query());
            builder.append_query("signature", signature);
            request.set_request_uri(builder.to_string());
            LOG_DEBUG("%s:%s", __FUNCTION__, builder.to_string().c_str());
#if 0
            hotHttpClient->hotHttpClient->request(request)
            .then([&](http_response response){
                rcmd.body.newOrder.tsNet = crypto::getCurrentTime();
                LOG_DEBUG("BINANCE,%s,internetDelay,%ld", __FUNCTION__, rcmd.body.newOrder.tsNet - rcmd.body.newOrder.tsSent);
                // cout << response.to_string() << endl;
                auto code = response.status_code();
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
                    if(rawData.HasMember("orderId")){
                        strcpy(rcmd.body.newOrder.orderId, rawData["orderId"].GetString());
                        // strcpy(rcmd.body.newOrder.orderSysId, v.at("clientOrderId").as_string().c_str());
                        rcmd.body.newOrder.volumeTraded = stod(rawData["executedQty"].GetString());//stod(v.at("executedQty").as_string().c_str());
                        rcmd.body.newOrder.tradePrice = stod(rawData["avgPrice"].GetString());//stod(v.at("avgPrice").as_string().c_str());
                        rcmd.body.newOrder.orderStatus = crypto::get_binance_orderstatus(tcmd.header.instTypeEnum, rawData);

                        PUSH_RCMD(rcmd)
                    }
                    else if(rawData.HasMember("code")) {
                        rcmd.body.newOrder.ErrorID = crypto::get_binance_errorid(stoi(rawData["code"].GetString()));
                        strncpy(rcmd.body.newOrder.originMsg, rawData["msg"].GetString(), sizeof(rcmd.body.newOrder.originMsg));
                        rcmd.body.newOrder.orderStatus =  OrderStatus_REJECTED;
                        PUSH_RCMD(rcmd)
                    }
                    else{
                        //这里为空，不用处理。肯定是上面网络错误了，已经有返回。
                        DEBUGLOG
                    }
                }
                else if(code == status_codes::ServiceUnavailable){//503
                    //"Service Unavailable."
                    const string &s = response.extract_string().get();
                    if(crypto::has_str(s.c_str(), "Unavailable")){//-5032
                        rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED ;
                        rcmd.body.newOrder.ErrorID = ERROR_NetworkServiceUnavailableError;
                    }
                    //{"code":-1001,"msg":"Internal error; unable to process your request. Please try again."}
                    else if(crypto::has_str(s.c_str(), "Internal")){//-5033
                        rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED ;
                        rcmd.body.newOrder.ErrorID = ERROR_NetworkInternalError;
                    }
                    //"Unknown error, please check your request or try again later."
                    else{//-5031
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
                    const string &s = response.extract_string().get();
                    rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED;
                    rcmd.body.newOrder.ErrorID = code;
                    // strncpy(rcmd.body.newOrder.originMsg, s.c_str(), sizeof(rcmd.body.newOrder.originMsg));
                    LOG_ERROR("%s", s.c_str());
                    PUSH_RCMD(rcmd)
                }
            });
#endif
            const http_response &response = hotHttpClient->request(request);
            if(tcmd.body.newOrder.clientOrderId == TESTCLIENTORDERID){
                return;
            }
            rcmd.body.newOrder.tsNet = crypto::getCurrentTime();
            LOG_DEBUG("BINANCE,%s,internetDelay,%ld", __FUNCTION__, rcmd.body.newOrder.tsNet - rcmd.body.newOrder.tsSent);
            // cout << response.to_string() << endl;
            auto code = response.status_code();
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
                if(rawData.HasMember("orderId")){
                    strcpy(rcmd.body.newOrder.orderId, rawData["orderId"].GetString());
                    // strcpy(rcmd.body.newOrder.orderSysId, v.at("clientOrderId").as_string().c_str());
                    rcmd.body.newOrder.volumeTraded = stod(rawData["executedQty"].GetString());//stod(v.at("executedQty").as_string().c_str());
                    if (tcmd.header.instTypeEnum == InstType_SPOT) {
                        double cummulativeQuoteQty = stod(rawData["cummulativeQuoteQty"].GetString());
                        if (rcmd.body.newOrder.volumeTraded > 0) {
                            rcmd.body.newOrder.tradePrice = cummulativeQuoteQty / rcmd.body.newOrder.volumeTraded;
                        }
                    } else {
                        rcmd.body.newOrder.tradePrice = stod(rawData["avgPrice"].GetString())*info.reduceNumber;//stod(v.at("avgPrice").as_string().c_str());
                    }
                    rcmd.body.newOrder.orderStatus = crypto::get_binance_orderstatus(tcmd.header.instTypeEnum, rawData);

                    PUSH_RCMD(rcmd)
                    return;
                }
                else if(rawData.HasMember("code")) {
                    rcmd.body.newOrder.ErrorID = crypto::get_binance_errorid(stoi(rawData["code"].GetString()));
                    strncpy(rcmd.body.newOrder.originMsg, rawData["msg"].GetString(), sizeof(rcmd.body.newOrder.originMsg));
                    rcmd.body.newOrder.orderStatus =  OrderStatus_REJECTED;
                    PUSH_RCMD(rcmd)
                    return;
                }
                else{
                    //这里为空，不用处理。肯定是上面网络错误了，已经有返回。
                    DEBUGLOG
                }
            }
            else if(code == status_codes::ServiceUnavailable){//503
                //"Service Unavailable."
                const string &s = response.extract_string().get();
                if(crypto::has_str(s.c_str(), "Unavailable")){//-5032
                    rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED ;
                    rcmd.body.newOrder.ErrorID = ERROR_NetworkServiceUnavailableError;
                }
                //{"code":-1001,"msg":"Internal error; unable to process your request. Please try again."}
                else if(crypto::has_str(s.c_str(), "Internal")){//-5033
                    rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED ;
                    rcmd.body.newOrder.ErrorID = ERROR_NetworkInternalError;
                }
                //"Unknown error, please check your request or try again later."
                else{//-5031
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
                return;
            }
            else{
                const string &s = response.extract_string().get();
                rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED;
                rcmd.body.newOrder.ErrorID = code;
                // strncpy(rcmd.body.newOrder.originMsg, s.c_str(), sizeof(rcmd.body.newOrder.originMsg));
                LOG_ERROR("%s", s.c_str());
                PUSH_RCMD(rcmd)
                return;
            }
#if 0
            hotHttpClient->request(request)
            .then([&](http_response response) -> pplx::task<json::value> {
                rcmd.body.newOrder.tsNet = crypto::getCurrentTime();
                auto code = response.status_code();
                LOG_DEBUG("add_new_order reponse status_code:%d", code);
                //200 400 429 401
                if(code == status_codes::OK || code == status_codes::BadRequest
                    || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
                    return response.extract_json();
                }
                else if(code == status_codes::ServiceUnavailable){//503
                    //"Service Unavailable."
                    auto s = response.extract_string().get();
                    if(crypto::has_str(s.c_str(),"Unavailable")){//-5032
                        rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED ;
                        rcmd.body.newOrder.ErrorID = ERROR_NetworkServiceUnavailableError;
                    }
                    //{"code":-1001,"msg":"Internal error; unable to process your request. Please try again."}
                    else if(crypto::has_str(s.c_str(),"Internal")){//-5033
                        rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED ;
                        rcmd.body.newOrder.ErrorID = ERROR_NetworkInternalError;
                    }
                    //"Unknown error, please check your request or try again later."
                    else{//-5031
                        rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN ;
                        rcmd.body.newOrder.ErrorID = ERROR_NetworkUnknownError;
                    }
                    strncpy(rcmd.body.newOrder.originMsg, s.c_str(), sizeof(rcmd.body.newOrder.originMsg));
                    LOG_ERROR("%s", s.c_str());
                    PUSH_RCMD(rcmd)
                    return pplx::task_from_result(json::value());
                }
                else{
                    auto s = response.extract_string().get();
                    rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED;
                    rcmd.body.newOrder.ErrorID = code;
                    strncpy(rcmd.body.newOrder.originMsg, s.c_str(), sizeof(rcmd.body.newOrder.originMsg));
                    LOG_ERROR("%s", s.c_str());
                    PUSH_RCMD(rcmd)
                    return pplx::task_from_result(json::value());
                }
            })// continue when the JSON value is available
            .then([&](pplx::task<json::value> previousTask) {
                json::value const &v = previousTask.get();
                LOG_DEBUG("add_new_order reponse: %s", v.serialize().c_str());
                if(v.has_field("orderId")){
                    strcpy(rcmd.body.newOrder.orderId, v.at("orderId").serialize().c_str());
                    // strcpy(rcmd.body.newOrder.orderSysId, v.at("clientOrderId").as_string().c_str());
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
                else if(v.has_field("code")) {
                    rcmd.body.newOrder.ErrorID = crypto::get_binance_errorid(v.at("code").as_integer());
                    strncpy(rcmd.body.newOrder.originMsg, v.at("msg").as_string().c_str(), sizeof(rcmd.body.newOrder.originMsg));
                    rcmd.body.newOrder.orderStatus =  OrderStatus_REJECTED;
                    PUSH_RCMD(rcmd)
                }
                else{
                    //这里为空，不用处理。肯定是上面网络错误了，已经有返回。
                }
            })
            .wait();
#endif
        }
        else{
            rcmd.body.newOrder.ErrorID = ERROR_SMCInstrumentNotExistError;
            LOG_ERROR("not found BINANCE.%s.%s smc info", InstTypeEnum2StrMap[tcmd.header.instTypeEnum].c_str(), tcmd.body.newOrder.instId);
            PUSH_RCMD(rcmd)
            return;
        }
    }
    catch(exception &e){
        rcmd.body.newOrder.ErrorID = ERROR_NetworkError;
        rcmd.body.newOrder.orderStatus = OrderStatus_UNKNOWN ;
        strncpy(rcmd.body.newOrder.originMsg, e.what(), sizeof(rcmd.body.newOrder.originMsg));
        LOG_ERROR("%s", e.what());
        PUSH_RCMD(rcmd)
        return;
    }
}


void BinanceUnifiedTradingClient::cancel_order(pubsub::TCommand &tcmd){
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    if (tcmd.header.instTypeEnum == InstType_SPOT) {
        m_ord_url = "/papi/v1/margin/order";
    }
    else if (tcmd.header.instTypeEnum == InstType_USDT_FUTURES || tcmd.header.instTypeEnum == InstType_USDT_SWAP) {
        m_ord_url = "/papi/v1/um/order";
    } else if (tcmd.header.instTypeEnum == InstType_C_FUTURES || tcmd.header.instTypeEnum == InstType_C_SWAP) {
        m_ord_url = "/papi/v1/cm/order";
    }

    // LOG_DEBUG("%s", tcmd.getString().c_str());
    try {
        md::InstrumentInfo info;
        if (smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.cancelOrder.instId, info)){
            // http_client restclient(m_curcfg.restBaseUrl);
            http_request request(methods::DEL);
            // request.headers().add("Connection", "Keep-Alive");
            // request.headers().add("Keep-Alive", "timeout=60, max=100000");
            request.headers().add("X-MBX-APIKEY", m_curcfg.apiKey);
            if(tcmd.body.cancelOrder.cancelOrderTypeEnum == pubsub::COT_ONE_INST){
                uri_builder builder(m_ord_url);
                builder.append_query("symbol", info.originInstId);

                if(tcmd.body.cancelOrder.clientOrderId != 0){
                    builder.append_query("origClientOrderId", tcmd.body.cancelOrder.orderSysId);
                }
                else if(crypto::str_cmp(tcmd.body.cancelOrder.orderId, "") == false){
                    builder.append_query("orderId", tcmd.body.cancelOrder.orderId);
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
                builder.append_query("timestamp",crypto::getCurrentTimeMilli());

                auto signature = get_signature_rest(m_curcfg.apiSecret, builder.query());
                builder.append_query("signature", signature);
                LOG_DEBUG("cancel order: %s ", builder.to_string().c_str());
                request.set_request_uri(builder.to_string());

                const http_response &response = hotHttpClient->request(request);
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
                        if(rcmd.body.cancelOrder.ErrorID == ERROR_OrderNotFoundError ||
                        rcmd.body.cancelOrder.ErrorID == ERROR_TooManyOrdersError ){
                            rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                        }
                        strncpy(rcmd.body.cancelOrder.originMsg, rawData["msg"].GetString(), ORIGINMSG_SIZE);
                    }
                    else{
                        strcpy(rcmd.body.cancelOrder.orderId, rawData["orderId"].GetString() );
                        // strcpy(rcmd.body.cancelOrder.orderSysId, v.at("clientOrderId").as_string().c_str());
                        rcmd.body.cancelOrder.volumeTraded = stod(rawData["executedQty"].GetString())*info.magnifyNumber;
                        if (tcmd.header.instTypeEnum == InstType_SPOT) {
                            double cummulativeQuoteQty = stod(rawData["cummulativeQuoteQty"].GetString());
                            if (rcmd.body.cancelOrder.volumeTraded > 0) {
                                rcmd.body.cancelOrder.tradePrice = cummulativeQuoteQty / rcmd.body.cancelOrder.volumeTraded;
                            }
                        } else {
                            rcmd.body.cancelOrder.tradePrice = stod(rawData["avgPrice"].GetString())*info.reduceNumber;//stod(v.at("avgPrice").as_string().c_str());
                        }
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
#if 0
                hotHttpClient->hotHttpClient->request(request)
                .then([&](http_response response){
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
                            if(rcmd.body.cancelOrder.ErrorID == ERROR_OrderNotFoundError ||
                            rcmd.body.cancelOrder.ErrorID == ERROR_TooManyOrdersError ){
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
                });

                hotHttpClient->request(request)
                .then([&](http_response response) -> pplx::task<json::value> {
                    rcmd.body.cancelOrder.tsNet = crypto::getCurrentTime();
                    auto code = response.status_code();
                    if (code == status_codes::OK || code == status_codes::BadRequest
                        || code == status_codes::TooManyRequests || code == status_codes::Unauthorized) {
                        return response.extract_json();
                    }
                    else{
                        auto s = response.extract_string().get();
                        LOG_ERROR("%s", s.c_str());
                        rcmd.body.cancelOrder.ErrorID = code;
                        strncpy(rcmd.body.cancelOrder.originMsg, s.c_str(), sizeof(rcmd.body.cancelOrder.originMsg));
                        rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                        PUSH_RCMD(rcmd)
                        return pplx::task_from_result(json::value());
                    }
                })
                .then([&](pplx::task<json::value> previousTask) {
                    try{
                        json::value const &v = previousTask.get();
                        LOG_DEBUG("cancel order response:%s", v.serialize().c_str());
                        if (v.has_field("code")){
                            rcmd.body.cancelOrder.orderStatus = OrderStatus_FAILED;
                            rcmd.body.cancelOrder.ErrorID = crypto::get_binance_errorid(v.at("code").as_integer());
                            if(rcmd.body.cancelOrder.ErrorID == ERROR_OrderNotFoundError){
                                rcmd.body.cancelOrder.orderStatus = OrderStatus_REJECTED;
                            }
                            strncpy(rcmd.body.cancelOrder.originMsg,v.at("msg").serialize().c_str(), ORIGINMSG_SIZE);
                        }
                        else{
                            strcpy(rcmd.body.cancelOrder.orderId, v.at("orderId").serialize().c_str());
                            strcpy(rcmd.body.cancelOrder.orderSysId, v.at("clientOrderId").as_string().c_str());
                            rcmd.body.cancelOrder.volumeTraded = stod(v.at("executedQty").as_string().c_str());
                            rcmd.body.cancelOrder.tradePrice   = stod(v.at("avgPrice").as_string().c_str());
                            rcmd.body.cancelOrder.orderStatus = OrderStatus_CANCELED;
                        }
                        PUSH_RCMD(rcmd)
                    }
                    catch(exception &e){
                        LOG_ERROR("%s", e.what());
                    }
                })
                .wait();
    #endif
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

void BinanceUnifiedTradingClient::query_one_order(pubsub::TCommand &tcmd){
    QUERY_ORDER_TCMD_2_RCMD(tcmd)

    if (tcmd.header.instTypeEnum == InstType_SPOT) {
        m_ord_url = "/papi/v1/margin/order";
    }
    else if (tcmd.header.instTypeEnum == InstType_USDT_FUTURES || tcmd.header.instTypeEnum == InstType_USDT_SWAP) {
        m_ord_url = "/papi/v1/um/order";
    } else if (tcmd.header.instTypeEnum == InstType_C_FUTURES || tcmd.header.instTypeEnum == InstType_C_SWAP) {
        m_ord_url = "/papi/v1/cm/order";
    }

    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info(tcmd.header.exchangeTypeEnum, tcmd.header.instTypeEnum, tcmd.body.queryOrder.instId, info)){
            auto ord = tcmd.body.queryOrder;
            // auto url = m_queryord_url[0];
            // http_client restclient(m_curcfg.restBaseUrl);
            http_request request(methods::GET);
            request.headers().add("X-MBX-APIKEY",m_curcfg.apiKey);
            uri_builder builder(m_ord_url);
            builder.append_query("symbol",info.originInstId);

            if(crypto::str_cmp(tcmd.body.queryOrder.orderId, "") == false){
                builder.append_query("orderId", ord.orderId);
            }
            else if(crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "") == false){
                builder.append_query("origClientOrderId", ord.orderSysId);
            }
            else{
                string errMsg = "query_one_order need orderId or orderSysId";
                rcmd.body.queryOrder.ErrorID = ERROR_NoOrderIdError;
                rcmd.body.queryOrder.orderStatus = OrderStatus_REJECTED;
                strncpy(rcmd.body.queryOrder.originMsg, errMsg.c_str(), sizeof(rcmd.body.queryOrder.originMsg));
                LOG_ERROR("%s", errMsg.c_str());
                PUSH_RCMD(rcmd)
                return;
            }

            builder.append_query("recvWindow",5000);
            builder.append_query("timestamp",crypto::getCurrentTimeMilli());

            auto signature = get_signature_rest(m_curcfg.apiSecret, builder.query());
            builder.append_query("signature", signature);
            LOG_DEBUG("query_one_order: %s ",builder.to_string().c_str());
            request.set_request_uri(builder.to_string());
            // restclient.request(request)
            const http_response &response = hotHttpClient->request(request);
            // const http_response &response = restclient.request(request).get();
            auto code = response.status_code();
            if(code == status_codes::OK || code == status_codes::BadRequest
            || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
                json::value const & v = response.extract_json().get();
                LOG_DEBUG("query_one_order reponse:%s", v.serialize().c_str());
                if(v.has_field("code")) {
                    rcmd.body.queryOrder.ErrorID = crypto::get_binance_errorid(v.at("code").as_integer());
                    if(rcmd.body.queryOrder.ErrorID == ERROR_TooManyOrdersError ){
                        rcmd.body.queryOrder.orderStatus = OrderStatus_UNKNOWN;
                    }
                    else{
                        rcmd.body.queryOrder.orderStatus = OrderStatus_REJECTED;
                    }
                    strncpy(rcmd.body.queryOrder.originMsg, v.serialize().c_str(), sizeof(rcmd.body.queryOrder.originMsg));
                    PUSH_RCMD(rcmd)
                }
                else if(v.has_field("orderId")){
                    rcmd.body.queryOrder.ErrorID = ERROR_NoError;
                    strcpy(rcmd.body.queryOrder.orderId, v.at("orderId").serialize().c_str());
                    // strcpy(rcmd.body.queryOrder.orderSysId, v.at("clientOrderId").as_string().c_str());
                    rcmd.body.queryOrder.volumeTotal  = stod(v.at("origQty").as_string().c_str())*info.magnifyNumber;
                    rcmd.body.queryOrder.limitPrice   = stod(v.at("price").as_string().c_str())*info.reduceNumber;
                    rcmd.body.queryOrder.volumeTraded = stod(v.at("executedQty").as_string().c_str())*info.magnifyNumber;
                    
                    if (tcmd.header.instTypeEnum == InstType_SPOT) {
                        double cummulativeQuoteQty = stod(v.at("cummulativeQuoteQty").as_string().c_str());
                        if (rcmd.body.queryOrder.volumeTraded > 0) {
                            rcmd.body.queryOrder.tradePrice = cummulativeQuoteQty / rcmd.body.queryOrder.volumeTraded;
                        }
                    } else {
                        rcmd.body.queryOrder.tradePrice = stod(v.at("avgPrice").as_string().c_str())*info.reduceNumber;//stod(v.at("avgPrice").as_string().c_str());
                    }
                    string status = v.at("status").as_string();
                    rcmd.body.queryOrder.orderStatus = crypto::get_binance_orderstatus(status);
                    PUSH_RCMD(rcmd)
                }
                else{
                    LOG_ERROR("cannot parse binance query order json:%s", v.as_string().c_str());
                }
            }
            else{
                auto s = response.extract_string().get();
                rcmd.body.queryOrder.ErrorID = code;
                rcmd.body.queryOrder.orderStatus = OrderStatus_UNKNOWN;
                // strncpy(rcmd.body.queryOrder.originMsg, s.c_str(), sizeof(rcmd.body.queryOrder.originMsg));
                LOG_ERROR("query_one_order:code:%d,response:%s",code, s.c_str());
                PUSH_RCMD(rcmd)
            }
#if 0
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
                    strncpy(rcmd.body.queryOrder.originMsg, s.c_str(), sizeof(rcmd.body.queryOrder.originMsg));
                    LOG_ERROR("query_one_order:code:%d,response:%s",code, s.c_str());
                    PUSH_RCMD(rcmd)
                    return pplx::task_from_result(json::value());
                }
            })
            .then([&](pplx::task<json::value> previousTask) {
                json::value const & v = previousTask.get();
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

                    rcmd.body.queryOrder.volumeTotal  = stod(v.at("origQty").as_string().c_str());
                    rcmd.body.queryOrder.limitPrice   = stod(v.at("price").as_string().c_str());
                    rcmd.body.queryOrder.volumeTraded = stod(v.at("executedQty").as_string().c_str()) ;
                    rcmd.body.queryOrder.tradePrice   = stod(v.at("avgPrice").as_string().c_str() );

                    string status = v.at("status").as_string();
                    rcmd.body.queryOrder.orderStatus = crypto::get_binance_orderstatus(status);
                    PUSH_RCMD(rcmd)
                }
                else{

                }
            })
            .wait();
#endif
        }
        else{
            LOG_ERROR("not found Binance %s smc info", tcmd.body.queryOrder.instId);
        }
    }
    catch(exception &e) {
        LOG_ERROR("%s", e.what());
    }
}

void BinanceUnifiedTradingClient::query_multi_orders(pubsub::TCommand &tcmd){

}

void BinanceUnifiedTradingClient::query_order(pubsub::TCommand &tcmd){
    if(tcmd.body.queryOrder.queryOrderTypeEnum == QOT_ONE_INST){
        query_one_order(tcmd);
    }
    else if(tcmd.body.queryOrder.queryOrderTypeEnum == QOT_MULTI_INST){
        // query_multi_orders(tcmd);
    }
    else{

    }
}
