#include "XTSpotTradingClient.h"

XTSpotTradingClient::XTSpotTradingClient(){

}

XTSpotTradingClient::~XTSpotTradingClient(){
//    delete smc;
}

bool XTSpotTradingClient::Initialize(AccountCfg& cfg, sm::SecurityManager *smc){
    this->smc = smc;
    m_balanceUrl = "/trade/api/v1/getBalance";
    m_orderUrl = "/trade/api/v1/order";
    m_cancelOrderUrl = "/trade/api/v1/cancel";
    m_batchCancelOrderUrl = "/trade/api/v1/batchCancel";

    m_getOrderUrl = "/trade/api/v1/getOrder";
    m_getOpenOrdersUrl = "/trade/api/v1/getOpenOrders";
    m_getBatchOrdersUrl = "/trade/api/v1/getBatchOrders";

//    m_cancelOrderUrl = "/spot/orders/";
    m_curcfg = cfg;

    return true;
}

void XTSpotTradingClient::Run() {
//    std::thread monitorThread(&XTSpotTradingClient::monitor, this);
//    monitorThread.detach();
}


string XTSpotTradingClient::get_signature_rest(map<string, string> &params){
    string spliceStr = param_to_url(params);
//    cout << spliceStr << endl;
    //xt的加密和binance类似
    std::string hmacsha256hex = crypto::encryptWithHMACForBinance(m_curcfg.apiSecret, spliceStr); //hmac<sha256>::calc_hex(spliceStr, m_asecretKey);
    return hmacsha256hex;
}

string XTSpotTradingClient::param_to_url(map<string, string> &params) {
    string spliceStr;
    for (auto iter = params.begin(); iter != params.end(); ++iter) {
        spliceStr.append(iter->first).append("=").append(iter->second);
        if (iter != --params.end()) {
            spliceStr.append("&");
        }
    }
    return spliceStr;
}

bool XTSpotTradingClient::param_to_builder(map<string, string> &params, uri_builder &builder) {
    string spliceStr;
    for (auto iter = params.begin(); iter != params.end(); ++iter) {
        builder.append_query(iter->first, iter->second);
    }
    return true;
}


bool XTSpotTradingClient::get_balances()//vector<Balance> &balanceVec
try {
    http_client restclient(m_curcfg.restBaseUrl);//bUrl
    http_request request(methods::GET);
    request.headers().add("Accept","application/json");
    request.headers().add("Content-Type","application/x-www-form-urlencoded");
    request.headers().add("charsets","utf-8");
    std::map<std::string, std::string> params;
    params["accesskey"] = m_curcfg.apiKey;
    params["nonce"] = to_string(crypto::getCurrentTimeMilli());
    string signature = get_signature_rest(params);
    params["signature"] = signature;
    string url =  m_balanceUrl.to_string() + "?" +param_to_url(params);//string("/trade/api/v1/getBalance?")
    LOG_DEBUG("%s", (string(m_curcfg.restBaseUrl) + url).c_str());
    uri_builder builder(url);// bUrl m_balanceUrl
//    cout << builder.to_string() << endl;
    request.set_request_uri(builder.to_string());
    restclient.request(request)
    .then([&](http_response response) -> pplx::task<json::value> {
        auto code = response.status_code();
        if(code == status_codes::OK || code == status_codes::BadRequest
            || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
            return response.extract_json();
        }
        string msg{"response code is not 400, 401, 418, 429, but:"};
        msg.append(to_string(code));
        throw crypto_exception(msg.c_str());
        return pplx::task_from_result(json::value());
    }) // continue when the JSON value is available
    .then([&](pplx::task<json::value> previousTask) {
        json::value const &v = previousTask.get();
        rapidjson::Document d;
        rapidjson::Value &res = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(v.serialize().c_str());
//            LOG_DEBUG("%s", v.serialize().c_str());
        return true;
        if (!d.HasParseError() && res.IsObject() && res.HasMember("code")
        && crypto::str_cmp(res["code"].GetString(), "200") == true) {
            rapidjson::Value &data = res["data"];
            for (rapidjson::Value::ConstMemberIterator it = data.MemberBegin(); it != data.MemberEnd(); ++it) {
                pubsub::RCommand cmd;
                memset(&cmd,0,sizeof(pubsub::RCommand));
                cmd.header.insertTime = crypto::getCurrentTime();
                cmd.header.exchangeTypeEnum = ExchangeType_XT;
                cmd.header.instTypeEnum = InstType_SPOT;
                cmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                strcpy(cmd.header.accountId, m_curcfg.accountId);
                cmd.body.balance.updateTime = crypto::getCurrentTime();
                strcpy(cmd.body.balance.currency, crypto::to_upper(it->name.GetString()).c_str());
                cmd.body.balance.available = stod(it->value["available"].GetString());
                cmd.body.balance.frozen    = stod(it->value["freeze"].GetString());
                g_rptInnerQueue.push(cmd);
//                    cout << cmd.getString() << endl;
            }
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

void XTSpotTradingClient::add_new_order(pubsub::TCommand &cmd){
    pubsub::RCommand rcmd;
    memset(&rcmd,0,sizeof(pubsub::RCommand));
//    memcpy(&rcmd.header,&cmd.header, sizeof(cmd.header));
    rcmd.header.exchangeTypeEnum = cmd.header.exchangeTypeEnum;
    rcmd.header.instTypeEnum = cmd.header.instTypeEnum;
    strcpy(rcmd.header.accountId, cmd.header.accountId);
    rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_NEW_ORDER;
    rcmd.header.insertTime = crypto::getCurrentTime();
    strcpy(rcmd.body.newOrder.instId,cmd.body.newOrder.instId);
    rcmd.body.newOrder.offsetFlag = cmd.body.newOrder.offsetFlag;
    rcmd.body.newOrder.direction = cmd.body.newOrder.direction;
    rcmd.body.newOrder.orderType = cmd.body.newOrder.orderType;
    rcmd.body.newOrder.orderStatus = OrderStatus_REJECTED;
    strcpy(rcmd.body.newOrder.clientOrderId, cmd.body.newOrder.clientOrderId);
//    memcpy(&rcmd.body.newOrder,&cmd.body.newOrder, sizeof(&rcmd.body.newOrder));
    try {
//    LOG_DEBUG("%s",cmd.body.newOrder.getString().c_str());
        md::InstrumentInfo info;
        if(smc->get_instrument_info("XT","InstType_SPOT",cmd.body.newOrder.instId,info)){
            string url = m_curcfg.restBaseUrl;
            url.append(m_orderUrl.to_string());
            http_client restclient(url);
//            http_client restclient(m_curcfg.restBaseUrl );//bUrl
            http_request request(methods::POST);
            request.headers().add("Accept","application/json");
            request.headers().add("Content-Type","application/x-www-form-urlencoded");
            request.headers().add("charsets","utf-8");

            std::map<std::string, std::string> params;
            params["accesskey"] = m_curcfg.apiKey;
            params["nonce"] = to_string(crypto::getCurrentTimeMilli());

            params["market"] = crypto::to_lower(info.originInstId);
            params["number"] = crypto::getFixedPrecision(cmd.body.newOrder.volumeTotal, info.lotSize);//to_string(order.volumeTotal);
            params["price"] = crypto::getFixedPrecision(cmd.body.newOrder.limitPrice, info.tickSize);

            //open
            if(cmd.body.newOrder.offsetFlag == OffsetFlag_OPEN){
                if (cmd.body.newOrder.direction == Direction_LONG) {// open long
                    params["type"] = "1";
                }
                else if(cmd.body.newOrder.direction == Direction_SHORT){
                    params["type"] = "0";//open short
                }
                else{
                    rcmd.body.newOrder.ErrorID = ERROR_DirectionError;
                    g_rptInnerQueue.push(rcmd);
                    return;
                }
            }
            else if(cmd.body.newOrder.offsetFlag == OffsetFlag_CLOSE){
                if (cmd.body.newOrder.direction == Direction_LONG) {// open long
                    params["type"] = "0";
                }
                else if(cmd.body.newOrder.direction == Direction_SHORT){
                    params["type"] = "1";//open short
                }
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
            if (cmd.body.newOrder.orderType == OrderType_LIMIT) {// 0.Limited price  1.Market price matching
                params["entrustType"] = "0";
            }
            else if (cmd.body.newOrder.orderType == OrderType_MARKET) {
                params["entrustType"] = "1";
            }
            else {
//                cryptothrow("xt not support ordertype except OrderType_LIMIT and OrderType_MARKET now", -1);
                rcmd.body.newOrder.ErrorID = ERROR_OrderTypeError;
                g_rptInnerQueue.push(rcmd);
                return ;
            }
            string signature = get_signature_rest(params);
            params["signature"] = signature;
            string paramStr = param_to_url(params);
            LOG_DEBUG("%s, url:%s, params:%s",__FUNCTION__, url.c_str(), paramStr.c_str());
            request.set_body(paramStr.c_str());
            restclient.request(request)
            .then([&](http_response response) -> pplx::task<json::value> {
                auto code = response.status_code();
                if(code == status_codes::OK || code == status_codes::BadRequest
                    || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
                    return response.extract_json();
                }
                string msg{"response code is not 400, 401, 418, 429, but:"};
                msg.append(to_string(code));
                throw crypto_exception(msg.c_str());
                return pplx::task_from_result(json::value());
            }) // continue when the JSON value is available
            .then([&](pplx::task<json::value> previousTask) {
                json::value const &v = previousTask.get();
//                    cout << v.serialize() << endl;
                rapidjson::Document d;
                rapidjson::Value &res = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(v.serialize().c_str());
                LOG_DEBUG("%s", v.serialize().c_str());
                if (!d.HasParseError()) {
                    if (crypto::str_cmp(res["code"].GetString(), "200")) {
                        rapidjson::Value &data = res["data"];
                        //{"code":200,"data":{"id":"6879295966433470464"},"info":"Success."}
                        strcpy(rcmd.body.newOrder.orderId, data["id"].GetString());
                        strcpy(rcmd.body.newOrder.clientOrderId,cmd.body.newOrder.clientOrderId);

                        rcmd.body.newOrder.orderStatus =  OrderStatus_REST_NEW;
                        rcmd.body.newOrder.volumeTotal = stod(params["number"].c_str());
                        rcmd.body.newOrder.limitPrice = stod(params["price"].c_str());
                        rcmd.body.newOrder.insertTime = cmd.body.newOrder.insertTime;
                        rcmd.body.newOrder.updateTime = crypto::getCurrentTime();
                        g_rptInnerQueue.push(rcmd);
                    } else {//TODO
                        //{"code":500,"msg":"price scale error","msgInfo":
                        // {"template":"price scale error","args":null,"code":null},"data":{}}
                        LOG_ERROR("%s",v.serialize().c_str());
                        rcmd.body.newOrder.ErrorID = stoi(res["code"].GetString()) ;
                        strncpy(rcmd.body.newOrder.originMsg, res["msgInfo"].GetString(), sizeof(rcmd.body.newOrder.originMsg));
                        g_rptInnerQueue.push(rcmd);
                    }
                }
                else {
                    LOG_ERROR("%s", v.serialize().c_str());
                }
            })
            .wait();
        }
        else{
            rcmd.body.newOrder.ErrorID = ERROR_SMCInstrumentNotExistError;
            g_rptInnerQueue.push(rcmd);
        }
    }
    catch(exception &e) {
        rcmd.body.newOrder.ErrorID = ERROR_NetworkError;
        strncpy(rcmd.body.newOrder.originMsg, e.what(), sizeof(rcmd.body.newOrder.originMsg));
        LOG_ERROR("%s", e.what());
        g_rptInnerQueue.push(rcmd);
    }
}

void XTSpotTradingClient::cancel_order(pubsub::TCommand &cmd){
    //这里用CMD_RPT_ORDER_RESPONSE代替CMD_RPT_CANCEL_ORDER是因为取消单子不会触发ws事件
    pubsub::RCommand rcmd;
    memset(&rcmd,0,sizeof(pubsub::RCommand));
    rcmd.header.exchangeTypeEnum = cmd.header.exchangeTypeEnum;
    rcmd.header.instTypeEnum = cmd.header.instTypeEnum;
    strcpy(rcmd.header.accountId, cmd.header.accountId);
    strcpy(rcmd.body.orderResponse.instId, cmd.body.cancelOrder.instId);
    strcpy(rcmd.body.orderResponse.orderId, cmd.body.cancelOrder.orderId);
    rcmd.header.insertTime = crypto::getCurrentTime();
    rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;

    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info("XT","InstType_SPOT",cmd.body.cancelOrder.instId, info)){
            http_request request(methods::POST);
            request.headers().add("Accept","application/json");
            request.headers().add("Content-Type","application/x-www-form-urlencoded");
            request.headers().add("charsets","utf-8");
            std::map<std::string, std::string> params;
            params["accesskey"] = m_curcfg.apiKey;
            params["nonce"] = to_string(crypto::getCurrentTimeMilli());
            params["market"] = crypto::to_lower(info.originInstId);//crypto::to_lower(order.instId);// map.put("market", "btc_usdt");

            string url = m_curcfg.restBaseUrl;
            url.append(m_cancelOrderUrl.to_string());
            //取消单笔订单
            if(cmd.body.cancelOrder.cancelOrderTypeEnum == COT_ONE_INST){
                //目前只支持交易所订单撤单，没有用户自定义订单撤单
                if(!crypto::str_cmp(cmd.body.cancelOrder.orderId,"")){
                    params["id"] = string(cmd.body.cancelOrder.orderId);
                }
                else{
                    LOG_ERROR("cancel order need orderId or clientOrderId" );
                    strcpy(rcmd.body.cancelOrder.originMsg, "cancel order need orderId");
                    rcmd.body.cancelOrder.ErrorID = ERROR_NoOrderId;
                    rcmd.body.cancelOrder.orderStatus = OrderStatus_UNKNOWN;
                    g_rptInnerQueue.push(rcmd);
                    return;
                }

                http_client restclient(url);
                string signature = get_signature_rest(params);
                params["signature"] = signature;
                string paramStr = param_to_url(params);
                LOG_DEBUG("%s, url:%s, params:%s",__FUNCTION__, url.c_str(), paramStr.c_str());
                request.set_body(paramStr.c_str());
                restclient.request(request)
                .then([&](http_response response) -> pplx::task<json::value> {
                    auto code = response.status_code();
                    if(code == status_codes::OK || code == status_codes::BadRequest
                       || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
                        return response.extract_json();
                    }
                    string msg{"response code is not 400, 401, 418, 429, but:"};
                    msg.append(to_string(code));
                    throw crypto_exception(msg.c_str());
                    return pplx::task_from_result(json::value());
                })
                .then([&](pplx::task<json::value> previousTask) {
                    json::value const &v = previousTask.get();
                    rapidjson::Document d;
                    rapidjson::Value &res = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(v.serialize().c_str());
                    LOG_DEBUG("cancel_order, response:%s", v.serialize().c_str());
                    if (res.IsObject() && res.HasMember("code")
                        && crypto::str_cmp(res["code"].GetString(), "200") == true) {
                        rcmd.body.orderResponse.orderStatus = OrderStatus_CANCELED;
                        rcmd.body.orderResponse.tsParse = crypto::getCurrentTime();
                        g_rptInnerQueue.push(rcmd);
//                        if(res.HasMember("data")){//批量撤单
//                            rapidjson::Value &data = res["data"];
//                            //{"code":200,"data":{"code":null,"id":"6931991954365925376","msg":null},"info":"Success."}
//                            rcmd.body.orderResponse.orderStatus = OrderStatus_CANCELED;
//                            g_rptInnerQueue.push(rcmd);
//                            if(data.IsArray()){
//                                for (rapidjson::SizeType i = 0; i < data.Size(); i++) {
//                                    pubsub::RCommand rrcmd;
//                                    memset(&rrcmd,0, sizeof(pubsub::RCommand));
//                                    rrcmd.header.exchangeTypeEnum = cmd.header.exchangeTypeEnum;
//                                    rrcmd.header.instTypeEnum = cmd.header.instTypeEnum;
//                                    strcpy(rrcmd.header.accountId, cmd.header.accountId);
//                                    strcpy(rrcmd.body.orderResponse.instId, cmd.body.queryOrder.instId);
//                                    rrcmd.header.insertTime = crypto::getCurrentTime();
//                                    rrcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
//                                    rrcmd.body.orderResponse.offsetFlag = OffsetFlag_OPEN;
//                                    string code = data[i]["code"].GetString();
//                                    strcpy(rrcmd.body.orderResponse.orderId, data[i]["id"].GetString());
//                                    if(crypto::str_cmp(code.c_str(),"120")){
//                                        rrcmd.body.orderResponse.orderStatus = OrderStatus_CANCELED;
//                                    }
//                                    else{
////                                    rrcmd.body.cancelOrder.success = false;
//                                        rrcmd.body.orderResponse.orderStatus = OrderStatus_UNKNOWN;
//                                        rrcmd.body.orderResponse.ErrorID = stod(code.c_str());
//                                        strncpy(rrcmd.body.orderResponse.originMsg,data[i]["msg"].GetString(),sizeof(rrcmd.body.cancelOrder.originMsg));
//                                    }
//                                    g_rptInnerQueue.push(rrcmd);
//                                }
//                            }
//                            else{//单个撤单
//
//                            }
//                        }
                    }
                    else{
                        LOG_ERROR("%s", v.serialize().c_str());
                        rcmd.body.orderResponse.orderStatus = OrderStatus_UNKNOWN;
                        rcmd.body.orderResponse.ErrorID = stoi(res["code"].GetString()) ;
                        strncpy(rcmd.body.orderResponse.originMsg, res["msgInfo"].GetString(), sizeof(rcmd.body.orderResponse.originMsg));
                        g_rptInnerQueue.push(rcmd);
                    }
                })
                .wait();
            }
            else if(cmd.body.cancelOrder.cancelOrderTypeEnum == COT_MULTI_INST){

            }
            else{
                rcmd.body.cancelOrder.ErrorID = ERROR_CancelOrQueryTypeError;
                g_rptInnerQueue.push(rcmd);
            }
        }
        else{
            rcmd.body.orderResponse.ErrorID = ERROR_SMCInstrumentNotExistError;
            g_rptInnerQueue.push(rcmd);
        }
    }
    catch(exception &e) {
        LOG_ERROR("%s", e.what());
        rcmd.body.orderResponse.ErrorID = ERROR_NetworkError;
        strncpy(rcmd.body.orderResponse.originMsg, e.what(), sizeof(rcmd.body.orderResponse.originMsg));
        g_rptInnerQueue.push(rcmd);
    }
    return;
}

void XTSpotTradingClient::query_one_order(pubsub::TCommand &cmd){
    pubsub::RCommand rcmd;
    memset(&rcmd, 0, sizeof(pubsub::RCommand));
    rcmd.header.exchangeTypeEnum = cmd.header.exchangeTypeEnum;
    rcmd.header.instTypeEnum = cmd.header.instTypeEnum;
    strcpy(rcmd.header.accountId, cmd.header.accountId);
    strcpy(rcmd.body.orderResponse.instId, cmd.body.queryOrder.instId);
    strcpy(rcmd.body.orderResponse.orderId, cmd.body.queryOrder.orderId);
    rcmd.header.insertTime = crypto::getCurrentTime();
    rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info("XT","InstType_SPOT",cmd.body.queryOrder.instId, info)){
            http_client restclient(m_curcfg.restBaseUrl);//bUrl
            http_request request(methods::GET);
            request.headers().add("Accept","application/json");
            request.headers().add("Content-Type","application/x-www-form-urlencoded");
            request.headers().add("charsets","utf-8");
            std::map<std::string, std::string> params;
            params["accesskey"] = m_curcfg.apiKey;
            params["nonce"] = to_string(crypto::getCurrentTimeMilli());
            params["market"] = crypto::to_lower(info.originInstId);//crypto::to_lower(order.instId);// map.put("market", "btc_usdt");
            string url = m_curcfg.restBaseUrl;
            url.append(m_getOrderUrl.to_string());
            //单笔订单
            params["id"] = string(cmd.body.queryOrder.orderId);

            string signature = get_signature_rest(params);
            params["signature"] = signature;
            string paramStr = param_to_url(params);
            url.append("?").append(paramStr);
            LOG_DEBUG("%s, url: %s", __FUNCTION__, url.c_str());
            uri_builder builder(url);
            request.set_request_uri(builder.to_string());
            restclient.request(request)
            .then([&](http_response response) -> pplx::task<json::value> {
                auto code = response.status_code();
                if(code == status_codes::OK || code == status_codes::BadRequest
                   || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
                    return response.extract_json();
                }
                string msg{"response code is not 400, 401, 418, 429, but:"};
                msg.append(to_string(code));
                throw crypto_exception(msg.c_str());
                return pplx::task_from_result(json::value());
            })
            .then([&](pplx::task<json::value> previousTask) {
                json::value const &v = previousTask.get();
                LOG_DEBUG("query_one_order response:%s", v.serialize().c_str());
                if(v.has_field("code") && crypto::str_cmp(v.at("code").serialize().c_str(), "200") == true){
                    auto data = v.at("data");
                    strcpy(rcmd.body.orderResponse.instId, info.instId);
                    strcpy(rcmd.body.orderResponse.orderId, data.at("id").as_string().c_str());

                    rcmd.body.orderResponse.offsetFlag = OffsetFlag_OPEN;

                    rcmd.body.orderResponse.direction = data.at("type").as_integer() == 0 ? Direction_SHORT : Direction_LONG;

                    int entrustType = data.at("entrustType").as_integer();
                    if(entrustType == 0){
                        rcmd.body.orderResponse.orderType = OrderType_LIMIT;
                    }
                    else if(entrustType == 1){
                        rcmd.body.orderResponse.orderType = OrderType_MARKET;
                    }
                    else{
                        LOG_ERROR("not support order type: %d", entrustType);
                    }
                    rcmd.body.orderResponse.volumeTraded = stod(data.at("completeNumber").as_string().c_str());
                    rcmd.body.orderResponse.volumeTotal  = stod(data.at("number").as_string().c_str());
                    rcmd.body.orderResponse.limitPrice   = stod(data.at("price").as_string().c_str());
                    int status = data.at("status").as_integer();
                    if(status == 0 ){//0、提交未撮合
                        rcmd.body.orderResponse.orderStatus = OrderStatus_NEW;
                    }
                    else if(status == 1){//1、未成交或部份成交
                        if(rcmd.body.orderResponse.volumeTraded > 0){
                            rcmd.body.orderResponse.orderStatus = OrderStatus_PARTFILLED;
                        }
                        else{
                            rcmd.body.orderResponse.orderStatus = OrderStatus_NEW;
                        }
                    }
                    else if(status == 2 || status == 4){//2、已完成，4、撮合完成结算中
                        rcmd.body.orderResponse.orderStatus = OrderStatus_FILLED;
                    }
                    else if(status == 3){//3、已取消
                        rcmd.body.orderResponse.orderStatus = OrderStatus_CANCELED;
                    }
                    else{
                        LOG_ERROR("not support order type: %d", entrustType);
                        rcmd.body.orderResponse.orderType = OrderType_UNKNOWN;
                    }
                    rcmd.body.orderResponse.insertTime = stoll(data.at("time").serialize().c_str()) * 1000;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    rcmd.body.orderResponse.tsParse = crypto::getCurrentTime();
                    g_rptInnerQueue.push(rcmd);

                }
                else {
                    rcmd.body.orderResponse.ErrorID = ERROR_UNKNOWN_ERROR;
                    strncpy(rcmd.body.orderResponse.originMsg,v.serialize().c_str(),sizeof(rcmd.body.orderResponse.originMsg));
                    g_rptInnerQueue.push(rcmd);
                }
            })
            .wait();
        }
        else{
            rcmd.body.orderResponse.ErrorID = ERROR_SMCInstrumentNotExistError;
            g_rptInnerQueue.push(rcmd);
            LOG_ERROR("not found XT.SPOT.%s smc info", cmd.body.queryOrder.instId);
        }
    }
    catch(exception &e) {
        rcmd.body.orderResponse.ErrorID = ERROR_NetworkError;
        strncpy(rcmd.body.orderResponse.originMsg, e.what(), sizeof(rcmd.body.orderResponse.originMsg));
        g_rptInnerQueue.push(rcmd);
        LOG_ERROR("%s", e.what());
    }
}

//获取某个交易对的所有未成交订单
void XTSpotTradingClient::query_multi_orders(pubsub::TCommand &cmd){
//    strcpy(rcmd.body.orderResponse.orderId, cmd.body.queryOrder.orderId);
//    rcmd.header.insertTime = crypto::getCurrentTime();
//    rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
    try {
        md::InstrumentInfo info;
        if(smc->get_instrument_info("XT","InstType_SPOT",cmd.body.queryOrder.instId, info)){
            http_client restclient(m_curcfg.restBaseUrl);//bUrl
            http_request request(methods::GET);
            request.headers().add("Accept","application/json");
            request.headers().add("Content-Type","application/x-www-form-urlencoded");
            request.headers().add("charsets","utf-8");
            std::map<std::string, std::string> params;
            params["accesskey"] = m_curcfg.apiKey;
            params["nonce"] = to_string(crypto::getCurrentTimeMilli());
            params["market"] = crypto::to_lower(info.originInstId);//crypto::to_lower(order.instId);// map.put("market", "btc_usdt");
            params["page"] = "1";
            params["pageSize"] = "1000";
            string url = m_curcfg.restBaseUrl;
            url.append(m_getOpenOrdersUrl.to_string());

            string signature = get_signature_rest(params);
            params["signature"] = signature;
            string paramStr = param_to_url(params);
            url.append("?").append(paramStr);
            LOG_DEBUG("%s, url: %s", __FUNCTION__, url.c_str());
            uri_builder builder(url);
            request.set_request_uri(builder.to_string());
            restclient.request(request)
            .then([&](http_response response) -> pplx::task<json::value> {
                auto code = response.status_code();
                if(code == status_codes::OK || code == status_codes::BadRequest
                   || code == status_codes::TooManyRequests || code == status_codes::Unauthorized){
                    return response.extract_json();
                }
                return pplx::task_from_result(json::value());
            })
            .then([&](pplx::task<json::value> previousTask) {
                try{
                    json::value const &v = previousTask.get();
                    LOG_DEBUG("query_multi_orders response:%s", v.serialize().c_str());
                    //{"code":200,"data":[{"avgPrice":"0","completeMoney":"0","completeNumber":"0","entrustType":0,"id":"6947959716505731072",
                    // "number":"1","price":"209","status":1,"time":1656522683264,"type":1},{"avgPrice":"0","completeMoney":"0","completeNumber":"0","entrustType":0,
                    // "id":"6947959682636130304","number":"1","price":"201","status":1,"time":1656522675189,"type":1}],"info":"Success."}
                    if(v.has_field("code") && crypto::str_cmp(v.at("code").serialize().c_str(), "200") == true){
                        auto data = v.at("data");
                        if(data.is_array() == true) {
                            auto array = data.as_array();
                            for(auto it : array){
                                pubsub::RCommand rcmd;
                                memset(&rcmd,0,sizeof(pubsub::RCommand));
                                rcmd.header.exchangeTypeEnum = cmd.header.exchangeTypeEnum;
                                rcmd.header.instTypeEnum = cmd.header.instTypeEnum;
                                strcpy(rcmd.header.accountId, cmd.header.accountId);
                                rcmd.header.insertTime = crypto::getCurrentTime();
                                rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                                strcpy(rcmd.body.orderResponse.instId, info.instId);
                                strcpy(rcmd.body.orderResponse.orderId, it.at("id").as_string().c_str());
                                rcmd.body.orderResponse.offsetFlag = OffsetFlag_OPEN;
                                rcmd.body.orderResponse.direction = it.at("type").as_integer() == 0 ? Direction_SHORT : Direction_LONG;

                                int entrustType = it.at("entrustType").as_integer();

                                if(entrustType == 0){
                                    rcmd.body.orderResponse.orderType = OrderType_LIMIT;
                                }
                                else if(entrustType == 1){
                                    rcmd.body.orderResponse.orderType = OrderType_MARKET;
                                }
                                else{
                                    LOG_ERROR("not support order type: %d", entrustType);
                                    rcmd.body.orderResponse.orderType = OrderType_UNKNOWN;
                                }
                                rcmd.body.orderResponse.volumeTraded = stod(it.at("completeNumber").as_string().c_str());
                                rcmd.body.orderResponse.volumeTotal  = stod(it.at("number").as_string().c_str());
                                rcmd.body.orderResponse.limitPrice   = stod(it.at("price").as_string().c_str());
                                int status = it.at("status").as_integer();
                                if(status == 0 ){//0、提交未撮合
                                    rcmd.body.orderResponse.orderStatus = OrderStatus_NEW;
                                }
                                else if(status == 1){//1、未成交或部份成交
                                    if(rcmd.body.orderResponse.volumeTraded > 0){
                                        rcmd.body.orderResponse.orderStatus = OrderStatus_PARTFILLED;
                                    }
                                    else{
                                        rcmd.body.orderResponse.orderStatus = OrderStatus_NEW;
                                    }
                                }
                                else if(status == 2 || status == 4){//2、已完成，4、撮合完成结算中
                                    rcmd.body.orderResponse.orderStatus = OrderStatus_FILLED;
                                }
                                else if(status == 3){//3、已取消
                                    rcmd.body.orderResponse.orderStatus = OrderStatus_CANCELED;
                                }
                                else{
                                    LOG_ERROR("not support order type: %d", entrustType);
                                    rcmd.body.orderResponse.orderType = OrderType_UNKNOWN;
                                }
                                rcmd.body.orderResponse.insertTime = stoll(it.at("time").serialize().c_str()) * 1000;
                                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                                rcmd.body.orderResponse.tsParse = crypto::getCurrentTime();
                                g_rptInnerQueue.push(rcmd);
                            }
                        }
                    }
                    else {
                        LOG_ERROR("%s", v.serialize().c_str());
                    }
                }
                catch (exception &e){
                    LOG_ERROR("%s", e.what());
                }
            })
            .wait();
        }
        else{
            LOG_ERROR("not found XT.SPOT.%s smc info", cmd.body.queryOrder.instId);
        }
    }
    catch(exception &e) {
        LOG_ERROR("%s", e.what());
    }
}

void XTSpotTradingClient::query_order(pubsub::TCommand &cmd){
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

