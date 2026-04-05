#pragma once
#include "Api/api_base/tradingapi_base.h"
#include <cpprest/ws_client.h>
#include <cpprest/http_client.h>
#include <cpprest/http_msg.h>
#include "cpprest/json.h"
#include <map>
//#include "utils.h"
//#include <cpprest/ws_client.h>
#include <cpprest/filestream.h>
//#include "redis_client.h"
//#include "data_struct.h"
#include "securitymanager.h"
#include <unordered_map>
#include "crypto_exception.h"
#include "time_util.h"
#include "string_util.h"
#include "data_struct.h"
#include "tb_global.h"
#include "pubsub_protocol.h"
#include "precision_util.h"


#define DEBUGLINE fprintf(stdout,"I am happy,%s,%d\n", __FUNCTION__ , __LINE__);

using namespace std;
using namespace web;
using namespace om;
using namespace pubsub;
using namespace web::websockets::client;
using namespace web::http;
using namespace web::http::client;
using namespace concurrency::streams;

class XTSpotTradingClient {

public:
    XTSpotTradingClient();
    virtual ~XTSpotTradingClient();

    void Run();
    void Terminate();
    void OnThreadIdle();

    const AccountCfg& getAccInfo(){return m_curcfg;}

    ///
    virtual bool Initialize(AccountCfg& cfg, sm::SecurityManager *smc);

//    virtual void pong();
//    virtual void ping();
//    virtual void SubWebSocket();
//    void on_websocket_msg(const websocket_incoming_message& msg);

//    string get_signature(string &channel, string &event, string &time);
//    string get_signature_ws(const char *channel, const char *event, const char *time);

//    void say_hello();
//    void start();
    virtual bool get_balances();//vector<Balance> &balanceVec
    virtual void add_new_order(pubsub::TCommand &cmd);
    virtual void cancel_order(pubsub::TCommand &cmd);
    virtual void query_order(pubsub::TCommand &cmd);
    virtual void query_one_order(pubsub::TCommand &cmd);
    virtual void query_multi_orders(pubsub::TCommand &cmd);
protected:
    string get_signature_rest(map<string, string> &params);
    string param_to_url(map<std::string, string> &params);
    bool param_to_builder(map<string, string> &params, uri_builder &builder);
//    bool param_to_builder(map<string, string> &params, uri_builder &builder);

    sm::SecurityManager *smc;

//    websocket_callback_client wsClient;
//    std::thread* m_handle;
//    bool m_Working;
//    bool m_IsConnected;
//    string m_listenkey;
//
    web::uri m_orderUrl;
    web::uri m_cancelOrderUrl;
    web::uri m_batchCancelOrderUrl;
    web::uri m_getOrderUrl;
    web::uri m_getOpenOrdersUrl;
    web::uri m_getBatchOrdersUrl;
//    web::uri m_trans_url;
//    web::uri m_wssb_url;
//    web::uri m_wsLiskey_url;
//    web::uri m_queryord_url[3];

    web::uri m_balanceUrl;

    AccountCfg m_curcfg;

//    void keepAlive();
//    virtual void monitor();
//    virtual void sub_websocket();
//    virtual void on_close_msg(websocket_close_status close_status, const utility::string_t& reason,
//                      const std::error_code& error);
//    virtual websocket_outgoing_message sub_balance_channel();
//    virtual websocket_outgoing_message sub_orders_channel();
//    virtual websocket_outgoing_message sub_trades_channel();
//    virtual void sub_websocket(websocket_client &client);

};
