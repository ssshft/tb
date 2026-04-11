#pragma once
#include "api/api_base/tradingapi_base.h"


#define GET_ORDERTYPE(rcmd)                                       \
    if (crypto::str_cmp(tif.c_str(), "gtc"))                      \
    {                                                             \
        rcmd.body.orderResponse.orderType = OrderType_LIMIT;      \
    }                                                             \
    else if (crypto::str_cmp(tif.c_str(), "ioc"))                 \
    {                                                             \
        if (rcmd.body.orderResponse.limitPrice == 0)              \
        {                                                         \
            rcmd.body.orderResponse.orderType = OrderType_MARKET; \
        }                                                         \
        else                                                      \
        {                                                         \
            rcmd.body.orderResponse.orderType = OrderType_IOC;    \
        }                                                         \
    }                                                             \
    else if (crypto::str_cmp(tif.c_str(), "poc"))                 \
    {                                                             \
        rcmd.body.orderResponse.orderType = OrderType_POST_ONLY;  \
    }                                                             \
    else                                                          \
    {                                                             \
        rcmd.body.orderResponse.orderType = OrderType_UNKNOWN;    \
    }

class GateioSpotTradingClient {

public:
    GateioSpotTradingClient();
    virtual ~GateioSpotTradingClient();

    void Run();
    const AccountCfg& getAccInfo(){return m_curcfg;}

    virtual bool Initialize(AccountCfg& cfg, sm::SecurityManager *smc);

    virtual void pong();
    virtual void ping();

    void on_websocket_msg(const websocket_incoming_message& msg);

    string get_signature_ws(const char *channel, const char *event, const char *time);
    string get_signature_rest(const char *method, const char *url,const char * time,
                         const char *queryString , const char *payloadString);
    string get_signature_ws_api(const char *channel, const char *event, const char *time, const char* reqPara = "");

    virtual bool get_balances();//vector<Balance> &balanceVec
    virtual void add_new_order(pubsub::TCommand &cmd);
    virtual void cancel_order(pubsub::TCommand &cmd);
    virtual void query_order(pubsub::TCommand &cmd);
//    virtual void query_one_order(pubsub::TCommand &cmd);
//    virtual void query_multi_orders(pubsub::TCommand &cmd);

    virtual bool get_unified_account();

    #ifdef USE_WEBSOCKET_API
    virtual void ws_add_new_order(pubsub::TCommand &tcmd);
    virtual void ws_cancel_order(pubsub::TCommand &tcmd);
    virtual void ws_query_order(pubsub::TCommand &tcmd);
    #endif

protected:
    sm::SecurityManager *smc;

    websocket_callback_client wsClient;
    bool m_IsConnected;
    web::uri m_orderUrl;
//    web::uri m_cancelOrderUrl;
//    web::uri m_trans_url;
//    web::uri m_wssb_url;
//    web::uri m_wsLiskey_url;
//    web::uri m_queryord_url[3];

    web::uri m_balanceUrl;

    web::uri m_unifiedUrl;

    AccountCfg m_curcfg;
    // http_client *addNewOrderRestClient;
    // http_client *cancelOrderRestClient;
    // http_client *hotHttpClient;
    crypto::RestClientCPP *hotHttpClient;
//    void keepAlive();
    virtual void monitor();
    virtual void sub_websocket();
    virtual void on_close_msg(websocket_close_status close_status, const utility::string_t& reason,
                      const std::error_code& error);
    virtual websocket_outgoing_message sub_balance_channel();
    virtual websocket_outgoing_message sub_cross_balance_channel();
    virtual websocket_outgoing_message sub_unified_balance_channel();
    virtual websocket_outgoing_message sub_orders_channel();
    virtual websocket_outgoing_message sub_trades_channel();
//    virtual void sub_websocket(websocket_client &client);
    websocket_outgoing_message sub_login();
};




#pragma once
#include "base/BaseTrade.h"


class GateioSpotTradeUnit : public BaseTradeUnit {

public:
    GateioSpotTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~GateioSpotTradeUnit();

    virtual void subWebsocekt();
    virtual void onWebsocketMsg(const web::websockets::client::websocket_incoming_message& msg);
    virtual void ping();
    virtual void pong();

    web:;websockets::client::websocket_outgoing_message sub_balance_channel();
    web:;websockets::client::websocket_outgoing_message sub_orders_channel();

    virtual void query_account(const pubsub::TCommand& tcmd);
    virtual void query_balance(const pubsub::TCommand& tcmd);
    virtual void query_position(const pubsub::TCommand& tcmd);
    virtual void add_new_order(const pubsub::TCommand& tcmd);
    virtual void cancel_order(const pubsub::TCommand& tcmd);
    virtual void query_order(const pubsub::TCommand& tcmd);

protected:
    web::uri unifiedUrl;
};