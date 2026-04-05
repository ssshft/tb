#pragma once
#include "GateioSpotTradingClient.h"

class GateioUSwapTradingClient : public GateioSpotTradingClient{
public:
//    GateioSpotTradingClient(const char *apiKey,const char *apiSecret,const char *accountId,
//                            const char *ip, const int port);
    GateioUSwapTradingClient();
    virtual ~GateioUSwapTradingClient();

    virtual void Run();
//    void Terminate();
//    void OnThreadIdle();

//    const AccountCfg& getAccInfo(){return m_curcfg;}
    ///
    virtual bool Initialize(AccountCfg& cfg, sm::SecurityManager *smc);
    virtual void pong();
    virtual void ping();

    virtual void on_websocket_msg(const websocket_incoming_message& msg);

//    string get_signature(string &channel, string &event, string &time);
//    virtual string get_signature_ws(const char *channel, const char *event, const char *time);
//    virtual string get_signature_rest(const char *method, const char *url,const char * time,
//                                      const char *queryString , const char *payloadString);
//    void say_hello();
//    virtual void start();
    virtual bool get_positions();
    virtual bool get_accounts();
    virtual void add_new_order(pubsub::TCommand &cmd);
    virtual void cancel_order(pubsub::TCommand &cmd);
    virtual void query_order(pubsub::TCommand &cmd);
    virtual void query_one_order(pubsub::TCommand &cmd);
    virtual void query_multi_orders(pubsub::TCommand &cmd);
protected:

//    sm::SecurityManager *smc;
//
//    websocket_callback_client wsClient;
//    std::thread* m_handle;
//    bool m_Working;
//    bool m_IsConnected;
//    web::uri m_orderUrl;
////    web::uri m_cancelOrderUrl;
//    web::uri m_trans_url;
//    web::uri m_wssb_url;
//    web::uri m_wsLiskey_url;
//    web::uri m_queryord_url[3];
//
//    web::uri m_balanceUrl;
    web::uri m_positionsUrl;
    web::uri m_accountsUrl;
//    AccountCfg m_curcfg;

//    void keepAlive();
    virtual void monitor();
    virtual void sub_websocket();
    virtual void on_close_msg(websocket_close_status close_status, const utility::string_t& reason,
                              const std::error_code& error);
    virtual websocket_outgoing_message sub_balance_channel();
    virtual websocket_outgoing_message sub_orders_channel();
    virtual websocket_outgoing_message sub_trades_channel();
    virtual websocket_outgoing_message sub_positions_channel();
    virtual websocket_outgoing_message sub_position_close_channel();
};
