#pragma once
#include "GateioSpotTradingClient.h"

class GateioUSwapTradingClient : public GateioSpotTradingClient{
public:
    GateioUSwapTradingClient();
    virtual ~GateioUSwapTradingClient();

    virtual void Run();
    bool Initialize(AccountCfg& cfg, sm::SecurityManager *smc);

    bool get_positions();
    bool get_position(pubsub::TCommand &tcmd);
    bool get_account(pubsub::TCommand &tcmd);
    virtual void add_new_order(pubsub::TCommand &tcmd);
    virtual void cancel_order(pubsub::TCommand &tcmd);
    virtual void query_order(pubsub::TCommand &tcmd);

#ifdef USE_WEBSOCKET_API
    virtual void ws_add_new_order(pubsub::TCommand &tcmd);
    virtual void ws_cancel_order(pubsub::TCommand &tcmd);
    virtual void ws_query_order(pubsub::TCommand &tcmd);
#endif

protected:
    void query_one_order(pubsub::TCommand &tcmd);
    void query_multi_orders(pubsub::TCommand &tcmd);
//    sm::SecurityManager *smc;
//    web::uri m_balanceUrl;
    web::uri m_positionUrl;
    web::uri m_positionsUrl;
    web::uri m_accountsUrl;
    web::uri m_orderUrl;
    void monitor();

    // virtual void pong();
    void ping();

    void on_websocket_msg(const websocket_incoming_message& msg);
    void sub_websocket();
    void on_close_msg(websocket_close_status close_status, const utility::string_t& reason,
                              const std::error_code& error);
    // http_client *addNewOrderRestClient;
    websocket_outgoing_message sub_balance_channel();
    websocket_outgoing_message sub_orders_channel();
    // websocket_outgoing_message sub_trades_channel();
    websocket_outgoing_message sub_positions_channel();
    // websocket_outgoing_message sub_position_close_channel();
    websocket_outgoing_message sub_login();

};
