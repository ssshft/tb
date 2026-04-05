#pragma once
#include "binance/BinanceSpotTradingClient.h"

class BinanceUnifiedTradingClient : public BinanceSpotTradingClient {

public:
    BinanceUnifiedTradingClient();
    ~BinanceUnifiedTradingClient();

    void Run();
    bool Initialize(AccountCfg& cfg, sm::SecurityManager *smc);

    bool get_balances(pubsub::TCommand &tcmd);
    bool get_account(pubsub::TCommand &tcmd);
    bool get_positions(pubsub::TCommand &tcmd);
    bool get_adlquantile(pubsub::TCommand &tcmd);
    void add_new_order(pubsub::TCommand &tcmd);
    void cancel_order(pubsub::TCommand &tcmd);
    void query_order(pubsub::TCommand &tcmd);

protected:
    web::uri m_accountUrl;
    
    void query_one_order(pubsub::TCommand &tcmd);
    void query_multi_orders(pubsub::TCommand &tcmd);
//    web::uri m_positionsUrl;
//    virtual void gen_listen_key();
//    virtual void keep_listen_key();
    void monitor();
    void sub_websocket();
    void on_websocket_msg(const websocket_incoming_message& msg);
//    void on_close_msg(websocket_close_status close_status, const utility::string_t& reason,
//                              const std::error_code& error);
};