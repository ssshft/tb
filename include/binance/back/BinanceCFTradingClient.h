#pragma once
#include "binance/BinanceSpotTradingClient.h"

class BinanceCFTradingClient : public BinanceSpotTradingClient {

public:
    BinanceCFTradingClient();
    virtual ~BinanceCFTradingClient();

    void Run();

    virtual bool Initialize(AccountCfg& cfg, sm::SecurityManager *smc);

    virtual void add_new_order(pubsub::TCommand &tcmd);
    virtual void cancel_order(pubsub::TCommand &tcmd);
    virtual void query_order(pubsub::TCommand &tcmd);
    bool get_balances();
    bool get_positions();
    bool get_adlquantile();
protected:
    virtual void query_one_order(pubsub::TCommand &tcmd);
    virtual void query_multi_orders(pubsub::TCommand &tcmd);
//    web::uri m_positionsUrl;
//    virtual void gen_listen_key();
//    virtual void keep_listen_key();
    virtual void monitor();
    virtual void sub_websocket();
    void on_websocket_msg(const websocket_incoming_message& msg);
//    void on_close_msg(websocket_close_status close_status, const utility::string_t& reason,
//                              const std::error_code& error);
};