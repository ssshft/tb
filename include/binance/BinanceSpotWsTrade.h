#pragma once
#include "base/BaseTrade.h"


class BinanceSpotWsTradeUnit : public BaseTradeUnit {

public:
    BinanceSpotWsTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~BinanceSpotWsTradeUnit();

    virtual void subWebsocekt();
    virtual void onWebsocketMsg(const web::websockets::client::websocket_incoming_message& msg);
    virtual void ping();
    virtual void pong();
    void pong(const std::string& payload);

    virtual void query_account(const pubsub::TCommand& tcmd);
    virtual void query_balance(const pubsub::TCommand& tcmd);
    virtual void query_position(const pubsub::TCommand& tcmd);
    virtual void add_new_order(const pubsub::TCommand& tcmd);
    virtual void cancel_order(const pubsub::TCommand& tcmd);
    virtual void query_order(const pubsub::TCommand& tcmd);

    std::string buildWsSigPayload(std::vector<std::pair<std::string, std::string>> kvs);

    bool init_ed25519_key_from_cfg();
    std::string sign_ed25519_base64(const std::string& payload) const;
    void init_trade_ws_client();
    void trade_ws_logon();
    void on_trade_ws_msg(cosnt std::string& body);
    void on_trade_ws_close(web::websockets::client::websocket_close_status st, const utility::string_t& reason, const std::error_code& ec);
    void on_trade_ws_order_rpt(pubsub::RCommand& rcmd); // ws 下单回报处理

    bool add_new_order_ws(const pubsub::TCommand& tcmd, pubsub::RCommand& rcmd, const sm::InstrumentInfo& info, const std::string& price, const std::string& amount);
    bool cancel_order_ws(const pubsub::TCommand& tcmd, pubsub::RCommand& rcmd, const sm::InstrumentInfo& info);

    void init_user_ws_client();
    void on_user_ws_msg(const std::string& body);
    void on_user_ws_close(web::websockets::client::websocket_close_status st, const utility::string_t& reason, const std::error_code& ec);
    void user_ws_subscribe_signature();

};