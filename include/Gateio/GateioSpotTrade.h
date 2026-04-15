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

    web::websockets::client::websocket_outgoing_message sub_balance_channel();
    web::websockets::client::websocket_outgoing_message sub_orders_channel();

    virtual void query_account(const pubsub::TCommand& tcmd);
    virtual void query_balance(const pubsub::TCommand& tcmd);
    virtual void query_position(const pubsub::TCommand& tcmd);
    virtual void add_new_order(const pubsub::TCommand& tcmd);
    virtual void cancel_order(const pubsub::TCommand& tcmd);
    virtual void query_order(const pubsub::TCommand& tcmd);

protected:
    web::uri unifiedUrl;
};