#pragma once
#include "base/BaseTrade.h"


class BinanceUFTradeUnit : public BaseTradeUnit {

public:
    BinanceUFTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~BinanceUFTradeUnit();

    virtual void monitorWs();
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

    void genListenKey();
    void keepListenKey();

private:
    web::uri wsSubUrl{""};
    web::uri listenKeyUrl{""};
    std::string listenKey;
};