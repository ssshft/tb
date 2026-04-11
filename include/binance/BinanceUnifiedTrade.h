#pragma once
#include "base/BaseTrade.h"


class BinanceUnifiedTradeUnit : public BaseTradeUnit {

public:
    BinanceUnifiedTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~BinanceUnifiedTradeUnit();

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

    void query_adl(const pubsub::TCommand& tcmd, std::unordered_map<std::string, double>& m);
    void genListenKey();
    void keepListenKey();

private:
    web::uri wsSubUrl{""};
    web::uri listenKeyUrl{""};
    web::uri adlUrl{""};
    std::string listenKey;
};