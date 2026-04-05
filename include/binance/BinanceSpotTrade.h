#pragma once
#include "base/BaseTrade.h"


class BinanceSpotTradeUnit : public BaseTradeUnit {

public:
    BinanceSpotTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~BinanceSpotTradeUnit();

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

private:
    web::uri newOrderUrl{""};
    web::uri cancelOrderUrl{""};
    web::uri queryOrderUrl{""};
    web::uri balanceUrl{""};
    web::uri positionUrl{""};
    web::uri adlUrl{""};
};