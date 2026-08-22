#include "bybit/BybitTradeClient.h"


BybitTradeClient::BybitTradeClient(rapidjson::Value& accCfg, sm::SecurityManager* s)
    : BaseTradeClient(accCfg, s) {
    if (!vAccount.empty()) {
        if (vAccount[0].apiMode == AM_REST) {
            tradeUnit = new BybitTradeUnit(vAccount[0], smc);
        }
        else if (vAccount[0].apiMode == AM_WS) {
            tradeUnit = new BybitWsTradeUnit(vAccount[0], smc);
        }
    }
}

BybitTradeClient::~BybitTradeClient() {
    if (tradeUnit) { 
        delete tradeUnit; 
        tradeUnit = nullptr; 
    }
}

void BybitTradeClient::start() {
    if (tradeUnit) {
        tradeUnit->start();
    }
    initial();
}

void BybitTradeClient::initial() {
    // Bybit WS auth 之后自动 push 一次账户/持仓, 这里不再主动 REST 拉。
}

void BybitTradeClient::query_account(const pubsub::TCommand& tcmd) { 
    if (tradeUnit) {
        tradeUnit->query_account(tcmd);
    }  
}

void BybitTradeClient::query_balance(const pubsub::TCommand& tcmd) { 
    if (tradeUnit) {
        tradeUnit->query_balance(tcmd); 
    } 
}

void BybitTradeClient::query_position(const pubsub::TCommand& tcmd) { 
    if (tradeUnit) {
        tradeUnit->query_position(tcmd);
    } 
}

void BybitTradeClient::add_new_order(const pubsub::TCommand& tcmd) { 
    if (tradeUnit) {
        tradeUnit->add_new_order(tcmd); 
    } 
}

void BybitTradeClient::cancel_order(const pubsub::TCommand& tcmd) { 
    if (tradeUnit) {
        tradeUnit->cancel_order(tcmd); 
    }  
}

void BybitTradeClient::query_order(const pubsub::TCommand& tcmd) { 
    if (tradeUnit) {
        tradeUnit->query_order(tcmd); 
    }   
}