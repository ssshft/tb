#include "okx/OkxTradeClient.h"


OkxTradeClient::OkxTradeClient(rapidjson::Value& accCfg, sm::SecurityManager* s)
    : BaseTradeClient(accCfg, s) {
    if (!vAccount.empty()) {
        // OKX 所有 instType 走一个 ws + rest 连接, 拿 vAccount[0] 建 unit 即可。
        tradeUnit = new OkxTradeUnit(vAccount[0], smc);
    }
}

OkxTradeClient::~OkxTradeClient() {
    if (tradeUnit) { delete tradeUnit; tradeUnit = nullptr; }
}

void OkxTradeClient::start() {
    if (tradeUnit) tradeUnit->start();
    initial();
}

void OkxTradeClient::initial() {
    // OKX 订阅时会自动 push 一次账户/持仓, 这里不再主动 REST 拉。
    // 若某些场景要保底, 可以 uncomment 下面两行:
    // pubsub::TCommand tcmd{}; query_balance(tcmd); query_position(tcmd);
}

void OkxTradeClient::query_account (const pubsub::TCommand& tcmd) { if (tradeUnit) tradeUnit->query_account(tcmd);  }
void OkxTradeClient::query_balance (const pubsub::TCommand& tcmd) { if (tradeUnit) tradeUnit->query_balance(tcmd);  }
void OkxTradeClient::query_position(const pubsub::TCommand& tcmd) { if (tradeUnit) tradeUnit->query_position(tcmd); }
void OkxTradeClient::add_new_order (const pubsub::TCommand& tcmd) { if (tradeUnit) tradeUnit->add_new_order(tcmd);  }
void OkxTradeClient::cancel_order  (const pubsub::TCommand& tcmd) { if (tradeUnit) tradeUnit->cancel_order(tcmd);   }
void OkxTradeClient::query_order   (const pubsub::TCommand& tcmd) { if (tradeUnit) tradeUnit->query_order(tcmd);    }