#include "base/BaseTrade.h"


BaseTradeUnit::BaseTradeUnit(AccountCfg& a, sm::SecurityManager* s) {
    isConnected = "";
    latestPingPongTime.store(0);
    pWsClient = nullptr;
    pRestClient = nullptr;
    acc = a;
    smc = s;
}

BaseTradeUnit::~BaseTradeUnit() {

}

void BaseTradeUnit::start() {
    try {
        std::thread monitorThread(&BaseTradeUnit::monitorWs, this);
        monitorThread.detach();
    }
    catch (const std::exception& e) {
        LOG_ERROR("trade start error: {}", e.what());
    }
}

void BaseTradeUnit::monitorWs() {
    while (1) {
        try {
            LOG_INFO("TB {} start to connect ws: {}", acc.accountId, acc.wsUrl);
            subWebsocekt();
            std::this_thread::sleep_for(std::chrono::seconds(5));
            while (isConnected) {
                std::this_thread::sleep_for(std::chrono::seconds(10));
                long now = crypto::getCurrentTimeSeconds();
                long latestTime = latestPingPongTime.load();

                long diff = now - latestTime;
                if (diff > kTradeTimeOutTime) {
                    LOG_ERROR("trade: {} lastPingPongTime: {} too old, time diff: {} seconds, will reconnect now.", acc.accountId, latestTime, diff);
                    break;
                }

                if (!isConnected) {
                    LOG_WARN("trade: {}, wsUrl: {} address disconnected, connecting now", acc.accountId, acc.wsUrl);
                    break;
                }
                else {
                    switch (acc.exchangeTypeEnum) {
                        case BINANCE: {
                            break;
                        }
                        default: {
                            LOG_INFO("{}.{}.{} ws is connected, will send ping!", ExchangeTypeEnum2StrMap[acc.exchangeTypeEnum], InstTypeEnum2StrMap[acc.instTypeEnum], acc.accountId);
                            ping();
                            break;
                        }  
                    }   
                }
            }
        }
        catch (const std::exception& e) {
            isConnected = false;
            LOG_ERROR("ws connect exception: {}", e.what());
        }

        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

void BaseTradeUnit::onCloseMsg(web::websockets::client::websocket_close_status status, const utility::string_t& reason, const std::error_code& code, std::shared_ptr<websocket_callback_client> selfWs) {
    try {
        if (selfWs != pWsClient) {
            LOG_INFO("DB old websocket callback client closed successfully!");
            return;
        }

        isConnected = false;
        LOG_ERROR("DB receive close msg, reason: {}, error: {}", reason, code.message());

        if (crypto::str_cmp(reason.c_str(), "End of File") || crypto::str_cmp(reason.c_str(), "Underlying Transport Error") || crypto::str_cmp(reason.c_str(), "Normal")) {
            return;
        }
    }
    catch (const std::exception& e) {
        LOG_ERROR("{}", e.what());
    }
}


BaseTradeClient::BaseTradeClient(rapidjson::Value& accCfg, sm::SecurityManager* s) {
    vAccount.clear();

    std::string exchId = accCfg["exchId"].GetString();
    std::string strategyId = accCfg["strategyId"].GetString();
    std::string accountId = accCfg["accountId"].GetString();
    for (rapidjson::SizeType i = 0; i < accCfg["accounts"].Size(); ++i) {
        rapidjson::Value& account = accCfg["accounts"][i];

        AccountCfg acc;
        acc.exchangeTypeEnum = ExchangeTypeStr2EnumMap[exchId];
        acc.instTypeEnum = InstTypeStr2EnumMap[account["instType"].GetString()];
        acc.apiKey = account["apiKey"].GetString();
        acc.secretKey = account["secretKey"].GetString();
        acc.password = account["password"].GetString();
        acc.isSimulated = account["isSimulated"].GetBool();
        acc.restUrl = account["restUrl"].GetString();
        acc.wsUrl = account["wsUrl"].GetString();
        vAccount.push_back(acc);
    }

    smc = s;
}

BaseTradeClient::~BaseTradeClient() {

}