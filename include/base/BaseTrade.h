#pragma once
#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include <chrono>
#include <thread>
#include <iostream>
#include <openssl/hmac.h>
#include "securitymanager.h"
#include <unordered_map>

#include "BeastRestClient.h"
#include "BeastWsClient.h"
#include "crypto_exception.h"
#include "time_util.h"
#include "string_util.h"
#include "data_struct.h"
#include "utils/tb_global.h"
#include "pubsub_protocol.h"
#include "precision_util.h"
#include "utils/order_util.h"
#include "log_engine.h"
#include <simdjson.h>
#include "net_helper.h"


class BaseTradeUnit {

public:
    BaseTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~BaseTradeUnit();

    void start();
    virtual void subWebsocekt() = 0;
    virtual void onWebsocketMsg(const uint8_t* data, size_t len, bool isBinary, int64_t recvNs) = 0;
    virtual void onCloseMsg(int code, const std::string& reason);
    virtual void onOpen();
    virtual void onError(const std::string& reason);

    virtual void query_account(const pubsub::TCommand& tcmd) = 0;
    virtual void query_balance(const pubsub::TCommand& tcmd) = 0;
    virtual void query_position(const pubsub::TCommand& tcmd) = 0;
    virtual void add_new_order(const pubsub::TCommand& tcmd) = 0;
    virtual void cancel_order(const pubsub::TCommand& tcmd) = 0;
    virtual void query_order(const pubsub::TCommand& tcmd) = 0;

protected:

    void initRestClient(const std::string& host, std::vector<std::pair<std::string, std::string>> default_headers = {}, size_t max_connections = 4);

    void asyncRequest(boost::beast::http::verb method, std::string path, std::string body, std::string content_type, net::HttpCallback cb);

    void asyncRequest(boost::beast::http::verb method, std::string path, std::string body, std::string content_type, std::vector<std::pair<std::string, std::string>> extra_headers, net::HttpCallback cb);

    void subWebsocketWithConfig(net::WsConfig cfg);

    AccountCfg acc;
    std::atomic<bool> isConnected{false};

    std::shared_ptr<net::RestClient> pRestClient;
    std::shared_ptr<net::WsClient> pWsClient;

    std::string restHost;
    std::vector<std::pair<std::string, std::string>> defaultHeaders_;

    sm::SecurityManager* smc{nullptr};

    thread_local simdjson::ondemand::parser g_parser;

    std::string newOrderUrl{""};
    std::string cancelOrderUrl{""};
    std::string queryOrderUrl{""};
    std::string accountUrl{""};
    std::string balanceUrl{""};
    std::string positionUrl{""};
};


class BaseTradeClient {

public:
    BaseTradeClient(rapidjson::Value& accCfg, sm::SecurityManager* s);
    virtual ~BaseTradeClient();

    virtual void start() = 0;
    virtual void initial() = 0;
    virtual void query_account(const pubsub::TCommand& tcmd) = 0;
    virtual void query_balance(const pubsub::TCommand& tcmd) = 0;
    virtual void query_position(const pubsub::TCommand& tcmd) = 0;
    virtual void add_new_order(const pubsub::TCommand& tcmd) = 0;
    virtual void cancel_order(const pubsub::TCommand& tcmd) = 0;
    virtual void query_order(const pubsub::TCommand& tcmd) = 0;


protected:
    std::vector<AccountCfg> vAccount;
    sm::SecurityManager* smc{nullptr};
};
