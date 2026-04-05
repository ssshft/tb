#pragma once
#include <vector>
#include <chrono>
#include <thread>
#include <iostream>
#include <openssl/hmac.h>
#include "securitymanager.h"
#include <cpprest/ws_client.h>
#include <cpprest/http_client.h>
#include <cpprest/http_msg.h>
#include "cpprest/json.h"
#include <cpprest/filestream.h>
#include <unordered_map>

#include "crypto_exception.h"
#include "time_util.h"
#include "string_util.h"
#include "data_struct.h"
#include "utils/tb_global.h"
#include "pubsub_protocol.h"
#include "precision_util.h"
#include "utils/order_util.h"


using namespace web;
using namespace om;
using namespace pubsub;
using namespace web::websockets::client;
using namespace web::http;
using namespace web::http::client;
using namespace concurrency::streams;

constexpr auto kTradeTimeOutTime = 240; //seconds

#define START_SUB_WEBSOCKET(builder) \
    LOG_INFO("start to sub websocket to {}", acc.wsUrl); \
    if (pWsClient != nullptr) { \
        auto oldWsClient = pWsClient; \
        std::thread([oldWsClient]() { \
            try { \
                oldWsClient->set_close_handler([](websocket_close_status, const utility::string_t&, const std::error_code&) { \
                    LOG_INFO("old websocket callback client closed successfully!"); \
                }); \
                oldWsClient->close().wait(); \
                LOG_INFO("old websocket callback client closed successfully!"); \
            } \
            catch (...) { \
                LOG_ERROR("Exception during old websocket cleanup!"); \
            } \
        }).detach(); \
        pWsClient = nullptr; \
    } \
    try { \
        LOG_INFO("creating new websocket callback client!"); \
        pWsClient = std::make_shared<websocket_callback_client>(); \
        LOG_INFO("new websocket callback client created!"); \
    } \
    catch (const std::exception& e) { \
        LOG_ERROR("failed to create websocket callback client: {}", e.what()); \
        return; \
    } \
    \
    LOG_INFO("{} connecting to {}", ExchangeTypeEnum2StrMap[acc.exchangeTypeEnum], builder.to_string()); \
    std::promise<bool> prom; \
    std::future<bool> fut = prom.get_future(); \
    \
    try { \
        pWsClient->connect(builder.to_string()) \
        .then([&]() { \
            auto selfWs = pWsClient; \
            pWsClient->set_message_handler([this](const web::websockets::client::websocket_incoming_message& msg) { \
                this->onWebsocketMsg(msg); \
            }); \
            pWsClient->set_close_handler([this, selfWs](websocket_close_status close_status, const utility::string_t& reason, const std::error_code& error) { \
                this->onCloseMsg(close_status, reason, error, selfWs); \
            }); \
            prom.set_value(true); \
        }); \
        \
        if (fut.wait_for(std::chrono::seconds(10)) == std::future_status::timeout) { \
            isConnected = false; \
            LOG_ERROR("connected with {} timeout!", builder.to_string()); \
            return; \
        } \
        \
        isConnected = true; \
        LOG_INFO("connected with {} successfully!", builder.to_string()); \
    } \
    catch (const std::exception& e) { \
        isConnected = false; \
        LOG_ERROR("connected with {} exception: {}", builder.to_string(), e.what()); \
    } 


#define FORMAT_REQUEST(request) \
    request.headers().add("Accept", "application/json"); \
    request.headers().add("Content-Type", "application/json"); \
    request.headers().add("Connection", "Keep-Alive"); \
    request.headers().add("Keep-Alive", "timeout=120, max=1000"); \


#define FROMAT_BINANCE_REQUEST(request) \
    request.headers().add("Accept", "application/json"); \
    request.headers().add("Content-Type", "application/json"); \
    request.headers().add("Connection", "Keep-Alive"); \
    request.headers().add("Keep-Alive", "timeout=120, max=1000"); \
    request.headers().add("X-MBX-APIKEY", acc.apiKey); \


#define START_FORMAT_RESPONSE(request) \
    restClient.request(request).then([=](web::http::http_response response) { \
        auto code = response.status_code(); \
        switch (code) { \
            case web::http::status_codes::Created: \
            case web::http::status_codes::OK: \
            case web::http::status_codes::Unauthorized: \
            case web::http::status_codes::ServiceUnavailable: \
            case web::http::status_codes::BadRequest: \
            case web::http::status_codes::NotFound: \
            case web::http::status_codes::TooManyRequests: \
            case web::http::status_codes::InternalError: \
            case web::http::status_codes::BadGateway: \
            case web::http::status_codes::Forbidden: \
            case web::http::status_codes::GatewayTimeout: \
            case web::http::status_codes::RequestTimeout: \
            case 418: { \
                return response.extract_json(); \
            } \
            default: { \
                return pplx::task_from_exception<web::json::value>(crypto::crypto_exception(response.to_string(), code, __FILE__, __LINE__)); \
            } \
        } \
    }).then([=](pplx::task<web::json::value> previousTask) mutable { \
        try {


#define END_FORMAT_RESPONSE(request) \
        } \
        catch (const crypto::crypto_exception& e) { \
            LOG_ERROR("excetion code: {}, error: {}", e.getErrorNum(), e.what()); \
        } \
        catch (const std::exception& e) { \
            LOG_ERROR("{}", e.what()); \
        } \
        catch (...) { \
            LOG_ERROR("unknown error: {}", acc.accountId); \
        } \
    });

#define ADD_NEW_ORDER_END_FROMAT_RESPONSE(request) \
        } \
        catch (const crypto::crypto_exception& e) { \
            rcmd.body.newOrder.errorId = e.getErrorNum(); \
            rcmd.body.newOrder.orderStatus = OS_REJECTED; \
            std::string originMsg(e.what()); \
            crypto::replace_string(originMsg, ":", ""); \
            crypto::replace_string(originMsg, "\"", ""); \
            strncpy(rcmd.body.newOrder.originMsg, originMsg.c_str(), ORIGINMSG_SIZE - 1); \
            PUSH_RCMD(rcmd); \
            return; \
        } \
        catch (const std::exception& e) { \
            rcmd.body.newOrder.orderStatus = OS_REJECTED; \
            std::string originMsg(e.what()); \
            crypto::replace_string(originMsg, ":", ""); \
            crypto::replace_string(originMsg, "\"", ""); \
            strncpy(rcmd.body.newOrder.originMsg, originMsg.c_str(), ORIGINMSG_SIZE - 1); \
            PUSH_RCMD(rcmd); \
            return; \
        } \
        catch(...) { \
            LOG_ERROR("unknown error: {}", acc.accountId); \
        } \
    });


   
#define CANCEL_ORDER_END_FROMAT_RESPONSE(request) \
        } \
        catch (const crypto::crypto_exception& e) { \
            rcmd.body.newOrder.errorId = e.getErrorNum(); \
            rcmd.body.newOrder.orderStatus = OS_FAILED; \
            std::string originMsg(e.what()); \
            crypto::replace_string(originMsg, ":", ""); \
            crypto::replace_string(originMsg, "\"", ""); \
            strncpy(rcmd.body.newOrder.originMsg, originMsg.c_str(), ORIGINMSG_SIZE - 1); \
            PUSH_RCMD(rcmd); \
            return; \
        } \
        catch (const std::exception& e) { \
            rcmd.body.newOrder.orderStatus = OS_FAILED; \
            std::string originMsg(e.what()); \
            crypto::replace_string(originMsg, ":", ""); \
            crypto::replace_string(originMsg, "\"", ""); \
            strncpy(rcmd.body.newOrder.originMsg, originMsg.c_str(), ORIGINMSG_SIZE - 1); \
            PUSH_RCMD(rcmd); \
            return; \
        } \
        catch(...) { \
            LOG_ERROR("unknown error: {}", acc.accountId); \
        } \
    });



class BaseTradeUnit {

public:
    BaseTradeUnit(AccountCfg& a, sm::SecurityManager* s);
    virtual ~BaseTradeUnit();

    void start();
    virtual void monitorWs();
    virtual void subWebsocekt() = 0;
    virtual void onWebsocketMsg(const web::websockets::client::websocket_incoming_message& msg) = 0;
    virtual void ping() = 0;
    virtual void pong() = 0;
    virtual void onCloseMsg(web::websockets::client::websocket_close_status status, const utility::string_t& reason, const std::error_code& code, std::shared_ptr<websocket_callback_client> selfWs);

    virtual void query_account(const pubsub::TCommand& tcmd) = 0;
    virtual void query_balance(const pubsub::TCommand& tcmd) = 0;
    virtual void query_position(const pubsub::TCommand& tcmd) = 0;
    virtual void add_new_order(const pubsub::TCommand& tcmd) = 0;
    virtual void cancel_order(const pubsub::TCommand& tcmd) = 0;
    virtual void query_order(const pubsub::TCommand& tcmd) = 0;

protected:
    AccountCfg acc;
    bool isConnected{false}; // 是否需要定义成atomic
    std::atomic_long latestPingPongTime;
    std::shared_ptr<websocket_callback_client> pWsClient{nullptr};
    web::http::client::http_client* pRestClient{nullptr};
    sm::SecurityManager* smc{nullptr};
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
