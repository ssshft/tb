#pragma once
#include "api/api_base/tradingapi_base.h"

#define FORMAT_BYBIT_REQUEST(request) \
    request.headers().add("Connection", "Keep-Alive");\
    request.headers().add("Keep-Alive", "timeout=3600, max=100000");\
    request.headers().add("X-BAPI-API-KEY", m_curcfg.apiKey);\
    request.headers().add("X-BAPI-TIMESTAMP", time);\
    request.headers().add("X-BAPI-RECV-WINDOW", rec_window);\
    request.headers().add("X-BAPI-SIGN", sign);\


class BybitSwapTradingClient{
public:
    BybitSwapTradingClient();
    virtual ~BybitSwapTradingClient();

    void Run();
    // void Terminate();
    // void OnThreadIdle();

    const AccountCfg& getAccInfo(){return m_curcfg;}
    virtual bool Initialize(AccountCfg& cfg, sm::SecurityManager *smc);
    virtual void add_new_order(pubsub::TCommand &tcmd);
    virtual void cancel_order(pubsub::TCommand &tcmd);
    virtual void query_order(pubsub::TCommand &tcmd);
    // virtual void query_account(pubsub::TCommand &tcmd);
    virtual bool get_balances(pubsub::TCommand &tcmd);
    virtual bool get_positions(pubsub::TCommand &tcmd);
private:
    virtual void ping();
    void on_websocket_msg(const websocket_incoming_message& msg);
    string params_string(std::map<std::string, std::string> const &param, int type);
    string get_signature_rest(long timestamp, int recv_window, std::map<std::string,
                 std::string> const &params, int type);
    string get_signature_ws(const string &timestamp, const string &data);

    virtual void query_one_order(pubsub::TCommand &tcmd);
    virtual void query_multi_orders(pubsub::TCommand &tcmd);
    virtual void monitor();
    virtual void sub_websocket();
    virtual void on_close_msg(websocket_close_status close_status, const utility::string_t& reason,
                      const std::error_code& error);
    virtual void login();
    virtual void sub_channels();
    // virtual void sub_orders_channel();
    // virtual void add_new_order_ws(pubsub::TCommand &tcmd);
    // virtual void cancel_order_ws(pubsub::TCommand &tcmd);

protected:
    sm::SecurityManager *smc;

    websocket_callback_client wsClient;
    bool m_IsConnected;
    web::uri m_orderUrl;
    web::uri m_cancelOrderUrl;
    web::uri m_queryOrderUrl;
    web::uri m_queryOrderHistoryUrl;
    web::uri m_balanceUrl;
    web::uri m_positionUrl;

    AccountCfg  m_curcfg;
    // http_client *addNewOrderRestClient;
    // http_client *cancelOrderRestClient;
    // http_client *hotHttpClient;
    crypto::RestClientCPP *hotHttpClient;
    // crypto::RestClientCPP *addHotHttpClient;
    // shared_ptr<crypto::RestClientCPP> hotHttpClient;
    // shared_ptr<crypto::RestClientCPP> addHotHttpClient;

};
