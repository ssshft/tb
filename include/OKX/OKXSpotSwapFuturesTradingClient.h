#pragma once
#include "api/api_base/tradingapi_base.h"


class OKXSpotSwapFuturesTradingClient{
public:
    OKXSpotSwapFuturesTradingClient();
    virtual ~OKXSpotSwapFuturesTradingClient();

    void Run();
    // void Terminate();
    // void OnThreadIdle();

    const AccountCfg& getAccInfo(){return m_curcfg;}
    virtual bool Initialize(AccountCfg& cfg, sm::SecurityManager *smc);
    virtual void add_new_order(pubsub::TCommand &tcmd);
    virtual void cancel_order(pubsub::TCommand &tcmd);
    virtual void query_order(pubsub::TCommand &tcmd);
    // virtual void query_account(pubsub::TCommand &tcmd);
    virtual bool get_balances();
    virtual bool get_positions();
private:
    virtual void ping();
    void on_websocket_msg(const websocket_incoming_message& msg);
    string get_signature_rest(const string &timestamp, const string &method,
            const string &requestPath,const string &body);
    virtual void query_one_order(pubsub::TCommand &tcmd);
    virtual void query_multi_orders(pubsub::TCommand &tcmd);
    virtual void monitor();
    virtual void sub_websocket();
    virtual void on_close_msg(websocket_close_status close_status, const utility::string_t& reason,
                      const std::error_code& error);
    virtual void login();
    virtual void sub_balances_positions_channel();
    virtual void sub_orders_channel();
    virtual void sub_account_channel();
    virtual void sub_positions_channel();
    virtual void add_new_order_ws(pubsub::TCommand &tcmd);
    virtual void cancel_order_ws(pubsub::TCommand &tcmd);

    std::string base64_encode(unsigned char const * input, size_t len);
    string base64_encode(std::string const & input);
    int HmacEncode(const char * algo,
               const char * key, unsigned int key_length,
               const char * input, unsigned int input_length,
               unsigned char * &output, unsigned int &output_length);
    string getSignature(const string &query,const string &apiSecret);

protected:
    sm::SecurityManager *smc;

    websocket_callback_client wsClient;
    bool m_IsConnected;
    web::uri m_orderUrl;
    web::uri m_cancelOrderUrl;
    web::uri m_balanceUrl;
    web::uri m_positionUrl;
    web::uri m_wssb_url;

    AccountCfg  m_curcfg;
    // http_client *addNewOrderRestClient;
    // http_client *cancelOrderRestClient;
    http_client *hotHttpClient;
};
