#include "base/BaseTrade.h"


BaseTradeUnit::BaseTradeUnit(AccountCfg& a, sm::SecurityManager* s) {
    isConnected.store(0);
    acc = a;
    smc = s;
}

BaseTradeUnit::~BaseTradeUnit() {
}

void BaseTradeUnit::start() {
    try {
        subWebsocekt();
    }
    catch (const std::exception& e) {
        LOG_ERROR("trade start error: {}", e.what());
    }
}

void BaseTradeUnit::onOpen() {
    isConnected.store(true);
    LOG_INFO("TB {}.{}.{} ws opened", ExchangeTypeEnum2StrMap[acc.exchangeTypeEnum], InstTypeEnum2StrMap[acc.instTypeEnum], acc.accountId);
}

void BaseTradeUnit::onCloseMsg(int code, const std::string& reason) {
    isConnected.store(false);
    LOG_ERROR("TB {} ws closed: code={} reason={} (BeastWsClient will auto-reconnect)", acc.accountId, code, reason);
}

void BaseTradeUnit::onError(const std::string& reason) {
    isConnected.store(false);
    LOG_ERROR("TB {} ws error: {}", acc.accountId, reason);
}

void BaseTradeUnit::initRestClient(const std::string& host, std::vector<std::pair<std::string, std::string>> default_headers, size_t max_connections) {
    net::RestClientConfig cfg;
    cfg.host = host;
    cfg.port = 443;
    cfg.use_tls = true;
    cfg.verify_peer = false;
    cfg.max_connections = max_connections;
    cfg.parallel_establish_threads = std::min<size_t>(4, max_connections);
    cfg.request_queue_capacity = 256;
    cfg.request_timeout_ms = 30000;

    try {
        pRestClient = std::make_shared<net::RestClient>(cfg);
        restHost = host;
        defaultHeaders_ = std::move(default_headers);
        for (auto& kv : defaultHeaders_) {
            pRestClient->set_default_header(kv.first, kv.second);
        }

        LOG_INFO("Tb {} rest client to {} ready ({} conns)", acc.accountId, host, max_connections);
    }
    catch(const std::exception& e) {
        LOG_ERROR("TB {} initRestClient({}) failed: {}", acc.accountId, host, e.what());
        pRestClient.reset();
    }
}

void BaseTradeUnit::asyncRequest(boost::beast::http::verb method, std::string path, std::string body, std::string content_type, net::HttpCallback cb) {
    if (!pRestClient) {
        LOG_ERROR("TB {} asyncRequest: pRestClient not initialized (call initRestClient first)", acc.accountId);
        if (cb) {
            boost::system::error_code ec(static_cast<int>(std::errc::not_connected), boost::system::system_category());
            cb(ec, net::HttpResponse());
        }
    }

    pRestClient->async_request(method, std::move(path), std::move(body), std::move(content_type), std::move(cb));
}

void BaseTradeUnit::asyncRequest(boost::beast::http::verb method, std::string path, std::string body, std::string content_type, std::vector<std::pair<std::string, std::string>> extra_headers, net::HttpCallback cb) {
    if (!pRestClient) {
        LOG_ERROR("TB {} asyncRequest: pRestClient not initialized (call initRestClient first)", acc.accountId);
        if (cb) {
            boost::system::error_code ec(static_cast<int>(std::errc::not_connected), boost::system::system_category());
            cb(ec, net::HttpResponse());
        }
    }

    pRestClient->async_request(method, std::move(path), std::move(body), std::move(content_type), std::move(extra_headers), std::move(cb));
}

void BaseTradeUnit::subWebsocketWithConfig(net::WsConfig cfg) {
    pWsClient = net::WsClient::create(std::move(cfg));

    pWsClient->on_open([this]() {
        this->onOpen();
    });

    pWsClient->on_message([this](const uint8_t* d, size_t n, bool b, int64_t t) {
        this->onWebsocketMsg(d, n, b, t);
    });

    pWsClient->on_close([this](int c, const::std::string& r) {
        this->onCloseMsg(c, r);
    });

    pWsClient->on_error([this](const std::string& m) {
        this->onError(m);
        
    });

    pWsClient->start();
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
        acc.accountId = accountId;
        acc.strategyId = strategyId;
        acc.apiKey = account["apiKey"].GetString();
        acc.secretKey = account["secretKey"].GetString();
        acc.password = account["password"].GetString();
        acc.userId = account["userId"].GetString();
        acc.isSimulated = account["isSimulated"].GetBool();
        acc.restUrl = account["restUrl"].GetString();
        acc.wsUrl = account["wsUrl"].GetString();
        acc.apiMode = ApiModeStr2EnumMap[account["apiMode"].GetString()];
        vAccount.push_back(acc);
    }
    smc = s;
}

BaseTradeClient::~BaseTradeClient() {

}