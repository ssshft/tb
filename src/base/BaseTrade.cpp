#include "base/BaseTrade.h"
#include "hft_metrics.h"


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
    LOG_INFO("TB {}.{}.{} ws opened", ExchangeTypeEnum2StrMap[acc.exchangeTypeEnum], InstTypeEnum2StrMap[acc.instTypeEnum], acc.accountName);
}

void BaseTradeUnit::onCloseMsg(int code, const std::string& reason) {
    isConnected.store(false);
    LOG_ERROR("TB {} ws closed: code={} reason={} (BeastWsClient will auto-reconnect)", acc.accountName, code, reason);
}

void BaseTradeUnit::onError(const std::string& reason) {
    isConnected.store(false);
    LOG_ERROR("TB {} ws error: {}", acc.accountName, reason);
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
    cfg.idle_close_after_ms = 15000;

    try {
        pRestClient = std::make_shared<net::RestClient>(cfg);
        restHost = host;
        defaultHeaders_ = std::move(default_headers);
        for (auto& kv : defaultHeaders_) {
            pRestClient->set_default_header(kv.first, kv.second);
        }

        LOG_INFO("Tb {} rest client to {} ready ({} conns)", acc.accountName, host, max_connections);
    }
    catch(const std::exception& e) {
        LOG_ERROR("TB {} initRestClient({}) failed: {}", acc.accountName, host, e.what());
        pRestClient.reset();
    }
}

void BaseTradeUnit::asyncRequest(boost::beast::http::verb method, std::string path, std::string body, std::string content_type, net::HttpCallback cb) {
    if (!pRestClient) {
        LOG_ERROR("TB {} asyncRequest: pRestClient not initialized (call initRestClient first)", acc.accountName);
        if (cb) {
            boost::system::error_code ec(static_cast<int>(std::errc::not_connected), boost::system::system_category());
            cb(ec, net::HttpResponse());
        }
    }

    pRestClient->async_request(method, std::move(path), std::move(body), std::move(content_type), std::move(cb));
}

void BaseTradeUnit::asyncRequest(boost::beast::http::verb method, std::string path, std::string body, std::string content_type, std::vector<std::pair<std::string, std::string>> extra_headers, net::HttpCallback cb) {
    if (!pRestClient) {
        LOG_ERROR("TB {} asyncRequest: pRestClient not initialized (call initRestClient first)", acc.accountName);
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
        // 一条私有 WS 报文从回调进来到 PUSH_RCMD 完成（解析 + 填 rcmd + 进进程内队列）。
        //
        // 这段跑在**每个连接一个的 WS 线程**上，不跟 OMS 串行线程抢同一个核；
        // 所以它慢不会拖慢 OMS，只会推迟"这条回报什么时候排进 tb2OmsRCommandInnerQueue"。
        // 真正要跟 tb.exec.idle 一起看：WS 侧几十 µs + 轮询上界 = 回报进 OMS 前的总延迟。
        //
        // 按交易所分开统计：四家的解析器完全不是一个形状（Binance 是单条 executionReport、
        // Gate 是数组要循环、OKX 没有 instType 要逐个试），混成一个指标时
        // p50 会被最快那家拉低、看不出是谁慢。
        // ⚠️ 名字必须是字面量（registry 只存 const char*，拼出来的会悬垂），
        // 所以只能写成 if/else 链 —— 加一家 = 加一条 else if。
        if (acc.exchangeTypeEnum == BINANCE) {
            HFT_SCOPE("tb.ws.binance");
            this->onWebsocketMsg(d, n, b, t);
        }
        else if (acc.exchangeTypeEnum == OKX) {
            HFT_SCOPE("tb.ws.okx");
            this->onWebsocketMsg(d, n, b, t);
        }
        else if (acc.exchangeTypeEnum == BYBIT) {
            HFT_SCOPE("tb.ws.bybit");
            this->onWebsocketMsg(d, n, b, t);
        }
        else if (acc.exchangeTypeEnum == GATEIO) {
            HFT_SCOPE("tb.ws.gateio");
            this->onWebsocketMsg(d, n, b, t);
        }
        else {
            // 未覆盖的交易所不埋点，但**照常处理** —— 埋点绝不能改变业务路径。
            // 同时计数：`acc.exchangeTypeEnum` 来自 `ExchangeTypeStr2EnumMap[exchId]`，
            // 而 operator[] 在 key 缺失时**不报错、插入 0**（= ExchangeType_MIN）
            // ⇒ 这个计数器非零就说明配置里的 exchId 没被映射到，上面四条一条都不会命中。
            // 不数它的话，现象是"tb.ws.* 一个指标都不出现"，看起来像没埋点，其实是被静默跳过。
            HFT_CNT("tb.ws.unlisted");
            this->onWebsocketMsg(d, n, b, t);
        }
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
    int accountId = std::stoi(accCfg["accountId"].GetString());
    std::string accountName = accCfg["accountName"].GetString();
    for (rapidjson::SizeType i = 0; i < accCfg["accounts"].Size(); ++i) {
        rapidjson::Value& account = accCfg["accounts"][i];

        AccountCfg acc;
        acc.exchangeTypeEnum = ExchangeTypeStr2EnumMap[exchId];
        acc.instTypeEnum = InstTypeStr2EnumMap[account["instType"].GetString()];
        acc.accountId = accountId;
        acc.accountName = accountName;
        acc.strategyId = strategyId;
        acc.apiKey = account["apiKey"].GetString();
        acc.secretKey = account["secretKey"].GetString();
        acc.password = account["password"].GetString();
        acc.userId = account["userId"].GetString();
        acc.isSimulated = account["isSimulated"].GetBool();
        acc.restUrl = account["restUrl"].GetString();
        acc.wsUrl = account["wsUrl"].GetString();
        if (account.HasMember("wsTradeUrl")) {
            acc.wsTradeUrl = account["wsTradeUrl"].GetString();
        }
        acc.apiMode = ApiModeStr2EnumMap[account["apiMode"].GetString()];
        vAccount.push_back(acc);
    }
    smc = s;
}

BaseTradeClient::~BaseTradeClient() {

}