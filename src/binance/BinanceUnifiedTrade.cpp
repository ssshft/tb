#include "binance/BinanceUnifiedTrade.h"

#include <cmath>
#include <future>

#include <fmt/format.h>
#include <simdjson.h>


namespace {
    constexpr int kListenKeyRenewSec = 30 * 60;

    // um / cm 判定
    inline bool is_um(InstType t) {
        return t == USDT_SWAP || t == USDT_FUTURES || t == USDC_SWAP;
    }
    inline bool is_cm(InstType t) {
        return t == C_SWAP || t == C_FUTURES;
    }
    inline bool is_spot_like(InstType t) {
        return t == SPOT || t == MARGIN;
    }
}


BinanceUnifiedTradeUnit::BinanceUnifiedTradeUnit(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {
}

BinanceUnifiedTradeUnit::~BinanceUnifiedTradeUnit() {
    renewStop_.store(true);
    renewCv_.notify_all();
    if (renewThread_.joinable()) renewThread_.join();
}


// ============================================================================
// URL 分流
// ============================================================================
const char* BinanceUnifiedTradeUnit::newOrderPath(InstType t) {
    if (is_spot_like(t)) return "/papi/v1/margin/order";
    if (is_um(t))        return "/papi/v1/um/order";
    if (is_cm(t))        return "/papi/v1/cm/order";
    return nullptr;
}
const char* BinanceUnifiedTradeUnit::cancelOrderPath(InstType t) { return newOrderPath(t); }
const char* BinanceUnifiedTradeUnit::queryOrderPath(InstType t)  { return newOrderPath(t); }
const char* BinanceUnifiedTradeUnit::positionPathFor(InstType t) {
    if (is_um(t)) return "/papi/v1/um/positionRisk";
    if (is_cm(t)) return "/papi/v1/cm/positionRisk";
    return nullptr;
}


// ============================================================================
// 签名 / URL 助手
// ============================================================================
std::string BinanceUnifiedTradeUnit::buildSignedPath(std::string_view basePath,
                                                    const std::vector<std::pair<std::string, std::string>>& kvs) const {
    std::string qs;
    qs.reserve(256);
    for (size_t i = 0; i < kvs.size(); ++i) {
        if (i) qs.push_back('&');
        qs += kvs[i].first;
        qs.push_back('=');
        qs += kvs[i].second;
    }
    std::string sig = crypto::getBinanceSignatureRest(acc.secretKey, qs);

    std::string full;
    full.reserve(basePath.size() + 1 + qs.size() + 11 + sig.size());
    full.append(basePath);
    full.push_back('?');
    full.append(qs);
    full.append("&signature=");
    full.append(sig);
    return full;
}


// ============================================================================
// listenKey
// ============================================================================
bool BinanceUnifiedTradeUnit::generateListenKeySync() {
    std::promise<std::string> prom;
    auto fut = prom.get_future();
    bool responded = false;

    asyncRequest(boost::beast::http::verb::post, listenKeyUrl, /*body=*/"", /*ct=*/"",
        [this, &prom, &responded](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (responded) return;
            responded = true;
            if (ec) { prom.set_value(""); return; }
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) { prom.set_value(""); return; }
                std::string_view lk;
                if (doc["listenKey"].get(lk) == simdjson::SUCCESS) prom.set_value(std::string(lk));
                else                                                prom.set_value("");
            } catch (...) { prom.set_value(""); }
        });

    if (fut.wait_for(std::chrono::seconds(15)) != std::future_status::ready) {
        LOG_ERROR("TB {} PAPI listenKey timeout", acc.accountId);
        return false;
    }
    listenKey_ = fut.get();
    if (listenKey_.empty()) return false;
    LOG_INFO("TB {} PAPI listenKey={}", acc.accountId, listenKey_);
    return true;
}

void BinanceUnifiedTradeUnit::renewListenKeyAsync() {
    if (listenKey_.empty() || !pRestClient) return;
    asyncRequest(boost::beast::http::verb::put, listenKeyUrl, /*body=*/"", /*ct=*/"",
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) { LOG_ERROR("TB {} PAPI listenKey renew ec: {}", acc.accountId, ec.message()); return; }
            if (resp.status_code != 200) {
                LOG_ERROR("TB {} PAPI listenKey renew status={}", acc.accountId, resp.status_code);
            }
        });
}

void BinanceUnifiedTradeUnit::listenKeyRenewLoop() {
    while (!renewStop_.load()) {
        std::unique_lock<std::mutex> lk(renewMtx_);
        if (renewCv_.wait_for(lk, std::chrono::seconds(kListenKeyRenewSec),
                              [this]{ return renewStop_.load(); })) {
            return;
        }
        renewListenKeyAsync();
    }
}


// ============================================================================
// subWebsocekt
// ============================================================================
void BinanceUnifiedTradeUnit::subWebsocekt() {
    std::string restHost = host_of(acc.restUrl);
    initRestClient(restHost, /*headers=*/{{"X-MBX-APIKEY", acc.apiKey}}, /*conns=*/8);

    if (!generateListenKeySync()) {
        LOG_ERROR("TB {} PAPI listenKey gen failed, ws NOT started", acc.accountId);
        return;
    }

    ::net::WsConfig cfg;
    cfg.url                = acc.wsUrl + wsSubPath + listenKey_;
    cfg.ping_mode          = ::net::WsConfig::PingMode::ServerOnly;
    cfg.auto_reconnect     = true;
    cfg.idle_timeout_sec   = 60;
    LOG_INFO("TB {} PAPI ws {} rest {}", acc.accountId, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));

    renewThread_ = std::thread([this]{ listenKeyRenewLoop(); });
}


// ============================================================================
// WS msg
// ============================================================================
void BinanceUnifiedTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool, int64_t) {
    if (len == 0) return;
    try {
        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc = g_parser.iterate(padded);
        if (doc.error()) return;

        simdjson::ondemand::object root;
        if (doc.get_object().get(root) != simdjson::SUCCESS) return;

        std::string_view e_sv;
        if (root["e"].get(e_sv) != simdjson::SUCCESS) return;

        if      (e_sv == "ACCOUNT_UPDATE")                     handleAccountUpdate(root);
        else if (e_sv == "ORDER_TRADE_UPDATE")                 handleOrderUpdate(root);
        else if (e_sv == "executionReport")                    handleExecutionReport(root);
        else if (e_sv == "listenKeyExpired") {
            LOG_WARN("TB {} PAPI listenKey expired", acc.accountId);
        }
    }
    catch (const std::exception& e) {
        LOG_ERROR("TB {} PAPI ws msg exc: {}", acc.accountId, e.what());
    }
}


// ---- ACCOUNT_UPDATE (positions, "fs" = UM/CM) ----
void BinanceUnifiedTradeUnit::handleAccountUpdate(simdjson::ondemand::object& root) {
    std::string_view fs_sv;
    root["fs"].get(fs_sv);   // "UM" or "CM"

    simdjson::ondemand::object a_obj;
    if (root["a"].get(a_obj) != simdjson::SUCCESS) return;

    simdjson::ondemand::array P_arr;
    if (a_obj["P"].get(P_arr) != simdjson::SUCCESS) return;

    std::vector<pubsub::RCommand> pending;
    for (auto p_val : P_arr) {
        auto p = p_val.get_object();
        if (p.error()) continue;
        std::string_view s_sv, ps_sv, pa_sv, iw_sv, ep_sv, up_sv;
        p["s"].get(s_sv);
        p["pa"].get(pa_sv);
        p["ps"].get(ps_sv);
        p["iw"].get(iw_sv);
        p["ep"].get(ep_sv);
        p["up"].get(up_sv);

        if (ps_sv.empty() || ps_sv[0] != 'B') continue;

        std::string originInstId(s_sv);
        md::InstrumentInfo info;
        InstType inst = USDT_SWAP;
        bool found = false;
        if (fs_sv == "UM") {
            if      (smc->get_instrument_info(BINANCE, USDT_SWAP,    originInstId.c_str(), info)) { inst = USDT_SWAP;    found = true; }
            else if (smc->get_instrument_info(BINANCE, USDT_FUTURES, originInstId.c_str(), info)) { inst = USDT_FUTURES; found = true; }
            else if (smc->get_instrument_info(BINANCE, USDC_SWAP,    originInstId.c_str(), info)) { inst = USDC_SWAP;    found = true; }
        } else if (fs_sv == "CM") {
            if      (smc->get_instrument_info(BINANCE, C_SWAP,    originInstId.c_str(), info)) { inst = C_SWAP;    found = true; }
            else if (smc->get_instrument_info(BINANCE, C_FUTURES, originInstId.c_str(), info)) { inst = C_FUTURES; found = true; }
        }
        if (!found) continue;

        double positionAmt = crypto::fast_atod(pa_sv);
        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
        rcmd.body.position.exchangeTypeEnum = BINANCE;
        rcmd.body.position.instTypeEnum     = inst;
        crypto::copy_sv_to_char_array(rcmd.body.position.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.position.instId,     std::string_view(info.instId));
        rcmd.body.position.direction     = positionAmt >= 0 ? DT_LONG : DT_SHORT;
        rcmd.body.position.volume        = std::abs(positionAmt) * info.magnifyNumber;
        rcmd.body.position.maintMargin   = crypto::fast_atod(iw_sv);
        rcmd.body.position.avgPrice      = crypto::fast_atod(ep_sv) * info.reduceNumber;
        rcmd.body.position.unrealizedPnl = crypto::fast_atod(up_sv);
        rcmd.body.position.updateTime    = crypto::getCurrentTime();
        rcmd.body.position.apiSourceEnum = AS_WEBSOCKET;
        pending.emplace_back(rcmd);
    }
    for (size_t i = 0; i < pending.size(); ++i) {
        pending[i].body.position.isLast = (i + 1 == pending.size());
        PUSH_RCMD(pending[i])
    }
}


// ---- ORDER_TRADE_UPDATE (期货订单) ----
void BinanceUnifiedTradeUnit::handleOrderUpdate(simdjson::ondemand::object& root) {
    simdjson::ondemand::object o;
    if (root["o"].get(o) != simdjson::SUCCESS) return;

    std::string_view s_sv, c_sv, fs_sv, S_sv, f_sv, ot_sv, q_sv, p_sv, X_sv, z_sv, ap_sv, l_sv, L_sv;
    o["s"].get(s_sv);
    o["c"].get(c_sv);
    o["fs"].get(fs_sv);
    o["S"].get(S_sv);
    o["f"].get(f_sv);
    o["o"].get(ot_sv);
    o["q"].get(q_sv);
    o["p"].get(p_sv);
    o["X"].get(X_sv);
    o["z"].get(z_sv);
    o["ap"].get(ap_sv);
    o["l"].get(l_sv);
    o["L"].get(L_sv);

    std::string originInstId(s_sv);
    md::InstrumentInfo info;
    InstType inst = USDT_SWAP;
    bool found = false;
    if (fs_sv == "UM") {
        if      (smc->get_instrument_info(BINANCE, USDT_SWAP,    originInstId.c_str(), info)) { inst = USDT_SWAP;    found = true; }
        else if (smc->get_instrument_info(BINANCE, USDC_SWAP,    originInstId.c_str(), info)) { inst = USDC_SWAP;    found = true; }
        else if (smc->get_instrument_info(BINANCE, USDT_FUTURES, originInstId.c_str(), info)) { inst = USDT_FUTURES; found = true; }
    } else if (fs_sv == "CM") {
        if      (smc->get_instrument_info(BINANCE, C_SWAP,    originInstId.c_str(), info)) { inst = C_SWAP;    found = true; }
        else if (smc->get_instrument_info(BINANCE, C_FUTURES, originInstId.c_str(), info)) { inst = C_FUTURES; found = true; }
    }
    if (!found) return;

    pubsub::RCommand rcmd;
    memset(&rcmd, 0, sizeof(pubsub::RCommand));
    rcmd.cmdTypeEnum = pubsub::CMD_RPT_NEW_ORDER;
    rcmd.body.orderResponse.exchangeTypeEnum = BINANCE;
    rcmd.body.orderResponse.instTypeEnum     = inst;
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountId,  acc.accountId);
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.strategyId, acc.strategyId);
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.instId,     std::string_view(info.instId));
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, c_sv);

    rcmd.body.orderResponse.offsetFlag = OF_OPEN;
    if (!S_sv.empty()) rcmd.body.orderResponse.direction = (S_sv[0] == 'B') ? DT_LONG : DT_SHORT;

    std::string tif(f_sv), oty(ot_sv);
    rcmd.body.orderResponse.orderType = crypto::get_binance_ordertype(tif.c_str(), oty.c_str());

    if (!q_sv.empty())  rcmd.body.orderResponse.volumeTotal  = crypto::fast_atod(q_sv)  * info.magnifyNumber;
    if (!p_sv.empty())  rcmd.body.orderResponse.limitPrice   = crypto::fast_atod(p_sv)  * info.reduceNumber;
    if (!z_sv.empty())  rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(z_sv)  * info.magnifyNumber;
    if (!ap_sv.empty()) rcmd.body.orderResponse.tradePrice   = crypto::fast_atod(ap_sv) * info.reduceNumber;
    if (!l_sv.empty())  rcmd.body.orderResponse.tradeDiff    = crypto::fast_atod(l_sv)  * info.magnifyNumber;
    if (!L_sv.empty())  rcmd.body.orderResponse.fillPrice    = crypto::fast_atod(L_sv)  * info.reduceNumber;

    if (!X_sv.empty()) {
        std::string X_str(X_sv);
        rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(X_str);
    }

    rcmd.body.orderResponse.updateTime    = crypto::getCurrentTime();
    rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
    PUSH_RCMD(rcmd)
}


// ---- executionReport (现货 / 保证金) ----
//   与 spot 一致的字段布局, 只是走 papi 通道
void BinanceUnifiedTradeUnit::handleExecutionReport(simdjson::ondemand::object& root) {
    std::string_view s_sv;
    if (root["s"].get(s_sv) != simdjson::SUCCESS) return;

    md::InstrumentInfo info;
    std::string originInstId(s_sv);
    if (!smc->get_instrument_info(BINANCE, SPOT, originInstId.c_str(), info)) {
        LOG_ERROR("TB {} PAPI exec smc miss: {}", acc.accountId, originInstId);
        return;
    }

    pubsub::RCommand rcmd;
    memset(&rcmd, 0, sizeof(pubsub::RCommand));
    rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
    rcmd.body.orderResponse.exchangeTypeEnum = BINANCE;
    rcmd.body.orderResponse.instTypeEnum     = SPOT;
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountId,  acc.accountId);
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.strategyId, acc.strategyId);
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.instId,     std::string_view(info.instId));

    int64_t i_val = 0;
    if (root.find_field_unordered("i").get(i_val) == simdjson::SUCCESS) {
        fmt::format_to(rcmd.body.orderResponse.orderId, "{}", i_val);
    } else {
        std::string_view i_sv;
        if (root.find_field_unordered("i").get(i_sv) == simdjson::SUCCESS) {
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, i_sv);
        }
    }

    std::string_view C_sv, c_sv;
    bool used_C = false;
    if (root.find_field_unordered("C").get(C_sv) == simdjson::SUCCESS && !C_sv.empty()) {
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, C_sv);
        used_C = true;
    }
    if (!used_C && root.find_field_unordered("c").get(c_sv) == simdjson::SUCCESS) {
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, c_sv);
    }

    rcmd.body.orderResponse.offsetFlag = OF_OPEN;
    std::string_view S_sv;
    if (root.find_field_unordered("S").get(S_sv) == simdjson::SUCCESS && !S_sv.empty()) {
        rcmd.body.orderResponse.direction = (S_sv[0] == 'B') ? DT_LONG : DT_SHORT;
    }

    std::string_view f_sv, ot_sv;
    root.find_field_unordered("f").get(f_sv);
    root.find_field_unordered("o").get(ot_sv);
    std::string tif(f_sv), oty(ot_sv);
    rcmd.body.orderResponse.orderType = crypto::get_binance_ordertype(tif.c_str(), oty.c_str());

    std::string_view X_sv;
    if (root.find_field_unordered("X").get(X_sv) == simdjson::SUCCESS) {
        std::string X_str(X_sv);
        rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(X_str);
    }

    std::string_view l_sv, L_sv, z_sv, Z_sv, q_sv, p_sv;
    root.find_field_unordered("l").get(l_sv);
    root.find_field_unordered("L").get(L_sv);
    root.find_field_unordered("z").get(z_sv);
    root.find_field_unordered("Z").get(Z_sv);
    root.find_field_unordered("q").get(q_sv);
    root.find_field_unordered("p").get(p_sv);

    if (!l_sv.empty()) rcmd.body.orderResponse.tradeDiff    = crypto::fast_atod(l_sv) * info.magnifyNumber;
    if (!L_sv.empty()) rcmd.body.orderResponse.fillPrice    = crypto::fast_atod(L_sv) * info.reduceNumber;
    if (!z_sv.empty()) rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(z_sv) * info.magnifyNumber;
    if (rcmd.body.orderResponse.volumeTraded > 0 && !Z_sv.empty()) {
        rcmd.body.orderResponse.tradePrice = crypto::fast_atod(Z_sv)
                                             / rcmd.body.orderResponse.volumeTraded * info.reduceNumber;
    }
    if (!q_sv.empty()) rcmd.body.orderResponse.volumeTotal = crypto::fast_atod(q_sv) * info.magnifyNumber;
    if (!p_sv.empty()) rcmd.body.orderResponse.limitPrice  = crypto::fast_atod(p_sv) * info.reduceNumber;

    rcmd.body.orderResponse.updateTime    = crypto::getCurrentTime();
    rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
    PUSH_RCMD(rcmd)
}


// ============================================================================
// Trade API
// ============================================================================

// ---- GET /papi/v1/account ----
void BinanceUnifiedTradeUnit::query_account(const pubsub::TCommand&) {
    if (!pRestClient) return;
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"recvWindow", "5000"},
        {"timestamp",  std::to_string(crypto::getCurrentTimeMilli())},
    };
    std::string path = buildSignedPath(accountUrl, kvs);

    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "",
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) { LOG_ERROR("TB {} PAPI query_account ec: {}", acc.accountId, ec.message()); return; }
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;

                std::string_view eq_sv, aeq_sv, mm_sv, mmr_sv;
                doc["actualEquity"].get(eq_sv);
                doc["accountEquity"].get(aeq_sv);
                doc["accountMaintMargin"].get(mm_sv);
                doc["uniMMR"].get(mmr_sv);

                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
                rcmd.body.totalAccount.exchangeTypeEnum = BINANCE;
                rcmd.body.totalAccount.instTypeEnum     = SPOT;
                crypto::copy_sv_to_char_array(rcmd.body.totalAccount.accountId,  acc.accountId);
                crypto::copy_sv_to_char_array(rcmd.body.totalAccount.strategyId, acc.strategyId);
                rcmd.body.totalAccount.totalEquity = crypto::fast_atod(eq_sv);
                rcmd.body.totalAccount.adjEquity   = crypto::fast_atod(aeq_sv);
                rcmd.body.totalAccount.mmr         = crypto::fast_atod(mm_sv);
                rcmd.body.totalAccount.mgnRatio    = mmr_sv.empty() ? 100.0 : crypto::fast_atod(mmr_sv);
                rcmd.body.totalAccount.updateTime  = crypto::getCurrentTime();
                rcmd.body.totalAccount.apiSourceEnum = AS_REST;
                PUSH_RCMD(rcmd)
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} PAPI query_account cb exc: {}", acc.accountId, e.what());
            }
        });
}


// ---- GET /papi/v1/balance (array 直出) ----
void BinanceUnifiedTradeUnit::query_balance(const pubsub::TCommand&) {
    if (!pRestClient) return;
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"recvWindow", "5000"},
        {"timestamp",  std::to_string(crypto::getCurrentTimeMilli())},
    };
    std::string path = buildSignedPath(balanceUrl, kvs);

    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "",
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) { LOG_ERROR("TB {} PAPI query_balance ec: {}", acc.accountId, ec.message()); return; }
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;

                simdjson::ondemand::array arr;
                if (doc.get_array().get(arr) != simdjson::SUCCESS) return;

                std::vector<pubsub::RCommand> pending;
                for (auto it : arr) {
                    auto o = it.get_object();
                    if (o.error()) continue;
                    std::string_view asset_sv, total_sv, borrow_sv;
                    o["asset"].get(asset_sv);
                    o["totalWalletBalance"].get(total_sv);
                    o["crossMarginBorrowed"].get(borrow_sv);

                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = BINANCE;
                    rcmd.body.balance.instTypeEnum     = SPOT;
                    crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.currency,   crypto::to_upper(std::string(asset_sv)));
                    rcmd.body.balance.total     = crypto::fast_atod(total_sv);
                    rcmd.body.balance.available = rcmd.body.balance.total;
                    rcmd.body.balance.borrowed  = crypto::fast_atod(borrow_sv);
                    rcmd.body.balance.updateTime    = crypto::getCurrentTime();
                    rcmd.body.balance.apiSourceEnum = AS_REST;
                    pending.emplace_back(rcmd);
                }
                if (pending.empty()) {
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = BINANCE;
                    rcmd.body.balance.instTypeEnum     = SPOT;
                    crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.currency,   std::string("USDT"));
                    rcmd.body.balance.updateTime    = crypto::getCurrentTime();
                    rcmd.body.balance.apiSourceEnum = AS_REST;
                    rcmd.body.balance.isLast = true;
                    PUSH_RCMD(rcmd);
                    return;
                }
                for (size_t i = 0; i < pending.size(); ++i) {
                    pending[i].body.balance.isLast = (i + 1 == pending.size());
                    PUSH_RCMD(pending[i]);
                }
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} PAPI query_balance cb exc: {}", acc.accountId, e.what());
            }
        });
}


// ---- query_position ----
// 先跑 adl (um / cm 分别端点), 拿到 map 后再发 positionRisk 请求
void BinanceUnifiedTradeUnit::query_position(const pubsub::TCommand& tcmd) {
    queryPositionWithAdl(tcmd.body.queryPosition.instTypeEnum);
}

void BinanceUnifiedTradeUnit::queryPositionWithAdl(InstType instType) {
    const char* posPath = positionPathFor(instType);
    if (!posPath) return;

    // adl 端点仅 UM 有 (papi 只在 UM 侧暴露 /adlQuantile)
    std::string adlPath = std::string(is_cm(instType) ? "/papi/v1/cm/adlQuantile" : "/papi/v1/um/adlQuantile");

    std::vector<std::pair<std::string, std::string>> kvs = {
        {"recvWindow", "5000"},
        {"timestamp",  std::to_string(crypto::getCurrentTimeMilli())},
    };
    std::string signedAdl = buildSignedPath(adlPath, kvs);

    // 拷贝所需字段进 shared_ptr, 让两次 async 都能捕获
    auto adlMap = std::make_shared<std::unordered_map<std::string, double>>();

    asyncRequest(boost::beast::http::verb::get, std::move(signedAdl), "", "",
        [this, adlMap, posPath, instType](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (!ec && resp.status_code == 200) {
                try {
                    simdjson::padded_string padded(resp.body);
                    auto doc = g_parser.iterate(padded);
                    if (!doc.error()) {
                        simdjson::ondemand::array arr;
                        if (doc.get_array().get(arr) == simdjson::SUCCESS) {
                            for (auto it : arr) {
                                auto o = it.get_object();
                                if (o.error()) continue;
                                std::string_view sym_sv;
                                o["symbol"].get(sym_sv);
                                auto q = o["adlQuantile"].get_object();
                                if (!q.error()) {
                                    std::string_view both_sv;
                                    if (q["BOTH"].get(both_sv) == simdjson::SUCCESS) {
                                        (*adlMap)[std::string(sym_sv)] = crypto::fast_atod(both_sv) + 1;
                                    }
                                }
                            }
                        }
                    }
                } catch (...) {}
            }

            // 触发 positionRisk 请求
            std::vector<std::pair<std::string, std::string>> kvs2 = {
                {"recvWindow", "5000"},
                {"timestamp",  std::to_string(crypto::getCurrentTimeMilli())},
            };
            std::string posSigned = buildSignedPath(posPath, kvs2);

            asyncRequest(boost::beast::http::verb::get, std::move(posSigned), "", "",
                [this, adlMap, instType](boost::system::error_code ec2, ::net::HttpResponse resp2) {
                    if (ec2) return;
                    try {
                        simdjson::padded_string padded(resp2.body);
                        auto doc = g_parser.iterate(padded);
                        if (doc.error()) return;

                        simdjson::ondemand::array arr;
                        if (doc.get_array().get(arr) != simdjson::SUCCESS) return;

                        std::vector<pubsub::RCommand> pending;
                        for (auto it : arr) {
                            auto o = it.get_object();
                            if (o.error()) continue;

                            std::string_view sym_sv, pa_sv, ps_sv, ep_sv, up_sv, mp_sv, lp_sv;
                            o["symbol"].get(sym_sv);
                            o["positionAmt"].get(pa_sv);
                            o["positionSide"].get(ps_sv);
                            o["entryPrice"].get(ep_sv);
                            o["unRealizedProfit"].get(up_sv);
                            o["markPrice"].get(mp_sv);
                            o["liquidationPrice"].get(lp_sv);

                            if (ps_sv.empty() || ps_sv[0] != 'B') continue;

                            std::string originInstId(sym_sv);
                            md::InstrumentInfo info;
                            InstType actualInst = instType;
                            bool found = false;
                            if (is_um(instType)) {
                                if      (smc->get_instrument_info(BINANCE, USDT_SWAP,    originInstId.c_str(), info)) { actualInst = USDT_SWAP;    found = true; }
                                else if (smc->get_instrument_info(BINANCE, USDT_FUTURES, originInstId.c_str(), info)) { actualInst = USDT_FUTURES; found = true; }
                            } else if (is_cm(instType)) {
                                if      (smc->get_instrument_info(BINANCE, C_SWAP,    originInstId.c_str(), info)) { actualInst = C_SWAP;    found = true; }
                                else if (smc->get_instrument_info(BINANCE, C_FUTURES, originInstId.c_str(), info)) { actualInst = C_FUTURES; found = true; }
                            }
                            if (!found) continue;

                            double positionAmt = crypto::fast_atod(pa_sv);
                            pubsub::RCommand rcmd;
                            memset(&rcmd, 0, sizeof(pubsub::RCommand));
                            rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                            rcmd.body.position.exchangeTypeEnum = BINANCE;
                            rcmd.body.position.instTypeEnum     = actualInst;
                            crypto::copy_sv_to_char_array(rcmd.body.position.accountId,  acc.accountId);
                            crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                            crypto::copy_sv_to_char_array(rcmd.body.position.instId,     std::string_view(info.instId));
                            rcmd.body.position.direction     = positionAmt >= 0 ? DT_LONG : DT_SHORT;
                            rcmd.body.position.volume        = std::abs(positionAmt) * info.magnifyNumber;
                            rcmd.body.position.avgPrice      = crypto::fast_atod(ep_sv) * info.reduceNumber;
                            rcmd.body.position.unrealizedPnl = crypto::fast_atod(up_sv);
                            rcmd.body.position.markPrice     = crypto::fast_atod(mp_sv) * info.reduceNumber;
                            rcmd.body.position.liquidPrice   = crypto::fast_atod(lp_sv) * info.reduceNumber;

                            auto itAdl = adlMap->find(originInstId);
                            if (itAdl != adlMap->end()) {
                                rcmd.body.position.adlQuantile = static_cast<int>(itAdl->second) + 1;
                            }
                            rcmd.body.position.updateTime    = crypto::getCurrentTime();
                            rcmd.body.position.apiSourceEnum = AS_REST;
                            pending.emplace_back(rcmd);
                        }
                        if (pending.empty()) {
                            pubsub::RCommand rcmd;
                            memset(&rcmd, 0, sizeof(pubsub::RCommand));
                            rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                            rcmd.body.position.exchangeTypeEnum = BINANCE;
                            rcmd.body.position.instTypeEnum     = USDT_SWAP;
                            crypto::copy_sv_to_char_array(rcmd.body.position.accountId,  acc.accountId);
                            crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                            crypto::copy_sv_to_char_array(rcmd.body.position.instId,     std::string_view("BTC-USDT"));
                            rcmd.body.position.updateTime    = crypto::getCurrentTime();
                            rcmd.body.position.apiSourceEnum = AS_REST;
                            rcmd.body.position.isLast = true;
                            PUSH_RCMD(rcmd);
                            return;
                        }
                        for (size_t i = 0; i < pending.size(); ++i) {
                            pending[i].body.position.isLast = (i + 1 == pending.size());
                            PUSH_RCMD(pending[i]);
                        }
                    }
                    catch (const std::exception& e) {
                        LOG_ERROR("TB {} PAPI query_position cb exc: {}", acc.accountId, e.what());
                    }
                });
        });
}


// ---- POST order ----
void BinanceUnifiedTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load()) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId     = TBDisconnectError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }
    const char* orderPath = newOrderPath(tcmd.body.newOrder.instTypeEnum);
    if (!orderPath) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId     = OrderTypeError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.newOrder.exchangeTypeEnum,
                                  tcmd.body.newOrder.instTypeEnum,
                                  tcmd.body.newOrder.instId, info)) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId     = SMCInstrumentNotExistError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const char* side = nullptr;
    if (tcmd.body.newOrder.offsetFlag == OF_OPEN) {
        if      (tcmd.body.newOrder.direction == DT_LONG)  side = "BUY";
        else if (tcmd.body.newOrder.direction == DT_SHORT) side = "SELL";
    } else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
        if      (tcmd.body.newOrder.direction == DT_LONG)  side = "SELL";
        else if (tcmd.body.newOrder.direction == DT_SHORT) side = "BUY";
    }
    if (!side) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId     =
            (tcmd.body.newOrder.offsetFlag == OF_OPEN || tcmd.body.newOrder.offsetFlag == OF_CLOSE)
                ? DirectionError : OffsetFlagError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    double price  = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice  * info.magnifyNumber, info.tickSize);
    double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber,  info.lotSize);

    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(14);
    kvs.emplace_back("recvWindow",       "5000");
    kvs.emplace_back("newClientOrderId", tcmd.body.newOrder.orderSysId);
    kvs.emplace_back("symbol",           info.originInstId);
    kvs.emplace_back("timestamp",        std::to_string(crypto::getCurrentTimeMilli()));
    kvs.emplace_back("side",             side);

    const bool spotLike = is_spot_like(tcmd.body.newOrder.instTypeEnum);

    switch (tcmd.body.newOrder.orderType) {
        case OT_LIMIT:
            kvs.emplace_back("type", "LIMIT");
            kvs.emplace_back("timeInForce", "GTC");
            kvs.emplace_back("price",    fmt::format("{}", price));
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "RESULT");
            break;
        case OT_MARKET:
            kvs.emplace_back("type", "MARKET");
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "RESULT");
            break;
        case OT_POST_ONLY:
            if (spotLike) {
                kvs.emplace_back("type", "LIMIT_MAKER");
            } else {
                kvs.emplace_back("type", "LIMIT");
                kvs.emplace_back("timeInForce", "GTX");
            }
            kvs.emplace_back("price",    fmt::format("{}", price));
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "RESULT");
            break;
        case OT_FOK:
            kvs.emplace_back("type", "LIMIT");
            kvs.emplace_back("timeInForce", "FOK");
            kvs.emplace_back("price",    fmt::format("{}", price));
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "RESULT");
            break;
        case OT_IOC:
            kvs.emplace_back("type", "LIMIT");
            kvs.emplace_back("timeInForce", "IOC");
            kvs.emplace_back("price",    fmt::format("{}", price));
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "RESULT");
            break;
        default:
            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            rcmd.body.orderResponse.errorId     = OrderTypeError;
            rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
            return;
    }
    kvs.emplace_back("reduceOnly", tcmd.body.newOrder.reduceOnly ? "true" : "false");
#ifdef AUTO_BORROW_REPAY
    if (spotLike) kvs.emplace_back("sideEffectType", "AUTO_BORROW_REPAY");
#endif

    std::string path = buildSignedPath(orderPath, kvs);
    LOG_INFO("TB {} PAPI add_new_order: {}", acc.accountId, path);

    auto ot = rcmd.body.orderResponse.orderType;
    auto info_captured = info;
    const bool spotLikeCaptured = spotLike;

    asyncRequest(boost::beast::http::verb::post, std::move(path), "", "",
        [this, rcmd, ot, info_captured, spotLikeCaptured](boost::system::error_code ec,
                                                         ::net::HttpResponse resp) mutable {
            if (ec) {
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                rcmd.body.orderResponse.errorId     = NetworkError;
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, ec.message());
                rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;
            }
            if (rcmd.body.orderResponse.clientOrderId == TESTCLIENTORDERID) return;

            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    rcmd.body.orderResponse.errorId     = UnknownError;
                    rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;
                }

                int64_t code = 0;
                if (doc.find_field_unordered("code").get(code) == simdjson::SUCCESS) {
                    std::string_view msg_sv;
                    doc.find_field_unordered("msg").get(msg_sv);
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    rcmd.body.orderResponse.errorId     = crypto::get_binance_errorid(static_cast<int>(code));
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, msg_sv);
                    rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;
                }

                int64_t oid = 0;
                if (doc.find_field_unordered("orderId").get(oid) == simdjson::SUCCESS) {
                    fmt::format_to(rcmd.body.orderResponse.orderId, "{}", oid);

                    std::string_view execQ_sv;
                    doc.find_field_unordered("executedQty").get(execQ_sv);
                    if (!execQ_sv.empty()) rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(execQ_sv) * info_captured.magnifyNumber;

                    if (spotLikeCaptured) {
                        std::string_view cumQ_sv;
                        doc.find_field_unordered("cummulativeQuoteQty").get(cumQ_sv);
                        if (rcmd.body.orderResponse.volumeTraded > 0 && !cumQ_sv.empty()) {
                            rcmd.body.orderResponse.tradePrice =
                                crypto::fast_atod(cumQ_sv) / rcmd.body.orderResponse.volumeTraded * info_captured.reduceNumber;
                        }
                    } else {
                        std::string_view avg_sv;
                        doc.find_field_unordered("avgPrice").get(avg_sv);
                        if (!avg_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avg_sv) * info_captured.reduceNumber;
                    }

                    if (ot == OT_IOC) {
                        rcmd.body.orderResponse.orderStatus =
                            (rcmd.body.orderResponse.volumeTraded < rcmd.body.orderResponse.volumeTotal)
                                ? OS_CANCELED : OS_FILLED;
                    } else {
                        if (rcmd.body.orderResponse.volumeTraded < ZERO_NUM)                                     rcmd.body.orderResponse.orderStatus = OS_NEW;
                        else if (rcmd.body.orderResponse.volumeTraded < rcmd.body.orderResponse.volumeTotal)      rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
                        else                                                                                     rcmd.body.orderResponse.orderStatus = OS_FILLED;
                    }
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                } else {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    rcmd.body.orderResponse.errorId     = UnknownError;
                    rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                }
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} PAPI add_new_order cb exc: {}", acc.accountId, e.what());
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                rcmd.body.orderResponse.errorId     = NetworkError;
                rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            }
        });
}


// ---- DELETE order ----
void BinanceUnifiedTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    const char* orderPath = cancelOrderPath(tcmd.body.cancelOrder.instTypeEnum);
    if (!orderPath) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = OrderTypeError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.cancelOrder.exchangeTypeEnum,
                                  tcmd.body.cancelOrder.instTypeEnum,
                                  tcmd.body.cancelOrder.instId, info)) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = SMCInstrumentNotExistError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(5);
    kvs.emplace_back("recvWindow", "5000");
    kvs.emplace_back("symbol",     info.originInstId);
    kvs.emplace_back("timestamp",  std::to_string(crypto::getCurrentTimeMilli()));

    if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
        kvs.emplace_back("orderId", tcmd.body.cancelOrder.orderId);
    } else if (!crypto::str_cmp(tcmd.body.cancelOrder.orderSysId, "")) {
        kvs.emplace_back("origClientOrderId", tcmd.body.cancelOrder.orderSysId);
    } else {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = OrderIdError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    std::string path = buildSignedPath(orderPath, kvs);
    LOG_INFO("TB {} PAPI cancel_order: {}", acc.accountId, path);

    auto info_captured = info;
    const bool spotLikeCaptured = is_spot_like(tcmd.body.cancelOrder.instTypeEnum);

    asyncRequest(boost::beast::http::verb::delete_, std::move(path), "", "",
        [this, rcmd, info_captured, spotLikeCaptured](boost::system::error_code ec,
                                                     ::net::HttpResponse resp) mutable {
            if (ec) {
                rcmd.body.orderResponse.orderStatus = OS_FAILED;
                rcmd.body.orderResponse.errorId     = NetworkError;
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, ec.message());
                rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;
            }
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;

                int64_t code = 0;
                if (doc.find_field_unordered("code").get(code) == simdjson::SUCCESS) {
                    std::string_view msg_sv;
                    doc.find_field_unordered("msg").get(msg_sv);
                    rcmd.body.orderResponse.errorId    = crypto::get_binance_errorid(static_cast<int>(code));
                    rcmd.body.orderResponse.orderStatus =
                        (rcmd.body.orderResponse.errorId == OrderNotFoundError) ? OS_REJECTED : OS_FAILED;
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, msg_sv);
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;
                }

                int64_t oid = 0;
                if (doc.find_field_unordered("orderId").get(oid) == simdjson::SUCCESS) {
                    fmt::format_to(rcmd.body.orderResponse.orderId, "{}", oid);
                }
                std::string_view execQ_sv;
                doc.find_field_unordered("executedQty").get(execQ_sv);
                if (!execQ_sv.empty()) rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(execQ_sv) * info_captured.magnifyNumber;

                if (spotLikeCaptured) {
                    std::string_view cumQ_sv;
                    doc.find_field_unordered("cummulativeQuoteQty").get(cumQ_sv);
                    if (rcmd.body.orderResponse.volumeTraded > 0 && !cumQ_sv.empty()) {
                        rcmd.body.orderResponse.tradePrice =
                            crypto::fast_atod(cumQ_sv) / rcmd.body.orderResponse.volumeTraded * info_captured.reduceNumber;
                    }
                } else {
                    std::string_view avg_sv;
                    doc.find_field_unordered("avgPrice").get(avg_sv);
                    if (!avg_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avg_sv) * info_captured.reduceNumber;
                }

                rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} PAPI cancel_order cb exc: {}", acc.accountId, e.what());
            }
        });
}


// ---- GET order ----
void BinanceUnifiedTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);

    const char* orderPath = queryOrderPath(tcmd.body.queryOrder.instTypeEnum);
    if (!orderPath) return;

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum,
                                  tcmd.body.queryOrder.instTypeEnum,
                                  tcmd.body.queryOrder.instId, info)) {
        LOG_INFO("TB {} PAPI query_order smc miss: {}", acc.accountId, tcmd.body.queryOrder.instId);
        return;
    }

    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(5);
    kvs.emplace_back("recvWindow", "5000");
    kvs.emplace_back("symbol",     info.originInstId);
    kvs.emplace_back("timestamp",  std::to_string(crypto::getCurrentTimeMilli()));

    if (!crypto::str_cmp(tcmd.body.queryOrder.orderId, "")) {
        kvs.emplace_back("orderId", tcmd.body.queryOrder.orderId);
    } else if (!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")) {
        kvs.emplace_back("origClientOrderId", tcmd.body.queryOrder.orderSysId);
    } else {
        return;
    }

    std::string path = buildSignedPath(orderPath, kvs);
    LOG_INFO("TB {} PAPI query_order: {}", acc.accountId, path);

    auto info_captured = info;
    const bool spotLikeCaptured = is_spot_like(tcmd.body.queryOrder.instTypeEnum);

    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "",
        [this, rcmd, info_captured, spotLikeCaptured](boost::system::error_code ec,
                                                     ::net::HttpResponse resp) mutable {
            if (ec) return;
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;

                int64_t code = 0;
                if (doc.find_field_unordered("code").get(code) == simdjson::SUCCESS) {
                    long now = crypto::getCurrentTime();
                    if (rcmd.body.orderResponse.clientOrderId > 0 &&
                        now - rcmd.body.orderResponse.clientOrderId > ORDER_REJECTED_TIME_OUT) {
                        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    } else {
                        rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                    }
                    rcmd.body.orderResponse.errorId    = crypto::get_binance_errorid(static_cast<int>(code));
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd);
                    return;
                }

                int64_t oid = 0;
                if (doc.find_field_unordered("orderId").get(oid) == simdjson::SUCCESS) {
                    fmt::format_to(rcmd.body.orderResponse.orderId, "{}", oid);
                }
                std::string_view origQ_sv, price_sv, execQ_sv, status_sv;
                doc.find_field_unordered("origQty").get(origQ_sv);
                doc.find_field_unordered("price").get(price_sv);
                doc.find_field_unordered("executedQty").get(execQ_sv);
                doc.find_field_unordered("status").get(status_sv);

                if (!origQ_sv.empty()) rcmd.body.orderResponse.volumeTotal  = crypto::fast_atod(origQ_sv) * info_captured.magnifyNumber;
                if (!price_sv.empty()) rcmd.body.orderResponse.limitPrice   = crypto::fast_atod(price_sv) * info_captured.reduceNumber;
                if (!execQ_sv.empty()) rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(execQ_sv) * info_captured.magnifyNumber;

                if (spotLikeCaptured) {
                    std::string_view cumQ_sv;
                    doc.find_field_unordered("cummulativeQuoteQty").get(cumQ_sv);
                    if (rcmd.body.orderResponse.volumeTraded > 0 && !cumQ_sv.empty()) {
                        rcmd.body.orderResponse.tradePrice =
                            crypto::fast_atod(cumQ_sv) / rcmd.body.orderResponse.volumeTraded * info_captured.reduceNumber;
                    }
                } else {
                    std::string_view avg_sv;
                    doc.find_field_unordered("avgPrice").get(avg_sv);
                    if (!avg_sv.empty()) rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avg_sv) * info_captured.reduceNumber;
                }

                if (!status_sv.empty()) {
                    std::string st(status_sv);
                    rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(st);
                }
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd);
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} PAPI query_order cb exc: {}", acc.accountId, e.what());
            }
        });
}
