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
    if (renewThread_.joinable()) {
        renewThread_.join();
    }
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
std::string BinanceUnifiedTradeUnit::buildSignedPath(std::string_view basePath, const std::vector<std::pair<std::string, std::string>>& kvs) const {
    std::string qs;
    qs.reserve(256);
    for (size_t i = 0; i < kvs.size(); ++i) {
        if (i) {
            qs.push_back('&');
        }
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

    asyncRequest(boost::beast::http::verb::post, listenKeyUrl, "", "", [this, &prom, &responded](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (responded) {
                return;
            }
            responded = true;
            if (ec) { 
                prom.set_value(""); 
                return; 
            }
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) { 
                    prom.set_value(""); 
                    return; 
                }
                std::string_view lk;
                if (doc["listenKey"].get(lk) == simdjson::SUCCESS) {
                    prom.set_value(std::string(lk));
                }
                else {
                    prom.set_value("");
                }
            } catch (...) { 
                prom.set_value(""); 
            }
        });

    if (fut.wait_for(std::chrono::seconds(15)) != std::future_status::ready) {
        LOG_ERROR("TB {} PAPI listenKey timeout", acc.accountId);
        return false;
    }

    listenKey_ = fut.get();
    if (listenKey_.empty()) {
        return false;
    }
    LOG_INFO("TB {} PAPI listenKey={}", acc.accountId, listenKey_);
    return true;
}

void BinanceUnifiedTradeUnit::renewListenKeyAsync() {
    if (listenKey_.empty()) {
        return;
    }

    asyncRequest(boost::beast::http::verb::put, listenKeyUrl, /*body=*/"", /*ct=*/"",
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) { 
                LOG_ERROR("TB {} PAPI listenKey renew ec: {}", acc.accountId, ec.message()); return; 
            }
            if (resp.status_code != 200) {
                LOG_ERROR("TB {} PAPI listenKey renew status={}", acc.accountId, resp.status_code);
            }
        });
}

void BinanceUnifiedTradeUnit::listenKeyRenewLoop() {
    while (!renewStop_.load()) {
        std::unique_lock<std::mutex> lk(renewMtx_);
        if (renewCv_.wait_for(lk, std::chrono::seconds(kListenKeyRenewSec), [this]{ 
            return renewStop_.load(); 
        })) {
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
    initRestClient(restHost, {{"X-MBX-APIKEY", acc.apiKey}}, 4);

    if (!generateListenKeySync()) {
        LOG_ERROR("TB {} PAPI listenKey gen failed, ws NOT started", acc.accountId);
        return;
    }

    net::WsConfig cfg;
    cfg.url = acc.wsUrl + wsSubPath + listenKey_;
    cfg.ping_mode = net::WsConfig::PingMode::ServerOnly;
    cfg.auto_reconnect = true;
    cfg.idle_timeout_sec = 60;
    LOG_INFO("TB {} PAPI ws {} rest {}", acc.accountId, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));

    renewThread_ = std::thread([this]{ 
        listenKeyRenewLoop(); 
    });
}

void BinanceUnifiedTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool, int64_t) {
    try {
        std::string msg(reinterpret_cast<const char*>(data), len);
        std::cout << "onWebsocketMsg: " << msg << std::endl;

        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc = g_parser.iterate(padded);
        if (doc.error()) {
            return;
        }

        auto doc_value = doc.get_object().value_unsafe();

        std::string_view e_sv;
        simdjson::ondemand::object o_obj;
        simdjson::ondemand::object a_obj;
        bool has_order_trade_update = false;
        bool has_account_update = false;
        bool has_execution_report = false;

        // execution report
        std::string_view s_sv;
        std::string_view c_sv;
        std::string_view C_sv;
        std::string_view S_sv;
        std::string_view f_sv;
        std::string_view o_sv;
        std::string_view X_sv;
        std::string_view l_sv;
        std::string_view L_sv;
        std::string_view z_sv;
        std::string_view Z_sv;
        std::string_view q_sv;
        std::string_view p_sv;
        int64_t i_val = 0;
        std::string_view i_sv;
        bool has_i = false;

        for (auto field : doc_value) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "e") {
                field.value().get(e_sv) == simdjson::SUCCESS;
                if (e_sv == "ORDER_TRADE_UPDATE") {
                   has_order_trade_update = true; 
                }
                else if (e_sv == "ACCOUNT_UPDATE") {
                    has_account_update = true;
                }
                else if (e_sv == "executionReport") {
                    has_execution_report = true;
                }
                if (e_sv == "listenKeyExpired") {
                    LOG_WARN("TB {} listenKey expired, will regen", acc.accountId);
                    std::thread([this]{
                        if (generateListenKeySync()) {
                            LOG_INFO("TB {} listenKey regenerated, will reconnect", acc.accountId);
            
                            net::WsConfig cfg;
                            cfg.url = acc.wsUrl + wsSubPath + listenKey_;
                            cfg.ping_mode = net::WsConfig::PingMode::ServerOnly;
                            cfg.auto_reconnect = true;
                            cfg.idle_timeout_sec = 60;
                            LOG_INFO("TB {} UF ws {} rest {}", acc.accountId, cfg.url, restHost);
                            subWebsocketWithConfig(std::move(cfg));
                        }
                    }).detach();
                }
            }
            else if (k == "o") {
                if (has_order_trade_update && field.value().get(o_obj) == simdjson::SUCCESS) {
                    handleOrderUpdate(o_obj);
                }
            }
            else if (k == "a") {
                if (has_account_update && field.value().get(a_obj) == simdjson::SUCCESS) {
                    handleAccountUpdate(a_obj);
                }
            }

            else if (k == "s") {
                field.value().get(s_sv);
            }
            else if (k == "c") {
                field.value().get(c_sv);
            }
            else if (k == "C") {
                field.value().get(C_sv);
            }
            else if (k == "S") {
                field.value().get(S_sv);
            }
            else if (k == "f") {
                field.value().get(f_sv);
            }
            else if (k == "o") {
                field.value().get(o_sv);
            }
            else if (k == "X") {
                field.value().get(X_sv);
            }
            else if (k == "i") {
                has_i = field.value().get(i_val) == simdjson::SUCCESS;
                if (!has_i) {
                    field.value().get(i_sv);
                }
            }
            else if (k == "l") {
                field.value().get(l_sv);
            }
            else if (k == "L") {
                field.value().get(L_sv);
            }
            else if (k == "z") {
                field.value().get(z_sv);
            }
            else if (k == "Z") {
                field.value().get(Z_sv);
            }
            else if (k == "q") {
                field.value().get(q_sv);
            }
            else if (k == "p") {
                field.value().get(p_sv);
            }
        }

        if (has_execution_report) {
            if (s_sv.empty()) {
                return;
            }

            md::InstrumentInfo info;
            std::string originInstId(s_sv);
            if (!smc->get_instrument_info(BINANCE, SPOT, originInstId.c_str(), info)) {
                LOG_ERROR("TB {} exec report smc miss: {}", acc.accountId, originInstId);
                return;
            }

            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
            rcmd.body.orderResponse.exchangeTypeEnum = BINANCE;
            rcmd.body.orderResponse.instTypeEnum = SPOT;
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountId, acc.accountId);
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.strategyId, acc.strategyId);
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.instId, std::string_view(info.instId));

            if (has_i) {
                fmt::format_to(rcmd.body.orderResponse.orderId, "{}", i_val);
            }
            else {
                if (!i_sv.empty()) {
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, i_sv);
                }
            }

            // orderSysId: 优先 C (origClientOrderId, 非空), 否则 c (clientOrderId)
            if (!C_sv.empty()) {
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, C_sv);
            }
            if (!c_sv.empty()) {
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, c_sv);
            }

            rcmd.body.orderResponse.offsetFlag = OF_OPEN;
            if (!S_sv.empty()) {
                rcmd.body.orderResponse.direction = (S_sv[0] == 'B') ? DT_LONG : DT_SHORT;
            }

            std::string tif(f_sv), oty(o_sv);
            rcmd.body.orderResponse.orderType = crypto::get_binance_ordertype(tif.c_str(), oty.c_str());

            if (!X_sv.empty()) {
                std::string X_str(X_sv);
                rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(X_str);
            }
            if (!l_sv.empty()) {
                rcmd.body.orderResponse.tradeDiff = crypto::fast_atod(l_sv) * info.magnifyNumber;
            }

            if (!L_sv.empty()) {
                rcmd.body.orderResponse.fillPrice = crypto::fast_atod(L_sv) * info.reduceNumber;
            }

            if (!z_sv.empty()) {
                rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(z_sv) * info.magnifyNumber;
            }

            if (rcmd.body.orderResponse.volumeTraded > 0 && !Z_sv.empty()) {
                rcmd.body.orderResponse.tradePrice = crypto::fast_atod(Z_sv) / rcmd.body.orderResponse.volumeTraded * info.reduceNumber;
            }

            if (!q_sv.empty()) {
                rcmd.body.orderResponse.volumeTotal = crypto::fast_atod(q_sv) * info.magnifyNumber;
            }

            if (!p_sv.empty()) {
                rcmd.body.orderResponse.limitPrice  = crypto::fast_atod(p_sv) * info.reduceNumber;
            }

            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
            PUSH_RCMD(rcmd)
        }
    }
    catch (const std::exception& e) {
        LOG_ERROR("TB {} UF ws msg exc: {}", acc.accountId, e.what());
    }
}

// ---- ACCOUNT_UPDATE ----
//   { "e":"ACCOUNT_UPDATE", "a":{ "B":[{a,cw,bc,wb}], "P":[{s,pa,ps,iw,ep,up}] } }
void BinanceUnifiedTradeUnit::handleAccountUpdate(simdjson::ondemand::object& a) {
    simdjson::ondemand::array balances;
    simdjson::ondemand::array positions;
    for (auto field : a) {
        std::string_view k = field.unescaped_key().value_unsafe();
        if (k == "P") {
            if (field.value().get(positions) == simdjson::SUCCESS) {      
                for (auto b_val : positions) {
                    auto b_res = b_val.get_object();
                    if (b_res.error()) {
                        continue;
                    }
                    auto& b = b_res.value_unsafe();

                    std::string_view s_sv;
                    std::string_view pa_sv;
                    std::string_view ep_sv;
                    std::string_view up_sv;
                    std::string_view iw_sv;
                    std::string_view ps_sv;   
                    for (auto field : b) {
                        std::string_view k = field.unescaped_key().value_unsafe();
                        if (k == "s") {
                            field.value().get(s_sv);
                        }
                        else if (k == "pa") {
                            field.value().get(pa_sv);
                        }
                        else if (k == "ep") {
                            field.value().get(ep_sv);
                        }
                        else if (k == "up") {
                            field.value().get(up_sv);
                        }
                        else if (k == "iw") {
                            field.value().get(iw_sv);
                        }
                        else if (k == "ps") {
                            field.value().get(ps_sv);
                        }
                    }

                    if (ps_sv.empty() || ps_sv[0] != 'B') {
                        continue;   // 只要 BOTH 单向持仓
                    }

                    std::string originInstId(s_sv);
                    md::InstrumentInfo info;
                    InstType inst = USDT_SWAP;
                    if (smc->get_instrument_info(BINANCE, USDT_SWAP, originInstId.c_str(), info)) {
                        inst = USDT_SWAP;
                    } else if (smc->get_instrument_info(BINANCE, USDC_SWAP, originInstId.c_str(), info)) {
                        inst = USDC_SWAP;
                    } 
                    else if (smc->get_instrument_info(BINANCE, USDT_FUTURES, originInstId.c_str(), info)) {
                        inst = USDT_FUTURES;
                    } 
                    else if (smc->get_instrument_info(BINANCE, C_SWAP, originInstId.c_str(), info)) {
                        inst = C_SWAP;
                    } 
                    else if (smc->get_instrument_info(BINANCE, C_FUTURES, originInstId.c_str(), info)) {
                        inst = C_FUTURES;
                    }
                    else {
                        continue;
                    }

                    double positionAmt = crypto::fast_atod(pa_sv);
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                    rcmd.body.position.exchangeTypeEnum = BINANCE;
                    rcmd.body.position.instTypeEnum = inst;
                    crypto::copy_sv_to_char_array(rcmd.body.position.accountId, acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view(info.instId));
                    rcmd.body.position.direction = positionAmt >= 0 ? DT_LONG : DT_SHORT;
                    rcmd.body.position.volume = std::fabs(positionAmt) * info.magnifyNumber;
                    rcmd.body.position.maintMargin = crypto::fast_atod(iw_sv);
                    rcmd.body.position.avgPrice = crypto::fast_atod(ep_sv) * info.reduceNumber;
                    rcmd.body.position.unrealizedPnl = crypto::fast_atod(up_sv);
                    rcmd.body.position.updateTime = crypto::getCurrentTime();
                    rcmd.body.position.apiSourceEnum = AS_WEBSOCKET;
                    PUSH_RCMD(rcmd);
                }
            }
        }
    }
}

// ---- ORDER_TRADE_UPDATE ----
//   { "e":"ORDER_TRADE_UPDATE", "o":{ "s":symbol, "c":cid, "S":side, "f":tif, "o":ot,
//                                     "q":qty, "p":px, "X":status, "z":cumQty, "ap":avgPx,
//                                     "l":lastQty, "L":lastPx } }
void BinanceUnifiedTradeUnit::handleOrderUpdate(simdjson::ondemand::object& o) {
    std::string_view s_sv;
    std::string_view c_sv;
    std::string_view S_sv;
    std::string_view o_sv;
    std::string_view f_sv;
    std::string_view q_sv;
    std::string_view p_sv;
    std::string_view ap_sv;
    std::string_view X_sv;
    int64_t i_val = 0;
    std::string_view l_sv;
    std::string_view z_sv;
    std::string_view L_sv;
    for (auto field : o) {
        std::string_view k = field.unescaped_key().value_unsafe();
        if (k == "s") {
            field.value().get(s_sv);
        }
        else if (k == "c") {
            field.value().get(c_sv);
        }
        else if (k == "S") {
            field.value().get(S_sv);
        }
        else if (k == "o") {
            field.value().get(o_sv);
        }
        else if (k == "f") {
            field.value().get(f_sv);
        }
        else if (k == "q") {
            field.value().get(q_sv);
        }
        else if (k == "p") {
            field.value().get(p_sv);
        }
        else if (k == "ap") {
            field.value().get(ap_sv);
        }
        else if (k == "X") {
            field.value().get(X_sv);
        }
        else if (k == "i") {
            field.value().get(i_val);
        }
        else if (k == "l") {
            field.value().get(l_sv);
        }
        else if (k == "z") {
            field.value().get(z_sv);
        }
        else if (k == "L") {
            field.value().get(L_sv);
        }
    }

    std::string originInstId(s_sv);
    md::InstrumentInfo info;
    InstType inst = USDT_SWAP;
    if (smc->get_instrument_info(BINANCE, USDT_SWAP, originInstId.c_str(), info)) {
        inst = USDT_SWAP;
    } 
    else if (smc->get_instrument_info(BINANCE, USDC_SWAP, originInstId.c_str(), info)) {
        inst = USDC_SWAP;
    }  
    else if (smc->get_instrument_info(BINANCE, USDT_FUTURES, originInstId.c_str(), info)) {
        inst = USDT_FUTURES;
    } 
    else if (smc->get_instrument_info(BINANCE, C_SWAP, originInstId.c_str(), info)) {
        inst = C_SWAP;
    }  
    else if (smc->get_instrument_info(BINANCE, C_FUTURES, originInstId.c_str(), info)) {
        inst = C_FUTURES;
    }
    else {
        LOG_ERROR("TB {} UF order upd smc miss: {}", acc.accountId, originInstId);
        return;
    }

    pubsub::RCommand rcmd;
    memset(&rcmd, 0, sizeof(pubsub::RCommand));
    rcmd.cmdTypeEnum = pubsub::CMD_RPT_NEW_ORDER;
    rcmd.body.orderResponse.exchangeTypeEnum = BINANCE;
    rcmd.body.orderResponse.instTypeEnum = inst;
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountId, acc.accountId);
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.strategyId, acc.strategyId);
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.instId, std::string_view(info.instId));
    fmt::format_to(rcmd.body.orderResponse.orderId, "{}", i_val);
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, c_sv);

    rcmd.body.orderResponse.offsetFlag = OF_OPEN;
    if (!S_sv.empty()) {
        rcmd.body.orderResponse.direction = (S_sv[0] == 'B') ? DT_LONG : DT_SHORT;
    }

    std::string tif(f_sv);
    std::string ot(o_sv);
    rcmd.body.orderResponse.orderType = crypto::get_binance_ordertype(tif.c_str(), ot.c_str());
    rcmd.body.orderResponse.volumeTotal = crypto::fast_atod(q_sv) * info.magnifyNumber;
    rcmd.body.orderResponse.limitPrice = crypto::fast_atod(p_sv) * info.reduceNumber;
    rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(z_sv) * info.magnifyNumber;
    rcmd.body.orderResponse.tradePrice = crypto::fast_atod(ap_sv) * info.reduceNumber;
    rcmd.body.orderResponse.tradeDiff = crypto::fast_atod(l_sv) * info.magnifyNumber;
    rcmd.body.orderResponse.fillPrice = crypto::fast_atod(L_sv) * info.reduceNumber;

    std::string X_str(X_sv);
    rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(X_str);
    
    if (c_sv[0] == 'a' && c_sv[2] == 't') {
        rcmd.body.orderResponse.errorId = LiquidationError;
    }
    else if (c_sv[0] == 'a' && c_sv[2] == 'l') {
        rcmd.body.orderResponse.errorId = ADLError;
    }

    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
    rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
    PUSH_RCMD(rcmd)
}

// ---- GET /papi/v1/account ----
void BinanceUnifiedTradeUnit::query_account(const pubsub::TCommand&) {
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"recvWindow", "5000"},
        {"timestamp",  std::to_string(crypto::getCurrentTimeMilli())},
    };
    std::string path = buildSignedPath(accountUrl, kvs);

    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "", [this](boost::system::error_code ec, ::net::HttpResponse resp) {
        if (ec) { 
            LOG_ERROR("TB {} PAPI query_account ec: {}", acc.accountId, ec.message()); 
            return; 
        }
        try {
            std::cout << "query account: " << resp.body << std::endl;
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                LOG_ERROR("TB {} query account parse err: {}", acc.accountId, resp.body);
                return;
            }

            auto doc_value = doc.get_object().value_unsafe();

            std::string_view eq_sv;
            std::string_view aeq_sv;
            std::string_view mm_sv;
            std::string_view mmr_sv;

            for (auto field : doc_value) {
                std::string_view k = field.unescaped_key().value_unsafe();
                if (k == "actualEquity") {
                    field.value().get(eq_sv);
                }
                else if (k == "accountEquity") {
                    field.value().get(aeq_sv);
                }
                else if (k == "accountMaintMargin") {
                    field.value().get(mm_sv);
                }
                else if (k == "uniMMR") {
                    field.value().get(mmr_sv);
                }
            }

            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
            rcmd.body.totalAccount.exchangeTypeEnum = BINANCE;
            rcmd.body.totalAccount.instTypeEnum = SPOT;
            crypto::copy_sv_to_char_array(rcmd.body.totalAccount.accountId, acc.accountId);
            crypto::copy_sv_to_char_array(rcmd.body.totalAccount.strategyId, acc.strategyId);
            rcmd.body.totalAccount.totalEquity = crypto::fast_atod(eq_sv);
            rcmd.body.totalAccount.adjEquity = crypto::fast_atod(aeq_sv);
            rcmd.body.totalAccount.mmr = crypto::fast_atod(mm_sv);
            rcmd.body.totalAccount.mgnRatio = mmr_sv.empty() ? 100.0 : crypto::fast_atod(mmr_sv);
            rcmd.body.totalAccount.updateTime = crypto::getCurrentTime();
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

    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "", [this](boost::system::error_code ec, ::net::HttpResponse resp) {
        if (ec) { 
            LOG_ERROR("TB {} PAPI query_balance ec: {}", acc.accountId, ec.message()); 
            return; 
        }

        try {
            std::cout << "query balances: " << resp.body << std::endl;
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                return; 
            }

            simdjson::ondemand::array balances;
            if (doc.get_array().get(balances) != simdjson::SUCCESS) {
                LOG_ERROR("TB {} UF query_balance not array: {}", acc.accountId, resp.body);
                return;
            }

            std::vector<pubsub::RCommand> pending;
            for (auto b_val : balances) {
                auto b_res = b_val.get_object();
                if (b_res.error()) {
                    continue;
                }
                auto& b = b_res.value_unsafe();

                std::string_view asset_sv;
                std::string_view total_sv;
                std::string_view borrow_sv;
                for (auto field : b) {
                    std::string_view k = field.unescaped_key().value_unsafe();
                    if (k == "asset") {
                        field.value().get(asset_sv);
                    }
                    else if (k == "totalWalletBalance") {
                        field.value().get(total_sv);
                    }
                    else if (k == "crossMarginBorrowed") {
                        field.value().get(borrow_sv);
                    }
                }

                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                rcmd.body.balance.exchangeTypeEnum = BINANCE;
                rcmd.body.balance.instTypeEnum = SPOT;
                crypto::copy_sv_to_char_array(rcmd.body.balance.accountId, acc.accountId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(asset_sv)));
                rcmd.body.balance.total = crypto::fast_atod(total_sv);
                rcmd.body.balance.available = rcmd.body.balance.total;
                rcmd.body.balance.borrowed = crypto::fast_atod(borrow_sv);
                rcmd.body.balance.updateTime = crypto::getCurrentTime();
                rcmd.body.balance.apiSourceEnum = AS_REST;
                pending.emplace_back(rcmd);
            }
            if (pending.empty()) {
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                rcmd.body.balance.exchangeTypeEnum = BINANCE;
                rcmd.body.balance.instTypeEnum = SPOT;
                crypto::copy_sv_to_char_array(rcmd.body.balance.accountId, acc.accountId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.currency, std::string("USDT"));
                rcmd.body.balance.updateTime = crypto::getCurrentTime();
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
    std::string posPath = "";
    std::string adlPath = "";
    if (tcmd.body.queryPosition.instTypeEnum == USDT_SWAP || tcmd.body.queryPosition.instTypeEnum == USDT_FUTURES || tcmd.body.queryPosition.instTypeEnum == USDC_SWAP) {
        posPath = "/papi/v1/um/positionRisk";
        adlPath = "/papi/v1/um/adlQuantile";
    }
    else if (tcmd.body.queryPosition.instTypeEnum == C_SWAP || tcmd.body.queryPosition.instTypeEnum == C_FUTURES) {
        posPath = "/papi/v1/cm/positionRisk";
        adlPath = "/papi/v1/cm/adlQuantile";
    }

    std::vector<std::pair<std::string, std::string>> kvs = {
        {"recvWindow", "5000"},
        {"timestamp",  std::to_string(crypto::getCurrentTimeMilli())},
    };
    std::string signedAdl = buildSignedPath(adlPath, kvs);

    // 拷贝所需字段进 shared_ptr, 让两次 async 都能捕获
    auto adlMap = std::make_shared<std::unordered_map<std::string, double>>();

    asyncRequest(boost::beast::http::verb::get, std::move(signedAdl), "", "", [this, adlMap, posPath](boost::system::error_code ec, ::net::HttpResponse resp) {
        if (!ec && resp.status_code == 200) {
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (!doc.error()) {
                    simdjson::ondemand::array arr;
                    if (doc.get_array().get(arr) == simdjson::SUCCESS) {
                        for (auto it : arr) {
                            auto o = it.get_object();
                            if (o.error()) {
                                continue;
                            }
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
            } catch (...) {

            }
        }

        // 触发 positionRisk 请求
        std::vector<std::pair<std::string, std::string>> kvs2 = {
            {"recvWindow", "5000"},
            {"timestamp",  std::to_string(crypto::getCurrentTimeMilli())},
        };
        std::string posSigned = buildSignedPath(posPath, kvs2);

        asyncRequest(boost::beast::http::verb::get, std::move(posSigned), "", "", [this, adlMap](boost::system::error_code ec2, ::net::HttpResponse resp2) {
            if (ec2) {
                return;
            }
            try {
                simdjson::padded_string padded(resp2.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) {
                    return;
                }

                simdjson::ondemand::array positions;
                if (doc.get_array().get(positions) != simdjson::SUCCESS) {
                    return;
                }

                std::vector<pubsub::RCommand> pending;
                for (auto b_val : positions) {
                    auto b_res = b_val.get_object();
                    if (b_res.error()) {
                        continue;
                    }
                    auto& b = b_res.value_unsafe();

                    std::string_view symbol_sv;
                    std::string_view positionSide_sv;
                    std::string_view positionAmt_sv;
                    std::string_view entryPrice_sv;
                    std::string_view markPrice_sv;
                    std::string_view unRealizedProfit_sv;
                    std::string_view liquidationPrice_sv;
                    for (auto field : b) {
                        std::string_view k = field.unescaped_key().value_unsafe();
                        if (k == "symbol") {
                            field.value().get(symbol_sv);
                        }
                        else if (k == "positionSide") {
                            field.value().get(positionSide_sv);
                        }
                        else if (k == "positionAmt") {
                            field.value().get(positionAmt_sv);
                        }
                        else if (k == "entryPrice") {
                            field.value().get(entryPrice_sv);
                        }
                        else if (k == "markPrice") {
                            field.value().get(markPrice_sv);
                        }
                        else if (k == "unRealizedProfit") {
                            field.value().get(unRealizedProfit_sv);
                        }
                        else if (k == "liquidationPrice") {
                            field.value().get(liquidationPrice_sv);
                        }
                    }

                    if (positionSide_sv.empty() || positionSide_sv[0] != 'B') {
                        continue;
                    }

                    std::string originInstId(symbol_sv);
                    md::InstrumentInfo info;
                    InstType inst = USDT_SWAP;
                    if (smc->get_instrument_info(BINANCE, USDT_SWAP, originInstId.c_str(), info)) {
                        inst = USDT_SWAP;
                    }
                    else if (smc->get_instrument_info(BINANCE, USDT_FUTURES, originInstId.c_str(), info)) {
                        inst = USDT_FUTURES;
                    } 
                    else if (smc->get_instrument_info(BINANCE, C_SWAP, originInstId.c_str(), info)) {
                        inst = C_SWAP;
                    } 
                    else {
                        continue;
                    }

                    double positionAmt = crypto::fast_atod(positionAmt_sv);
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                    rcmd.body.position.exchangeTypeEnum = BINANCE;
                    rcmd.body.position.instTypeEnum = inst;
                    crypto::copy_sv_to_char_array(rcmd.body.position.accountId, acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view(info.instId));
                    rcmd.body.position.direction = positionAmt >= 0 ? DT_LONG : DT_SHORT;
                    rcmd.body.position.volume = std::abs(positionAmt) * info.magnifyNumber;
                    rcmd.body.position.avgPrice = crypto::fast_atod(entryPrice_sv) * info.reduceNumber;
                    rcmd.body.position.unrealizedPnl = crypto::fast_atod(unRealizedProfit_sv);
                    rcmd.body.position.markPrice = crypto::fast_atod(markPrice_sv) * info.reduceNumber;
                    rcmd.body.position.liquidPrice = crypto::fast_atod(liquidationPrice_sv) * info.reduceNumber;
                    
                    auto itAdl = adlMap->find(originInstId);
                    if (itAdl != adlMap->end()) {
                        rcmd.body.position.adlQuantile = static_cast<int>(itAdl->second) + 1;
                    }
                    
                    rcmd.body.position.updateTime = crypto::getCurrentTime();
                    rcmd.body.position.apiSourceEnum = AS_REST;
                    pending.emplace_back(rcmd);
                }
                if (pending.empty()) {
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                    rcmd.body.position.exchangeTypeEnum = BINANCE;
                    rcmd.body.position.instTypeEnum = USDT_SWAP;
                    crypto::copy_sv_to_char_array(rcmd.body.position.accountId, acc.accountId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view("BTC-USDT"));
                    rcmd.body.position.updateTime = crypto::getCurrentTime();
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

void BinanceUnifiedTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load()) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId = TBDisconnectError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    std::string orderPath = "";
    if (tcmd.body.newOrder.instTypeEnum == SPOT || tcmd.body.newOrder.instTypeEnum == MARGIN) {
        orderPath = "/papi/v1/margin/order";
    }
    else if (tcmd.body.newOrder.instTypeEnum == USDT_SWAP || tcmd.body.newOrder.instTypeEnum == USDT_FUTURES || tcmd.body.newOrder.instTypeEnum == USDC_SWAP) {
        orderPath = "/papi/v1/um/order";
    }
    else if (tcmd.body.newOrder.instTypeEnum == C_SWAP || tcmd.body.newOrder.instTypeEnum == C_FUTURES) {
        orderPath = "/papi/v1/cm/order";
    }

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.newOrder.exchangeTypeEnum, tcmd.body.newOrder.instTypeEnum, tcmd.body.newOrder.instId, info)) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId = SMCInstrumentNotExistError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const char* side = nullptr;
    if (tcmd.body.newOrder.offsetFlag == OF_OPEN) {
        if (tcmd.body.newOrder.direction == DT_LONG) {
            side = "BUY";
        } 
        else if (tcmd.body.newOrder.direction == DT_SHORT) {
            side = "SELL";
        }
    } else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
        if (tcmd.body.newOrder.direction == DT_LONG) {
            side = "SELL";
        }
        else if (tcmd.body.newOrder.direction == DT_SHORT) {
            side = "BUY";
        }
    }
    if (!side) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId = (tcmd.body.newOrder.offsetFlag == OF_OPEN || tcmd.body.newOrder.offsetFlag == OF_CLOSE) ? DirectionError : OffsetFlagError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    double price = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice  * info.magnifyNumber, info.tickSize);
    double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber, info.lotSize);

    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(14);
    kvs.emplace_back("recvWindow", "5000");
    kvs.emplace_back("newClientOrderId", tcmd.body.newOrder.orderSysId);
    kvs.emplace_back("symbol", info.originInstId);
    kvs.emplace_back("timestamp", std::to_string(crypto::getCurrentTimeMilli()));
    kvs.emplace_back("side", side);

    switch (tcmd.body.newOrder.orderType) {
        case OT_LIMIT:
            kvs.emplace_back("type", "LIMIT");
            kvs.emplace_back("timeInForce", "GTC");
            kvs.emplace_back("price", fmt::format("{}", price));
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "RESULT");
            break;
        case OT_MARKET:
            kvs.emplace_back("type", "MARKET");
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "RESULT");
            break;
        case OT_POST_ONLY:
            if (tcmd.body.newOrder.instTypeEnum == SPOT || tcmd.body.newOrder.instTypeEnum == MARGIN) {
                kvs.emplace_back("type", "LIMIT_MAKER");
            } else {
                kvs.emplace_back("type", "LIMIT");
                kvs.emplace_back("timeInForce", "GTX");
            }
            kvs.emplace_back("price", fmt::format("{}", price));
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "RESULT");
            break;
        case OT_FOK:
            kvs.emplace_back("type", "LIMIT");
            kvs.emplace_back("timeInForce", "FOK");
            kvs.emplace_back("price", fmt::format("{}", price));
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "RESULT");
            break;
        case OT_IOC:
            kvs.emplace_back("type", "LIMIT");
            kvs.emplace_back("timeInForce", "IOC");
            kvs.emplace_back("price", fmt::format("{}", price));
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "RESULT");
            break;
        default:
            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            rcmd.body.orderResponse.errorId = OrderTypeError;
            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
            return;
    }

    kvs.emplace_back("reduceOnly", tcmd.body.newOrder.reduceOnly ? "true" : "false");

#ifdef AUTO_BORROW_REPAY
    if (tcmd.body.newOrder.instTypeEnum == SPOT || tcmd.body.newOrder.instTypeEnum == MARGIN) {
        kvs.emplace_back("sideEffectType", "AUTO_BORROW_REPAY");
    }
#endif

    std::string path = buildSignedPath(orderPath, kvs);
    LOG_INFO("TB {} PAPI add_new_order: {}", acc.accountId, path);

    asyncRequest(boost::beast::http::verb::post, std::move(path), "", "", [this, rcmd, info](boost::system::error_code ec, net::HttpResponse resp) mutable {
        if (ec) {
            LOG_ERROR("TB {} add_new_order ec: {}", acc.accountId, ec.message());

            if (ec == boost::system::errc::no_stream_resources || ec == boost::system::errc::no_buffer_space || ec == boost::system::errc::not_connected) {
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            }
            else {
                rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
            }

            rcmd.body.orderResponse.errorId = NetworkError;
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, ec.message());
            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
            return;
        }
        if (rcmd.body.orderResponse.clientOrderId == TESTCLIENTORDERID) {
            return;
        }

        try {
            std::cout << "add new order: " << resp.body << std::endl;
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                LOG_ERROR("TB {} add_new_order parse err: {}", acc.accountId, resp.body);
                rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                rcmd.body.orderResponse.errorId = UnknownError;
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;
            }

            auto doc_value = doc.get_object().value_unsafe();

            int64_t code = 0;
            int64_t orderId = 0;
            std::string_view msg_sv;
            std::string_view execQ_sv;

            bool has_code = false;
            bool has_orderId = false;

            for (auto field : doc_value) {
                std::string_view k = field.unescaped_key().value_unsafe();

                if (k == "code") {
                    has_code = field.value().get(code) == simdjson::SUCCESS;
                }
                else if (k == "msg") {
                    field.value().get(msg_sv);
                }
                else if (k == "orderId") {
                    has_orderId = field.value().get(orderId) == simdjson::SUCCESS;
                }
                else if (k == "executedQty") {
                    field.value().get(execQ_sv);
                }
            }

            if (has_code) {
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                rcmd.body.orderResponse.errorId = crypto::get_binance_errorid(static_cast<int>(code));
                if (!msg_sv.empty()) {
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, msg_sv);
                }
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;     
            }

            if (!has_orderId) {
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                rcmd.body.orderResponse.errorId = UnknownError;
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            }
            else {
                fmt::format_to(rcmd.body.orderResponse.orderId, "{}", orderId);
                if (!execQ_sv.empty()) {
                    rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(execQ_sv) * info.magnifyNumber;
                }

                if (rcmd.body.orderResponse.orderType == OT_IOC) {
                    rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.volumeTraded < rcmd.body.orderResponse.volumeTotal) ? OS_CANCELED : OS_FILLED;
                } else {
                    if (rcmd.body.orderResponse.volumeTraded < ZERO_NUM) {
                        rcmd.body.orderResponse.orderStatus = OS_NEW;
                    } else if (rcmd.body.orderResponse.volumeTraded < rcmd.body.orderResponse.volumeTotal) {
                        rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
                    } else {
                        rcmd.body.orderResponse.orderStatus = OS_FILLED;
                    }
                }
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)      
            }
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} add_new_order cb exception: {}", acc.accountId, e.what());
            rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
            rcmd.body.orderResponse.errorId = NetworkError;
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, std::string_view(e.what()));
            rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
        }
    });
}


// ---- DELETE order ----
void BinanceUnifiedTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load()) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = TBDisconnectError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    std::string orderPath = "";
    if (tcmd.body.cancelOrder.instTypeEnum == SPOT || tcmd.body.cancelOrder.instTypeEnum == MARGIN) {
        orderPath = "/papi/v1/margin/order";
    }
    else if (tcmd.body.cancelOrder.instTypeEnum == USDT_SWAP || tcmd.body.cancelOrder.instTypeEnum == USDT_FUTURES || tcmd.body.cancelOrder.instTypeEnum == USDC_SWAP) {
        orderPath = "/papi/v1/um/order";
    }
    else if (tcmd.body.cancelOrder.instTypeEnum == C_SWAP || tcmd.body.cancelOrder.instTypeEnum == C_FUTURES) {
        orderPath = "/papi/v1/cm/order";
    }

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.cancelOrder.exchangeTypeEnum, tcmd.body.cancelOrder.instTypeEnum, tcmd.body.cancelOrder.instId, info)) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = SMCInstrumentNotExistError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(5);
    kvs.emplace_back("recvWindow", "5000");
    kvs.emplace_back("symbol", info.originInstId);
    kvs.emplace_back("timestamp", std::to_string(crypto::getCurrentTimeMilli()));

    if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
        kvs.emplace_back("orderId", tcmd.body.cancelOrder.orderId);
    } else if (!crypto::str_cmp(tcmd.body.cancelOrder.orderSysId, "")) {
        kvs.emplace_back("origClientOrderId", tcmd.body.cancelOrder.orderSysId);
    } else {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = OrderIdError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    std::string path = buildSignedPath(orderPath, kvs);
    LOG_INFO("TB {} PAPI cancel_order: {}", acc.accountId, path);

    asyncRequest(boost::beast::http::verb::delete_, std::move(path), "", "", [this, rcmd, info](boost::system::error_code ec, net::HttpResponse resp) mutable {
        if (ec) {
            LOG_ERROR("TB {} cancel_order ec: {}", acc.accountId, ec.message());
            rcmd.body.orderResponse.orderStatus = OS_FAILED;
            rcmd.body.orderResponse.errorId = NetworkError;
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, ec.message());
            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
            return;
        }
        
        try {
            std::cout << "cancel order: " << resp.body << std::endl;
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                LOG_ERROR("TB {} cancel_order parse err: {}", acc.accountId, resp.body);
                return;
            }

            auto doc_value = doc.get_object().value_unsafe();

            int64_t code = 0;
            int64_t orderId = 0;
            std::string_view msg_sv;
            std::string_view execQ_sv;

            bool has_code = false;
            bool has_orderId = false;

            for (auto field : doc_value) {
                std::string_view k = field.unescaped_key().value_unsafe();
                if (k == "code") {
                    has_code = field.value().get(code) == simdjson::SUCCESS;
                }
                else if (k == "msg") {
                    field.value().get(msg_sv);
                }
                else if (k == "orderId") {
                    has_orderId = field.value().get(orderId) == simdjson::SUCCESS;
                }
                else if (k == "executedQty") {
                    field.value().get(execQ_sv);
                }
            }

            if (has_code) {
                rcmd.body.orderResponse.errorId = crypto::get_binance_errorid(static_cast<int>(code));
                rcmd.body.orderResponse.orderStatus = (rcmd.body.orderResponse.errorId == OrderNotFoundError) ? OS_REJECTED : OS_FAILED;
                if (!msg_sv.empty()) {
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, msg_sv);
                }
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;     
            }

            if (has_orderId) {
                fmt::format_to(rcmd.body.orderResponse.orderId, "{}", orderId);
                if (!execQ_sv.empty()) {
                    rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(execQ_sv) * info.magnifyNumber;
                }

                rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            }
        
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} PAPI cancel_order cb exc: {}", acc.accountId, e.what());
        }
    });
}


// ---- GET order ----
void BinanceUnifiedTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);

    std::string orderPath = "";
    if (tcmd.body.queryOrder.instTypeEnum == SPOT || tcmd.body.queryOrder.instTypeEnum == MARGIN) {
        orderPath = "/papi/v1/margin/order";
    }
    else if (tcmd.body.queryOrder.instTypeEnum == USDT_SWAP || tcmd.body.queryOrder.instTypeEnum == USDT_FUTURES || tcmd.body.queryOrder.instTypeEnum == USDC_SWAP) {
        orderPath = "/papi/v1/um/order";
    }
    else if (tcmd.body.queryOrder.instTypeEnum == C_SWAP || tcmd.body.queryOrder.instTypeEnum == C_FUTURES) {
        orderPath = "/papi/v1/cm/order";
    }

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum, tcmd.body.queryOrder.instTypeEnum, tcmd.body.queryOrder.instId, info)) {
        LOG_INFO("TB {} PAPI query_order smc miss: {}", acc.accountId, tcmd.body.queryOrder.instId);
        return;
    }

    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(5);
    kvs.emplace_back("recvWindow", "5000");
    kvs.emplace_back("symbol", info.originInstId);
    kvs.emplace_back("timestamp", std::to_string(crypto::getCurrentTimeMilli()));

    if (!crypto::str_cmp(tcmd.body.queryOrder.orderId, "")) {
        kvs.emplace_back("orderId", tcmd.body.queryOrder.orderId);
    } else if (!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")) {
        kvs.emplace_back("origClientOrderId", tcmd.body.queryOrder.orderSysId);
    } else {
        return;
    }

    std::string path = buildSignedPath(orderPath, kvs);
    LOG_INFO("TB {} PAPI query_order: {}", acc.accountId, path);

    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "", [this, rcmd, info](boost::system::error_code ec, net::HttpResponse resp) mutable {
        if (ec) {
            LOG_ERROR("TB {} query_order ec: {}", acc.accountId, ec.message());
            return;
        }

        try {
            std::cout << "query order: " << resp.body << std::endl;
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                LOG_ERROR("TB {} query_order parse err: {}", acc.accountId, resp.body);
                return;
            }

            auto doc_value = doc.get_object().value_unsafe();

            int64_t code = 0;
            int64_t orderId = 0;
            std::string_view origQ_sv;
            std::string_view price_sv;
            std::string_view execQ_sv;
            std::string_view cumQ_sv;
            std::string_view avgP_sv;
            std::string_view status_sv;

            bool has_code = false;
            bool has_orderId = false;

            for (auto field : doc_value) {
                std::string_view k = field.unescaped_key().value_unsafe();
                if (k == "code") {
                    has_code = field.value().get(code) == simdjson::SUCCESS;
                }
                else if (k == "orderId") {
                    has_orderId = field.value().get(orderId) == simdjson::SUCCESS;
                }
                else if (k == "origQty") {
                    field.value().get(origQ_sv);
                }
                else if (k == "price") {
                    field.value().get(price_sv);
                }
                else if (k == "executedQty") {
                    field.value().get(execQ_sv);
                }
                else if (k == "cummulativeQuoteQty") {
                    field.value().get(cumQ_sv);
                }
                else if (k == "avgPrice") {
                    field.value().get(avgP_sv);
                }
                else if (k == "status") {
                    field.value().get(status_sv);
                }
            }

            if (has_code) {
                long now = crypto::getCurrentTime();
                if (rcmd.body.orderResponse.clientOrderId > 0 && now - rcmd.body.orderResponse.clientOrderId > ORDER_REJECTED_TIME_OUT) {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    rcmd.body.orderResponse.errorId = crypto::get_binance_errorid(static_cast<int>(code));
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd);
                }
                return;   
            }

            if (has_orderId) {
                fmt::format_to(rcmd.body.orderResponse.orderId, "{}", orderId);
            
                if (!origQ_sv.empty()) {
                    rcmd.body.orderResponse.volumeTotal = crypto::fast_atod(origQ_sv) * info.magnifyNumber;
                }

                if (!price_sv.empty()) {
                    rcmd.body.orderResponse.limitPrice = crypto::fast_atod(price_sv) * info.reduceNumber;
                }

                if (!execQ_sv.empty()) {
                    rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(execQ_sv) * info.magnifyNumber;
                }

                if (rcmd.body.orderResponse.instTypeEnum == SPOT || rcmd.body.orderResponse.instTypeEnum == MARGIN) {
                    if (rcmd.body.orderResponse.volumeTraded > 0 && !cumQ_sv.empty()) {
                        rcmd.body.orderResponse.tradePrice = crypto::fast_atod(cumQ_sv) / rcmd.body.orderResponse.volumeTraded * info.reduceNumber;
                    }
                }
                else {
                    if (!avgP_sv.empty()) {
                        rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avgP_sv) * info.reduceNumber;
                    }
                }

                if (!status_sv.empty()) {
                    std::string st(status_sv);
                    rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(st);
                }
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd);
            }
        
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} PAPI query_order cb exc: {}", acc.accountId, e.what());
        }
    });
}
