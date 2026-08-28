#include "binance/BinanceUFTrade.h"
#include <cmath>
#include <future>
#include <fmt/format.h>
#include <simdjson.h>


BinanceUFTradeUnit::BinanceUFTradeUnit(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {

}

BinanceUFTradeUnit::~BinanceUFTradeUnit() {
    renewStop_.store(true);
    renewCv_.notify_all();
    if (renewThread_.joinable()) {
        renewThread_.join();
    }
}

// ============================================================================
// 签名 / URL
// ============================================================================
std::string BinanceUFTradeUnit::buildSignedPath(std::string_view basePath, const std::vector<std::pair<std::string, std::string>>& kvs) const {
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
bool BinanceUFTradeUnit::generateListenKeySync() {
    // POST /fapi/v1/listenKey (需要 X-MBX-APIKEY header, 不需要 signature)
    std::promise<std::string> prom;
    auto fut = prom.get_future();
    bool responded = false;

    asyncRequest(boost::beast::http::verb::post, listenKeyUrl, /*body=*/"", /*ct=*/"",
        [this, &prom, &responded](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (responded) {
                return;   // 双回调保护
            }
            responded = true;
            if (ec) {
                LOG_ERROR("TB {} listenKey req ec: {}", acc.accountName, ec.message());
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
                } else {
                    LOG_ERROR("TB {} listenKey resp missing: {}", acc.accountName, resp.body);
                    prom.set_value("");
                }
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} listenKey parse exc: {}", acc.accountName, e.what());
                prom.set_value("");
            }
        });

    if (fut.wait_for(std::chrono::seconds(15)) != std::future_status::ready) {
        LOG_ERROR("TB {} listenKey req timeout", acc.accountName);
        return false;
    }
    listenKey_ = fut.get();
    if (listenKey_.empty()) {
        return false;
    }
    LOG_INFO("TB {} listenKey={}", acc.accountName, listenKey_);
    return true;
}

void BinanceUFTradeUnit::renewListenKeyAsync() {
    if (listenKey_.empty()) {
        return;
    }

    // PUT /fapi/v1/listenKey (仅需 X-MBX-APIKEY header)
    asyncRequest(boost::beast::http::verb::put, listenKeyUrl, /*body=*/"", /*ct=*/"",
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) {
                LOG_ERROR("TB {} listenKey renew ec: {}", acc.accountName, ec.message());
                return;
            }
            if (resp.status_code != 200) {
                LOG_ERROR("TB {} listenKey renew status={} body={}", acc.accountName, resp.status_code, resp.body);
                return;
            }
            LOG_DEBUG("TB {} listenKey renewed", acc.accountName);
        });
}

void BinanceUFTradeUnit::listenKeyRenewLoop() {
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
void BinanceUFTradeUnit::subWebsocekt() {
    // 1. REST
    std::string restHost = host_of(acc.restUrl);
    initRestClient(restHost, {{"X-MBX-APIKEY", acc.apiKey}}, 4);

    // 2. listenKey (启动路径, 允许短暂 block ≤15s)
    if (!generateListenKeySync()) {
        LOG_ERROR("TB {} listenKey gen failed, ws NOT started", acc.accountName);
        return;
    }

    // 3. WS
    net::WsConfig cfg;
    cfg.url = acc.wsUrl + wsSubPath + listenKey_;
    cfg.ping_mode = net::WsConfig::PingMode::ServerOnly;
    cfg.auto_reconnect = true;
    cfg.idle_timeout_sec = 60;
    LOG_INFO("TB {} UF ws {} rest {}", acc.accountName, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));

    // 4. 起 renew 后台线程 (每 30min PUT 一次)
    renewThread_ = std::thread([this] { 
        listenKeyRenewLoop(); 
    });
}


// ============================================================================
// WS msg
// ============================================================================
void BinanceUFTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool, int64_t) {
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

        for (auto field : doc_value) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "e") {
                field.value().get(e_sv);
                if (e_sv == "ORDER_TRADE_UPDATE") {
                   has_order_trade_update = true; 
                }
                else if (e_sv == "ACCOUNT_UPDATE") {
                    has_account_update = true;
                }
                if (e_sv == "listenKeyExpired") {
                    LOG_WARN("TB {} listenKey expired, will regen", acc.accountName);
                    std::thread([this]{
                        if (generateListenKeySync()) {
                            LOG_INFO("TB {} listenKey regenerated, will reconnect", acc.accountName);
            
                            net::WsConfig cfg;
                            cfg.url = acc.wsUrl + wsSubPath + listenKey_;
                            cfg.ping_mode = net::WsConfig::PingMode::ServerOnly;
                            cfg.auto_reconnect = true;
                            cfg.idle_timeout_sec = 60;
                            LOG_INFO("TB {} UF ws {} rest {}", acc.accountName, cfg.url, restHost);
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
        }
    }
    catch (const std::exception& e) {
        LOG_ERROR("TB {} UF ws msg exc: {}", acc.accountName, e.what());
    }
}

// ---- ACCOUNT_UPDATE ----
//   { "e":"ACCOUNT_UPDATE", "a":{ "B":[{a,cw,bc,wb}], "P":[{s,pa,ps,iw,ep,up}] } }
void BinanceUFTradeUnit::handleAccountUpdate(simdjson::ondemand::object& a) {
    simdjson::ondemand::array balances;
    simdjson::ondemand::array positions;
    for (auto field : a) {
        std::string_view k = field.unescaped_key().value_unsafe();
        if (k == "B") {
            if (field.value().get(balances) == simdjson::SUCCESS) {      
                for (auto b_val : balances) {
                    auto b_res = b_val.get_object();
                    if (b_res.error()) {
                        continue;
                    }
                    auto& b = b_res.value_unsafe();

                    std::string_view a_sv;
                    std::string_view wb_sv;
                    std::string_view cw_sv;
                    std::string_view bc_sv;
                    for (auto field : b) {
                        std::string_view k = field.unescaped_key().value_unsafe();
                        if (k == "a") {
                            field.value().get(a_sv);
                        }
                        else if (k == "wb") {
                            field.value().get(wb_sv);
                        }
                        else if (k == "cw") {
                            field.value().get(cw_sv);
                        }
                        else if (k == "bc") {
                            field.value().get(bc_sv);
                        }
                    }

                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                    rcmd.body.balance.exchangeTypeEnum = BINANCE;
                    rcmd.body.balance.instTypeEnum = USDT_SWAP;
                    crypto::copy_sv_to_char_array(rcmd.body.balance.accountName, acc.accountName);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(a_sv)));
                    rcmd.body.balance.total = crypto::fast_atod(wb_sv);
                    rcmd.body.balance.available = crypto::fast_atod(cw_sv);
                    rcmd.body.balance.frozen = crypto::fast_atod(bc_sv);
                    rcmd.body.balance.updateTime = crypto::getCurrentTime();
                    rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;
                    PUSH_RCMD(rcmd)
                }
            }
        }
        else if (k == "P") {
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
                    } else if (smc->get_instrument_info(BINANCE, USDT_FUTURES, originInstId.c_str(), info)) {
                        inst = USDT_FUTURES;
                    } else {
                        continue;
                    }

                    double positionAmt = crypto::fast_atod(pa_sv);
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                    rcmd.body.position.exchangeTypeEnum = BINANCE;
                    rcmd.body.position.instTypeEnum = inst;
                    crypto::copy_sv_to_char_array(rcmd.body.position.accountName, acc.accountName);
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
void BinanceUFTradeUnit::handleOrderUpdate(simdjson::ondemand::object& o) {
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
    } else if (smc->get_instrument_info(BINANCE, USDT_FUTURES, originInstId.c_str(), info)) {
        inst = USDT_FUTURES;
    } else {
        LOG_ERROR("TB {} UF order upd smc miss: {}", acc.accountName, originInstId);
        return;
    }

    pubsub::RCommand rcmd;
    memset(&rcmd, 0, sizeof(pubsub::RCommand));
    rcmd.cmdTypeEnum = pubsub::CMD_RPT_NEW_ORDER;
    rcmd.body.orderResponse.exchangeTypeEnum = BINANCE;
    rcmd.body.orderResponse.instTypeEnum = inst;
    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountName, acc.accountName);
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

void BinanceUFTradeUnit::query_account(const pubsub::TCommand& tcmd) {
    query_balance(tcmd);
}

void BinanceUFTradeUnit::query_balance(const pubsub::TCommand& tcmd) {
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"recvWindow", "5000"},
        {"timestamp",  std::to_string(crypto::getCurrentTimeMilli())},
    };
    std::string path = buildSignedPath(balanceUrl, kvs);

    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "", [this](boost::system::error_code ec, ::net::HttpResponse resp) {
        if (ec) { 
            LOG_ERROR("TB {} UF query_balance ec: {}", acc.accountName, ec.message()); 
            return; 
        }
        
        try {
            std::cout << "query balance: " << resp.body << std::endl;
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                return;
            }

            simdjson::ondemand::array assets;
            if (doc["assets"].get(assets) != simdjson::SUCCESS) {
                LOG_ERROR("TB {} UF query_balance no 'assets': {}", acc.accountName, resp.body);
                return;
            }

            std::vector<pubsub::RCommand> pendingBalances;
            pendingBalances.reserve(8);

            for (auto b_val : assets) {
                auto b_res = b_val.get_object();
                if (b_res.error()) {
                    continue;
                }
                auto& b = b_res.value_unsafe();

                std::string_view asset_sv;
                std::string_view walletBalance_sv;
                std::string_view marginBalance_sv;
                std::string_view availableBalance_sv;
                for (auto field : b) {
                    std::string_view k = field.unescaped_key().value_unsafe();
                    if (k == "asset") {
                        field.value().get(asset_sv);
                    }
                    else if (k == "walletBalance") {
                        field.value().get(walletBalance_sv);
                    }
                    else if (k == "marginBalance") {
                        field.value().get(marginBalance_sv);
                    }
                    else if (k == "availableBalance") {
                        field.value().get(availableBalance_sv);
                    }
                }

                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                rcmd.body.balance.exchangeTypeEnum = BINANCE;
                rcmd.body.balance.instTypeEnum = USDT_SWAP;
                crypto::copy_sv_to_char_array(rcmd.body.balance.accountName, acc.accountName);
                crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(asset_sv)));
                rcmd.body.balance.available = crypto::fast_atod(availableBalance_sv);
                rcmd.body.balance.frozen = crypto::fast_atod(marginBalance_sv);
                rcmd.body.balance.total = crypto::fast_atod(walletBalance_sv);
                rcmd.body.balance.updateTime = crypto::getCurrentTime();
                rcmd.body.balance.apiSourceEnum = AS_REST;
                pendingBalances.emplace_back(rcmd);
            }

            if (pendingBalances.empty()) {
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                rcmd.body.balance.exchangeTypeEnum = BINANCE;
                rcmd.body.balance.instTypeEnum = USDT_SWAP;
                crypto::copy_sv_to_char_array(rcmd.body.balance.accountName, acc.accountName);
                crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.currency, std::string("USDT"));
                rcmd.body.balance.updateTime = crypto::getCurrentTime();
                rcmd.body.balance.apiSourceEnum = AS_REST;
                rcmd.body.balance.isLast = true;
                PUSH_RCMD(rcmd);
                return;
            }
            for (size_t i = 0; i < pendingBalances.size(); ++i) {
                pendingBalances[i].body.balance.isLast = (i + 1 == pendingBalances.size());
                PUSH_RCMD(pendingBalances[i]);
            }


            simdjson::ondemand::array positions;
            if (doc["positions"].get(positions) != simdjson::SUCCESS) {
                LOG_ERROR("TB {} UF query_position not array: {}", acc.accountName, resp.body);
                return;
            }

            std::vector<pubsub::RCommand> pendingPositions;
            pendingPositions.reserve(8);
            
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
                std::string_view maintMargin_sv;
                int64_t adl = 0;
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
                    else if (k == "maintMargin") {
                        field.value().get(maintMargin_sv);
                    }
                    else if (k == "adl") {
                        field.value().get(adl);
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
                else {
                    continue;
                }

                double positionAmt = crypto::fast_atod(positionAmt_sv);
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                rcmd.body.position.exchangeTypeEnum = BINANCE;
                rcmd.body.position.instTypeEnum = inst;
                crypto::copy_sv_to_char_array(rcmd.body.position.accountName, acc.accountName);
                crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view(info.instId));
                rcmd.body.position.direction = positionAmt >= 0 ? DT_LONG : DT_SHORT;
                rcmd.body.position.volume = std::abs(positionAmt) * info.magnifyNumber;
                rcmd.body.position.maintMargin = crypto::fast_atod(maintMargin_sv);
                rcmd.body.position.avgPrice = crypto::fast_atod(entryPrice_sv) * info.reduceNumber;
                rcmd.body.position.unrealizedPnl = crypto::fast_atod(unRealizedProfit_sv);
                rcmd.body.position.markPrice = crypto::fast_atod(markPrice_sv) * info.reduceNumber;
                rcmd.body.position.liquidPrice = crypto::fast_atod(liquidationPrice_sv) * info.reduceNumber;
                rcmd.body.position.adlQuantile = static_cast<int>(adl) + 1;
                rcmd.body.position.updateTime = crypto::getCurrentTime();
                rcmd.body.position.apiSourceEnum = AS_REST;
                pendingPositions.emplace_back(rcmd);
            }

            if (pendingPositions.empty()) {
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                rcmd.body.position.exchangeTypeEnum = BINANCE;
                rcmd.body.position.instTypeEnum = USDT_SWAP;
                crypto::copy_sv_to_char_array(rcmd.body.position.accountName, acc.accountName);
                crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view("BTC-USDT"));
                rcmd.body.position.updateTime = crypto::getCurrentTime();
                rcmd.body.position.apiSourceEnum = AS_REST;
                rcmd.body.position.isLast = true;
                PUSH_RCMD(rcmd);
                return;
            }
            for (size_t i = 0; i < pendingPositions.size(); ++i) {
                pendingPositions[i].body.position.isLast = (i + 1 == pendingPositions.size());
                PUSH_RCMD(pendingPositions[i]);
            }
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} UF query_balance cb exc: {}", acc.accountName, e.what());
        }
    });
}

// ---- GET /fapi/v3/positionRisk?... ----
void BinanceUFTradeUnit::query_position(const pubsub::TCommand&) {
    std::vector<std::pair<std::string, std::string>> kvs = {
        {"recvWindow", "5000"},
        {"timestamp",  std::to_string(crypto::getCurrentTimeMilli())},
    };
    std::string path = buildSignedPath(positionUrl, kvs);

    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "", [this](boost::system::error_code ec, ::net::HttpResponse resp) {
        if (ec) { 
            LOG_ERROR("TB {} UF query_position ec: {}", acc.accountName, ec.message()); 
            return; 
        }
        try {
            std::cout << "query position: " << resp.body << std::endl;
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                return; 
            }

            simdjson::ondemand::array positions;
            if (doc.get_array().get(positions) != simdjson::SUCCESS) {
                LOG_ERROR("TB {} UF query_position not array: {}", acc.accountName, resp.body);
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
                std::string_view maintMargin_sv;
                int64_t adl = 0;
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
                    else if (k == "maintMargin") {
                        field.value().get(maintMargin_sv);
                    }
                    else if (k == "adl") {
                        field.value().get(adl);
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
                else {
                    continue;
                }

                double positionAmt = crypto::fast_atod(positionAmt_sv);
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                rcmd.body.position.exchangeTypeEnum = BINANCE;
                rcmd.body.position.instTypeEnum = inst;
                crypto::copy_sv_to_char_array(rcmd.body.position.accountName, acc.accountName);
                crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view(info.instId));
                rcmd.body.position.direction = positionAmt >= 0 ? DT_LONG : DT_SHORT;
                rcmd.body.position.volume = std::abs(positionAmt) * info.magnifyNumber;
                rcmd.body.position.maintMargin = crypto::fast_atod(maintMargin_sv);
                rcmd.body.position.avgPrice = crypto::fast_atod(entryPrice_sv) * info.reduceNumber;
                rcmd.body.position.unrealizedPnl = crypto::fast_atod(unRealizedProfit_sv);
                rcmd.body.position.markPrice = crypto::fast_atod(markPrice_sv) * info.reduceNumber;
                rcmd.body.position.liquidPrice = crypto::fast_atod(liquidationPrice_sv) * info.reduceNumber;
                rcmd.body.position.adlQuantile = static_cast<int>(adl) + 1;
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
                crypto::copy_sv_to_char_array(rcmd.body.position.accountName, acc.accountName);
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
            LOG_ERROR("TB {} UF query_position cb exc: {}", acc.accountName, e.what());
        }
    });
}

// ---- POST /fapi/v1/order?... ----
void BinanceUFTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load()) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId = TBDisconnectError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
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
    }
    else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
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
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    double price = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice * info.magnifyNumber, info.tickSize);
    double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber, info.lotSize);

    std::vector<std::pair<std::string, std::string>> kvs;
    kvs.reserve(12);
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
            kvs.emplace_back("newOrderRespType", "ACK");
            break;
        case OT_MARKET:
            kvs.emplace_back("type", "MARKET");
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "RESULT");
            break;
        case OT_POST_ONLY:
            // UF 用 GTX (post-only), 不是 LIMIT_MAKER
            kvs.emplace_back("type", "LIMIT");
            kvs.emplace_back("timeInForce", "GTX");
            kvs.emplace_back("price", fmt::format("{}", price));
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "ACK");
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

    std::string path = buildSignedPath(newOrderUrl, kvs);
    LOG_INFO("TB {} UF add_new_order: {}", acc.accountName, path);

    asyncRequest(boost::beast::http::verb::post, std::move(path), "", "", [this, rcmd, info](boost::system::error_code ec, net::HttpResponse resp) mutable {
        if (ec) {
            LOG_ERROR("TB {} add_new_order ec: {}", acc.accountName, ec.message());

            if (ec == boost::system::errc::no_stream_resources || ec == boost::system::errc::no_buffer_space || ec == boost::system::errc::not_connected) {
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            }
            else {
                rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
            }

            rcmd.body.orderResponse.errorId = NetworkError;
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, ec.message());
            rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
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
                LOG_ERROR("TB {} add_new_order parse err: {}", acc.accountName, resp.body);
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

                if (rcmd.body.orderResponse.orderStatus != OS_PARTFILLED && rcmd.body.orderResponse.orderStatus != OS_FILLED) { // 有成交的订单不应推送，已经没有成交均价的字段
                    PUSH_RCMD(rcmd)  
                }    
            }
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} add_new_order cb exception: {}", acc.accountName, e.what());
            rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
            rcmd.body.orderResponse.errorId = NetworkError;
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, std::string_view(e.what()));
            rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
        }
    });
}

// ---- DELETE /fapi/v1/order?... ----
void BinanceUFTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load()) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = TBDisconnectError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
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

    std::string path = buildSignedPath(cancelOrderUrl, kvs);
    LOG_INFO("TB {} UF cancel_order: {}", acc.accountName, path);

    asyncRequest(boost::beast::http::verb::delete_, std::move(path), "", "", [this, rcmd, info](boost::system::error_code ec, net::HttpResponse resp) mutable {
        if (ec) {
            LOG_ERROR("TB {} cancel_order ec: {}", acc.accountName, ec.message());
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
                LOG_ERROR("TB {} cancel_order parse err: {}", acc.accountName, resp.body);
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
            LOG_ERROR("TB {} cancel_order cb exception: {}", acc.accountName, e.what());
        }

    });
}

// ---- GET /fapi/v1/order?... ----
void BinanceUFTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum, tcmd.body.queryOrder.instTypeEnum, tcmd.body.queryOrder.instId, info)) {
        LOG_INFO("TB {} UF query_order smc miss: {}", acc.accountName, tcmd.body.queryOrder.instId);
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

    std::string path = buildSignedPath(queryOrderUrl, kvs);
    LOG_INFO("TB {} UF query_order: {}", acc.accountName, path);

    asyncRequest(boost::beast::http::verb::get, std::move(path), "", "", [this, rcmd, info](boost::system::error_code ec, net::HttpResponse resp) mutable {
        if (ec) {
            LOG_ERROR("TB {} query_order ec: {}", acc.accountName, ec.message());
            return;
        }
        try {
            std::cout << "query order: " << resp.body << std::endl;
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                LOG_ERROR("TB {} query_order parse err: {}", acc.accountName, resp.body);
                return;
            }

            auto doc_value = doc.get_object().value_unsafe();

            int64_t code = 0;
            int64_t orderId = 0;
            std::string_view origQ_sv;
            std::string_view price_sv;
            std::string_view execQ_sv;
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
                else if (k == "avgPrice") {
                    field.value().get(avgP_sv);
                }
                else if (k == "status") {
                    field.value().get(status_sv);
                }
            }

            if (has_code) {
                int64_t now = crypto::getCurrentTime();
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

                if (!avgP_sv.empty()) {
                    rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avgP_sv) * info.reduceNumber;;
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
            LOG_ERROR("TB {} query_order cb exception: {}", acc.accountName, e.what());
        }
    });
}