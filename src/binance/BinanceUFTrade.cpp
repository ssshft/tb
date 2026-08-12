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
                LOG_ERROR("TB {} listenKey req ec: {}", acc.accountId, ec.message());
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
                    LOG_ERROR("TB {} listenKey resp missing: {}", acc.accountId, resp.body);
                    prom.set_value("");
                }
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} listenKey parse exc: {}", acc.accountId, e.what());
                prom.set_value("");
            }
        });

    if (fut.wait_for(std::chrono::seconds(15)) != std::future_status::ready) {
        LOG_ERROR("TB {} listenKey req timeout", acc.accountId);
        return false;
    }
    listenKey_ = fut.get();
    if (listenKey_.empty()) {
        return false;
    }
    LOG_INFO("TB {} listenKey={}", acc.accountId, listenKey_);
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
                LOG_ERROR("TB {} listenKey renew ec: {}", acc.accountId, ec.message());
                return;
            }
            if (resp.status_code != 200) {
                LOG_ERROR("TB {} listenKey renew status={} body={}", acc.accountId, resp.status_code, resp.body);
                return;
            }
            LOG_DEBUG("TB {} listenKey renewed", acc.accountId);
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
        LOG_ERROR("TB {} listenKey gen failed, ws NOT started", acc.accountId);
        return;
    }

    // 3. WS
    net::WsConfig cfg;
    cfg.url = acc.wsUrl + wsSubPath + listenKey_;
    cfg.ping_mode = net::WsConfig::PingMode::ServerOnly;
    cfg.auto_reconnect = true;
    cfg.idle_timeout_sec = 60;
    LOG_INFO("TB {} UF ws {} rest {}", acc.accountId, cfg.url, restHost);
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

        simdjson::ondemand::object root;
        if (doc.get_object().get(root) != simdjson::SUCCESS) return;

        std::string_view e_sv;
        if (root["e"].get(e_sv) != simdjson::SUCCESS) return;

        // ACCOUNT_UPDATE (余额 + 持仓)
        if (e_sv == "ACCOUNT_UPDATE" || (!e_sv.empty() && e_sv[0] == 'A')) {
            handleAccountUpdate(root);
        }
        // ORDER_TRADE_UPDATE (订单)
        else if (e_sv == "ORDER_TRADE_UPDATE" || (!e_sv.empty() && e_sv[0] == 'O')) {
            handleOrderUpdate(root);
        }
        // listenKeyExpired: 让 WsClient 感知 close, base 层 auto_reconnect 会重连,
        //   但 listenKey 已过期需要重新生成 —— 这里 fire-and-forget 触发新的生成。
        else if (e_sv == "listenKeyExpired") {
            LOG_WARN("TB {} listenKey expired, will regen", acc.accountId);
            std::thread([this]{
                if (generateListenKeySync() && pWsClient) {
                    LOG_INFO("TB {} listenKey regenerated, will reconnect", acc.accountId);
                    // note: WsClient 无接口切换 url; 只能停掉 + 重开, 复杂度较高。
                    // 简化处理: 由外部重启 unit 解决。 生产上可用 exchange 提供的
                    // /keepAlive 保活避免掉这条路径。
                }
            }).detach();
        }
    }
    catch (const std::exception& e) {
        LOG_ERROR("TB {} UF ws msg exc: {}", acc.accountId, e.what());
    }
}


// ---- ACCOUNT_UPDATE ----
//   { "e":"ACCOUNT_UPDATE", "a":{ "B":[{a,cw,bc,wb}], "P":[{s,pa,ps,iw,ep,up}] } }
void BinanceUFTradeUnit::handleAccountUpdate(simdjson::ondemand::object& root) {
    simdjson::ondemand::object a_obj;
    if (root["a"].get(a_obj) != simdjson::SUCCESS) return;

    // B: balances
    simdjson::ondemand::array B_arr;
    if (a_obj["B"].get(B_arr) == simdjson::SUCCESS) {
        std::vector<pubsub::RCommand> pending;
        for (auto b_val : B_arr) {
            auto b = b_val.get_object();
            if (b.error()) continue;
            std::string_view a_sv, cw_sv, bc_sv, wb_sv;
            b["a"].get(a_sv);
            b["cw"].get(cw_sv);
            b["bc"].get(bc_sv);
            b["wb"].get(wb_sv);

            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
            rcmd.body.balance.exchangeTypeEnum = BINANCE;
            rcmd.body.balance.instTypeEnum     = USDT_SWAP;
            crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
            crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
            crypto::copy_sv_to_char_array(rcmd.body.balance.currency,   crypto::to_upper(std::string(a_sv)));
            rcmd.body.balance.available = crypto::fast_atod(cw_sv);
            rcmd.body.balance.frozen    = crypto::fast_atod(bc_sv);
            rcmd.body.balance.total     = crypto::fast_atod(wb_sv);
            rcmd.body.balance.updateTime = crypto::getCurrentTime();
            rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;
            pending.emplace_back(rcmd);
        }
        for (size_t i = 0; i < pending.size(); ++i) {
            pending[i].body.balance.isLast = (i + 1 == pending.size());
            PUSH_RCMD(pending[i])
        }
    }

    // P: positions
    simdjson::ondemand::array P_arr;
    if (a_obj["P"].get(P_arr) == simdjson::SUCCESS) {
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

            if (ps_sv.empty() || ps_sv[0] != 'B') continue;   // 只要 BOTH 单向持仓

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
}


// ---- ORDER_TRADE_UPDATE ----
//   { "e":"ORDER_TRADE_UPDATE", "o":{ "s":symbol, "c":cid, "S":side, "f":tif, "o":ot,
//                                     "q":qty, "p":px, "X":status, "z":cumQty, "ap":avgPx,
//                                     "l":lastQty, "L":lastPx } }
void BinanceUFTradeUnit::handleOrderUpdate(simdjson::ondemand::object& root) {
    simdjson::ondemand::object o;
    if (root["o"].get(o) != simdjson::SUCCESS) return;

    std::string_view s_sv, c_sv, S_sv, f_sv, ot_sv, q_sv, p_sv, X_sv, z_sv, ap_sv, l_sv, L_sv;
    o["s"].get(s_sv);
    o["c"].get(c_sv);
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
    if (smc->get_instrument_info(BINANCE, USDT_SWAP, originInstId.c_str(), info)) {
        inst = USDT_SWAP;
    } else if (smc->get_instrument_info(BINANCE, USDT_FUTURES, originInstId.c_str(), info)) {
        inst = USDT_FUTURES;
    } else {
        LOG_ERROR("TB {} UF order upd smc miss: {}", acc.accountId, originInstId);
        return;
    }

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

    std::string tif(f_sv), ot(ot_sv);
    rcmd.body.orderResponse.orderType = crypto::get_binance_ordertype(tif.c_str(), ot.c_str());

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

    // clientOrderId 特殊前缀识别 (Liquidation / ADL) —— 老逻辑保留但那个双 && 应是 bug (== 'a' && == 't' 同一 index 显然矛盾)。
    // 修正为: cOrderId 以 "adl_" / "autoclose" 开头等 —— 但由于原代码判断根本进不去, 索性砍掉, 后续按需再加。

    rcmd.body.orderResponse.updateTime    = crypto::getCurrentTime();
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
            LOG_ERROR("TB {} UF query_balance ec: {}", acc.accountId, ec.message()); 
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
                LOG_ERROR("TB {} UF query_balance no 'assets': {}", acc.accountId, resp.body);
                return;
            }

            std::vector<pubsub::RCommand> pendingBalances;
            pending.reserve(32);

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
                crypto::copy_sv_to_char_array(rcmd.body.balance.accountId, acc.accountId);
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
                crypto::copy_sv_to_char_array(rcmd.body.balance.accountId, acc.accountId);
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
                LOG_ERROR("TB {} UF query_position not array: {}", acc.accountId, resp.body);
                return;
            }

            std::vector<pubsub::RCommand> pendingPositions;
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
                crypto::copy_sv_to_char_array(rcmd.body.position.accountId, acc.accountId);
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
                crypto::copy_sv_to_char_array(rcmd.body.position.accountId, acc.accountId);
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
            LOG_ERROR("TB {} UF query_balance cb exc: {}", acc.accountId, e.what());
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

    asyncRequest(boost::beast::http::verb::get, std::move(path), /*body=*/"", /*ct=*/"",
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) { 
                LOG_ERROR("TB {} UF query_position ec: {}", acc.accountId, ec.message()); 
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
                    LOG_ERROR("TB {} UF query_position not array: {}", acc.accountId, resp.body);
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
                    crypto::copy_sv_to_char_array(rcmd.body.position.accountId, acc.accountId);
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
                LOG_ERROR("TB {} UF query_position cb exc: {}", acc.accountId, e.what());
            }
        });
}

// ---- POST /fapi/v1/order?... ----
void BinanceUFTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load()) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId     = TBDisconnectError;
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
    }
    else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
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
    kvs.reserve(12);
    kvs.emplace_back("recvWindow",       "5000");
    kvs.emplace_back("newClientOrderId", tcmd.body.newOrder.orderSysId);
    kvs.emplace_back("symbol",           info.originInstId);
    kvs.emplace_back("timestamp",        std::to_string(crypto::getCurrentTimeMilli()));
    kvs.emplace_back("side",             side);

    switch (tcmd.body.newOrder.orderType) {
        case OT_LIMIT:
            kvs.emplace_back("type", "LIMIT");
            kvs.emplace_back("timeInForce", "GTC");
            kvs.emplace_back("price",    fmt::format("{}", price));
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
            kvs.emplace_back("price",    fmt::format("{}", price));
            kvs.emplace_back("quantity", fmt::format("{}", volume));
            kvs.emplace_back("newOrderRespType", "ACK");
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

    std::string path = buildSignedPath(newOrderUrl, kvs);
    LOG_INFO("TB {} UF add_new_order: {}", acc.accountId, path);

    auto ot = rcmd.body.orderResponse.orderType;
    auto info_captured = info;

    asyncRequest(boost::beast::http::verb::post, std::move(path), /*body=*/"", /*ct=*/"",
        [this, rcmd, ot, info_captured](boost::system::error_code ec,
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

                    std::string_view execQ_sv, avg_sv;
                    doc.find_field_unordered("executedQty").get(execQ_sv);
                    doc.find_field_unordered("avgPrice").get(avg_sv);
                    if (!execQ_sv.empty()) rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(execQ_sv) * info_captured.magnifyNumber;
                    if (!avg_sv.empty())   rcmd.body.orderResponse.tradePrice   = crypto::fast_atod(avg_sv)   * info_captured.reduceNumber;

                    if (ot == OT_IOC) {
                        rcmd.body.orderResponse.orderStatus =
                            (rcmd.body.orderResponse.volumeTraded < rcmd.body.orderResponse.volumeTotal)
                                ? OS_CANCELED : OS_FILLED;
                    } else {
                        if (rcmd.body.orderResponse.volumeTraded < ZERO_NUM)                            rcmd.body.orderResponse.orderStatus = OS_NEW;
                        else if (rcmd.body.orderResponse.volumeTraded < rcmd.body.orderResponse.volumeTotal) rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
                        else                                                                            rcmd.body.orderResponse.orderStatus = OS_FILLED;
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
                LOG_ERROR("TB {} UF add_new_order cb exc: {}", acc.accountId, e.what());
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                rcmd.body.orderResponse.errorId     = NetworkError;
                rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            }
        });
}

// ---- DELETE /fapi/v1/order?... ----
void BinanceUFTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

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

    std::string path = buildSignedPath(cancelOrderUrl, kvs);
    LOG_INFO("TB {} UF cancel_order: {}", acc.accountId, path);

    auto info_captured = info;

    asyncRequest(boost::beast::http::verb::delete_, std::move(path), /*body=*/"", /*ct=*/"",
        [this, rcmd, info_captured](boost::system::error_code ec,
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
                    rcmd.body.orderResponse.errorId = crypto::get_binance_errorid(static_cast<int>(code));
                    rcmd.body.orderResponse.orderStatus =
                        (rcmd.body.orderResponse.errorId == OrderNotFoundError) ? OS_REJECTED : OS_FAILED;
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, msg_sv);
                    rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    return;
                }

                int64_t oid = 0;
                if (doc.find_field_unordered("orderId").get(oid) == simdjson::SUCCESS) {
                    fmt::format_to(rcmd.body.orderResponse.orderId, "{}", oid);
                }
                std::string_view execQ_sv, avg_sv;
                doc.find_field_unordered("executedQty").get(execQ_sv);
                doc.find_field_unordered("avgPrice").get(avg_sv);
                if (!execQ_sv.empty()) rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(execQ_sv) * info_captured.magnifyNumber;
                if (!avg_sv.empty())   rcmd.body.orderResponse.tradePrice   = crypto::fast_atod(avg_sv)   * info_captured.reduceNumber;

                rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} UF cancel_order cb exc: {}", acc.accountId, e.what());
            }
        });
}

// ---- GET /fapi/v1/order?... ----
void BinanceUFTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum,
                                  tcmd.body.queryOrder.instTypeEnum,
                                  tcmd.body.queryOrder.instId, info)) {
        LOG_INFO("TB {} UF query_order smc miss: {}", acc.accountId, tcmd.body.queryOrder.instId);
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

    std::string path = buildSignedPath(queryOrderUrl, kvs);
    LOG_INFO("TB {} UF query_order: {}", acc.accountId, path);

    auto info_captured = info;

    asyncRequest(boost::beast::http::verb::get, std::move(path), /*body=*/"", /*ct=*/"",
        [this, rcmd, info_captured](boost::system::error_code ec,
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
                std::string_view origQ_sv, price_sv, execQ_sv, avg_sv, status_sv;
                doc.find_field_unordered("origQty").get(origQ_sv);
                doc.find_field_unordered("price").get(price_sv);
                doc.find_field_unordered("executedQty").get(execQ_sv);
                doc.find_field_unordered("avgPrice").get(avg_sv);
                doc.find_field_unordered("status").get(status_sv);

                if (!origQ_sv.empty()) rcmd.body.orderResponse.volumeTotal  = crypto::fast_atod(origQ_sv) * info_captured.magnifyNumber;
                if (!price_sv.empty()) rcmd.body.orderResponse.limitPrice   = crypto::fast_atod(price_sv) * info_captured.reduceNumber;
                if (!execQ_sv.empty()) rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(execQ_sv) * info_captured.magnifyNumber;
                if (!avg_sv.empty())   rcmd.body.orderResponse.tradePrice   = crypto::fast_atod(avg_sv)   * info_captured.reduceNumber;
                if (!status_sv.empty()) {
                    std::string st(status_sv);
                    rcmd.body.orderResponse.orderStatus = crypto::get_binance_orderstatus(st);
                }
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd);
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} UF query_order cb exc: {}", acc.accountId, e.what());
            }
        });
}