#include "okx/OkxTrade.h"
#include <chrono>
#include <cmath>
#include <ctime>
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include "base64.hpp"
#include <fmt/format.h>
#include <simdjson.h>


OkxTradeUnit::OkxTradeUnit(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {

}

OkxTradeUnit::~OkxTradeUnit() {

}

// ============================================================================
// WS login / subscribe payload
// ============================================================================
std::string OkxTradeUnit::buildLoginJson() const {
    std::string ts = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign = crypto::getOkxSignatureWsLogin(acc.secretKey, ts, "GET/users/self/verify");

    return fmt::format(
        R"({{"op":"login","args":[{{"apiKey":"{}","passphrase":"{}","timestamp":"{}","sign":"{}"}}]}})",
        acc.apiKey, acc.password, ts, sign);
}

std::string OkxTradeUnit::buildSubscribeJson() const {
    // 一条 subscribe 里带三个 channel: account (updateInterval=0), positions (ANY), orders (ANY)
    return
        R"({"op":"subscribe","args":[)"
        R"({"channel":"account","extraParams":"{\"updateInterval\":0}"},)"
        R"({"channel":"positions","instType":"ANY","extraParams":"{\"updateInterval\":0}"},)"
        R"({"channel":"orders","instType":"ANY"}])"
        R"(})";
}


// ============================================================================
// subWebsocekt
// ============================================================================
void OkxTradeUnit::subWebsocekt() {
    std::string restHost = crypto::host_of(acc.restUrl);

    // OKX 有的仿真环境需要额外 header, 由 isSimulated 决定
    std::vector<std::pair<std::string, std::string>> defaultHeaders;
    if (acc.isSimulated) {
        defaultHeaders.emplace_back("x-simulated-trading", "1");
    }
    initRestClient(restHost, std::move(defaultHeaders), 4);

    net::WsConfig cfg;
    cfg.url = acc.wsUrl;
    cfg.ping_mode = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text = "ping";   // OKX 接受原始 "ping" 文本
    cfg.auto_reconnect = true;
    cfg.idle_timeout_sec = 60;
    LOG_INFO("TB {} OKX ws {} rest {}", acc.accountName, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));
}


// ============================================================================
// onOpen: 先 login, 收到 login ack 后再 subscribe。
// 这里把 login 直接 send_text 出去; subscribe 在 handleWsAck 里发。
// 简化: 也可以直接连发, OKX server 会先处理 login。
// ============================================================================
void OkxTradeUnit::onOpen() {
    BaseTradeUnit::onOpen();
    // 先 login, 再直接 subscribe (server 按顺序处理, subscribe 会在 login ack 后被拒或通过)。
    // 更严格的做法是等 login "code":"0" 后再 subscribe, 但那个状态机成本大。
    // OKX 实测直接连发也可以 —— subscribe 会被 buffer, login 成功后 server 挨个响应。
    pWsClient->send_text(buildLoginJson());
    pWsClient->send_text(buildSubscribeJson());
}

// ============================================================================
// onWebsocketMsg
// ============================================================================
void OkxTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool, int64_t) {
    try {
        std::string msg(reinterpret_cast<const char*>(data), len);
        std::cout << "onWebsocketMsg: " << msg << std::endl;

        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc = g_parser.iterate(padded);
        if (doc.error()) {
            return;
        }

        auto doc_value = doc.get_object().value_unsafe();

        simdjson::ondemand::object arg_obj;
        std::string_view channel_sv;
        simdjson::ondemand::array data_arr;

        for (auto field : doc_value) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "arg") {
                field.value().get(arg_obj);
                arg_obj["channel"].get(channel_sv);
            }
            else if (k == "data") {
                field.value().get(data_arr);
                if (channel_sv == "account") {
                    handleAccountUpdate(data_arr);
                }
                else if (channel_sv == "positions") {
                    handlePositionsUpdate(data_arr);
                }
                else if (channel_sv == "orders") {
                    handleOrdersUpdate(data_arr);
                }
            }
        }
    }
    catch (const std::exception& e) {
        LOG_ERROR("TB {} OKX ws exc: {}", acc.accountName, e.what());
    }
}


// ---- account update ----
// data = [{totalEq, adjEq, mmr, mgnRatio, details:[{ccy, cashBal, availEq, frozenBal, upl}]}]
void OkxTradeUnit::handleAccountUpdate(simdjson::ondemand::array& arr) {
    for (auto b_val : arr) {
        auto b_res = b_val.get_object();
        if (b_res.error()) {
            continue;
        }
        auto& b = b_res.value_unsafe();

        std::string_view teq_sv;
        std::string_view aeq_sv;
        std::string_view mmr_sv;
        std::string_view mgnR_sv;

        simdjson::ondemand::array detailsArr;
        std::vector<pubsub::RCommand> pending;
        for (auto field : b) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "totalEq") {
                field.value().get(teq_sv);
            }
            else if (k == "adjEq") {
                field.value().get(aeq_sv);
            }
            else if (k == "mmr") {
                field.value().get(mmr_sv);
            }
            else if (k == "mgnRatio") {
                field.value().get(mgnR_sv);
            }
            else if (k == "details") {
                if (field.value().get(detailsArr) == simdjson::SUCCESS) {
                    for (auto d_val : detailsArr) {
                        auto d_res = d_val.get_object();
                        if (d_res.error()) {
                            continue;
                        }
                        auto& d = d_res.value_unsafe();

                        std::string_view ccy_sv;
                        std::string_view cash_sv;
                        std::string_view avail_sv;
                        std::string_view frozen_sv;
                        std::string_view eq_sv;              
                        for (auto bf : d) {
                            std::string_view bk = bf.unescaped_key().value_unsafe();
                            if (bk == "ccy") {
                                bf.value().get(ccy_sv);
                            }
                            else if (bk == "cashBal") {
                                bf.value().get(cash_sv);
                            }
                            else if (bk == "availEq") {
                                bf.value().get(avail_sv);
                            }
                            else if (bk == "frozenBal") {
                                bf.value().get(frozen_sv);
                            }
                            else if (bk == "eq") {
                                bf.value().get(eq_sv);
                            }
                        }

                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                        rcmd.body.balance.exchangeTypeEnum = OKX;
                        rcmd.body.balance.instTypeEnum = SPOT;
                        crypto::copy_sv_to_char_array(rcmd.body.balance.accountName, acc.accountName);
                        crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                        crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(ccy_sv)));
                        rcmd.body.balance.available = crypto::fast_atod(avail_sv);
                        rcmd.body.balance.frozen = crypto::fast_atod(frozen_sv);
                        rcmd.body.balance.total = crypto::fast_atod(eq_sv);
                        rcmd.body.balance.updateTime = crypto::getCurrentTime();
                        rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;
                        PUSH_RCMD(rcmd)
                    }
                }
            }
        }

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
        rcmd.body.totalAccount.exchangeTypeEnum = OKX;
        rcmd.body.totalAccount.instTypeEnum = SPOT;
        crypto::copy_sv_to_char_array(rcmd.body.totalAccount.accountName, acc.accountName);
        crypto::copy_sv_to_char_array(rcmd.body.totalAccount.strategyId, acc.strategyId);
        rcmd.body.totalAccount.totalEquity = crypto::fast_atod(teq_sv);
        rcmd.body.totalAccount.adjEquity = crypto::fast_atod(aeq_sv);
        rcmd.body.totalAccount.mmr = crypto::fast_atod(mmr_sv);
        rcmd.body.totalAccount.mgnRatio = mgnR_sv.empty() ? 100.0 : crypto::fast_atod(mgnR_sv);
        rcmd.body.totalAccount.updateTime = crypto::getCurrentTime();
        rcmd.body.totalAccount.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd)
    }
}


// ---- positions update ----
// data = [{instType, instId, pos, avgPx, mmr, upl, markPx, liqPx, adl}]
void OkxTradeUnit::handlePositionsUpdate(simdjson::ondemand::array& arr) {
    for (auto b_val : arr) {
        auto b_res = b_val.get_object();
        if (b_res.error()) {
            continue;
        }
        auto& b = b_res.value_unsafe();

        std::string_view iType_sv;
        std::string_view iId_sv;
        std::string_view pos_sv;
        std::string_view avg_sv;
        std::string_view mmr_sv;
        std::string_view upl_sv;
        std::string_view mark_sv;
        std::string_view liq_sv;
        std::string_view adl_sv;

        for (auto field : b) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "instType") {
                field.value().get(iType_sv);
            }
            else if (k == "instId") {
                field.value().get(iId_sv);
            }
            else if (k == "pos") {
                field.value().get(pos_sv);
            }
            else if (k == "avgPx") {
                field.value().get(avg_sv);
            }
            else if (k == "mmr") {
                field.value().get(mmr_sv);
            }
            else if (k == "upl") {
                field.value().get(upl_sv);
            }
            else if (k == "markPx") {
                field.value().get(mark_sv);
            }
            else if (k == "liqPx") {
                field.value().get(liq_sv);
            }
            else if (k == "adl") {
                field.value().get(adl_sv);
            }
        }

        std::string originInstId(iId_sv);
        md::InstrumentInfo info;
        InstType instType;

        if (iType_sv == "SPOT") {
            if (smc->get_instrument_info(OKX, SPOT, originInstId.c_str(), info)) { 
                instType = SPOT; 
            }
        } else if (iType_sv == "MARGIN") {
            if (smc->get_instrument_info(OKX, MARGIN, originInstId.c_str(), info)) { 
                instType = MARGIN;
            }
        } else if (iType_sv == "SWAP" || iType_sv == "FUTURES") {
            // 依次试 USDT_* → C_* (根据 instId 是否含 -USDT- 大致预判也可, 但这么写更 robust)
            InstType u_swap = (iType_sv == "SWAP") ? USDT_SWAP : USDT_FUTURES;
            InstType c_swap = (iType_sv == "SWAP") ? C_SWAP : C_FUTURES;

            if (smc->get_instrument_info(OKX, u_swap, originInstId.c_str(), info)) { 
                instType = u_swap;
            }

            if (smc->get_instrument_info(OKX, c_swap, originInstId.c_str(), info)) { 
                instType = c_swap;
            }
        } else {
            continue;
        }

        double positionAmt = crypto::fast_atod(pos_sv);
        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
        rcmd.body.position.exchangeTypeEnum = OKX;
        rcmd.body.position.instTypeEnum = instType;
        crypto::copy_sv_to_char_array(rcmd.body.position.accountName, acc.accountName);
        crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view(info.instId));
        rcmd.body.position.direction = positionAmt > 0 ? DT_LONG : DT_SHORT;
        rcmd.body.position.volume = std::fabs(positionAmt);
        rcmd.body.position.maintMargin = crypto::fast_atod(mmr_sv);
        rcmd.body.position.avgPrice = crypto::fast_atod(avg_sv);
        rcmd.body.position.unrealizedPnl = crypto::fast_atod(upl_sv);
        rcmd.body.position.markPrice = crypto::fast_atod(mark_sv);
        if (!liq_sv.empty()) rcmd.body.position.liquidPrice = crypto::fast_atod(liq_sv);
        rcmd.body.position.adlQuantile = static_cast<int>(crypto::fast_atod(adl_sv));
        rcmd.body.position.updateTime = crypto::getCurrentTime();
        rcmd.body.position.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd);
    }
}


// ---- orders update ----
// data = [{instType, instId, ordId, clOrdId, sz, px, side, ordType, state, accFillSz, avgPx}]
void OkxTradeUnit::handleOrdersUpdate(simdjson::ondemand::array& arr) {
    for (auto b_val : arr) {
        auto b_res = b_val.get_object();
        if (b_res.error()) {
            continue;
        }
        auto& b = b_res.value_unsafe();

        for (auto field : b) {
            std::string_view iType_sv;
            std::string_view iId_sv;
            std::string_view ordId_sv;
            std::string_view clOrdId_sv;
            std::string_view sz_sv;
            std::string_view px_sv;
            std::string_view side_sv;
            std::string_view oType_sv;
            std::string_view state_sv;
            std::string_view accFill_sv;
            std::string_view avgPx_sv;
            std::string_view category_sv;

            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "instType") {
                field.value().get(iType_sv);
            }
            else if (k == "instId") {
                field.value().get(iId_sv);
            }
            else if (k == "ordId") {
                field.value().get(ordId_sv);
            }
            else if (k == "clOrdId") {
                field.value().get(clOrdId_sv);
            }
            else if (k == "sz") {
                field.value().get(sz_sv);
            }
            else if (k == "px") {
                field.value().get(px_sv);
            }
            else if (k == "side") {
                field.value().get(side_sv);
            }
            else if (k == "ordType") {
                field.value().get(oType_sv);
            }
            else if (k == "accFillSz") {
                field.value().get(accFill_sv);
            }
            else if (k == "avgPx") {
                field.value().get(avgPx_sv);
            }
            else if (k == "state") {
                field.value().get(state_sv);
            }
            else if (k == "category") {
                field.value().get(category_sv);
            }

            std::string originInstId(iId_sv);
            md::InstrumentInfo info;
            InstType instType;

            if (iType_sv == "SPOT") {
                if (smc->get_instrument_info(OKX, SPOT, originInstId.c_str(), info)) { 
                    instType = SPOT; 
                }
            } else if (iType_sv == "MARGIN") {
                if (smc->get_instrument_info(OKX, MARGIN, originInstId.c_str(), info)) { 
                    instType = MARGIN;
                }
            } else if (iType_sv == "SWAP" || iType_sv == "FUTURES") {
                // 依次试 USDT_* → C_* (根据 instId 是否含 -USDT- 大致预判也可, 但这么写更 robust)
                InstType u_swap = (iType_sv == "SWAP") ? USDT_SWAP : USDT_FUTURES;
                InstType c_swap = (iType_sv == "SWAP") ? C_SWAP : C_FUTURES;

                if (smc->get_instrument_info(OKX, u_swap, originInstId.c_str(), info)) { 
                    instType = u_swap;
                }

                if (smc->get_instrument_info(OKX, c_swap, originInstId.c_str(), info)) { 
                    instType = c_swap;
                }
            } else {
                continue;
            }

            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
            rcmd.body.orderResponse.exchangeTypeEnum = OKX;
            rcmd.body.orderResponse.instTypeEnum = instType;
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountName, acc.accountName);
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.strategyId, acc.strategyId);
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.instId, std::string_view(info.instId));
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, ordId_sv);
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, clOrdId_sv);

            rcmd.body.orderResponse.offsetFlag = OF_OPEN;
            if (!side_sv.empty()) {
                rcmd.body.orderResponse.direction = (side_sv[0] == 'b') ? DT_LONG : DT_SHORT;
            }

            if (!sz_sv.empty()) {
                rcmd.body.orderResponse.volumeTotal = crypto::fast_atod(sz_sv);
            }

            if (!px_sv.empty()) {
                rcmd.body.orderResponse.limitPrice = crypto::fast_atod(px_sv);
            }

            if (!accFill_sv.empty()) {
                rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(accFill_sv);
            }

            if (!avgPx_sv.empty()) {
                rcmd.body.orderResponse.tradePrice = crypto::fast_atod(avgPx_sv);
            }

            if (!oType_sv.empty()) {
                switch (oType_sv[0]) {
                    case 'l': 
                        rcmd.body.orderResponse.orderType = OT_LIMIT;      
                        break;
                    case 'm': 
                        rcmd.body.orderResponse.orderType = OT_MARKET;     
                        break;
                    case 'p': 
                        rcmd.body.orderResponse.orderType = OT_POST_ONLY;  
                        break;
                    case 'f': 
                        rcmd.body.orderResponse.orderType = OT_FOK;        
                        break;
                    case 'i': 
                        rcmd.body.orderResponse.orderType = OT_IOC;        
                        break;
                    case 'o': 
                        rcmd.body.orderResponse.orderType = OT_MARKET;     
                        break;   // optimal_limit_ioc
                    default:  
                        break;
                }
            }

            // state: live / partially_filled / filled / canceled / mmp_canceled
            if (!state_sv.empty()) {
                if (state_sv == "live") {
                    rcmd.body.orderResponse.orderStatus = OS_NEW;
                }         
                else if (state_sv == "partially_filled") {
                    rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
                }
                else if (state_sv == "filled") {
                    rcmd.body.orderResponse.orderStatus = OS_FILLED;
                }
                else if (state_sv == "canceled" || state_sv == "mmp_canceled") {
                    rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                }                                    
                else {
                    rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                }                                
            }

            if (category_sv == "adl") {
                rcmd.body.orderResponse.errorId = ADLError;
            }
            else if (category_sv == "twap") {
                rcmd.body.orderResponse.errorId = TwapError;
            }
            else if (category_sv == "full_liquidation") {
                rcmd.body.orderResponse.errorId = LiquidationError;
            }

            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
            PUSH_RCMD(rcmd)
        }
    }
}

// ============================================================================
// query_account / balance / position: 都是 REST 查询, 大同小异
// ============================================================================
void OkxTradeUnit::query_account(const pubsub::TCommand& tcmd) {
    query_balance(tcmd);   // account 主要靠 balance 返回的 totalEq / adjEq
}

void OkxTradeUnit::query_balance(const pubsub::TCommand& tcmd) {
    std::string ts = crypto::getTimestampIso();
    std::string sign = crypto::getOkxSignatureRest(acc.secretKey, ts, "GET", balanceUrl, "");
    std::vector<std::pair<std::string, std::string>> headers = {{"OK-ACCESS-KEY", acc.apiKey}, {"OK-ACCESS-TIMESTAMP", ts}, {"OK-ACCESS-SIGN", sign}, {"OK-ACCESS-PASSPHRASE", acc.password}};

    asyncRequest(boost::beast::http::verb::get, balanceUrl, "", "", std::move(headers), [this](boost::system::error_code ec, net::HttpResponse resp) {
        if (ec) { 
            LOG_ERROR("TB {} OKX query_balance ec: {}", acc.accountName, ec.message()); 
            return; 
        }

        try {
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                return;
            }

            simdjson::ondemand::array arr;
            if (doc["data"].get(arr) != simdjson::SUCCESS) {
                for (auto b_val : arr) {
                    auto b_res = b_val.get_object();
                    if (b_res.error()) {
                        continue;
                    }
                    auto& b = b_res.value_unsafe();

                    std::string_view teq_sv;
                    std::string_view aeq_sv;
                    std::string_view mmr_sv;
                    std::string_view mgnR_sv;

                    simdjson::ondemand::array detailsArr;
                    std::vector<pubsub::RCommand> pending;
                    for (auto field : b) {
                        std::string_view k = field.unescaped_key().value_unsafe();
                        if (k == "totalEq") {
                            field.value().get(teq_sv);
                        }
                        else if (k == "adjEq") {
                            field.value().get(aeq_sv);
                        }
                        else if (k == "mmr") {
                            field.value().get(mmr_sv);
                        }
                        else if (k == "mgnRatio") {
                            field.value().get(mgnR_sv);
                        }
                        else if (k == "details") {
                            if (field.value().get(detailsArr) == simdjson::SUCCESS) {
                                for (auto d_val : detailsArr) {
                                    auto d_res = d_val.get_object();
                                    if (d_res.error()) {
                                        continue;
                                    }
                                    auto& d = d_res.value_unsafe();

                                    std::string_view ccy_sv;
                                    std::string_view cash_sv;
                                    std::string_view avail_sv;
                                    std::string_view frozen_sv;
                                    std::string_view eq_sv;              
                                    for (auto bf : d) {
                                        std::string_view bk = bf.unescaped_key().value_unsafe();
                                        if (bk == "ccy") {
                                            bf.value().get(ccy_sv);
                                        }
                                        else if (bk == "cashBal") {
                                            bf.value().get(cash_sv);
                                        }
                                        else if (bk == "availEq") {
                                            bf.value().get(avail_sv);
                                        }
                                        else if (bk == "frozenBal") {
                                            bf.value().get(frozen_sv);
                                        }
                                        else if (bk == "eq") {
                                            bf.value().get(eq_sv);
                                        }
                                    }

                                    pubsub::RCommand rcmd;
                                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                                    rcmd.body.balance.exchangeTypeEnum = OKX;
                                    rcmd.body.balance.instTypeEnum = SPOT;
                                    crypto::copy_sv_to_char_array(rcmd.body.balance.accountName, acc.accountName);
                                    crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                                    crypto::copy_sv_to_char_array(rcmd.body.balance.currency, crypto::to_upper(std::string(ccy_sv)));
                                    rcmd.body.balance.available = crypto::fast_atod(avail_sv);
                                    rcmd.body.balance.frozen = crypto::fast_atod(frozen_sv);
                                    rcmd.body.balance.total = crypto::fast_atod(eq_sv);
                                    rcmd.body.balance.updateTime = crypto::getCurrentTime();
                                    rcmd.body.balance.apiSourceEnum = AS_REST;
                                    pending.emplace_back(rcmd);
                                }
                            }
                        }
                    }

                    for (size_t i = 0; i < pending.size(); ++i) {
                        pending[i].body.balance.isLast = (i + 1 == pending.size());
                        PUSH_RCMD(pending[i])
                    }

                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
                    rcmd.body.totalAccount.exchangeTypeEnum = OKX;
                    rcmd.body.totalAccount.instTypeEnum = SPOT;
                    crypto::copy_sv_to_char_array(rcmd.body.totalAccount.accountName, acc.accountName);
                    crypto::copy_sv_to_char_array(rcmd.body.totalAccount.strategyId, acc.strategyId);
                    rcmd.body.totalAccount.totalEquity = crypto::fast_atod(teq_sv);
                    rcmd.body.totalAccount.adjEquity = crypto::fast_atod(aeq_sv);
                    rcmd.body.totalAccount.mmr = crypto::fast_atod(mmr_sv);
                    rcmd.body.totalAccount.mgnRatio = mgnR_sv.empty() ? 100.0 : crypto::fast_atod(mgnR_sv);
                    rcmd.body.totalAccount.updateTime = crypto::getCurrentTime();
                    rcmd.body.totalAccount.apiSourceEnum = AS_REST;
                    PUSH_RCMD(rcmd)
                }
            }
        } catch (const std::exception& e) {
            LOG_ERROR("TB {} OKX query_balance cb exc: {}", acc.accountName, e.what());
        }
    });
}

void OkxTradeUnit::query_position(const pubsub::TCommand&) {
    std::string ts = crypto::getTimestampIso();
    std::string sign = crypto::getOkxSignatureRest(acc.secretKey, ts, "GET", positionUrl, "");
    std::vector<std::pair<std::string, std::string>> headers = {{"OK-ACCESS-KEY", acc.apiKey}, {"OK-ACCESS-TIMESTAMP", ts}, {"OK-ACCESS-SIGN", sign}, {"OK-ACCESS-PASSPHRASE", acc.password}};

    asyncRequest(boost::beast::http::verb::get, positionUrl, "", "", std::move(headers), [this](boost::system::error_code ec, net::HttpResponse resp) {
        if (ec) { 
            LOG_ERROR("TB {} OKX query_position ec: {}", acc.accountName, ec.message()); 
            return; 
        }    
        
        try {
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                return;
            }

            simdjson::ondemand::array arr;
            if (doc["data"].get(arr) != simdjson::SUCCESS) {
                std::vector<pubsub::RCommand> pending;

                for (auto b_val : arr) {
                    auto b_res = b_val.get_object();
                    if (b_res.error()) {
                        continue;
                    }
                    auto& b = b_res.value_unsafe();

                    std::string_view iType_sv;
                    std::string_view iId_sv;
                    std::string_view pos_sv;
                    std::string_view avg_sv;
                    std::string_view mmr_sv;
                    std::string_view upl_sv;
                    std::string_view mark_sv;
                    std::string_view liq_sv;
                    std::string_view adl_sv;

                    for (auto field : b) {
                        std::string_view k = field.unescaped_key().value_unsafe();
                        if (k == "instType") {
                            field.value().get(iType_sv);
                        }
                        else if (k == "instId") {
                            field.value().get(iId_sv);
                        }
                        else if (k == "pos") {
                            field.value().get(pos_sv);
                        }
                        else if (k == "avgPx") {
                            field.value().get(avg_sv);
                        }
                        else if (k == "mmr") {
                            field.value().get(mmr_sv);
                        }
                        else if (k == "upl") {
                            field.value().get(upl_sv);
                        }
                        else if (k == "markPx") {
                            field.value().get(mark_sv);
                        }
                        else if (k == "liqPx") {
                            field.value().get(liq_sv);
                        }
                        else if (k == "adl") {
                            field.value().get(adl_sv);
                        }
                    }

                    std::string originInstId(iId_sv);
                    md::InstrumentInfo info;
                    InstType instType;

                    if (iType_sv == "SPOT") {
                        if (smc->get_instrument_info(OKX, SPOT, originInstId.c_str(), info)) { 
                            instType = SPOT; 
                        }
                    } else if (iType_sv == "MARGIN") {
                        if (smc->get_instrument_info(OKX, MARGIN, originInstId.c_str(), info)) { 
                            instType = MARGIN;
                        }
                    } else if (iType_sv == "SWAP" || iType_sv == "FUTURES") {
                        // 依次试 USDT_* → C_* (根据 instId 是否含 -USDT- 大致预判也可, 但这么写更 robust)
                        InstType u_swap = (iType_sv == "SWAP") ? USDT_SWAP : USDT_FUTURES;
                        InstType c_swap = (iType_sv == "SWAP") ? C_SWAP : C_FUTURES;

                        if (smc->get_instrument_info(OKX, u_swap, originInstId.c_str(), info)) { 
                            instType = u_swap;
                        }

                        if (smc->get_instrument_info(OKX, c_swap, originInstId.c_str(), info)) { 
                            instType = c_swap;
                        }
                    } else {
                        continue;
                    }

                    double positionAmt = crypto::fast_atod(pos_sv);
                    pubsub::RCommand rcmd;
                    memset(&rcmd, 0, sizeof(pubsub::RCommand));
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
                    rcmd.body.position.exchangeTypeEnum = OKX;
                    rcmd.body.position.instTypeEnum = instType;
                    crypto::copy_sv_to_char_array(rcmd.body.position.accountName, acc.accountName);
                    crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
                    crypto::copy_sv_to_char_array(rcmd.body.position.instId, std::string_view(info.instId));
                    rcmd.body.position.direction = positionAmt > 0 ? DT_LONG : DT_SHORT;
                    rcmd.body.position.volume = std::fabs(positionAmt);
                    rcmd.body.position.maintMargin = crypto::fast_atod(mmr_sv);
                    rcmd.body.position.avgPrice = crypto::fast_atod(avg_sv);
                    rcmd.body.position.unrealizedPnl = crypto::fast_atod(upl_sv);
                    rcmd.body.position.markPrice = crypto::fast_atod(mark_sv);
                    if (!liq_sv.empty()) rcmd.body.position.liquidPrice = crypto::fast_atod(liq_sv);
                    rcmd.body.position.adlQuantile = static_cast<int>(crypto::fast_atod(adl_sv));
                    rcmd.body.position.updateTime = crypto::getCurrentTime();
                    rcmd.body.position.apiSourceEnum = AS_REST;
                    pending.emplace_back(rcmd);
                }

                for (size_t i = 0; i < pending.size(); ++i) {
                    pending[i].body.position.isLast = (i + 1 == pending.size());
                    PUSH_RCMD(pending[i])
                }
            }
        } catch (const std::exception& e) {
            LOG_ERROR("TB {} OKX query_position cb exc: {}", acc.accountName, e.what());
        }
    });
}

// ============================================================================
// add_new_order —— POST /api/v5/trade/order (body 是 JSON)
// ============================================================================
void OkxTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
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
            side = "buy";
        }
        else if (tcmd.body.newOrder.direction == DT_SHORT) {
            side = "sell";
        }
    } 
    else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
        if (tcmd.body.newOrder.direction == DT_LONG) {
            side = "sell";
        }
        else if (tcmd.body.newOrder.direction == DT_SHORT) {
            side = "buy";
        }
    }

    if (!side) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId = (tcmd.body.newOrder.offsetFlag == OF_OPEN || tcmd.body.newOrder.offsetFlag == OF_CLOSE) ? DirectionError : OffsetFlagError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const char* ordType = nullptr;
    switch (tcmd.body.newOrder.orderType) {
        case OT_LIMIT:     
            ordType = "limit";     
            break;
        case OT_MARKET:    
            ordType = "market";    
            break;
        case OT_POST_ONLY: 
            ordType = "post_only"; 
            break;
        case OT_FOK:       
            ordType = "fok";       
            break;
        case OT_IOC:       
            ordType = "ioc";       
            break;
        default:
            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            rcmd.body.orderResponse.errorId = OrderTypeError;
            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
            return;
    }

    double price = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice * info.magnifyNumber, info.tickSize);
    double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber, info.lotSize);
    std::string price_str = fmt::format("{}", price);
    std::string sz_str = fmt::format("{}", volume);

    // OKX 必需字段: instId / tdMode / side / ordType / sz; 限价单还要 px。
    std::string body;
    if (tcmd.body.newOrder.orderType == OT_MARKET) {
        body = fmt::format(
            R"({{"instId":"{}","tdMode":"cross","side":"{}","ordType":"{}","sz":"{}","clOrdId":"{}","reduceOnly":{}}})",
            info.originInstId, side, ordType, sz_str, tcmd.body.newOrder.orderSysId, tcmd.body.newOrder.reduceOnly ? "true" : "false");
    } 
    else {
        body = fmt::format(
            R"({{"instId":"{}","tdMode":"cross","side":"{}","ordType":"{}","px":"{}","sz":"{}","clOrdId":"{}","reduceOnly":{}}})",
            info.originInstId, side, ordType, price_str, sz_str, tcmd.body.newOrder.orderSysId, tcmd.body.newOrder.reduceOnly ? "true" : "false");
    }

    std::string ts = crypto::getTimestampIso();
    std::string sign = crypto::getOkxSignatureRest(acc.secretKey, ts, "POST", orderUrl, body);
    std::vector<std::pair<std::string, std::string>> headers = {{"OK-ACCESS-KEY", acc.apiKey}, {"OK-ACCESS-TIMESTAMP", ts}, {"OK-ACCESS-SIGN", sign}, {"OK-ACCESS-PASSPHRASE", acc.password}};

    LOG_INFO("TB {} OKX add_new_order body={}", acc.accountName, body);

    asyncRequest(boost::beast::http::verb::post, orderUrl, std::move(body), "application/json", std::move(headers), [this, rcmd, info](boost::system::error_code ec, net::HttpResponse resp) mutable {
        if (ec) {
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
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                rcmd.body.orderResponse.errorId = UnknownError;
                rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;
            }

            auto doc_value = doc.get_object().value_unsafe();

            std::string_view code_sv;
            std::string_view msg_sv;
            simdjson::ondemand::array data_arr;

            std::string_view orderId_sv;
            std::string_view sCode_sv;
            std::string_view sMsg_sv;

            for (auto field : doc_value) {
                std::string_view k = field.unescaped_key().value_unsafe();
                if (k == "code") {
                    field.value().get(code_sv);
                }
                else if (k == "msg") {
                    field.value().get(msg_sv);
                }
                else if (k == "data") {
                    field.value().get(data_arr);

                    for (auto b_val : data_arr) {
                        auto b_res = b_val.get_object();
                        if (b_res.error()) {
                            continue;
                        }
                        auto& b = b_res.value_unsafe();

                        for (auto field : b) {
                            std::string_view k = field.unescaped_key().value_unsafe();
                            if (k == "orderId") {
                                field.value().get(orderId_sv);
                            }
                            else if (k == "sCode") {
                                field.value().get(sCode_sv);
                            }
                            else if (k == "sMsg") {
                                field.value().get(sMsg_sv);
                            }
                        }
                    }
                }
            }

            if (code_sv == "0") {
                if (sCode_sv == "0") {
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, orderId_sv);
                    rcmd.body.orderResponse.orderStatus = OS_NEW;
                }
                else {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    rcmd.body.orderResponse.errorId = crypto::get_okx_errorid(crypto::fast_atol(sCode_sv));   // TODO sCode 映射
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, sMsg_sv);  
                }
            }
            else {
                int code = 0;
                if (!sCode_sv.empty()) {
                    code = crypto::fast_atol(sCode_sv);
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, sMsg_sv);
                }
                else {
                    code = crypto::fast_atol(code_sv);
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, msg_sv);
                }

                rcmd.body.orderResponse.errorId = crypto::get_okx_errorid(code);
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            }

            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} OKX add_new_order cb exc: {}", acc.accountName, e.what());
        }
    });
}


// ============================================================================
// cancel_order —— POST /api/v5/trade/cancel-order (body = {instId, ordId 或 clOrdId})
// ============================================================================
void OkxTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.cancelOrder.exchangeTypeEnum, tcmd.body.cancelOrder.instTypeEnum, tcmd.body.cancelOrder.instId, info)) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = SMCInstrumentNotExistError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    std::string body;
    if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
        body = fmt::format(R"({{"instId":"{}","ordId":"{}"}})", info.originInstId, tcmd.body.cancelOrder.orderId);
    } 
    else if (!crypto::str_cmp(tcmd.body.cancelOrder.orderSysId, "")) {
        body = fmt::format(R"({{"instId":"{}","clOrdId":"{}"}})", info.originInstId, tcmd.body.cancelOrder.orderSysId);
    } 
    else {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = OrderIdError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    std::string ts = crypto::getTimestampIso();
    std::string sign = crypto::getOkxSignatureRest(acc.secretKey, ts, "POST", cancelOrderUrl, "");
    std::vector<std::pair<std::string, std::string>> headers = {{"OK-ACCESS-KEY", acc.apiKey}, {"OK-ACCESS-TIMESTAMP", ts}, {"OK-ACCESS-SIGN", sign}, {"OK-ACCESS-PASSPHRASE", acc.password}};

    LOG_INFO("TB {} OKX cancel_order body={}", acc.accountName, body);

    asyncRequest(boost::beast::http::verb::post, cancelOrderUrl, std::move(body), "application/json", std::move(headers), [this, rcmd](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
        if (ec) {
            rcmd.body.orderResponse.orderStatus = OS_FAILED;
            rcmd.body.orderResponse.errorId = NetworkError;
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, ec.message());
            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
            return;
        }
        try {
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                rcmd.body.orderResponse.orderStatus = OS_FAILED;
                rcmd.body.orderResponse.errorId = UnknownError;
                rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
                return;
            }

            auto doc_value = doc.get_object().value_unsafe();

            std::string_view code_sv;
            std::string_view msg_sv;
            simdjson::ondemand::array data_arr;

            std::string_view sCode_sv;
            std::string_view sMsg_sv;

            for (auto field : doc_value) {
                std::string_view k = field.unescaped_key().value_unsafe();
                if (k == "code") {
                    field.value().get(code_sv);
                }
                else if (k == "msg") {
                    field.value().get(msg_sv);
                }
                else if (k == "data") {
                    field.value().get(data_arr);

                    for (auto b_val : data_arr) {
                        auto b_res = b_val.get_object();
                        if (b_res.error()) {
                            continue;
                        }
                        auto& b = b_res.value_unsafe();

                        for (auto field : b) {
                            std::string_view k = field.unescaped_key().value_unsafe();
                            if (k == "sCode") {
                                field.value().get(sCode_sv);
                            }
                            else if (k == "sMsg") {
                                field.value().get(sMsg_sv);
                            }
                        }
                    }
                }
            }

            if (code_sv == "0") {
                rcmd.body.orderResponse.orderStatus = OS_CANCELED;
            }
            else {
                int code = 0;
                if (!sCode_sv.empty()) {
                    code = crypto::fast_atol(sCode_sv);
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, sMsg_sv);
                }
                else {
                    code = crypto::fast_atol(code_sv);
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, msg_sv);
                }

                rcmd.body.orderResponse.errorId = crypto::get_okx_errorid(code);

                if (rcmd.body.orderResponse.errorId == OrderNotFoundError) {
                    rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                }
                else if (rcmd.body.orderResponse.errorId == OrderAlreadyFinishedError) {
                    rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                }
                else {
                    rcmd.body.orderResponse.orderStatus = OS_FAILED;
                }
            }

            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} OKX cancel_order cb exc: {}", acc.accountName, e.what());
        }
    });
}

// ============================================================================
// query_order —— GET /api/v5/trade/order?instId=X&ordId=Y
// ============================================================================
void OkxTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum, tcmd.body.queryOrder.instTypeEnum, tcmd.body.queryOrder.instId, info)) {
        LOG_INFO("TB {} OKX query_order smc miss: {}", acc.accountName, tcmd.body.queryOrder.instId);
        return;
    }

    // 构造带 query 的 path
    std::string query = "?instId=" + std::string(info.originInstId);
    if (!crypto::str_cmp(tcmd.body.queryOrder.orderId, "")) {
        query += "&ordId=" + std::string(tcmd.body.queryOrder.orderId);
    } 
    else if (!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")) {
        query += "&clOrdId=" + std::string(tcmd.body.queryOrder.orderSysId);
    } 
    else {
        return;
    }
    std::string fullPath = queryOrderUrl + query;

    std::string ts = crypto::getTimestampIso();
    std::string sign = crypto::getOkxSignatureRest(acc.secretKey, ts, "GET", fullPath, "");
    std::vector<std::pair<std::string, std::string>> headers = {{"OK-ACCESS-KEY", acc.apiKey}, {"OK-ACCESS-TIMESTAMP", ts}, {"OK-ACCESS-SIGN", sign}, {"OK-ACCESS-PASSPHRASE", acc.password}};

    LOG_INFO("TB {} OKX query_order: {}", acc.accountName, fullPath);

    asyncRequest(boost::beast::http::verb::get, std::move(fullPath), "", "", std::move(headers), [this, rcmd, info](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
        if (ec) {
            LOG_ERROR("TB {} query_order ec: {}", acc.accountName, ec.message());
            return;
        }

        try {
            simdjson::padded_string padded(resp.body);
            auto doc = g_parser.iterate(padded);
            if (doc.error()) {
                return;
            }

            auto doc_value = doc.get_object().value_unsafe();

            std::string_view code_sv;
            simdjson::ondemand::array data_arr;

            std::string_view ordId_sv;
            std::string_view clOrdId_sv;
            std::string_view sz_sv;
            std::string_view px_sv;
            std::string_view accFill_sv;
            std::string_view avgPx_sv;
            std::string_view state_sv;
            std::string_view sCode_sv;

            for (auto field : doc_value) {
                std::string_view k = field.unescaped_key().value_unsafe();
                if (k == "code") {
                    field.value().get(code_sv);
                }
                else if (k == "data") {
                    field.value().get(data_arr);

                    for (auto b_val : data_arr) {
                        auto b_res = b_val.get_object();
                        if (b_res.error()) {
                            continue;
                        }
                        auto& b = b_res.value_unsafe();

                        for (auto field : b) {
                            std::string_view k = field.unescaped_key().value_unsafe();
                            if (k == "sCode") {
                                field.value().get(sCode_sv);
                            }
                            else if (k == "ordId") {
                                field.value().get(ordId_sv);
                            }
                            else if (k == "clOrdId") {
                                field.value().get(clOrdId_sv);
                            }
                            else if (k == "sz") {
                                field.value().get(sz_sv);
                            }
                            else if (k == "px") {
                                field.value().get(px_sv);
                            }
                            else if (k == "accFillSz") {
                                field.value().get(accFill_sv);
                            }
                            else if (k == "avgPx") {
                                field.value().get(avgPx_sv);
                            }
                            else if (k == "state") {
                                field.value().get(state_sv);
                            }
                        }
                    }
                }
            }

            if (code_sv == "0") {
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, ordId_sv);
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, clOrdId_sv);
                if (!sz_sv.empty()) {
                    rcmd.body.orderResponse.volumeTotal = crypto::fast_atod(sz_sv);
                }

                if (!px_sv.empty()) {
                    rcmd.body.orderResponse.limitPrice = crypto::fast_atod(px_sv);
                }

                if (!accFill_sv.empty()) {
                    rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(accFill_sv);
                }

                if (!avgPx_sv.empty()) {
                    rcmd.body.orderResponse.tradePrice  = crypto::fast_atod(avgPx_sv);
                }

                if (state_sv == "live") {
                    rcmd.body.orderResponse.orderStatus = OS_NEW;
                }            
                else if (state_sv == "partially_filled") {
                    rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
                }
                else if (state_sv == "filled") {
                    rcmd.body.orderResponse.orderStatus = OS_FILLED;
                }
                else if (state_sv == "canceled" || state_sv == "mmp_canceled") {
                    rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                }                                       
                else {
                    rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                }                                  
            }
            else {
                int code = 0;
                if (!sCode_sv.empty()) {
                    code = crypto::fast_atol(sCode_sv);
                }
                else {
                    code = crypto::fast_atol(code_sv);
                }

                rcmd.body.orderResponse.errorId = crypto::get_okx_errorid(code);
                crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, std::string_view(resp.body));
            }

            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
        }
        catch (const std::exception& e) {
            LOG_ERROR("TB {} OKX query_order cb exc: {}", acc.accountName, e.what());
        }
    });
}