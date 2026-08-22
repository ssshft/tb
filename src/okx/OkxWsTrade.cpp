#include "okx/OkxWsTrade.h"
#include <fmt/format.h>
#include <simdjson.h>



OkxWsTradeUnit::OkxWsTradeUnit(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {

}

OkxWsTradeUnit::~OkxWsTradeUnit() = default;

// ============================================================================
// WS JSON builders
// ============================================================================
std::string OkxWsTradeUnit::buildLoginJson() {
    std::string ts = std::to_string(crypto::getCurrentTimeSeconds());
    std::string sign = crypto::getOkxSignatureWsLogin(acc.secretKey, ts, "GET/users/self/verify");

    return fmt::format(
        R"({{"op":"login","args":[{{"apiKey":"{}","passphrase":"{}","timestamp":"{}","sign":"{}"}}]}})",
        acc.apiKey, acc.password, ts, sign);
}

std::string OkxWsTradeUnit::buildSubscribeJson() const {
    // login 后订阅 3 个 channel: account / positions / orders (instType=ANY)
    return
        R"({"op":"subscribe","args":[)"
        R"({"channel":"account","extraParams":"{\"updateInterval\":0}"},)"
        R"({"channel":"positions","instType":"ANY","extraParams":"{\"updateInterval\":0}"},)"
        R"({"channel":"orders","instType":"ANY"}])"
        R"(})";
}

std::string OkxWsTradeUnit::buildOrderPlaceJson(int reqId,
                        const pubsub::TCommand& tcmd,
                        const md::InstrumentInfo& info,
                        const std::string& price,
                        const std::string& amount,
                        const char* side, const char* ordType) const {
    std::string j;
    j.reserve(400);
    j.append(R"({"id":")"); 
    j.append(std::to_string(reqId));
    j.push_back('"');
    j.append(R"(,"op":"order","args":[{)");
    j.append(R"("instId":")"); 
    j.append(info.originInstId);                    
    j.push_back('"');
    j.append(R"(,"tdMode":"cross","side":")"); 
    j.append(side);                 
    j.push_back('"');
    j.append(R"(,"ordType":")"); 
    j.append(ordType);                            
    j.push_back('"');
    j.append(R"(,"sz":")"); 
    j.append(amount);                                  
    j.push_back('"');
    if (ordType[0] != 'm') {   // market 无 px
        j.append(R"(,"px":")"); 
        j.append(price);                               
        j.push_back('"');
    }
    j.append(R"(,"clOrdId":")"); 
    j.append(escape_json(tcmd.body.newOrder.orderSysId));  
    j.push_back('"');
    j.append(R"(,"reduceOnly":)"); 
    j.append(tcmd.body.newOrder.reduceOnly ? "true" : "false");
    j.append("}]}");
    return j;
}

std::string OkxWsTradeUnit::buildOrderCancelJson(int reqId, const pubsub::TCommand& tcmd, const md::InstrumentInfo& info) const {
    std::string j;
    j.reserve(200);
    j.append(R"({"id":")"); 
    j.append(std::to_string(reqId));                    
    j.push_back('"');
    j.append(R"(,"op":"cancel-order","args":[{)");
    j.append(R"("instId":")"); 
    j.append(info.originInstId);                    
    j.push_back('"');
    if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
        j.append(R"(,"ordId":")"); 
        j.append(tcmd.body.cancelOrder.orderId);    
        j.push_back('"');
    } else {
        j.append(R"(,"clOrdId":")"); 
        j.append(escape_json(tcmd.body.cancelOrder.orderSysId));  
        j.push_back('"');
    }
    j.append("}]}");
    return j;
}

// ============================================================================
// pending map
// ============================================================================
void OkxWsTradeUnit::recordPending(int id, pubsub::CommandType type, const pubsub::RCommand& rcmd) {
    const int64_t now_ms = crypto::getCurrentTimeMilli();
    const int64_t last_gc = pendingLastGcMs_.load(std::memory_order_relaxed);
    const bool need_gc = (now_ms - last_gc > kGcIntervalMs);
    if (need_gc) {
        pendingLastGcMs_.store(now_ms, std::memory_order_relaxed);

        clearPending();
    }

    tbb::concurrent_hash_map<int, WsPending>::accessor acc;
    pendingMap_.insert(acc, id);
    acc->second.type = type;
    acc->second.rcmd = rcmd;
    acc->second.ts_ms = now_ms;
}

bool OkxWsTradeUnit::takePending(int id, WsPending& out) {
    tbb::concurrent_hash_map<int, WsPending>::accessor acc;
    if (!pendingMap_.find(acc, id)) {
        return false;
    }

    out = std::move(acc->second);
    pendingMap_.erase(acc);
    return true;
}

void OkxWsTradeUnit::clearPending() {
    const int64_t now_ms = crypto::getCurrentTimeMilli();
    if (pendingMap_.size() > kPendingHardMax) {
        pendingMap_.clear();
        return;
    }

    for (auto it = pendingMap_.begin(); it != pendingMap_.end(); ++it) {
        if (now_ms - it->second.ts_ms > kPendingTtlMs) {
            pendingMap_.erase(it->first);
        }
    }
}

// ============================================================================
// subWebsocekt / onOpen / onCloseMsg
// ============================================================================
void OkxWsTradeUnit::subWebsocekt() {
    std::string restHost = host_of(acc.restUrl);
    std::vector<std::pair<std::string, std::string>> defaultHeaders;
    if (acc.isSimulated) {
        defaultHeaders.emplace_back("x-simulated-trading", "1");
    }

    initRestClient(restHost, std::move(defaultHeaders), 4);

    net::WsConfig cfg;
    cfg.url = acc.wsUrl;
    cfg.ping_mode = net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text = "ping";   // OKX 用原始文本 "ping" 不是 JSON
    cfg.auto_reconnect = true;
    cfg.idle_timeout_sec = 60;
    LOG_INFO("TB {} OKX ws {} rest {}", acc.accountId, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));
}

void OkxWsTradeUnit::onOpen() {
    BaseTradeUnit::onOpen();
    wsLoggedIn_.store(false);
    LOG_INFO("TB {} OKX ws send op:login", acc.accountId);
    pWsClient->send_text(buildLoginJson());
}

void OkxWsTradeUnit::onCloseMsg(int code, const std::string& reason) {
    BaseTradeUnit::onCloseMsg(code, reason);
    wsLoggedIn_.store(false);
    clearPending();
}


// ============================================================================
// onWebsocketMsg
// ============================================================================
void OkxWsTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool /*isBinary*/, int64_t /*recv_ns*/) {
   try {
        std::string msg(reinterpret_cast<const char*>(data), len);
        std::cout << "onWebsocketMsg: " << msg << std::endl;

        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc = g_parser.iterate(padded);
        if (doc.error()) {
            return;
        }

        auto doc_value = doc.get_object().value_unsafe();

        std::string_view event_sv;
        OrderResultFields orf;
        ErrorFields ef;

        std::string_view id_sv;

        simdjson::ondemand::object arg_obj;
        std::string_view channel_sv;
        simdjson::ondemand::array data_arr;

        for (auto field : doc_value) {
            std::string_view k = field.unescaped_key().value_unsafe();
            if (k == "event") {
                field.value().get(event_sv);
            }
            else if (k == "code") {
                field.value().get(ef.code_sv);
            }
            else if (k == "msg") {
                field.value().get(ef.msg_sv);
            }
            else if (k == "id") {
                field.value().get(id_sv);
            }      
            else if (k == "arg") {
                field.value().get(arg_obj);
                arg_obj["channel"].get(channel_sv);
            }
            else if (k == "data") {
                field.value().get(data_arr);

                if (!id_sv.empty()) {
                    for (auto b_val : data_arr) {
                        auto b_res = b_val.get_object();
                        if (b_res.error()) {
                            continue;
                        }
                        auto& b = b_res.value_unsafe();

                        for (auto f : b) {
                            std::string_view mk = f.unescaped_key().value_unsafe();
                            if (mk == "ordId") {
                                f.value().get(orf.ordId_sv);
                            }
                            else if (mk == "sCode") {
                                f.value().get(orf.sCode_sv);
                            }
                            else if (mk == "sMsg") {
                                f.value().get(orf.sMsg_sv);
                            }
                        }
                    }
                }
                else {
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

            if (event_sv == "login") {
                if (ef.code_sv == "0") {
                    wsLoggedIn_.store(true);
                    LOG_INFO("TB {} OKX login OK, will subscribe channels", acc.accountId);
                    if (pWsClient) {
                        pWsClient->send_text(buildSubscribeJson());
                    }
                } else {
                    wsLoggedIn_.store(false);
                    LOG_ERROR("TB {} OKX login FAILED code={} msg={}", acc.accountId, ef.code_sv, ef.msg_sv);
                } 
            }

            if (!id_sv.empty()) {
                int id = crypto::fast_atol(id_sv);
                WsPending pending;
                if (ef.code_sv == "0") {
                    if (takePending(id, pending)) {
                        handleWsApiResponse(pending, orf);
                    }
                }
                else {
                    if (takePending(id, pending)) {
                        handleWsApiError(pending, ef);
                    }
                }
            }
        }
    }
    catch (const std::exception& e) {
        LOG_ERROR("TB {} OKX ws exc: {}", acc.accountId, e.what());
    }
}

// ============================================================================
// ws-api 响应分派 (order.place / cancel-order)
// ============================================================================
void OkxWsTradeUnit::handleWsApiResponse(WsPending& pending, const OrderResultFields& fields) {
    pubsub::RCommand& rcmd = pending.rcmd;

    // 测试单不上报
    if (rcmd.body.orderResponse.clientOrderId == TESTCLIENTORDERID) {
        return;
    }
 
    md::InstrumentInfo info;
    if (!smc->get_instrument_info(rcmd.body.orderResponse.exchangeTypeEnum, rcmd.body.orderResponse.instTypeEnum, rcmd.body.orderResponse.instId, info)) {
        LOG_ERROR("TB {} exec report smc miss: {}", acc.accountId, rcmd.body.orderResponse.instId);
        return;
    }

    if (pending.type == pubsub::CMD_NEW_ORDER) {
        if (fields.sCode_sv == "0") {
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, fields.ordId_sv);
            rcmd.body.orderResponse.orderStatus = OS_NEW;
        }
        else {
            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            rcmd.body.orderResponse.errorId = crypto::get_okx_errorid(crypto::fast_atol(fields.sCode_sv));
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, fields.sMsg_sv);  
        }

        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
    } else if (pending.type == pubsub::CMD_CANCEL_ORDER) {
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, fields.ordId_sv);
        rcmd.body.orderResponse.orderStatus = OS_CANCELED;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
    }
}

void OkxWsTradeUnit::handleWsApiError(WsPending& pending, const ErrorFields& fields) {
    pubsub::RCommand& rcmd = pending.rcmd;

    // 测试单不上报
    if (rcmd.body.orderResponse.clientOrderId == TESTCLIENTORDERID) {
        return;
    }

    int code = crypto::fast_atol(fields.code_sv);
    rcmd.body.orderResponse.errorId = crypto::get_okx_errorid(code);

    if (pending.type == pubsub::CMD_NEW_ORDER) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;    
    } else if (pending.type == pubsub::CMD_CANCEL_ORDER) {
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

    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, fields.msg_sv);
    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
    PUSH_RCMD(rcmd)
}

// ---- account update ----
// data = [{totalEq, adjEq, mmr, mgnRatio, details:[{ccy, cashBal, availEq, frozenBal, upl}]}]
void OkxWsTradeUnit::handleAccountUpdate(simdjson::ondemand::array& arr) {
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
                        crypto::copy_sv_to_char_array(rcmd.body.balance.accountId, acc.accountId);
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
        crypto::copy_sv_to_char_array(rcmd.body.totalAccount.accountId, acc.accountId);
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
void OkxWsTradeUnit::handlePositionsUpdate(simdjson::ondemand::array& arr) {
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
        crypto::copy_sv_to_char_array(rcmd.body.position.accountId, acc.accountId);
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
void OkxWsTradeUnit::handleOrdersUpdate(simdjson::ondemand::array& arr) {
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
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountId, acc.accountId);
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
void OkxWsTradeUnit::query_account(const pubsub::TCommand& tcmd) {
    query_balance(tcmd);   // account 主要靠 balance 返回的 totalEq / adjEq
}

void OkxWsTradeUnit::query_balance(const pubsub::TCommand& tcmd) {
    std::string ts = crypto::getTimestampIso();
    std::string sign = crypto::getOkxSignatureRest(acc.secretKey, ts, "GET", balanceUrl, "");
    std::vector<std::pair<std::string, std::string>> headers = {{"OK-ACCESS-KEY", acc.apiKey}, {"OK-ACCESS-TIMESTAMP", ts}, {"OK-ACCESS-SIGN", sign}, {"OK-ACCESS-PASSPHRASE", acc.password}};

    asyncRequest(boost::beast::http::verb::get, balanceUrl, "", "", std::move(headers), [this](boost::system::error_code ec, net::HttpResponse resp) {
        if (ec) { 
            LOG_ERROR("TB {} OKX query_balance ec: {}", acc.accountId, ec.message()); 
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
                                    crypto::copy_sv_to_char_array(rcmd.body.balance.accountId, acc.accountId);
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
                    crypto::copy_sv_to_char_array(rcmd.body.totalAccount.accountId, acc.accountId);
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
            LOG_ERROR("TB {} OKX query_balance cb exc: {}", acc.accountId, e.what());
        }
    });
}

void OkxWsTradeUnit::query_position(const pubsub::TCommand&) {
    std::string ts = crypto::getTimestampIso();
    std::string sign = crypto::getOkxSignatureRest(acc.secretKey, ts, "GET", positionUrl, "");
    std::vector<std::pair<std::string, std::string>> headers = {{"OK-ACCESS-KEY", acc.apiKey}, {"OK-ACCESS-TIMESTAMP", ts}, {"OK-ACCESS-SIGN", sign}, {"OK-ACCESS-PASSPHRASE", acc.password}};

    asyncRequest(boost::beast::http::verb::get, positionUrl, "", "", std::move(headers), [this](boost::system::error_code ec, net::HttpResponse resp) {
        if (ec) { 
            LOG_ERROR("TB {} OKX query_position ec: {}", acc.accountId, ec.message()); 
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
                    crypto::copy_sv_to_char_array(rcmd.body.position.accountId, acc.accountId);
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
            LOG_ERROR("TB {} OKX query_position cb exc: {}", acc.accountId, e.what());
        }
    });
}

// ============================================================================
// query_order —— GET /api/v5/trade/order?instId=X&ordId=Y
// ============================================================================
void OkxWsTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum, tcmd.body.queryOrder.instTypeEnum, tcmd.body.queryOrder.instId, info)) {
        LOG_INFO("TB {} OKX query_order smc miss: {}", acc.accountId, tcmd.body.queryOrder.instId);
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

    LOG_INFO("TB {} OKX query_order: {}", acc.accountId, fullPath);

    asyncRequest(boost::beast::http::verb::get, std::move(fullPath), "", "", std::move(headers), [this, rcmd, info](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
        if (ec) {
            LOG_ERROR("TB {} query_order ec: {}", acc.accountId, ec.message());
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
            LOG_ERROR("TB {} OKX query_order cb exc: {}", acc.accountId, e.what());
        }
    });
}

// ============================================================================
// add_new_order (WS op:order)
// ============================================================================
void OkxWsTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load() || !wsLoggedIn_.load()) {
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
    } else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
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
            rcmd.body.orderResponse.errorId     = OrderTypeError;
            rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
            return;
    }

    double price = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice * info.magnifyNumber, info.tickSize);
    double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber, info.lotSize);
    std::string price_str = fmt::format("{}", price);
    std::string sz_str = fmt::format("{}", volume);

    const int reqId = nextWsId_.fetch_add(1, std::memory_order_relaxed);
    std::string msg = buildOrderPlaceJson(reqId, tcmd, info, price_str, sz_str, side, ordType);

    recordPending(reqId, pubsub::CMD_NEW_ORDER, rcmd);
    LOG_INFO("TB {} OKX ws op:order id={} msg={}", acc.accountId, reqId, msg);
    pWsClient->send_text(std::move(msg));
}


// ============================================================================
// cancel_order (WS op:cancel-order)
// ============================================================================
void OkxWsTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load() || !wsLoggedIn_.load()) {
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

    if (crypto::str_cmp(tcmd.body.cancelOrder.orderId, "") && crypto::str_cmp(tcmd.body.cancelOrder.orderSysId, "")) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId = OrderIdError;
        rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const int reqId = nextWsId_.fetch_add(1, std::memory_order_relaxed);
    std::string msg = buildOrderCancelJson(reqId, tcmd, info);

    recordPending(reqId, pubsub::CMD_CANCEL_ORDER, rcmd);
    LOG_INFO("TB {} OKX ws op:cancel-order id={} msg={}", acc.accountId, reqId, msg);
    pWsClient->send_text(std::move(msg));
}