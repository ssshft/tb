#include "okx/OkxWsTrade.h"

#include <chrono>
#include <cmath>
#include <cstdlib>
#include <ctime>

#include <openssl/hmac.h>
#include <openssl/evp.h>

#include "base64.hpp"

#include <fmt/format.h>
#include <simdjson.h>


namespace {
    constexpr int64_t kPendingTtlMs   = 30 * 1000;
    constexpr size_t  kPendingHardMax = 10000;
    constexpr int64_t kGcIntervalMs   = 5000;

    inline std::string hmac_sha256_base64(const std::string& secret, const std::string& data) {
        unsigned char digest[EVP_MAX_MD_SIZE];
        unsigned int  digest_len = 0;
        ::HMAC(EVP_sha256(),
               secret.data(), static_cast<int>(secret.size()),
               reinterpret_cast<const unsigned char*>(data.data()),
               data.size(),
               digest, &digest_len);
        return websocketpp::base64_encode(digest, digest_len);
    }

    inline int parse_int_id(std::string_view sv) {
        if (sv.empty()) return 0;
        return std::atoi(std::string(sv).c_str());
    }
}


OkxWsTradeUnit::OkxWsTradeUnit(AccountCfg& a, sm::SecurityManager* s)
    : BaseTradeUnit(a, s) {}
OkxWsTradeUnit::~OkxWsTradeUnit() = default;


// ============================================================================
// 签名 helpers
// ============================================================================
std::string OkxWsTradeUnit::signBase64(const std::string& payload) const {
    return hmac_sha256_base64(acc.secretKey, payload);
}

std::string OkxWsTradeUnit::okxIsoTimestamp() {
    // ISO8601 with ms: "2024-01-01T00:00:00.000Z"
    using namespace std::chrono;
    auto now = system_clock::now();
    auto ms  = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
    std::time_t t = system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&t, &tm);
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%03lldZ",
                  tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                  tm.tm_hour, tm.tm_min, tm.tm_sec, static_cast<long long>(ms.count()));
    return buf;
}

std::string OkxWsTradeUnit::restSign(const std::string& timestamp, const std::string& method,
                                       const std::string& requestPath, const std::string& body) const {
    return signBase64(timestamp + method + requestPath + body);
}

std::vector<std::pair<std::string, std::string>>
OkxWsTradeUnit::restAuthHeaders(const std::string& method, const std::string& requestPath,
                                  const std::string& body) const {
    std::string ts   = okxIsoTimestamp();
    std::string sign = restSign(ts, method, requestPath, body);
    return {
        {"OK-ACCESS-KEY",        acc.apiKey},
        {"OK-ACCESS-TIMESTAMP",  ts},
        {"OK-ACCESS-SIGN",       sign},
        {"OK-ACCESS-PASSPHRASE", acc.password},
    };
}


// ============================================================================
// InstType 查找
// ============================================================================
bool OkxWsTradeUnit::lookupInstrument(const std::string& originInstId,
                                        std::string_view okxInstType,
                                        md::InstrumentInfo& info,
                                        InstType& out) const {
    if (okxInstType == "SPOT") {
        if (smc->get_instrument_info(OKX, SPOT, originInstId.c_str(), info)) { out = SPOT; return true; }
    } else if (okxInstType == "MARGIN") {
        if (smc->get_instrument_info(OKX, MARGIN, originInstId.c_str(), info)) { out = MARGIN; return true; }
    } else if (okxInstType == "SWAP" || okxInstType == "FUTURES") {
        InstType u_swap = (okxInstType == "SWAP") ? USDT_SWAP : USDT_FUTURES;
        InstType c_swap = (okxInstType == "SWAP") ? C_SWAP    : C_FUTURES;
        if (smc->get_instrument_info(OKX, u_swap, originInstId.c_str(), info)) { out = u_swap; return true; }
        if (smc->get_instrument_info(OKX, c_swap, originInstId.c_str(), info)) { out = c_swap; return true; }
    } else {
        for (InstType t : {SPOT, USDT_SWAP, USDT_FUTURES, C_SWAP, C_FUTURES, MARGIN}) {
            if (smc->get_instrument_info(OKX, t, originInstId.c_str(), info)) { out = t; return true; }
        }
    }
    return false;
}


// ============================================================================
// WS JSON builders
// ============================================================================
std::string OkxWsTradeUnit::buildLoginJson() {
    long ts_sec = crypto::getCurrentTimeSeconds();
    std::string ts = std::to_string(ts_sec);
    std::string sign = signBase64(ts + "GET/users/self/verify");
    return fmt::format(
        R"({{"op":"login","args":[{{"apiKey":"{}","passphrase":"{}","timestamp":"{}","sign":"{}"}}]}})",
        escape_json(acc.apiKey), escape_json(acc.password), ts, sign);
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
                                                  const char* side, const char* ordType) const
{
    std::string j;
    j.reserve(400);
    j.append(R"({"id":")"); j.append(std::to_string(reqId));                    j.push_back('"');
    j.append(R"(,"op":"order","args":[{)");
    j.append(R"("instId":")"); j.append(info.originInstId);                    j.push_back('"');
    j.append(R"(,"tdMode":"cross","side":")"); j.append(side);                 j.push_back('"');
    j.append(R"(,"ordType":")"); j.append(ordType);                            j.push_back('"');
    j.append(R"(,"sz":")"); j.append(amount);                                  j.push_back('"');
    if (ordType[0] != 'm') {   // market 无 px
        j.append(R"(,"px":")"); j.append(price);                               j.push_back('"');
    }
    j.append(R"(,"clOrdId":")"); j.append(escape_json(tcmd.body.newOrder.orderSysId));  j.push_back('"');
    j.append(R"(,"reduceOnly":)"); j.append(tcmd.body.newOrder.reduceOnly ? "true" : "false");
    j.append("}]}");
    return j;
}

std::string OkxWsTradeUnit::buildOrderCancelJson(int reqId,
                                                   const pubsub::TCommand& tcmd,
                                                   const md::InstrumentInfo& info) const
{
    std::string j;
    j.reserve(200);
    j.append(R"({"id":")"); j.append(std::to_string(reqId));                    j.push_back('"');
    j.append(R"(,"op":"cancel-order","args":[{)");
    j.append(R"("instId":")"); j.append(info.originInstId);                    j.push_back('"');
    if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
        j.append(R"(,"ordId":")"); j.append(tcmd.body.cancelOrder.orderId);    j.push_back('"');
    } else {
        j.append(R"(,"clOrdId":")"); j.append(escape_json(tcmd.body.cancelOrder.orderSysId));  j.push_back('"');
    }
    j.append("}]}");
    return j;
}


// ============================================================================
// pending map
// ============================================================================
void OkxWsTradeUnit::recordPending(int id, WsReqType type,
                                     const pubsub::RCommand& rcmd,
                                     const md::InstrumentInfo& info) {
    const int64_t now_ms  = crypto::getCurrentTimeMilli();
    const int64_t last_gc = pendingLastGcMs_.load(std::memory_order_relaxed);
    const bool need_gc    = (now_ms - last_gc > kGcIntervalMs);
    if (need_gc) pendingLastGcMs_.store(now_ms, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lk(pendingMtx_);
    if (need_gc) gcPendingLocked(now_ms);
    pendingMap_[id] = WsPending{rcmd, type, now_ms, info};
}

bool OkxWsTradeUnit::takePending(int id, WsPending& out) {
    std::lock_guard<std::mutex> lk(pendingMtx_);
    auto it = pendingMap_.find(id);
    if (it == pendingMap_.end()) return false;
    out = std::move(it->second);
    pendingMap_.erase(it);
    return true;
}

void OkxWsTradeUnit::clearPending() {
    std::lock_guard<std::mutex> lk(pendingMtx_);
    for (auto& kv : pendingMap_) {
        pubsub::RCommand& rc = kv.second.rcmd;
        if (kv.second.type == WsReqType::NEW_ORDER) {
            rc.body.orderResponse.orderStatus = OS_REJECTED;
            rc.body.orderResponse.errorId     = TBDisconnectError;
        } else {
            rc.body.orderResponse.orderStatus = OS_FAILED;
            rc.body.orderResponse.errorId     = TBDisconnectError;
        }
        rc.body.orderResponse.updateTime = crypto::getCurrentTime();
        PUSH_RCMD(rc)
    }
    pendingMap_.clear();
}

void OkxWsTradeUnit::gcPendingLocked(int64_t now_ms) {
    if (pendingMap_.size() > kPendingHardMax) {
        LOG_ERROR("TB {} OKX pending over hard cap ({}), clearing.", acc.accountId, pendingMap_.size());
        pendingMap_.clear();
        return;
    }
    for (auto it = pendingMap_.begin(); it != pendingMap_.end();) {
        if (now_ms - it->second.ts_ms > kPendingTtlMs) {
            pubsub::RCommand& rc = it->second.rcmd;
            if (it->second.type == WsReqType::NEW_ORDER) {
                rc.body.orderResponse.orderStatus = OS_REJECTED;
                rc.body.orderResponse.errorId     = NetworkError;
            } else {
                rc.body.orderResponse.orderStatus = OS_FAILED;
                rc.body.orderResponse.errorId     = NetworkError;
            }
            rc.body.orderResponse.updateTime = crypto::getCurrentTime();
            PUSH_RCMD(rc)
            it = pendingMap_.erase(it);
        } else {
            ++it;
        }
    }
}


// ============================================================================
// subWebsocekt / onOpen / onCloseMsg
// ============================================================================
void OkxWsTradeUnit::subWebsocekt() {
    std::string restHost = host_of(acc.restUrl);
    std::vector<std::pair<std::string, std::string>> defaultHeaders;
    if (acc.isSimulated) defaultHeaders.emplace_back("x-simulated-trading", "1");
    initRestClient(restHost, std::move(defaultHeaders), /*conns=*/4);

    ::net::WsConfig cfg;
    cfg.url                      = acc.wsUrl + wsPath_;
    cfg.ping_mode                = ::net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text         = "ping";   // OKX 用原始文本 "ping" 不是 JSON
    cfg.auto_reconnect           = true;
    cfg.idle_timeout_sec         = 60;
    LOG_INFO("TB {} OKX ws {} rest {}", acc.accountId, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));
}

void OkxWsTradeUnit::onOpen() {
    BaseTradeUnit::onOpen();
    wsLoggedIn_.store(false);
    if (!pWsClient) return;
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
void OkxWsTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len,
                                     bool /*isBinary*/, int64_t /*recv_ns*/) {
    if (len == 0) return;
    // "pong" 是纯文本, 不是 JSON, 提前 skip
    if (len == 4 && std::string_view(reinterpret_cast<const char*>(data), 4) == "pong") return;

    try {
        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc_res = g_parser.iterate(padded);
        if (doc_res.error()) return;

        auto& doc = doc_res.value_unsafe();

        // 三种消息 (基于 top-level 字段区分):
        //   1) event  → login / subscribe / error ack
        //   2) id + op → order.place / cancel-order 响应
        //   3) arg + data → account / positions / orders 推送
        std::string_view event_sv;
        if (doc.find_field_unordered("event").get(event_sv) == simdjson::SUCCESS) {
            if (event_sv == "login") {
                onLoginResponse(doc);
            } else if (event_sv == "subscribe") {
                LOG_INFO("TB {} OKX ws subscribe ack", acc.accountId);
            } else if (event_sv == "error") {
                std::string_view code_sv, msg_sv;
                doc["code"].get(code_sv);
                doc["msg"].get(msg_sv);
                LOG_ERROR("TB {} OKX ws error code={} msg={}", acc.accountId, code_sv, msg_sv);
            }
            return;
        }

        std::string_view id_sv;
        if (doc.find_field_unordered("id").get(id_sv) == simdjson::SUCCESS) {
            handleWsApiResponse(doc);
            return;
        }

        // 剩下就是订阅推送
        handleSubUpdate(doc);
    } catch (const std::exception& e) {
        LOG_ERROR("TB {} OKX ws exc: {}", acc.accountId, e.what());
    }
}


// ============================================================================
// login 响应
// ============================================================================
void OkxWsTradeUnit::onLoginResponse(simdjson::ondemand::document& doc) {
    // {"event":"login","code":"0","msg":"","connId":"..."}
    std::string_view code_sv, msg_sv;
    doc["code"].get(code_sv);
    doc["msg"].get(msg_sv);

    if (code_sv == "0") {
        wsLoggedIn_.store(true);
        LOG_INFO("TB {} OKX login OK, will subscribe channels", acc.accountId);
        if (pWsClient) pWsClient->send_text(buildSubscribeJson());
    } else {
        wsLoggedIn_.store(false);
        LOG_ERROR("TB {} OKX login FAILED code={} msg={}", acc.accountId, code_sv, msg_sv);
    }
}


// ============================================================================
// ws-api 响应分派 (order.place / cancel-order)
// ============================================================================
void OkxWsTradeUnit::handleWsApiResponse(simdjson::ondemand::document& doc) {
    std::string_view id_sv;
    doc.find_field_unordered("id").get(id_sv);
    const int reqId = parse_int_id(id_sv);

    std::string_view op_sv;
    doc.find_field_unordered("op").get(op_sv);

    WsPending pending;
    if (!takePending(reqId, pending)) {
        LOG_WARN("TB {} OKX ws-api unknown id={} op={}", acc.accountId, reqId, op_sv);
        return;
    }
    if (pending.type == WsReqType::NEW_ORDER) {
        onOrderPlaceResponse(pending, doc);
    } else {
        onOrderCancelResponse(pending, doc);
    }
}

// ---- order.place 响应 ----
// {"id":"reqId","op":"order","code":"0","msg":"",
//  "data":[{"tag":"","ordId":"...","clOrdId":"...","sCode":"0","sMsg":""}]}
void OkxWsTradeUnit::onOrderPlaceResponse(WsPending& pending,
                                            simdjson::ondemand::document& doc) {
    pubsub::RCommand& rcmd = pending.rcmd;
    if (rcmd.body.orderResponse.clientOrderId == TESTCLIENTORDERID) return;

    std::string_view topCode_sv, topMsg_sv;
    doc.find_field_unordered("code").get(topCode_sv);
    doc.find_field_unordered("msg").get(topMsg_sv);

    simdjson::ondemand::array data_arr;
    if (doc.find_field_unordered("data").get_array().get(data_arr) != simdjson::SUCCESS) {
        rcmd.body.orderResponse.orderStatus = OS_REJECTED;
        rcmd.body.orderResponse.errorId     = UnknownError;
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, topMsg_sv);
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }
    // 单个订单响应, 只取 data[0]
    for (auto it : data_arr) {
        auto o = it.get_object();
        if (o.error()) continue;
        std::string_view sCode_sv, ordId_sv, sMsg_sv;
        o.find_field_unordered("sCode").get(sCode_sv);
        o.find_field_unordered("ordId").get(ordId_sv);
        o.find_field_unordered("sMsg").get(sMsg_sv);

        if (sCode_sv == "0") {
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, ordId_sv);
            // OKX ack 只有 ordId, 无成交细节 —— 状态默认 NEW, 成交进度靠 orders 订阅推送
            rcmd.body.orderResponse.orderStatus = OS_NEW;
            rcmd.body.orderResponse.errorId     = NoError;
        } else {
            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            rcmd.body.orderResponse.errorId     = UnknownError;   // TODO sCode 映射
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, sMsg_sv);
        }
        break;
    }
    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
    PUSH_RCMD(rcmd)
}

// ---- cancel-order 响应 ----
void OkxWsTradeUnit::onOrderCancelResponse(WsPending& pending,
                                             simdjson::ondemand::document& doc) {
    pubsub::RCommand& rcmd = pending.rcmd;
    std::string_view topMsg_sv;
    doc.find_field_unordered("msg").get(topMsg_sv);

    simdjson::ondemand::array data_arr;
    if (doc.find_field_unordered("data").get_array().get(data_arr) != simdjson::SUCCESS) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = UnknownError;
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, topMsg_sv);
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }
    for (auto it : data_arr) {
        auto o = it.get_object();
        if (o.error()) continue;
        std::string_view sCode_sv, ordId_sv, sMsg_sv;
        o.find_field_unordered("sCode").get(sCode_sv);
        o.find_field_unordered("ordId").get(ordId_sv);
        o.find_field_unordered("sMsg").get(sMsg_sv);

        if (sCode_sv == "0") {
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, ordId_sv);
            rcmd.body.orderResponse.orderStatus = OS_CANCELED;
        } else {
            rcmd.body.orderResponse.orderStatus = OS_FAILED;
            rcmd.body.orderResponse.errorId     = UnknownError;
            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, sMsg_sv);
        }
        break;
    }
    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
    PUSH_RCMD(rcmd)
}


// ============================================================================
// 订阅推送分派 (account / positions / orders)
// ============================================================================
void OkxWsTradeUnit::handleSubUpdate(simdjson::ondemand::document& doc) {
    simdjson::ondemand::object arg;
    if (doc.find_field_unordered("arg").get(arg) != simdjson::SUCCESS) return;
    std::string_view channel_sv;
    if (arg.find_field_unordered("channel").get(channel_sv) != simdjson::SUCCESS) return;

    simdjson::ondemand::value data_val;
    if (doc.find_field_unordered("data").get(data_val) != simdjson::SUCCESS) return;

    if      (channel_sv == "account")   handleAccountUpdate(data_val);
    else if (channel_sv == "positions") handlePositionsUpdate(data_val);
    else if (channel_sv == "orders")    handleOrdersUpdate(data_val);
}

// ---- account 推送 ----
void OkxWsTradeUnit::handleAccountUpdate(simdjson::ondemand::value& dataArr) {
    simdjson::ondemand::array outer;
    if (dataArr.get_array().get(outer) != simdjson::SUCCESS) return;

    for (auto entry : outer) {
        auto d = entry.get_object();
        if (d.error()) continue;

        // details 里是资产细项
        simdjson::ondemand::array details;
        if (d["details"].get(details) == simdjson::SUCCESS) {
            std::vector<pubsub::RCommand> pending;
            for (auto it : details) {
                auto o = it.get_object();
                if (o.error()) continue;
                std::string_view ccy_sv, cash_sv, avail_sv, frozen_sv, upl_sv, eq_sv;
                o["ccy"].get(ccy_sv);
                o["cashBal"].get(cash_sv);
                o["availEq"].get(avail_sv);
                o["frozenBal"].get(frozen_sv);
                o["upl"].get(upl_sv);
                o["eq"].get(eq_sv);

                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
                rcmd.body.balance.exchangeTypeEnum = OKX;
                rcmd.body.balance.instTypeEnum     = SPOT;
                crypto::copy_sv_to_char_array(rcmd.body.balance.accountId,  acc.accountId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.strategyId, acc.strategyId);
                crypto::copy_sv_to_char_array(rcmd.body.balance.currency,   crypto::to_upper(std::string(ccy_sv)));
                rcmd.body.balance.available     = crypto::fast_atod(avail_sv.empty() ? cash_sv : avail_sv);
                rcmd.body.balance.frozen        = crypto::fast_atod(frozen_sv);
                rcmd.body.balance.total         = crypto::fast_atod(eq_sv.empty() ? cash_sv : eq_sv);
                rcmd.body.balance.updateTime    = crypto::getCurrentTime();
                rcmd.body.balance.apiSourceEnum = AS_WEBSOCKET;
                pending.emplace_back(rcmd);
            }
            for (size_t i = 0; i < pending.size(); ++i) {
                pending[i].body.balance.isLast = (i + 1 == pending.size());
                PUSH_RCMD(pending[i])
            }
        }

        // totalAccount
        std::string_view teq_sv, aeq_sv, mmr_sv, mgnR_sv;
        d["totalEq"].get(teq_sv);
        d["adjEq"].get(aeq_sv);
        d["mmr"].get(mmr_sv);
        d["mgnRatio"].get(mgnR_sv);
        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_TOTAL_ACCOUNT;
        rcmd.body.totalAccount.exchangeTypeEnum = OKX;
        rcmd.body.totalAccount.instTypeEnum     = SPOT;
        crypto::copy_sv_to_char_array(rcmd.body.totalAccount.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.totalAccount.strategyId, acc.strategyId);
        rcmd.body.totalAccount.totalEquity = crypto::fast_atod(teq_sv);
        rcmd.body.totalAccount.adjEquity   = crypto::fast_atod(aeq_sv);
        rcmd.body.totalAccount.mmr         = crypto::fast_atod(mmr_sv);
        rcmd.body.totalAccount.mgnRatio    = mgnR_sv.empty() ? 100.0 : crypto::fast_atod(mgnR_sv);
        rcmd.body.totalAccount.updateTime  = crypto::getCurrentTime();
        rcmd.body.totalAccount.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd)
    }
}

// ---- positions 推送 ----
void OkxWsTradeUnit::handlePositionsUpdate(simdjson::ondemand::value& dataArr) {
    simdjson::ondemand::array arr;
    if (dataArr.get_array().get(arr) != simdjson::SUCCESS) return;
    std::vector<pubsub::RCommand> pending;
    for (auto it : arr) {
        auto o = it.get_object();
        if (o.error()) continue;
        std::string_view iType_sv, iId_sv, pos_sv, avg_sv, mmr_sv, upl_sv, mark_sv, liq_sv, adl_sv;
        o["instType"].get(iType_sv);
        o["instId"].get(iId_sv);
        o["pos"].get(pos_sv);
        o["avgPx"].get(avg_sv);
        o["mmr"].get(mmr_sv);
        o["upl"].get(upl_sv);
        o["markPx"].get(mark_sv);
        o["liqPx"].get(liq_sv);
        o["adl"].get(adl_sv);

        std::string originInstId(iId_sv);
        md::InstrumentInfo info;
        InstType instType;
        if (!lookupInstrument(originInstId, iType_sv, info, instType)) continue;

        double positionAmt = crypto::fast_atod(pos_sv);
        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
        rcmd.body.position.exchangeTypeEnum = OKX;
        rcmd.body.position.instTypeEnum     = instType;
        crypto::copy_sv_to_char_array(rcmd.body.position.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.position.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.position.instId,     std::string_view(info.instId));
        rcmd.body.position.direction     = positionAmt > 0 ? DT_LONG : DT_SHORT;
        rcmd.body.position.volume        = std::fabs(positionAmt);
        rcmd.body.position.maintMargin   = crypto::fast_atod(mmr_sv);
        rcmd.body.position.avgPrice      = crypto::fast_atod(avg_sv);
        rcmd.body.position.unrealizedPnl = crypto::fast_atod(upl_sv);
        rcmd.body.position.markPrice     = crypto::fast_atod(mark_sv);
        if (!liq_sv.empty()) rcmd.body.position.liquidPrice = crypto::fast_atod(liq_sv);
        rcmd.body.position.adlQuantile   = static_cast<int>(crypto::fast_atod(adl_sv));
        rcmd.body.position.updateTime    = crypto::getCurrentTime();
        rcmd.body.position.apiSourceEnum = AS_WEBSOCKET;
        pending.emplace_back(rcmd);
    }
    for (size_t i = 0; i < pending.size(); ++i) {
        pending[i].body.position.isLast = (i + 1 == pending.size());
        PUSH_RCMD(pending[i])
    }
}

// ---- orders 推送 (成交状态变化 —— Bybit/OKX 都靠这个补齐 ack 之后的细节) ----
void OkxWsTradeUnit::handleOrdersUpdate(simdjson::ondemand::value& dataArr) {
    simdjson::ondemand::array arr;
    if (dataArr.get_array().get(arr) != simdjson::SUCCESS) return;

    for (auto it : arr) {
        auto o = it.get_object();
        if (o.error()) continue;
        std::string_view iType_sv, iId_sv, ordId_sv, clOrdId_sv, sz_sv, px_sv, side_sv, oType_sv,
                          state_sv, accFill_sv, avgPx_sv;
        o["instType"].get(iType_sv);
        o["instId"].get(iId_sv);
        o["ordId"].get(ordId_sv);
        o["clOrdId"].get(clOrdId_sv);
        o["sz"].get(sz_sv);
        o["px"].get(px_sv);
        o["side"].get(side_sv);
        o["ordType"].get(oType_sv);
        o["state"].get(state_sv);
        o["accFillSz"].get(accFill_sv);
        o["avgPx"].get(avgPx_sv);

        std::string originInstId(iId_sv);
        md::InstrumentInfo info;
        InstType instType;
        if (!lookupInstrument(originInstId, iType_sv, info, instType)) {
            LOG_ERROR("TB {} OKX orders smc miss: {} ({})", acc.accountId, originInstId, iType_sv);
            continue;
        }

        pubsub::RCommand rcmd;
        memset(&rcmd, 0, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
        rcmd.body.orderResponse.exchangeTypeEnum = OKX;
        rcmd.body.orderResponse.instTypeEnum     = instType;
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.accountId,  acc.accountId);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.strategyId, acc.strategyId);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.instId,     std::string_view(info.instId));
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId,    ordId_sv);
        crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderSysId, clOrdId_sv);

        rcmd.body.orderResponse.offsetFlag = OF_OPEN;
        if (!side_sv.empty())  rcmd.body.orderResponse.direction = (side_sv[0] == 'b') ? DT_LONG : DT_SHORT;
        if (!sz_sv.empty())    rcmd.body.orderResponse.volumeTotal  = crypto::fast_atod(sz_sv);
        if (!px_sv.empty())    rcmd.body.orderResponse.limitPrice   = crypto::fast_atod(px_sv);
        if (!accFill_sv.empty()) rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(accFill_sv);
        if (!avgPx_sv.empty()) rcmd.body.orderResponse.tradePrice  = crypto::fast_atod(avgPx_sv);

        if (!oType_sv.empty()) {
            switch (oType_sv[0]) {
                case 'l': rcmd.body.orderResponse.orderType = OT_LIMIT;     break;
                case 'm': rcmd.body.orderResponse.orderType = OT_MARKET;    break;
                case 'p': rcmd.body.orderResponse.orderType = OT_POST_ONLY; break;
                case 'f': rcmd.body.orderResponse.orderType = OT_FOK;       break;
                case 'i': rcmd.body.orderResponse.orderType = OT_IOC;       break;
                default:  break;
            }
        }
        if (!state_sv.empty()) {
            if      (state_sv == "live")             rcmd.body.orderResponse.orderStatus = OS_NEW;
            else if (state_sv == "partially_filled") rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
            else if (state_sv == "filled")           rcmd.body.orderResponse.orderStatus = OS_FILLED;
            else if (state_sv == "canceled" || state_sv == "mmp_canceled")
                                                    rcmd.body.orderResponse.orderStatus = OS_CANCELED;
            else                                     rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
        }
        rcmd.body.orderResponse.updateTime    = crypto::getCurrentTime();
        rcmd.body.orderResponse.apiSourceEnum = AS_WEBSOCKET;
        PUSH_RCMD(rcmd)
    }
}


// ============================================================================
// query_* : REST (跟 OkxTradeUnit 一致)
// ============================================================================
void OkxWsTradeUnit::query_account(const pubsub::TCommand& tcmd) { query_balance(tcmd); }

void OkxWsTradeUnit::query_balance(const pubsub::TCommand&) {
    if (!pRestClient) return;
    auto headers = restAuthHeaders("GET", balanceUrl, "");
    asyncRequest(boost::beast::http::verb::get, balanceUrl, "", "", std::move(headers),
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) { LOG_ERROR("TB {} OKX query_balance ec: {}", acc.accountId, ec.message()); return; }
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;
                simdjson::ondemand::value data_val;
                if (doc["data"].get(data_val) != simdjson::SUCCESS) return;
                handleAccountUpdate(data_val);
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} OKX query_balance cb exc: {}", acc.accountId, e.what());
            }
        });
}

void OkxWsTradeUnit::query_position(const pubsub::TCommand&) {
    if (!pRestClient) return;
    auto headers = restAuthHeaders("GET", positionUrl, "");
    asyncRequest(boost::beast::http::verb::get, positionUrl, "", "", std::move(headers),
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) return;
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;
                simdjson::ondemand::value data_val;
                if (doc["data"].get(data_val) != simdjson::SUCCESS) return;
                handlePositionsUpdate(data_val);
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} OKX query_position cb exc: {}", acc.accountId, e.what());
            }
        });
}

void OkxWsTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);
    if (!pRestClient) return;
    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum,
                                  tcmd.body.queryOrder.instTypeEnum,
                                  tcmd.body.queryOrder.instId, info)) return;

    std::string query = "?instId=" + std::string(info.originInstId);
    if (!crypto::str_cmp(tcmd.body.queryOrder.orderId, "")) {
        query += "&ordId=" + std::string(tcmd.body.queryOrder.orderId);
    } else if (!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")) {
        query += "&clOrdId=" + std::string(tcmd.body.queryOrder.orderSysId);
    } else return;
    std::string fullPath = queryOrderUrl + query;
    auto headers = restAuthHeaders("GET", fullPath, "");
    auto info_captured = info;
    asyncRequest(boost::beast::http::verb::get, std::move(fullPath), "", "", std::move(headers),
        [this, rcmd, info_captured](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
            if (ec) return;
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;
                simdjson::ondemand::array data_arr;
                if (doc["data"].get_array().get(data_arr) != simdjson::SUCCESS) return;
                for (auto it : data_arr) {
                    auto o = it.get_object();
                    if (o.error()) continue;
                    std::string_view ordId_sv, sz_sv, px_sv, accFill_sv, avgPx_sv, state_sv;
                    o["ordId"].get(ordId_sv);
                    o["sz"].get(sz_sv);
                    o["px"].get(px_sv);
                    o["accFillSz"].get(accFill_sv);
                    o["avgPx"].get(avgPx_sv);
                    o["state"].get(state_sv);
                    crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, ordId_sv);
                    if (!sz_sv.empty())    rcmd.body.orderResponse.volumeTotal  = crypto::fast_atod(sz_sv);
                    if (!px_sv.empty())    rcmd.body.orderResponse.limitPrice   = crypto::fast_atod(px_sv);
                    if (!accFill_sv.empty()) rcmd.body.orderResponse.volumeTraded = crypto::fast_atod(accFill_sv);
                    if (!avgPx_sv.empty()) rcmd.body.orderResponse.tradePrice  = crypto::fast_atod(avgPx_sv);
                    if      (state_sv == "live")             rcmd.body.orderResponse.orderStatus = OS_NEW;
                    else if (state_sv == "partially_filled") rcmd.body.orderResponse.orderStatus = OS_PARTFILLED;
                    else if (state_sv == "filled")           rcmd.body.orderResponse.orderStatus = OS_FILLED;
                    else if (state_sv == "canceled" || state_sv == "mmp_canceled")
                                                             rcmd.body.orderResponse.orderStatus = OS_CANCELED;
                    else                                     rcmd.body.orderResponse.orderStatus = OS_UNKNOWN;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    PUSH_RCMD(rcmd)
                    break;
                }
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} OKX query_order cb exc: {}", acc.accountId, e.what());
            }
        });
}


// ============================================================================
// add_new_order (WS op:order)
// ============================================================================
void OkxWsTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
    ADD_NEW_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load() || !wsLoggedIn_.load() || !pWsClient) {
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
        if      (tcmd.body.newOrder.direction == DT_LONG)  side = "buy";
        else if (tcmd.body.newOrder.direction == DT_SHORT) side = "sell";
    } else if (tcmd.body.newOrder.offsetFlag == OF_CLOSE) {
        if      (tcmd.body.newOrder.direction == DT_LONG)  side = "sell";
        else if (tcmd.body.newOrder.direction == DT_SHORT) side = "buy";
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

    const char* ordType = nullptr;
    switch (tcmd.body.newOrder.orderType) {
        case OT_LIMIT:     ordType = "limit";     break;
        case OT_MARKET:    ordType = "market";    break;
        case OT_POST_ONLY: ordType = "post_only"; break;
        case OT_FOK:       ordType = "fok";       break;
        case OT_IOC:       ordType = "ioc";       break;
        default:
            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            rcmd.body.orderResponse.errorId     = OrderTypeError;
            rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
            PUSH_RCMD(rcmd)
            return;
    }

    double price  = crypto::getFixedPrecision(tcmd.body.newOrder.limitPrice  * info.magnifyNumber, info.tickSize);
    double volume = crypto::getFixedPrecision(tcmd.body.newOrder.volumeTotal * info.reduceNumber,  info.lotSize);
    std::string price_str = fmt::format("{}", price);
    std::string sz_str    = fmt::format("{}", volume);

    const int reqId = nextWsId_.fetch_add(1, std::memory_order_relaxed);
    std::string msg = buildOrderPlaceJson(reqId, tcmd, info, price_str, sz_str, side, ordType);

    recordPending(reqId, WsReqType::NEW_ORDER, rcmd, info);
    LOG_INFO("TB {} OKX ws op:order id={} msg={}", acc.accountId, reqId, msg);
    pWsClient->send_text(std::move(msg));
}


// ============================================================================
// cancel_order (WS op:cancel-order)
// ============================================================================
void OkxWsTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
    CANCEL_ORDER_TCMD_2_RCMD(tcmd)

    if (!isConnected.load() || !wsLoggedIn_.load() || !pWsClient) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = TBDisconnectError;
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
    if (crypto::str_cmp(tcmd.body.cancelOrder.orderId, "") &&
        crypto::str_cmp(tcmd.body.cancelOrder.orderSysId, "")) {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = OrderIdError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    const int reqId = nextWsId_.fetch_add(1, std::memory_order_relaxed);
    std::string msg = buildOrderCancelJson(reqId, tcmd, info);

    recordPending(reqId, WsReqType::CANCEL_ORDER, rcmd, info);
    LOG_INFO("TB {} OKX ws op:cancel-order id={} msg={}", acc.accountId, reqId, msg);
    pWsClient->send_text(std::move(msg));
}