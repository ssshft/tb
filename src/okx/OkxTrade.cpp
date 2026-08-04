#include "okx/OkxTrade.h"

#include <chrono>
#include <cmath>
#include <ctime>

#include <openssl/hmac.h>
#include <openssl/evp.h>

#include "base64.hpp"

#include <fmt/format.h>
#include <simdjson.h>


namespace {
    inline std::string host_of(const std::string& url) {
        std::string h = url;
        auto p = h.find("://");
        if (p != std::string::npos) h = h.substr(p + 3);
        auto q = h.find('/');
        if (q != std::string::npos) h = h.substr(0, q);
        return h;
    }
    thread_local simdjson::ondemand::parser g_parser;

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

    inline std::string escape_json(const std::string& s) {
        std::string out; out.reserve(s.size() + 4);
        for (char c : s) {
            switch (c) {
                case '"': out += "\\\""; break;
                case '\\': out += "\\\\"; break;
                case '\n': out += "\\n"; break;
                case '\r': out += "\\r"; break;
                case '\t': out += "\\t"; break;
                default:
                    if (static_cast<unsigned char>(c) < 0x20) {
                        char buf[8]; std::snprintf(buf, sizeof(buf), "\\u%04x", c); out += buf;
                    } else {
                        out += c;
                    }
            }
        }
        return out;
    }

    inline InstType okx_inst_to_enum(std::string_view s) {
        // OKX instType: SPOT / MARGIN / SWAP / FUTURES / OPTION (需要 instId 前缀进一步判 USDT/coin)
        // 这里直接返回粗粒度, lookupInstrument 再细分。
        if (s == "SPOT")    return SPOT;
        if (s == "MARGIN")  return MARGIN;
        if (s == "SWAP")    return USDT_SWAP;
        if (s == "FUTURES") return USDT_FUTURES;
        return SPOT;
    }
}


OkxTradeUnit::OkxTradeUnit(AccountCfg& a, sm::SecurityManager* s) : BaseTradeUnit(a, s) {}
OkxTradeUnit::~OkxTradeUnit() {}


// ============================================================================
// 签名 / 时间戳
// ============================================================================
std::string OkxTradeUnit::okxTimestamp() {
    // ISO8601 with millisecond: "2024-01-01T00:00:00.000Z"
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

std::string OkxTradeUnit::okxSign(const std::string& timestamp,
                                  const std::string& method,
                                  const std::string& requestPath,
                                  const std::string& body) const {
    std::string data = timestamp + method + requestPath + body;
    return hmac_sha256_base64(acc.secretKey, data);
}

std::vector<std::pair<std::string, std::string>>
OkxTradeUnit::okxAuthHeaders(const std::string& method,
                             const std::string& requestPath,
                             const std::string& body) const {
    std::string ts   = okxTimestamp();
    std::string sign = okxSign(ts, method, requestPath, body);
    return {
        {"OK-ACCESS-KEY",        acc.apiKey},
        {"OK-ACCESS-TIMESTAMP",  ts},
        {"OK-ACCESS-SIGN",       sign},
        {"OK-ACCESS-PASSPHRASE", acc.password},
    };
}


// ============================================================================
// WS login / subscribe payload
// ============================================================================
std::string OkxTradeUnit::buildLoginJson() const {
    // OKX WS login: timestamp 是秒 (整数字符串), sign = base64(HMAC-SHA256(secret, ts + "GET" + "/users/self/verify"))
    long ts_sec = crypto::getCurrentTimeSeconds();
    std::string ts = std::to_string(ts_sec);
    std::string sign = hmac_sha256_base64(acc.secretKey, ts + "GET/users/self/verify");
    return fmt::format(
        R"({{"op":"login","args":[{{"apiKey":"{}","passphrase":"{}","timestamp":"{}","sign":"{}"}}]}})",
        escape_json(acc.apiKey), escape_json(acc.password), ts, sign);
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
    std::string restHost = host_of(acc.restUrl);
    // OKX 有的仿真环境需要额外 header, 由 isSimulated 决定
    std::vector<std::pair<std::string, std::string>> defaultHeaders;
    if (acc.isSimulated) defaultHeaders.emplace_back("x-simulated-trading", "1");
    initRestClient(restHost, std::move(defaultHeaders), /*conns=*/4);

    ::net::WsConfig cfg;
    cfg.url                      = acc.wsUrl + wsPath;
    cfg.ping_mode                = ::net::WsConfig::PingMode::ClientPeriodicText;
    cfg.client_ping_interval_sec = 20;
    cfg.client_ping_text         = "ping";   // OKX 接受原始 "ping" 文本
    cfg.auto_reconnect           = true;
    cfg.idle_timeout_sec         = 60;
    LOG_INFO("TB {} OKX ws {} rest {}", acc.accountId, cfg.url, restHost);
    subWebsocketWithConfig(std::move(cfg));
}


// ============================================================================
// onOpen: 先 login, 收到 login ack 后再 subscribe。
// 这里把 login 直接 send_text 出去; subscribe 在 handleWsAck 里发。
// 简化: 也可以直接连发, OKX server 会先处理 login。
// ============================================================================
void OkxTradeUnit::onOpen() {
    BaseTradeUnit::onOpen();
    if (!pWsClient) return;
    // 先 login, 再直接 subscribe (server 按顺序处理, subscribe 会在 login ack 后被拒或通过)。
    // 更严格的做法是等 login "code":"0" 后再 subscribe, 但那个状态机成本大。
    // OKX 实测直接连发也可以 —— subscribe 会被 buffer, login 成功后 server 挨个响应。
    pWsClient->send_text(buildLoginJson());
    pWsClient->send_text(buildSubscribeJson());
}


// ============================================================================
// InstType lookup
// ============================================================================
bool OkxTradeUnit::lookupInstrument(const std::string& originInstId,
                                    std::string_view okxInstType,
                                    md::InstrumentInfo& info,
                                    InstType& out) const {
    // OKX SPOT → SPOT, MARGIN → MARGIN
    // SWAP: instId 后缀 -SWAP; 币本位 (INVERSE) 是 BTC-USD-SWAP; U 本位 (LINEAR) 是 BTC-USDT-SWAP
    // FUTURES: BTC-USDT-YYMMDD (USDT) / BTC-USD-YYMMDD (币本位)
    if (okxInstType == "SPOT") {
        if (smc->get_instrument_info(OKX, SPOT, originInstId.c_str(), info)) { out = SPOT; return true; }
    } else if (okxInstType == "MARGIN") {
        if (smc->get_instrument_info(OKX, MARGIN, originInstId.c_str(), info)) { out = MARGIN; return true; }
    } else if (okxInstType == "SWAP" || okxInstType == "FUTURES") {
        // 依次试 USDT_* → C_* (根据 instId 是否含 -USDT- 大致预判也可, 但这么写更 robust)
        InstType u_swap  = (okxInstType == "SWAP") ? USDT_SWAP    : USDT_FUTURES;
        InstType c_swap  = (okxInstType == "SWAP") ? C_SWAP       : C_FUTURES;
        if (smc->get_instrument_info(OKX, u_swap, originInstId.c_str(), info)) { out = u_swap; return true; }
        if (smc->get_instrument_info(OKX, c_swap, originInstId.c_str(), info)) { out = c_swap; return true; }
    } else {
        // 未知 okxInstType, 遍历常见类型试一遍
        for (InstType t : {SPOT, USDT_SWAP, USDT_FUTURES, C_SWAP, C_FUTURES, MARGIN}) {
            if (smc->get_instrument_info(OKX, t, originInstId.c_str(), info)) { out = t; return true; }
        }
    }
    return false;
}


// ============================================================================
// onWebsocketMsg
// ============================================================================
void OkxTradeUnit::onWebsocketMsg(const uint8_t* data, size_t len, bool, int64_t) {
    if (len == 0) return;
    // "pong" 心跳直接 skip
    if (len == 4 && std::string_view(reinterpret_cast<const char*>(data), 4) == "pong") return;
    try {
        simdjson::padded_string padded(reinterpret_cast<const char*>(data), len);
        auto doc = g_parser.iterate(padded);
        if (doc.error()) return;

        // login/subscribe ack: {"event":"login/subscribe","code":"0",...}
        std::string_view event_sv;
        if (doc["event"].get(event_sv) == simdjson::SUCCESS) {
            std::string_view code_sv, msg_sv;
            doc["code"].get(code_sv);
            doc["msg"].get(msg_sv);
            if (event_sv == "login") {
                LOG_INFO("TB {} OKX login code={} msg={}", acc.accountId, code_sv, msg_sv);
            } else if (event_sv == "subscribe") {
                LOG_INFO("TB {} OKX subscribe ack", acc.accountId);
            } else if (event_sv == "error") {
                LOG_ERROR("TB {} OKX WS error code={} msg={}", acc.accountId, code_sv, msg_sv);
            }
            return;
        }

        // channel message: {"arg":{"channel":"..."}, "data":[...]}
        simdjson::ondemand::object arg;
        if (doc["arg"].get(arg) != simdjson::SUCCESS) return;
        std::string_view channel_sv;
        if (arg["channel"].get(channel_sv) != simdjson::SUCCESS) return;

        simdjson::ondemand::value data_val;
        if (doc["data"].get(data_val) != simdjson::SUCCESS) return;

        if      (channel_sv == "account")   handleAccountUpdate(data_val);
        else if (channel_sv == "positions") handlePositionsUpdate(data_val);
        else if (channel_sv == "orders")    handleOrdersUpdate(data_val);
    }
    catch (const std::exception& e) {
        LOG_ERROR("TB {} OKX ws exc: {}", acc.accountId, e.what());
    }
}


// ---- account update ----
// data = [{totalEq, adjEq, mmr, mgnRatio, details:[{ccy, cashBal, availEq, frozenBal, upl}]}]
void OkxTradeUnit::handleAccountUpdate(simdjson::ondemand::value& dataArr) {
    simdjson::ondemand::array outer;
    if (dataArr.get_array().get(outer) != simdjson::SUCCESS) return;

    for (auto entry : outer) {
        auto d = entry.get_object();
        if (d.error()) continue;

        // details -> balance array
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
                rcmd.body.balance.available    = crypto::fast_atod(avail_sv.empty() ? cash_sv : avail_sv);
                rcmd.body.balance.frozen       = crypto::fast_atod(frozen_sv);
                rcmd.body.balance.total        = crypto::fast_atod(eq_sv.empty() ? cash_sv : eq_sv);
                rcmd.body.balance.unrealizedPnl = crypto::fast_atod(upl_sv);
                rcmd.body.balance.updateTime   = crypto::getCurrentTime();
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


// ---- positions update ----
// data = [{instType, instId, pos, avgPx, mmr, upl, markPx, liqPx, adl}]
void OkxTradeUnit::handlePositionsUpdate(simdjson::ondemand::value& dataArr) {
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


// ---- orders update ----
// data = [{instType, instId, ordId, clOrdId, sz, px, side, ordType, state, accFillSz, avgPx}]
void OkxTradeUnit::handleOrdersUpdate(simdjson::ondemand::value& dataArr) {
    simdjson::ondemand::array arr;
    if (dataArr.get_array().get(arr) != simdjson::SUCCESS) return;

    for (auto it : arr) {
        auto o = it.get_object();
        if (o.error()) continue;
        std::string_view iType_sv, iId_sv, ordId_sv, clOrdId_sv, sz_sv, px_sv, side_sv, oType_sv, state_sv, accFill_sv, avgPx_sv;
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
            LOG_ERROR("TB {} OKX orders not found in smc: {} ({})", acc.accountId, originInstId, iType_sv);
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
                case 'l': rcmd.body.orderResponse.orderType = OT_LIMIT;      break;
                case 'm': rcmd.body.orderResponse.orderType = OT_MARKET;     break;
                case 'p': rcmd.body.orderResponse.orderType = OT_POST_ONLY;  break;
                case 'f': rcmd.body.orderResponse.orderType = OT_FOK;        break;
                case 'i': rcmd.body.orderResponse.orderType = OT_IOC;        break;
                case 'o': rcmd.body.orderResponse.orderType = OT_MARKET;     break;   // optimal_limit_ioc
                default:  break;
            }
        }

        // state: live / partially_filled / filled / canceled / mmp_canceled
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
// query_account / balance / position: 都是 REST 查询, 大同小异
// ============================================================================
void OkxTradeUnit::query_account(const pubsub::TCommand&) {
    query_balance(pubsub::TCommand{});   // account 主要靠 balance 返回的 totalEq / adjEq
}

void OkxTradeUnit::query_balance(const pubsub::TCommand&) {
    if (!pRestClient) return;
    auto headers = okxAuthHeaders("GET", balanceUrl, "");
    asyncRequest(boost::beast::http::verb::get, balanceUrl, "", "", std::move(headers),
        [this](boost::system::error_code ec, ::net::HttpResponse resp) {
            if (ec) { LOG_ERROR("TB {} OKX query_balance ec: {}", acc.accountId, ec.message()); return; }
            try {
                simdjson::padded_string padded(resp.body);
                auto doc = g_parser.iterate(padded);
                if (doc.error()) return;
                simdjson::ondemand::value data_val;
                if (doc["data"].get(data_val) != simdjson::SUCCESS) return;
                handleAccountUpdate(data_val);   // 复用 WS 侧解析
            } catch (const std::exception& e) {
                LOG_ERROR("TB {} OKX query_balance cb exc: {}", acc.accountId, e.what());
            }
        });
}

void OkxTradeUnit::query_position(const pubsub::TCommand&) {
    if (!pRestClient) return;
    auto headers = okxAuthHeaders("GET", positionUrl, "");
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


// ============================================================================
// add_new_order —— POST /api/v5/trade/order (body 是 JSON)
// ============================================================================
void OkxTradeUnit::add_new_order(const pubsub::TCommand& tcmd) {
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

    // OKX 必需字段: instId / tdMode / side / ordType / sz; 限价单还要 px。
    std::string body;
    if (tcmd.body.newOrder.orderType == OT_MARKET) {
        body = fmt::format(
            R"({{"instId":"{}","tdMode":"cross","side":"{}","ordType":"{}","sz":"{}","clOrdId":"{}","reduceOnly":{}}})",
            info.originInstId, side, ordType, sz_str,
            escape_json(tcmd.body.newOrder.orderSysId),
            tcmd.body.newOrder.reduceOnly ? "true" : "false");
    } else {
        body = fmt::format(
            R"({{"instId":"{}","tdMode":"cross","side":"{}","ordType":"{}","px":"{}","sz":"{}","clOrdId":"{}","reduceOnly":{}}})",
            info.originInstId, side, ordType, price_str, sz_str,
            escape_json(tcmd.body.newOrder.orderSysId),
            tcmd.body.newOrder.reduceOnly ? "true" : "false");
    }

    auto headers = okxAuthHeaders("POST", orderUrl, body);
    LOG_INFO("TB {} OKX add_new_order body={}", acc.accountId, body);

    auto info_captured = info;

    asyncRequest(boost::beast::http::verb::post, orderUrl, std::move(body), "application/json",
                 std::move(headers),
        [this, rcmd, info_captured](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
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
                // OKX resp: {"code":"0","msg":"","data":[{"sCode":"0","ordId":"...","clOrdId":"...","sMsg":""}]}
                std::string_view topCode_sv;
                doc["code"].get(topCode_sv);
                simdjson::ondemand::array data_arr;
                if (doc["data"].get_array().get(data_arr) == simdjson::SUCCESS) {
                    for (auto it : data_arr) {
                        auto o = it.get_object();
                        if (o.error()) continue;
                        std::string_view sCode_sv, ordId_sv, sMsg_sv;
                        o["sCode"].get(sCode_sv);
                        o["ordId"].get(ordId_sv);
                        o["sMsg"].get(sMsg_sv);
                        if (sCode_sv == "0") {
                            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.orderId, ordId_sv);
                            rcmd.body.orderResponse.orderStatus = OS_NEW;
                        } else {
                            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                            rcmd.body.orderResponse.errorId     = UnknownError;
                            crypto::copy_sv_to_char_array(rcmd.body.orderResponse.originMsg, sMsg_sv);
                        }
                        break;   // 单笔下单一条 data
                    }
                } else {
                    rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                    rcmd.body.orderResponse.errorId     = UnknownError;
                }
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} OKX add_new_order cb exc: {}", acc.accountId, e.what());
            }
        });
}


// ============================================================================
// cancel_order —— POST /api/v5/trade/cancel-order (body = {instId, ordId 或 clOrdId})
// ============================================================================
void OkxTradeUnit::cancel_order(const pubsub::TCommand& tcmd) {
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

    std::string body;
    if (!crypto::str_cmp(tcmd.body.cancelOrder.orderId, "")) {
        body = fmt::format(R"({{"instId":"{}","ordId":"{}"}})",
                           info.originInstId, tcmd.body.cancelOrder.orderId);
    } else if (!crypto::str_cmp(tcmd.body.cancelOrder.orderSysId, "")) {
        body = fmt::format(R"({{"instId":"{}","clOrdId":"{}"}})",
                           info.originInstId, escape_json(tcmd.body.cancelOrder.orderSysId));
    } else {
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.errorId     = OrderIdError;
        rcmd.body.orderResponse.updateTime  = crypto::getCurrentTime();
        PUSH_RCMD(rcmd)
        return;
    }

    auto headers = okxAuthHeaders("POST", cancelOrderUrl, body);
    LOG_INFO("TB {} OKX cancel_order body={}", acc.accountId, body);

    asyncRequest(boost::beast::http::verb::post, cancelOrderUrl, std::move(body), "application/json",
                 std::move(headers),
        [this, rcmd](boost::system::error_code ec, ::net::HttpResponse resp) mutable {
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
                simdjson::ondemand::array data_arr;
                if (doc["data"].get_array().get(data_arr) == simdjson::SUCCESS) {
                    for (auto it : data_arr) {
                        auto o = it.get_object();
                        if (o.error()) continue;
                        std::string_view sCode_sv, sMsg_sv, ordId_sv;
                        o["sCode"].get(sCode_sv);
                        o["sMsg"].get(sMsg_sv);
                        o["ordId"].get(ordId_sv);
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
                }
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                PUSH_RCMD(rcmd)
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} OKX cancel_order cb exc: {}", acc.accountId, e.what());
            }
        });
}


// ============================================================================
// query_order —— GET /api/v5/trade/order?instId=X&ordId=Y
// ============================================================================
void OkxTradeUnit::query_order(const pubsub::TCommand& tcmd) {
    QUERY_ORDER_TCMD_2_RCMD(tcmd);

    md::InstrumentInfo info;
    if (!smc->get_instrument_info(tcmd.body.queryOrder.exchangeTypeEnum,
                                  tcmd.body.queryOrder.instTypeEnum,
                                  tcmd.body.queryOrder.instId, info)) {
        LOG_INFO("TB {} OKX query_order smc miss: {}", acc.accountId, tcmd.body.queryOrder.instId);
        return;
    }

    // 构造带 query 的 path
    std::string query = "?instId=" + std::string(info.originInstId);
    if (!crypto::str_cmp(tcmd.body.queryOrder.orderId, "")) {
        query += "&ordId=" + std::string(tcmd.body.queryOrder.orderId);
    } else if (!crypto::str_cmp(tcmd.body.queryOrder.orderSysId, "")) {
        query += "&clOrdId=" + std::string(tcmd.body.queryOrder.orderSysId);
    } else {
        return;
    }
    std::string fullPath = queryOrderUrl + query;

    // OKX 签名的 requestPath 包含 query
    auto headers = okxAuthHeaders("GET", fullPath, "");
    LOG_INFO("TB {} OKX query_order: {}", acc.accountId, fullPath);

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
            }
            catch (const std::exception& e) {
                LOG_ERROR("TB {} OKX query_order cb exc: {}", acc.accountId, e.what());
            }
        });
}