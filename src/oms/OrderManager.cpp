#include "oms/OrderManager.h"


Tb2OmsRCommandInnerQueue tb2OmsRCommandInnerQueue;
RcmdInnerQueue rcmdInnerQueue;


// 老宏保持不变: 从 CMD_NEW_ORDER tcmd 造 CMD_RPT_ORDER_RESPONSE rcmd
#define ADD_NEW_ORDER_2_ORDER_RESPONSE(tcmd) \
    pubsub::RCommand rcmd; \
    memset(&rcmd, 0, sizeof(pubsub::RCommand)); \
    rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE; \
    rcmd.body.orderResponse.exchangeTypeEnum = tcmd.body.newOrder.exchangeTypeEnum; \
    rcmd.body.orderResponse.instTypeEnum = tcmd.body.newOrder.instTypeEnum; \
    strncpy(rcmd.body.orderResponse.accountId, tcmd.body.newOrder.accountId, ACCOUNTID_SIZE); \
    strncpy(rcmd.body.orderResponse.strategyId, tcmd.body.newOrder.strategyId, STRATEGYID_SIZE); \
    strncpy(rcmd.body.orderResponse.instId, tcmd.body.newOrder.instId, INSTID_SIZE); \
    rcmd.body.orderResponse.clientOrderId = tcmd.body.newOrder.clientOrderId; \
    const std::string& orderSysId = getOrderSysId(tcmd.body.newOrder.exchangeTypeEnum, tcmd.body.newOrder.strategyId); \
    strncpy(rcmd.body.orderResponse.orderSysId, orderSysId.c_str(), ORDER_SIZE); \
    strncpy(rcmd.body.orderResponse.strategyRef, tcmd.body.newOrder.strategyRef, ORDER_SIZE); \
    rcmd.body.orderResponse.offsetFlag = tcmd.body.newOrder.offsetFlag; \
    rcmd.body.orderResponse.direction = tcmd.body.newOrder.direction; \
    rcmd.body.orderResponse.orderType = tcmd.body.newOrder.orderType; \
    rcmd.body.orderResponse.volumeTotal = tcmd.body.newOrder.volumeTotal; \
    rcmd.body.orderResponse.limitPrice = tcmd.body.newOrder.limitPrice; \
    rcmd.body.orderResponse.reduceOnly = tcmd.body.newOrder.reduceOnly; \
    rcmd.body.orderResponse.orderStatus = OS_PENDING_NEW; \
    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime(); \
    rcmd.body.orderResponse.apiSourceEnum = AS_ADD_NEW_ORDER;


om::OrderManager::OrderManager() {
    accountManager = am::AccountManager();
    // storage = tb_sqlite::SqliteStorage();
}

om::OrderManager::~OrderManager() {
    // OmsShmWriter 析构会自动 munmap
}

void om::OrderManager::preStart() {
    // ------------------------------------------------------------------
    // 打开 / 创建共享内存报单表
    //   - 不存在: 建新的
    //   - 存在: 校验 magic+version 后打开, 自动 recover orphan RECLAIMING slot
    //   - 崩溃恢复策略: preStart 里做, 保证 run() 阶段的 alloc 无残留
    // ------------------------------------------------------------------
    try {
        shm_.open(shm_path_, shm_capacity_);
        LOG_INFO("OMS SHM ready path={} capacity={} slot_size={} total_size={:.1f}MB",
                 shm_path_, shm_capacity_, sizeof(oms::shm::OmsSlot),
                 (double)oms::shm::OmsShmLayout::compute_total_size(
                     shm_capacity_, shm_capacity_ * 2) / (1024.0 * 1024.0));
        auto st = shm_.stats();
        LOG_INFO("OMS SHM initial state: empty={} live={} finished={} reclaims={}",
                 st.empty, st.live, st.finished, st.total_reclaims);
    } catch (const std::exception& e) {
        LOG_ERROR("OMS SHM open failed path={} err={}", shm_path_, e.what());
        throw;   // 存储不可用直接崩, 免得静默丢单
    }

    accountManager.preStart();
}

void om::OrderManager::run() {
}

void om::OrderManager::preStop() {
    accountManager.preStop();
    // shm_ 析构会 munmap; owner_pid 保留在 header 里, 便于运维追踪
}

bool om::OrderManager::processTcmd(pubsub::TCommand& tcmd) {
    switch (tcmd.cmdTypeEnum) {
        case pubsub::CMD_NEW_ORDER: {
            if (tcmd.body.newOrder.clientOrderId == TESTCLIENTORDERID) {
                return true;
            }
            ADD_NEW_ORDER_2_ORDER_RESPONSE(tcmd)
            rcmdInnerQueue.push(rcmd);
            strncpy(tcmd.body.newOrder.orderSysId, rcmd.body.orderResponse.orderSysId, ORDER_SIZE);

            // 落 shm: 一次 upsert 自动建 3 层索引 (orderSysId / clientOrderId 复合 / orderId)
            uint32_t idx = shm_.upsert(rcmd);
            if (idx == oms::shm::kInvalidSlot) {
                LOG_ERROR("OMS SHM upsert FAILED (ring exhausted?) orderSysId={} clientOrderId={}",
                          rcmd.body.orderResponse.orderSysId,
                          rcmd.body.orderResponse.clientOrderId);
                // 上报 REJECTED, 免得策略以为发出去了
                pubsub::RCommand fail;
                memcpy(&fail, &rcmd, sizeof(fail));
                fail.body.orderResponse.orderStatus = OS_REJECTED;
                fail.body.orderResponse.errorId     = UnknownError;
                fail.body.orderResponse.updateTime  = crypto::getCurrentTime();
                strncpy(fail.body.orderResponse.originMsg, "OMS SHM ring exhausted", ORIGINMSG_SIZE);
                rcmdInnerQueue.push(fail);
                return false;
            }
            return true;
            break;
        }
        case pubsub::CMD_CANCEL_ORDER: {
            // ★ 一次调用同时拿到 sysId + 完整 RCommand, 避免二次 lookup。
            //   findOrder 内部 seqlock 一致快照, 拿到就是拿到, 没有"索引 hit 但 slot 没了"的中间态
            pubsub::RCommand stored;
            bool found = findOrder(tcmd.body.cancelOrder.clientOrderId,
                                   tcmd.body.cancelOrder.strategyId,
                                   tcmd.body.cancelOrder.orderId,
                                   stored);
            if (found) {
                // 回填 tcmd.orderSysId 供下游 TradeUnit 用
                strncpy(tcmd.body.cancelOrder.orderSysId,
                        stored.body.orderResponse.orderSysId, ORDER_SIZE);

                // 拷现有状态 → 改成 CANCELLING → push, 但**不改 shm** (等交易所回报再动)
                pubsub::RCommand rcmd;
                memcpy(&rcmd, &stored, sizeof(pubsub::RCommand));
                rcmd.body.orderResponse.orderStatus  = OS_CANCELLING;
                rcmd.body.orderResponse.updateTime   = crypto::getCurrentTime();
                rcmd.body.orderResponse.apiSourceEnum = AS_CANCEL_ORDER;
                rcmdInnerQueue.push(rcmd);

                // 检查是否已终结 (幂等: 已 filled/cancelled/rejected 直接返)
                OrderStatus st = stored.body.orderResponse.orderStatus;
                if (st == OS_CANCELED || st == OS_FILLED || st == OS_REJECTED) {
                    LOG_INFO("order clientOrderId: {} already finished, return oms result.",
                             stored.body.orderResponse.clientOrderId);
                    rcmd.body.orderResponse.orderStatus  = st;
                    rcmd.body.orderResponse.errorId      = OrderAlreadyFinishedError;
                    rcmd.body.orderResponse.updateTime   = stored.body.orderResponse.updateTime;
                    rcmd.body.orderResponse.apiSourceEnum = AS_CANCEL_ORDER;
                    rcmdInnerQueue.push(rcmd);
                    return false;
                }
                return true;
            }
            else {
                LOG_ERROR("oms not found orderSysId, tcmd: {}", tcmd.getString());
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                rcmd.body.orderResponse.exchangeTypeEnum = tcmd.body.cancelOrder.exchangeTypeEnum;
                rcmd.body.orderResponse.instTypeEnum     = tcmd.body.cancelOrder.instTypeEnum;
                strncpy(rcmd.body.orderResponse.accountId,  tcmd.body.cancelOrder.accountId,  ACCOUNTID_SIZE);
                strncpy(rcmd.body.orderResponse.strategyId, tcmd.body.cancelOrder.strategyId, STRATEGYID_SIZE);
                strncpy(rcmd.body.orderResponse.instId,     tcmd.body.cancelOrder.instId,     INSTID_SIZE);
                rcmd.body.orderResponse.clientOrderId = tcmd.body.cancelOrder.clientOrderId;
                strncpy(rcmd.body.orderResponse.orderSysId, tcmd.body.cancelOrder.orderSysId, ORDER_SIZE);
                strncpy(rcmd.body.orderResponse.orderId,    tcmd.body.cancelOrder.orderId,    ORDER_SIZE);
                rcmd.body.orderResponse.orderStatus  = OS_REJECTED;
                rcmd.body.orderResponse.errorId      = OMSOrderNotFoundError;
                rcmd.body.orderResponse.updateTime   = crypto::getCurrentTime();
                rcmd.body.orderResponse.apiSourceEnum = AS_CANCEL_ORDER;
                rcmdInnerQueue.push(rcmd);
                return false;
            }
            break;
        }
        case pubsub::CMD_QUERY_ORDER: {
            pubsub::RCommand stored;
            bool found = findOrder(tcmd.body.queryOrder.clientOrderId,
                                   tcmd.body.queryOrder.strategyId,
                                   tcmd.body.queryOrder.orderId,
                                   stored);
            if (found) {
                strncpy(tcmd.body.queryOrder.orderSysId,
                        stored.body.orderResponse.orderSysId, ORDER_SIZE);
                if (stored.body.orderResponse.orderStatus != OS_FILLED) {
                    // 非 FILLED: 补 orderId 让下层查交易所
                    strncpy(tcmd.body.queryOrder.orderId,
                            stored.body.orderResponse.orderId, INSTID_SIZE);
                    return true;
                }
                // FILLED: 直接从本地 shm 返回, 不打扰交易所
                pubsub::RCommand rcmd;
                memcpy(&rcmd, &stored, sizeof(pubsub::RCommand));
                rcmd.body.orderResponse.apiSourceEnum = AS_QUERY_ORDER;
                rcmdInnerQueue.push(rcmd);
                return false;
            }
            else {
                LOG_ERROR("oms not found orderSysId, tcmd: {}", tcmd.getString());
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                rcmd.body.orderResponse.exchangeTypeEnum = tcmd.body.queryOrder.exchangeTypeEnum;
                rcmd.body.orderResponse.instTypeEnum     = tcmd.body.queryOrder.instTypeEnum;
                strncpy(rcmd.body.orderResponse.accountId,  tcmd.body.queryOrder.accountId,  ACCOUNTID_SIZE);
                strncpy(rcmd.body.orderResponse.strategyId, tcmd.body.queryOrder.strategyId, STRATEGYID_SIZE);
                strncpy(rcmd.body.orderResponse.instId,     tcmd.body.queryOrder.instId,     INSTID_SIZE);
                rcmd.body.orderResponse.clientOrderId = tcmd.body.queryOrder.clientOrderId;
                strncpy(rcmd.body.orderResponse.orderSysId, tcmd.body.queryOrder.orderSysId, ORDER_SIZE);
                strncpy(rcmd.body.orderResponse.orderId,    tcmd.body.queryOrder.orderId,    ORDER_SIZE);
                rcmd.body.orderResponse.orderStatus  = OS_REJECTED;
                rcmd.body.orderResponse.errorId      = OMSOrderNotFoundError;
                rcmd.body.orderResponse.updateTime   = crypto::getCurrentTime();
                rcmd.body.orderResponse.apiSourceEnum = AS_QUERY_ORDER;
                rcmdInnerQueue.push(rcmd);
                return false;
            }
        }
        case pubsub::CMD_QUERY_ACCOUNT:
        case pubsub::CMD_QUERY_BALANCE:
        case pubsub::CMD_QUERY_POSITION: {
            return true;
        }
        default: {
            LOG_ERROR("unimplemented cmd type.");
            return false;
        }
    }
}

bool om::OrderManager::processRcmd(pubsub::RCommand& rcmd) {
    switch (rcmd.cmdTypeEnum) {
        case pubsub::CMD_RPT_NEW_ORDER:
        case pubsub::CMD_RPT_CANCEL_ORDER:
        case pubsub::CMD_RPT_QUERY_ORDER:
        case pubsub::CMD_RPT_ORDER_RESPONSE: {
            return onOrderUpdate(rcmd);
        }
        case pubsub::CMD_RPT_TOTAL_ACCOUNT: {
            return true;
        }
        case pubsub::CMD_RPT_BALANCE:
        case pubsub::CMD_RPT_POSITION: {
            return accountManager.processRcmd(rcmd);
        }
        default: {
            LOG_ERROR("got an unimplement cmd type!");
            return false;
        }
    }
}

// -------------------------------------------------------------------------
// onOrderUpdate: read-modify-write
//   1) lookup 现有 slot (拿一致快照 cur)
//   2) merge new rcmd 到 cur (status 优先级、volumeTraded 单调、tradeDiff 计算、
//      orderId 补索引、REJECTED 保留 errorId/originMsg)
//   3) upsert(cur) 原子写回 (shm 内部会自动 update slot + 补别名索引)
//   4) 决定是否 push 到上层 (跟老逻辑完全一致)
//
// 因为 OMS 是单写者, lookup + upsert 之间无并发写, 无 race。
// -------------------------------------------------------------------------
bool om::OrderManager::onOrderUpdate(pubsub::RCommand& rcmd) {
    pubsub::RCommand cur;
    bool has_cur = shm_.lookup_by_orderSysId(
        std::string_view(rcmd.body.orderResponse.orderSysId,
                         strlen(rcmd.body.orderResponse.orderSysId)),
        cur);
    if (!has_cur) return false;

    bool statusAdvanced  = false;
    bool volumeIncreased = false;

    if (rcmd.body.orderResponse.volumeTraded
            > cur.body.orderResponse.volumeTotal + ZERO_NUM) {
        LOG_ERROR("Overfilled order! orderSysId: {} volumeTraded: {} volumeTotal: {}",
                  cur.body.orderResponse.orderSysId,
                  rcmd.body.orderResponse.volumeTraded,
                  cur.body.orderResponse.volumeTotal);
    }

    // Binance REST 下单响应带 PARTFILLED/FILLED 但无成交价, 丢
    if (rcmd.body.orderResponse.orderStatus == OS_PARTFILLED
            || rcmd.body.orderResponse.orderStatus == OS_FILLED) {
        if (rcmd.body.orderResponse.apiSourceEnum == AS_ADD_NEW_ORDER
                && rcmd.body.orderResponse.exchangeTypeEnum == BINANCE) {
            return false;
        }
    }
    // Binance 撤单响应 volumeTraded < 已知 → 丢 (WS 更快, REST 数据可能落后)
    if (rcmd.body.orderResponse.orderStatus == OS_CANCELED) {
        if (rcmd.body.orderResponse.apiSourceEnum == AS_CANCEL_ORDER
                && rcmd.body.orderResponse.exchangeTypeEnum == BINANCE) {
            if (cur.body.orderResponse.volumeTraded < rcmd.body.orderResponse.volumeTraded) {
                return false;
            }
        }
    }

    // 成交量单调递增
    if (rcmd.body.orderResponse.volumeTraded > cur.body.orderResponse.volumeTraded) {
        cur.body.orderResponse.tradeDiff =
            rcmd.body.orderResponse.volumeTraded - cur.body.orderResponse.volumeTraded;
        cur.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTraded;
        cur.body.orderResponse.tradePrice   = rcmd.body.orderResponse.tradePrice;
        cur.body.orderResponse.fillPrice    = rcmd.body.orderResponse.fillPrice;
        cur.body.orderResponse.updateTime   = crypto::getCurrentTime();
        volumeIncreased = true;
    }

    // 状态优先级前进 (只允许 status 递进, 不能回退)
    if (!crypto::isFinalOrderStatus(cur.body.orderResponse.orderStatus)) {
        int oldP = crypto::getOrderStatusPriority(cur.body.orderResponse.orderStatus);
        int newP = crypto::getOrderStatusPriority(rcmd.body.orderResponse.orderStatus);
        if (newP > oldP) {
            cur.body.orderResponse.orderStatus = rcmd.body.orderResponse.orderStatus;
            cur.body.orderResponse.updateTime  = crypto::getCurrentTime();
            statusAdvanced = true;
        }
    }

    // 补 orderId (交易所 ACK 之后才有值). upsert 会自动补 IDX_EXCHANGE_ID 索引。
    if (!crypto::str_cmp(rcmd.body.orderResponse.orderId, "")) {
        strncpy(cur.body.orderResponse.orderId, rcmd.body.orderResponse.orderId, ORDER_SIZE);
    }

    if (rcmd.body.orderResponse.orderStatus == OS_REJECTED) {
        cur.body.orderResponse.errorId = rcmd.body.orderResponse.errorId;
        strncpy(cur.body.orderResponse.originMsg, rcmd.body.orderResponse.originMsg, ORIGINMSG_SIZE);
    }

    // 撤单失败 (OS_FAILED) 特殊 push: 拷 cur → rcmd 让上层看到当前状态
    if (rcmd.body.orderResponse.orderStatus == OS_FAILED) {
        // 原子写回 shm (状态 = FAILED, 由下面的 upsert 承载)
        cur.body.orderResponse.orderStatus = OS_FAILED;
        cur.body.orderResponse.apiSourceEnum = AS_CANCEL_ORDER;
        cur.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
        shm_.upsert(cur);
        memcpy(&rcmd, &cur, sizeof(pubsub::RCommand));
        return true;
    }

    bool isQueryOrder = rcmd.cmdTypeEnum == pubsub::CMD_RPT_QUERY_ORDER;

    // === 原子写回 shm ===
    // 无论是否 push 上层, 状态都要落 shm; upsert 内部会 update 现有 slot 并补索引。
    cur.body.orderResponse.apiSourceEnum = rcmd.body.orderResponse.apiSourceEnum;
    cur.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
    shm_.upsert(cur);

    if (!statusAdvanced && !volumeIncreased && !isQueryOrder) {
        return false;
    }

    memcpy(&rcmd, &cur, sizeof(pubsub::RCommand));
    return true;
}

void om::OrderManager::load_data() {
    // 老 SQLite 加载路径, 暂 no-op。 shm 里 restart 后 LIVE slot 自动保留, 无需从 SQLite 恢复。
    // 如果需要冷层持久历史 (超过 shm 环容量), 独立起个后台线程 iterate_finished 转 SQLite。
}

void om::OrderManager::store_rcmd_data(std::vector<pubsub::RCommand> & /*rcmdVec*/) {
    // 冷层持久化未启用。 想接 SQLite 的话在这里遍历 rcmdVec 写库。
}


// -------------------------------------------------------------------------
// findOrder: 用 (strategyId, cid) 或 orderId 反查完整报单
//
// 相比老 `getOrderSysId` 只吐 orderSysId, 这里**直接返回整个 RCommand**, 让 caller
// 少一次 lookup_by_orderSysId (老代码是 2 次查询)。 对 CANCEL/QUERY 路径每次省 200ns
// 级别的 shm 读, 更重要的是省掉了"索引在但 slot 没了"的中间态分支处理。
//
// 语义: 主查 (strategyId, cid) 复合 key; 失败退回 exchange orderId; 都失败返 false。
// -------------------------------------------------------------------------
bool om::OrderManager::findOrder(int64_t clientOrderId,
                                 const char* strategyId,
                                 const char* orderId,
                                 pubsub::RCommand& out)
{
    if (strategyId && *strategyId) {
        if (shm_.lookup_by_client(std::string_view(strategyId, strlen(strategyId)),
                                  clientOrderId, out)) {
            return true;
        }
    }
    if (orderId && *orderId) {
        if (shm_.lookup_by_orderId(std::string_view(orderId, strlen(orderId)), out)) {
            return true;
        }
    }
    return false;
}

// -------------------------------------------------------------------------
// getOrderSysId (生成版): 各交易所的 clOrdId 命名规范前缀 + rdtsc
//   ★ 与存储无关, 保持老逻辑
// -------------------------------------------------------------------------
std::string om::OrderManager::getOrderSysId(ExchangeType exchangeTypeEnum, const char* strategyId) {
    switch (exchangeTypeEnum) {
        case BINANCE:
        case BITGET: return fmt::format("x-{}{}", strategyId, crypto::get_rdtsc_timestamp());
        case OKX:    return fmt::format("ok{}{}", strategyId, crypto::get_rdtsc_timestamp());
        case GATEIO: return fmt::format("t-{}{}", strategyId, crypto::get_rdtsc_timestamp());
        case BYBIT:  return fmt::format("by{}{}", strategyId, crypto::get_rdtsc_timestamp());
        case HTX:    return fmt::format("{}",     crypto::get_rdtsc_timestamp());
        default:     return fmt::format("{}{}",   strategyId, crypto::get_rdtsc_timestamp());
    }
}
