#include "oms/OrderManager.h"

#include <cstdio>    // std::snprintf (originMsg 回写, 见 CMD_NEW_ORDER 的环满分支)
#include <cstring>   // std::strncpy / std::memset

Tb2OmsRCommandInnerQueue tb2OmsRCommandInnerQueue;
RcmdInnerQueue rcmdInnerQueue;


#define ADD_NEW_ORDER_2_ORDER_RESPONSE(tcmd) \
    pubsub::RCommand rcmd; \
    memset(&rcmd, 0, sizeof(pubsub::RCommand)); \
    rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE; \
    rcmd.body.orderResponse.exchangeTypeEnum = tcmd.body.newOrder.exchangeTypeEnum; \
    rcmd.body.orderResponse.instTypeEnum = tcmd.body.newOrder.instTypeEnum; \
    strncpy(rcmd.body.orderResponse.accountName, tcmd.body.newOrder.accountName, 32); \
    strncpy(rcmd.body.orderResponse.strategyId, tcmd.body.newOrder.strategyId, 32); \
    strncpy(rcmd.body.orderResponse.instId, tcmd.body.newOrder.instId, 32); \
    rcmd.body.orderResponse.clientOrderId = tcmd.body.newOrder.clientOrderId; \
    const std::string& orderSysId = getOrderSysId(tcmd.body.newOrder.exchangeTypeEnum, tcmd.body.newOrder.strategyId); \
    strncpy(rcmd.body.orderResponse.orderSysId, orderSysId.c_str(), 64); \
    strncpy(rcmd.body.orderResponse.strategyRef, tcmd.body.newOrder.strategyRef, 64); \
    rcmd.body.orderResponse.offsetFlag = tcmd.body.newOrder.offsetFlag; \
    rcmd.body.orderResponse.direction = tcmd.body.newOrder.direction; \
    rcmd.body.orderResponse.orderType = tcmd.body.newOrder.orderType; \
    rcmd.body.orderResponse.volumeTotal = tcmd.body.newOrder.volumeTotal; \
    rcmd.body.orderResponse.limitPrice = tcmd.body.newOrder.limitPrice; \
    rcmd.body.orderResponse.reduceOnly = tcmd.body.newOrder.reduceOnly; \
    rcmd.body.orderResponse.orderStatus = OS_PENDING_NEW; \
    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime(); \
    rcmd.body.orderResponse.apiSourceEnum = AS_ADD_NEW_ORDER; \
    

om::OrderManager::OrderManager() {
    accountManager = am::AccountManager();
    // storage = tb_sqlite::SqliteStorage();
}

om::OrderManager::~OrderManager(){
    // omsShm_ 的析构 (OmsShmSegment::~OmsShmSegment) 会 munmap, 不需要在这里手动 close。
}

void om::OrderManager::preStart(const OmsShmConfig& cfg){
    // storage->create_oms_table();
    // load_data();

    try {
        omsShm_.open(cfg.path, cfg.slot_capacity);
    }
    catch (const std::exception& e) {
        // 开不了 shm 就没法跟踪订单 —— 继续跑下去会变成"发出去的单全都不认识",
        // 比启动失败更危险。直接抛, 让 tb 起不来。
        LOG_ERROR("oms shm open failed: path={} err={}", cfg.path, e.what());
        cryptothrow(e.what(), -1);
    }
    omsShm_.set_min_reclaim_age_ns(cfg.min_reclaim_age_ns);
    omsShm_.set_max_live_stale_ns(cfg.max_live_stale_ns);
    cfg_ = cfg;
    shm_reject_count_ = 0;
    shm_busy_count_   = 0;

    const oms::shm::OmsShmSegment::Stats st = omsShm_.stats();
    LOG_INFO("oms shm ready: path={} created={} slot_cap={} index_cap={} "
             "empty={} live={} finished={} min_reclaim_age={}ms max_live_stale={}ms "
             "sustainable_insert_rate={}/s",
             cfg.path, omsShm_.created_new(), omsShm_.slot_capacity(), omsShm_.index_capacity(),
             st.empty, st.live, st.finished,
             cfg.min_reclaim_age_ns / 1000000ULL, cfg.max_live_stale_ns / 1000000ULL,
             cfg.sustainable_insert_rate());

    accountManager.preStart();
}

void om::OrderManager::run(){

}

void om::OrderManager::preStop(){
    // ★ 这里**故意不** close()/munmap。
    //   preStop 是从 main 的 signal_handler 调的, 跑在**另一个线程**上; 而 execute()
    //   线程可能正卡在 upsert 中间。munmap 掉正在写的映射 = 段错误。
    //   进程退出时内核会回收映射, 不 close 没有任何副作用 (MAP_SHARED 的数据早已可见)。
    if (omsShm_.is_open()) {
        const oms::shm::OmsShmSegment::Stats st = omsShm_.stats();
        LOG_INFO("oms shm final: inserts={} updates={} reclaims={} alloc_failures={} "
                 "(exhausted={} alias_insert_failures={} key_sync_failures={})",
                 st.total_inserts, st.total_updates, st.total_reclaims, st.total_alloc_failures,
                 st.total_alloc_exhausted, st.total_alias_insert_failures, st.total_key_sync_failures);
        if (st.total_alloc_failures > 0) {
            LOG_ERROR("oms shm dropped {} order(s) —— 可持续速率上限 = slot_cap / min_reclaim_age "
                      "= {}/s, 请核对实际下单速率", st.total_alloc_failures,
                      cfg_.sustainable_insert_rate());
        }
    }
    accountManager.preStop();
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

            // ★ 取代老代码的三行:
            //     orderSysId2OrderResponseMap[orderSysId] = rcmd;
            //     clientOrderId2OrderSysIdMap[strategyId+cid] = orderSysId;
            //   现在一次 upsert 就同时建立: 报单体 + orderSysId 主索引 + 复合 client 索引。
            //   (orderId 索引要等交易所回报带了 orderId 才建, 由 update_slot 的 key 同步补上)
            const uint32_t slot = omsShm_.upsert(rcmd);
            if (slot == oms::shm::kInvalidSlot) {
                // 环写满 / 索引写不进 —— 这张单**没进 SHM**。
                // 老代码此处必然返回 true (tbb map 无上限), 于是会把一张"OMS 不认识"的单
                // 发给交易所: 之后撤单/查询全部落空, 只能等交易所回报兜底。
                // 现在返回 false → TbOperation 不会把它投给 trade client (宁可不下, 不可失控)。
                //
                // ★ 限流: 环写满时**每一单**都会走到这里。逐条打日志 = 每秒上万条 ERROR,
                //   正是 §2.9 那个刷屏事故的形态。OmsShm 内部的 alloc_exhausted_report 已经
                //   限流并打出完整现场 (环占用 / TTL / 可持续速率), 这里只补上层视角。
                //   也**不要**在这里调 omsShm_.stats() —— 它是 O(slot_capacity) 的全表扫描,
                //   放在丢单热路径上会把一次故障放大成 CPU 打满。
                const uint64_t n = ++shm_reject_count_;
                if (should_log_limited(n)) {
                    LOG_ERROR("oms shm upsert failed (ring full?), reject new order. rejected={} "
                              "orderSysId={} strategyId={} clientOrderId={} slot_cap={} "
                              "min_reclaim_age={}ms sustainable_insert_rate={}/s",
                              n,
                              rcmd.body.orderResponse.orderSysId, rcmd.body.orderResponse.strategyId,
                              rcmd.body.orderResponse.clientOrderId,
                              cfg_.slot_capacity, cfg_.min_reclaim_age_ns / 1000000ULL,
                              cfg_.sustainable_insert_rate());
                }

                // 上报 REJECTED, 免得策略以为发出去了
                pubsub::RCommand fail;
                memcpy(&fail, &rcmd, sizeof(fail));
                fail.body.orderResponse.orderStatus = OS_REJECTED;
                fail.body.orderResponse.errorId = UnknownError;
                fail.body.orderResponse.updateTime = crypto::getCurrentTime();
                // ★ 这里**不能**用 ORIGINMSG_SIZE: 它是 256, 而 originMsg 只有 char[128]。
                //   `strncpy(dst, src, n)` 会按 n **补 NUL**, 也就是无条件写满 256 字节 ——
                //   越过 originMsg 尾部 128 字节, 把**上面两行刚设好的** updateTime /
                //   apiSourceEnum 清零, 并继续溢出到结构体之外 (sizeof(OrderResponse)=528)。
                //   实测: updateTime 1234567890123456 → 0, apiSourceEnum 2 → 0。
                //   用 sizeof(字段) 而不是宏: 边界跟着字段走, 以后字段变长也不会再漂。
                std::snprintf(fail.body.orderResponse.originMsg, sizeof(fail.body.orderResponse.originMsg), "%s", "OMS SHM ring exhausted");
                rcmdInnerQueue.push(fail);
                return false;
            }
            return true;
            break;
        }
        case pubsub::CMD_CANCEL_ORDER: {
            // ★ 一次查询就拿到完整报单体 (老代码要查两次: client key→sysId, sysId→报单体)。
            pubsub::RCommand cached;
            const oms::shm::OmsShmSegment::LookupStatus ls = getOrderSysId(tcmd.body.cancelOrder.clientOrderId, tcmd.body.cancelOrder.strategyId, cached, tcmd.body.cancelOrder.orderId);

            if (ls == oms::shm::OmsShmSegment::LookupStatus::OK) {
                // 回填 orderSysId —— 后续拼回包 / 转发给 trade client 都要用。
                strncpy(tcmd.body.cancelOrder.orderSysId, cached.body.orderResponse.orderSysId, ORDER_SIZE);

                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                memcpy(&rcmd, &cached, sizeof(pubsub::RCommand));
                rcmd.body.orderResponse.orderStatus = OS_CANCELLING;
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                rcmd.body.orderResponse.apiSourceEnum = AS_CANCEL_ORDER;
                rcmdInnerQueue.push(rcmd);

                if (cached.body.orderResponse.orderStatus == OS_CANCELED || cached.body.orderResponse.orderStatus == OS_FILLED || cached.body.orderResponse.orderStatus == OS_REJECTED) {
                    LOG_INFO("order clientOrderId: {} already finished, return oms result.", cached.body.orderResponse.clientOrderId);
                    rcmd.body.orderResponse.orderStatus = cached.body.orderResponse.orderStatus;
                    rcmd.body.orderResponse.errorId = OrderAlreadyFinishedError;
                    rcmd.body.orderResponse.updateTime = cached.body.orderResponse.updateTime;
                    rcmd.body.orderResponse.apiSourceEnum = AS_CANCEL_ORDER;
                    rcmdInnerQueue.push(rcmd);
                    return false;
                }
                return true;
            }

            // 非 OK 有两种, 动作**完全不同**, 所以不能压成一个 bool:
            //   BUSY      = 索引里**有**这张单, 只是这一瞬间拿不到一致快照 (写者正忙 /
            //               该 slot 正在被回收) → "存在但暂时读不到" → **转发**给交易所。
            //   NOT_FOUND = 真的没有这张单 → 回 OS_REJECTED, 不转发。
            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
            rcmd.body.orderResponse.exchangeTypeEnum = tcmd.body.cancelOrder.exchangeTypeEnum;
            rcmd.body.orderResponse.instTypeEnum = tcmd.body.cancelOrder.instTypeEnum;
            strncpy(rcmd.body.orderResponse.accountName, tcmd.body.cancelOrder.accountName, ACCOUNTID_SIZE);
            strncpy(rcmd.body.orderResponse.strategyId, tcmd.body.cancelOrder.strategyId, STRATEGYID_SIZE);
            strncpy(rcmd.body.orderResponse.instId, tcmd.body.cancelOrder.instId, INSTID_SIZE);
            rcmd.body.orderResponse.clientOrderId = tcmd.body.cancelOrder.clientOrderId;
            strncpy(rcmd.body.orderResponse.orderSysId, tcmd.body.cancelOrder.orderSysId, ORDER_SIZE);
            strncpy(rcmd.body.orderResponse.orderId, tcmd.body.cancelOrder.orderId, ORDER_SIZE);
            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            rcmd.body.orderResponse.apiSourceEnum = AS_CANCEL_ORDER;
            
            if (ls == oms::shm::OmsShmSegment::LookupStatus::BUSY) {
                // 限流: BUSY 在高压下会成片出现, 逐条打日志就是刷屏。
                const uint64_t n = ++shm_busy_count_;
                if (should_log_limited(n)) {
                    LOG_ERROR("oms lookup BUSY (writer busy / slot reclaiming), forward cancel anyway. count={} clientOrderId={} strategyId={}", n, tcmd.body.cancelOrder.clientOrderId, tcmd.body.cancelOrder.strategyId);
                }

                rcmd.body.orderResponse.orderStatus = OS_CANCELLING;;
                rcmdInnerQueue.push(rcmd);
                return true;
            } 
            else {
                LOG_ERROR("oms not found orderSysId, tcmd: {}", tcmd.getString());
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                rcmd.body.orderResponse.errorId = OMSOrderNotFoundError;
                rcmdInnerQueue.push(rcmd);
                return false;
            }
        }
        case pubsub::CMD_QUERY_ORDER: {
            // ★ 一次查询就拿到完整报单体 (与 CANCEL 同理)。
            pubsub::RCommand cached;
            const oms::shm::OmsShmSegment::LookupStatus ls = getOrderSysId(tcmd.body.queryOrder.clientOrderId, tcmd.body.queryOrder.strategyId, cached, tcmd.body.queryOrder.orderId);

            if (ls == oms::shm::OmsShmSegment::LookupStatus::OK) {
                strncpy(tcmd.body.queryOrder.orderSysId, cached.body.orderResponse.orderSysId, ORDER_SIZE);

                if (cached.body.orderResponse.orderStatus != OS_FILLED) { // 非成交，发到交易所查询
                    strncpy(tcmd.body.queryOrder.orderId, cached.body.orderResponse.orderId, INSTID_SIZE);
                    return true;
                }
                // filled 状态直接返回缓存结果
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                memcpy(&rcmd, &cached, sizeof(pubsub::RCommand));
                rcmd.body.orderResponse.apiSourceEnum = AS_QUERY_ORDER;
                rcmdInnerQueue.push(rcmd);
                return false;
            }

            // BUSY = 索引里**有**这张单, 只是这一瞬间读不到一致快照 → 转发到交易所查询,
            if (ls == oms::shm::OmsShmSegment::LookupStatus::BUSY) {
                const uint64_t n = ++shm_busy_count_;
                if (should_log_limited(n)) {
                    LOG_ERROR("oms lookup BUSY, query goes to exchange. count={} clientOrderId={} strategyId={}", n, tcmd.body.queryOrder.clientOrderId, tcmd.body.queryOrder.strategyId);
                }
                return true;
            }

            // NOT_FOUND —— 真的没有这张单
            LOG_ERROR("oms not found orderSysId, tcmd: {}", tcmd.getString());
            pubsub::RCommand rcmd;
            memset(&rcmd, 0, sizeof(pubsub::RCommand));
            rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
            rcmd.body.orderResponse.exchangeTypeEnum = tcmd.body.queryOrder.exchangeTypeEnum;
            rcmd.body.orderResponse.instTypeEnum = tcmd.body.queryOrder.instTypeEnum;
            strncpy(rcmd.body.orderResponse.accountName, tcmd.body.queryOrder.accountName, ACCOUNTID_SIZE);
            strncpy(rcmd.body.orderResponse.strategyId, tcmd.body.queryOrder.strategyId, STRATEGYID_SIZE);
            strncpy(rcmd.body.orderResponse.instId, tcmd.body.queryOrder.instId, INSTID_SIZE);
            rcmd.body.orderResponse.clientOrderId = tcmd.body.queryOrder.clientOrderId;
            strncpy(rcmd.body.orderResponse.orderSysId, tcmd.body.queryOrder.orderSysId, ORDER_SIZE);
            strncpy(rcmd.body.orderResponse.orderId, tcmd.body.queryOrder.orderId, ORDER_SIZE);
            rcmd.body.orderResponse.orderStatus = OS_REJECTED;
            rcmd.body.orderResponse.errorId = OMSOrderNotFoundError;
            rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
            rcmd.body.orderResponse.apiSourceEnum = AS_QUERY_ORDER;
            rcmdInnerQueue.push(rcmd);
            return false;
        }
        case pubsub::CMD_QUERY_ACCOUNT: {
            return true;
            break;
        }
        case pubsub::CMD_QUERY_BALANCE: {
            return true;
            break;
        }
        case pubsub::CMD_QUERY_POSITION: {
            return true;
            break;
        }
        default: {
            LOG_ERROR("unimplemented cmd type.");
            return false;
            break;
        }
    }
}

bool om::OrderManager::processRcmd(pubsub::RCommand& rcmd) {
    switch (rcmd.cmdTypeEnum) {
        case pubsub::CMD_RPT_NEW_ORDER: {
            return onOrderUpdate(rcmd);
            break;
        }
        case pubsub::CMD_RPT_CANCEL_ORDER: {
            return onOrderUpdate(rcmd);
            break;
        }
        case pubsub::CMD_RPT_QUERY_ORDER: {
            return onOrderUpdate(rcmd);
            break;
        }
        case pubsub::CMD_RPT_TOTAL_ACCOUNT: {
            return true;
            break;
        }
        case pubsub::CMD_RPT_BALANCE:
        case pubsub::CMD_RPT_POSITION: {
            return accountManager.processRcmd(rcmd);
            break;
        }
        case pubsub::CMD_RPT_ORDER_RESPONSE: {
            return onOrderUpdate(rcmd);
            break;
        }
        default: {
            LOG_ERROR("got an unimplement cmd type!");
            break;
        }
    }
}

// 把合并后的报单体写回 SHM。本函数只在"已经找到 slot"之后调用, 走的是 update_slot 分支 (不 alloc), 不会因环满失败。
static inline void oms_flush_slot(oms::shm::OmsShmWriter& shm, const pubsub::RCommand& op) {
    const uint32_t idx = shm.upsert(op);
    if (idx == oms::shm::kInvalidSlot) {
        // 只可能是 slot 在 lookup 与 upsert 之间被回收了 (FINISHED 超龄) —— 极罕见。
        LOG_ERROR("oms shm write-back failed, orderSysId: {}", op.body.orderResponse.orderSysId);
    }
}

bool om::OrderManager::onOrderUpdate(pubsub::RCommand& rcmd) {
    pubsub::RCommand cached;
    const oms::shm::OmsShmSegment::LookupStatus ls = omsShm_.lookup_by_orderSysId_ex(rcmd.body.orderResponse.orderSysId, cached);

    if (ls == oms::shm::OmsShmSegment::LookupStatus::BUSY) {
        // ★ B3: 索引里**有**这张单, 只是这一瞬间读不到一致快照 (写者正忙 / 正在回收)。
        //   当成"没有缓存的回报"处理会把一次成交/撤单回报丢掉, 且没有任何提示。
        const uint64_t n = ++shm_busy_count_;
        if (should_log_limited(n)) {
            LOG_ERROR("oms lookup BUSY, drop this report (exchange will resend / query will retry). count={} orderSysId: {}", n, rcmd.body.orderResponse.orderSysId);
        }
        return false;
    }

    if (ls == oms::shm::OmsShmSegment::LookupStatus::NOT_FOUND) {
        // 没有缓存的报单是否应该推给策略，比如adl类型的订单或者不是通过该系统下的单
        return false;
    }

    pubsub::RCommand& op = cached;

    bool statusAdvanced = false;
    bool volumeIncreased = false;

    if (rcmd.body.orderResponse.volumeTraded > op.body.orderResponse.volumeTotal + ZERO_NUM) {
        LOG_ERROR("Overfilled order! orderSysId: {} volumeTraded: {} volumeTotal: {}", op.body.orderResponse.orderSysId, rcmd.body.orderResponse.volumeTraded, op.body.orderResponse.volumeTotal);
    }

    if (rcmd.body.orderResponse.orderStatus == OS_CANCELED) {
        if (rcmd.body.orderResponse.apiSourceEnum == AS_CANCEL_ORDER && rcmd.body.orderResponse.exchangeTypeEnum == BINANCE) {
            if (op.body.orderResponse.volumeTraded < rcmd.body.orderResponse.volumeTraded) {
                return false;   // ← 此处尚未修改 op, 不需要写回
            }
        }    
    }

    if (rcmd.body.orderResponse.volumeTraded > op.body.orderResponse.volumeTraded) {
        op.body.orderResponse.tradeDiff = rcmd.body.orderResponse.volumeTraded - op.body.orderResponse.volumeTraded;
        op.body.orderResponse.fillPrice = 0.0;

        if (rcmd.body.orderResponse.instTypeEnum == C_SWAP || rcmd.body.orderResponse.instTypeEnum == C_FUTURES) {
            if (op.body.orderResponse.volumeTraded > ZERO_NUM) {
                op.body.orderResponse.fillPrice = op.body.orderResponse.tradeDiff / (rcmd.body.orderResponse.volumeTraded / rcmd.body.orderResponse.tradePrice - op.body.orderResponse.volumeTraded / op.body.orderResponse.tradePrice);
            }
            else {
                op.body.orderResponse.fillPrice = rcmd.body.orderResponse.tradePrice;
            }
        }
        else {
            op.body.orderResponse.fillPrice = (rcmd.body.orderResponse.volumeTraded * rcmd.body.orderResponse.tradePrice - op.body.orderResponse.volumeTraded * op.body.orderResponse.tradePrice) / op.body.orderResponse.tradeDiff;

        }

        op.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTraded;
        op.body.orderResponse.tradePrice = rcmd.body.orderResponse.tradePrice;
        op.body.orderResponse.updateTime = crypto::getCurrentTime();
        volumeIncreased = true;
    }
    else { // 如果没有新的成交，本次成交量和成交价设为0
        op.body.orderResponse.tradeDiff = 0.0;
        op.body.orderResponse.fillPrice = 0.0;
    }

    if (!crypto::isFinalOrderStatus(op.body.orderResponse.orderStatus)) {
        int oldP = crypto::getOrderStatusPriority(op.body.orderResponse.orderStatus);
        int newP = crypto::getOrderStatusPriority(rcmd.body.orderResponse.orderStatus);
        if (newP > oldP) {
            op.body.orderResponse.orderStatus = rcmd.body.orderResponse.orderStatus;
            op.body.orderResponse.updateTime = crypto::getCurrentTime();
            statusAdvanced = true;
        }
    }

    if (!crypto::str_cmp(rcmd.body.orderResponse.orderId, "")) {
        strncpy(op.body.orderResponse.orderId, rcmd.body.orderResponse.orderId, 64);
    }

    if (rcmd.body.orderResponse.orderStatus == OS_REJECTED) {
        op.body.orderResponse.errorId = rcmd.body.orderResponse.errorId;
        strncpy(op.body.orderResponse.originMsg, rcmd.body.orderResponse.originMsg, 128); 
    }

    if (rcmd.body.orderResponse.orderStatus == OS_FAILED) { // 撤单失败的状态要推送给策略
        memcpy(&rcmd, &op, sizeof(pubsub::RCommand));
        rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
        rcmd.body.orderResponse.orderStatus = OS_FAILED;
        rcmd.body.orderResponse.apiSourceEnum = AS_CANCEL_ORDER;
        oms_flush_slot(omsShm_, op);      // ← 上面已改 op, 必须写回
        return true;
    }

    bool isQueryOrder = rcmd.cmdTypeEnum == pubsub::CMD_RPT_QUERY_ORDER;

    if (!statusAdvanced && !volumeIncreased && !isQueryOrder) {
        // 走到这里 op 一定被改过 (tradeDiff/fillPrice 归零), 老代码是原地生效的 → 写回。
        oms_flush_slot(omsShm_, op);
        return false;
    }
    
    op.body.orderResponse.apiSourceEnum = rcmd.body.orderResponse.apiSourceEnum;
    op.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
    memcpy(&rcmd, &op, sizeof(pubsub::RCommand));
    oms_flush_slot(omsShm_, op);
    return true;
}

oms::shm::OmsShmSegment::LookupStatus om::OrderManager::getOrderSysId(const int64_t clientOrderId, const char* strategyId, pubsub::RCommand& out, const char* orderId) {
    const oms::shm::OmsShmSegment::LookupStatus ls = omsShm_.lookup_by_client_ex(strategyId, clientOrderId, out);
    if (ls == oms::shm::OmsShmSegment::LookupStatus::OK) {
        return ls;
    }

    // client 索引没命中: 再用交易所 orderId 兜底 (策略只给 orderId 的撤单/查询走这里)。
    //   ★ 只有 BUSY 才覆盖 ls —— NOT_FOUND 不该盖掉一个 BUSY:
    //     BUSY 表示"索引里有, 只是这一瞬间读不到", 那是**存在**, 不能降级成"不存在"。
    if (!crypto::str_cmp(orderId, "")) {
        const oms::shm::OmsShmSegment::LookupStatus ls2 = omsShm_.lookup_by_orderId_ex(orderId, out);
        if (ls2 == oms::shm::OmsShmSegment::LookupStatus::OK || ls2 == oms::shm::OmsShmSegment::LookupStatus::BUSY) {
            return ls2;
        }
    }

    return ls;
}

std::string om::OrderManager::getOrderSysId(ExchangeType exchangeTypeEnum, const char* strategyId) {
    switch (exchangeTypeEnum) {
        case BINANCE:
        case BITGET: {
            return fmt::format("x-{}{}", strategyId, crypto::get_rdtsc_timestamp());
        }
        case OKX: {
            return fmt::format("ok{}{}", strategyId, crypto::get_rdtsc_timestamp());
        }
        case GATEIO: {
            return fmt::format("t-{}{}", strategyId, crypto::get_rdtsc_timestamp());
        }
        case BYBIT: {
            return fmt::format("by{}{}", strategyId, crypto::get_rdtsc_timestamp());
        }
        case HTX: {
            return fmt::format("{}", crypto::get_rdtsc_timestamp());
        }
        default: {
            return fmt::format("{}{}", strategyId, crypto::get_rdtsc_timestamp());
        }
    }
}
