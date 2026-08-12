#include "oms/OrderManager.h"


Tb2OmsRCommandInnerQueue tb2OmsRCommandInnerQueue;
RcmdInnerQueue rcmdInnerQueue;


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
    rcmd.body.orderResponse.apiSourceEnum = AS_ADD_NEW_ORDER; \
    rcmdInnerQueue.push(rcmd);\
    

om::OrderManager::OrderManager() {
    accountManager = am::AccountManager();
    // storage = tb_sqlite::SqliteStorage();
}

om::OrderManager::~OrderManager(){

}

void om::OrderManager::preStart(){
    // storage->create_oms_table();
    // load_data();
    accountManager.preStart();
}

void om::OrderManager::run(){

}

void om::OrderManager::preStop(){
    accountManager.preStop();
}

bool om::OrderManager::processTcmd(pubsub::TCommand& tcmd) {
    switch (tcmd.cmdTypeEnum) {
        case pubsub::CMD_NEW_ORDER: {
            if (tcmd.body.newOrder.clientOrderId == TESTCLIENTORDERID) {
                return true;
            }
            ADD_NEW_ORDER_2_ORDER_RESPONSE(tcmd)
            strncpy(tcmd.body.newOrder.orderSysId, rcmd.body.orderResponse.orderSysId, ORDER_SIZE);
            orderSysId2OrderResponseMap[rcmd.body.orderResponse.orderSysId] = rcmd;
            rcmdInnerQueue.push(rcmd);
            const std::string& clientOrderIdStra = fmt::format("{}{}", rcmd.body.orderResponse.strategyId, rcmd.body.orderResponse.clientOrderId);
            clientOrderId2OrderSysIdMap[clientOrderIdStra] = rcmd.body.orderResponse.orderSysId;
            return true;
            break;
        }
        case pubsub::CMD_CANCEL_ORDER: {
            const std::string& clientOrderIdStra = fmt::format("{}{}", tcmd.body.cancelOrder.strategyId, tcmd.body.cancelOrder.clientOrderId);
            bool found = getOrderSysId(tcmd.body.cancelOrder.clientOrderId, tcmd.body.cancelOrder.strategyId, tcmd.body.cancelOrder.orderSysId, tcmd.body.cancelOrder.orderId);
            if (found) {
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                auto iter = orderSysId2OrderResponseMap.find(tcmd.body.cancelOrder.orderSysId);
                if (iter != orderSysId2OrderResponseMap.end()) {
                    memcpy(&rcmd, &(iter->second), sizeof(pubsub::RCommand));
                    rcmd.body.orderResponse.orderStatus = OS_CANCELLING;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    rcmd.body.orderResponse.apiSourceEnum = AS_CANCEL_ORDER;
                    rcmdInnerQueue.push(rcmd);
                    
                    auto& r = iter->second;
                    if (r.body.orderResponse.orderStatus == OS_CANCELED || r.body.orderResponse.orderStatus == OS_FILLED || r.body.orderResponse.orderStatus == OS_REJECTED) {
                        LOG_INFO("order clientOrderId: {} already finished, return oms result.", r.body.orderResponse.clientOrderId);
                        rcmd.body.orderResponse.orderStatus = r.body.orderResponse.orderStatus;
                        rcmd.body.orderResponse.errorId = OrderAlreadyFinishedError;
                        rcmd.body.orderResponse.updateTime = r.body.orderResponse.updateTime;
                        rcmd.body.orderResponse.apiSourceEnum = AS_CANCEL_ORDER;
                        rcmdInnerQueue.push(rcmd);
                        return false;
                    }
                }
                else {
                    LOG_ERROR("oms not found order response, orderSysId: {}", tcmd.body.cancelOrder.orderSysId);
                    rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                    rcmd.body.orderResponse.exchangeTypeEnum = tcmd.body.cancelOrder.exchangeTypeEnum;
                    rcmd.body.orderResponse.instTypeEnum = tcmd.body.cancelOrder.instTypeEnum;
                    strncpy(rcmd.body.orderResponse.accountId, tcmd.body.cancelOrder.accountId, ACCOUNTID_SIZE);
                    strncpy(rcmd.body.orderResponse.strategyId, tcmd.body.cancelOrder.strategyId, STRATEGYID_SIZE);
                    strncpy(rcmd.body.orderResponse.instId, tcmd.body.cancelOrder.instId, INSTID_SIZE);
                    rcmd.body.orderResponse.clientOrderId = tcmd.body.cancelOrder.clientOrderId;
                    strncpy(rcmd.body.orderResponse.orderSysId, tcmd.body.cancelOrder.orderSysId, ORDER_SIZE);
                    strncpy(rcmd.body.orderResponse.orderId, tcmd.body.cancelOrder.orderId, ORDER_SIZE); \
                    rcmd.body.orderResponse.orderStatus = OS_CANCELLING;
                    rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                    rcmd.body.orderResponse.apiSourceEnum = AS_CANCEL_ORDER;
                    rcmdInnerQueue.push(rcmd);   
                }
                return true;
            }
            else {
                LOG_ERROR("oms not found orderSysId, tcmd: {}", tcmd.getString());
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));  
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                rcmd.body.orderResponse.exchangeTypeEnum = tcmd.body.cancelOrder.exchangeTypeEnum;
                rcmd.body.orderResponse.instTypeEnum = tcmd.body.cancelOrder.instTypeEnum;
                strncpy(rcmd.body.orderResponse.accountId, tcmd.body.cancelOrder.accountId, ACCOUNTID_SIZE);
                strncpy(rcmd.body.orderResponse.strategyId, tcmd.body.cancelOrder.strategyId, STRATEGYID_SIZE);
                strncpy(rcmd.body.orderResponse.instId, tcmd.body.cancelOrder.instId, INSTID_SIZE);
                rcmd.body.orderResponse.clientOrderId = tcmd.body.cancelOrder.clientOrderId;
                strncpy(rcmd.body.orderResponse.orderSysId, tcmd.body.cancelOrder.orderSysId, ORDER_SIZE);
                strncpy(rcmd.body.orderResponse.orderId, tcmd.body.cancelOrder.orderId, ORDER_SIZE); \
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                rcmd.body.orderResponse.errorId = OMSOrderNotFoundError;
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                rcmd.body.orderResponse.apiSourceEnum = AS_CANCEL_ORDER;
                rcmdInnerQueue.push(rcmd);
                return false;
            }
            break;
        }
        case pubsub::CMD_QUERY_ORDER: {
            const std::string& clientOrderIdStra = fmt::format("{}{}", tcmd.body.queryOrder.strategyId, tcmd.body.queryOrder.clientOrderId);
            bool found = getOrderSysId(tcmd.body.queryOrder.clientOrderId, tcmd.body.queryOrder.strategyId, tcmd.body.queryOrder.orderSysId, tcmd.body.queryOrder.orderId);
            if (found) {
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));
                auto iter = orderSysId2OrderResponseMap.find(tcmd.body.queryOrder.orderSysId);
                if (iter != orderSysId2OrderResponseMap.end()) {
                    if (iter->second.body.orderResponse.orderStatus != OS_FILLED) { // 非成交，发到交易所查询
                        strncpy(tcmd.body.queryOrder.orderId, iter->second.body.orderResponse.orderId, INSTID_SIZE);
                        return true;
                    }
                    else { // filled状态直接返回
                        pubsub::RCommand rcmd;
                        memset(&rcmd, 0, sizeof(pubsub::RCommand));
                        memcpy(&rcmd, &(iter->second), sizeof(pubsub::RCommand));
                        rcmd.body.orderResponse.apiSourceEnum = AS_QUERY_ORDER;
                        rcmdInnerQueue.push(rcmd);  
                        return false;
                    }
                }
                else { // 可以查询到orderSysId，但是查不到缓存的rcmd，则发到交易所查询
                   return true; 
                }
            }
            else {
                LOG_ERROR("oms not found orderSysId, tcmd: {}", tcmd.getString());
                pubsub::RCommand rcmd;
                memset(&rcmd, 0, sizeof(pubsub::RCommand));  
                rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
                rcmd.body.orderResponse.exchangeTypeEnum = tcmd.body.queryOrder.exchangeTypeEnum;
                rcmd.body.orderResponse.instTypeEnum = tcmd.body.queryOrder.instTypeEnum;
                strncpy(rcmd.body.orderResponse.accountId, tcmd.body.queryOrder.accountId, ACCOUNTID_SIZE);
                strncpy(rcmd.body.orderResponse.strategyId, tcmd.body.queryOrder.strategyId, STRATEGYID_SIZE);
                strncpy(rcmd.body.orderResponse.instId, tcmd.body.queryOrder.instId, INSTID_SIZE);
                rcmd.body.orderResponse.clientOrderId = tcmd.body.queryOrder.clientOrderId;
                strncpy(rcmd.body.orderResponse.orderSysId, tcmd.body.queryOrder.orderSysId, ORDER_SIZE);
                strncpy(rcmd.body.orderResponse.orderId, tcmd.body.queryOrder.orderId, ORDER_SIZE); \
                rcmd.body.orderResponse.orderStatus = OS_REJECTED;
                rcmd.body.orderResponse.errorId = OMSOrderNotFoundError;
                rcmd.body.orderResponse.updateTime = crypto::getCurrentTime();
                rcmd.body.orderResponse.apiSourceEnum = AS_QUERY_ORDER;
                rcmdInnerQueue.push(rcmd);
                return false;
            }

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

bool om::OrderManager::onOrderUpdate(pubsub::RCommand& rcmd) {
    auto iter = orderSysId2OrderResponseMap.find(rcmd.body.orderResponse.orderSysId);
    if (iter != orderSysId2OrderResponseMap.end()) {
        auto& op = iter->second;

        bool statusAdvanced = false;
        bool volumeIncreased = false;

        if (rcmd.body.orderResponse.volumeTraded > op.body.orderResponse.volumeTotal + ZERO_NUM) {
            LOG_ERROR("Overfilled order! orderSysId: {} volumeTraded: {} volumeTotal: {}", op.body.orderResponse.orderSysId, rcmd.body.orderResponse.volumeTraded, op.body.orderResponse.volumeTotal);
        }

        // binance得rest有成交得回报不推送(返回字段中去掉了成交均价)
        if (rcmd.body.orderResponse.orderStatus == OS_PARTFILLED || rcmd.body.orderResponse.orderStatus == OS_FILLED) { 
            if (rcmd.body.orderResponse.apiSourceEnum == AS_ADD_NEW_ORDER && rcmd.body.orderResponse.exchangeTypeEnum == BINANCE) {
                return false;
            }
        }

        if (rcmd.body.orderResponse.orderStatus == OS_CANCELED) {
            if (rcmd.body.orderResponse.apiSourceEnum == AS_CANCEL_ORDER && rcmd.body.orderResponse.exchangeTypeEnum == BINANCE) {
                if (op.body.orderResponse.volumeTraded < rcmd.body.orderResponse.volumeTraded) {
                    return false;
                }
            }    
        }

        if (rcmd.body.orderResponse.volumeTraded > op.body.orderResponse.volumeTraded) {
            op.body.orderResponse.tradeDiff = rcmd.body.orderResponse.volumeTraded - op.body.orderResponse.volumeTraded;
            op.body.orderResponse.volumeTraded = rcmd.body.orderResponse.volumeTraded;
            op.body.orderResponse.tradePrice = rcmd.body.orderResponse.tradePrice;
            op.body.orderResponse.fillPrice = rcmd.body.orderResponse.fillPrice;
            op.body.orderResponse.updateTime = crypto::getCurrentTime();

            volumeIncreased = true;
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
            strncpy(op.body.orderResponse.orderId, rcmd.body.orderResponse.orderId, ORDER_SIZE);
            orderId2OrderSysIdMap[op.body.orderResponse.orderId] = op.body.orderResponse.orderSysId;
        }

        if (rcmd.body.orderResponse.orderStatus == OS_REJECTED) {
            op.body.orderResponse.errorId = rcmd.body.orderResponse.errorId;
            strncpy(op.body.orderResponse.originMsg, rcmd.body.orderResponse.originMsg, ORIGINMSG_SIZE); 
        }

        if (rcmd.body.orderResponse.orderStatus == OS_FAILED) { // 撤单失败的状态要推送给策略
            memcpy(&rcmd, &op, sizeof(pubsub::RCommand));
            rcmd.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
            rcmd.body.orderResponse.orderStatus = OS_FAILED;
            return true;
        }

        bool isQueryOrder = rcmd.cmdTypeEnum == pubsub::CMD_QUERY_ORDER;

        if (!statusAdvanced && !volumeIncreased && !isQueryOrder) {
            return false;
        }
        
        op.body.orderResponse.apiSourceEnum = rcmd.body.orderResponse.apiSourceEnum;
        op.cmdTypeEnum = pubsub::CMD_RPT_ORDER_RESPONSE;
        memcpy(&rcmd, &op, sizeof(pubsub::RCommand));
        return true;
    }
    return false;
}

void om::OrderManager::load_data(){
    // auto now = crypto::getCurrentTime();
    // vector<om::OrderTrade> oList = storage->get_ordertrades_by_inserttime(now - 1 * OneHourMicroSeconds);
    // for(auto &ot : oList){
    //     string orderSysId = ot.orderSysId;
    //     // orderSysId2OrderTradeMap.insert(orderSysId, ot);
    //     orderSysId2OrderTradeMap[orderSysId] = ot;
    //     if(crypto::str_cmp(ot.orderId, "") == false){
    //         orderId2orderSysIdMap[ot.orderId] = orderSysId;
    //     }
    //     if(ot.clientOrderId != 0 && crypto::str_cmp(ot.orderSysId, "") == false){
    //         clientOrderId2orderSysIdMap[ot.clientOrderId] = orderSysId;
    //     }
    // }
    // LOG_INFO("oms loaded %zu orders from sqlite", oList.size());
}


void om::OrderManager::store_rcmd_data(vector<pubsub::RCommand> &rcmdVec){
    // auto startTime = crypto::getCurrentTimeMilli();
    // vector<tb_sqlite::OrderTradeSqlite> otVec;
    // for(auto &rcmd : rcmdVec){
    //     tb_sqlite::OrderTradeSqlite ot;
    //     auto &orderTrade = rcmd.body.orderTrade;
    //     ot.exchangeTypeEnum = ExchangeTypeEnum2StrMap[orderTrade.exchangeTypeEnum];
    //     ot.instTypeEnum = InstTypeEnum2StrMap[orderTrade.instTypeEnum];
    //     ot.accountId = orderTrade.accountId;
    //     ot.strategyId = orderTrade.strategyId;
    //     ot.instId = orderTrade.instId;
    //     ot.clientOrderId = orderTrade.clientOrderId;
    //     ot.orderSysId = orderTrade.orderSysId;
    //     ot.orderId = orderTrade.orderId;
    //     ot.strategyRef = orderTrade.strategyRef;

    //     ot.offsetFlag = OffsetFlagEnum2StrMap[orderTrade.offsetFlag];

    //     ot.direction = DirectionEnum2StrMap[orderTrade.direction];
    //     ot.orderType = OrderTypeEnum2StrMap[orderTrade.orderType];
    //     ot.orderStatus = OrderStatusEnum2StrMap[orderTrade.orderStatus];
    //     ot.volumeTotal = orderTrade.volumeTotal;
    //     ot.limitPrice = orderTrade.limitPrice;

    //     ot.reduceOnly = orderTrade.reduceOnly;
    //     ot.tradePrice = orderTrade.tradePrice;
    //     ot.volumeTraded = orderTrade.volumeTraded;
    //     ot.isMaker = orderTrade.isMaker;
    //     ot.tradedDiff = orderTrade.tradedDiff;

    //     ot.apiSourceEnum = ApiSourceEnum2StrMap[orderTrade.apiSourceEnum];
    //     ot.insertTime = orderTrade.insertTime;
    //     ot.updateTime = orderTrade.updateTime;
    //     ot.ErrorID = orderTrade.ErrorID;
    //     ot.originMsg = orderTrade.originMsg;

    //     ot.tsSent = orderTrade.tsSent;
    //     ot.tsNet = orderTrade.tsNet;

    //     otVec.push_back(ot);
    // }
    // auto endTime = crypto::getCurrentTimeMilli();
    // if(storage->replace_order_trades(otVec) == false){
    //     LOG_ERROR("Insert %ld OrderTrades into sqlite failed", otVec.size());
    // }
    // else{
    //     LOG_INFO("Insert %ld OrderTrades into sqlite successfully, cost:%.4f seconds",
    //         otVec.size(), (endTime-startTime)*0.001);
    // }
}


bool om::OrderManager::getOrderSysId(const int64_t clientOrderId, const char* strategyId, char* orderSysId, const char* orderId) {
    const std::string& clientOrderIdStra = fmt::format("{}{}", strategyId, clientOrderId);
    auto iter = clientOrderId2OrderSysIdMap.find(clientOrderIdStra);
    if (iter != clientOrderId2OrderSysIdMap.end()) {
        strncpy(orderSysId, iter->second.c_str(), INSTID_SIZE);
        return true;
    }

    if (!crypto::str_cmp(orderId, "")) {
        auto it = orderId2OrderSysIdMap.find(orderId);
        if (it != orderId2OrderSysIdMap.end()) {
           strncpy(orderSysId, it->second.c_str(), INSTID_SIZE);
           return true; 
        }
    }

    return false;
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