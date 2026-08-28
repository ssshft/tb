#pragma once

#include "shm_global.h"


constexpr auto ORDER_REJECTED_TIME_OUT = 30 * 1e6;

#define PUSH_RCMD(rcmd) \
    tb2OmsRCommandInnerQueue.push(rcmd); \
   


#define ADD_NEW_ORDER_TCMD_2_RCMD(tcmd) \
    pubsub::RCommand rcmd; \
    memset(&rcmd, 0, sizeof(pubsub::RCommand)); \
    rcmd.cmdTypeEnum = pubsub::CMD_RPT_NEW_ORDER; \
    rcmd.body.orderResponse.exchangeTypeEnum = tcmd.body.newOrder.exchangeTypeEnum; \
    rcmd.body.orderResponse.instTypeEnum = tcmd.body.newOrder.instTypeEnum; \
    strncpy(rcmd.body.orderResponse.accountName, tcmd.body.newOrder.accountName, 32); \
    strncpy(rcmd.body.orderResponse.strategyId, tcmd.body.newOrder.strategyId, 32); \
    strncpy(rcmd.body.orderResponse.instId, tcmd.body.newOrder.instId, 32); \
    rcmd.body.orderResponse.clientOrderId = tcmd.body.newOrder.clientOrderId; \
    strncpy(rcmd.body.orderResponse.orderSysId, tcmd.body.newOrder.orderSysId, 64); \
    strncpy(rcmd.body.orderResponse.strategyRef, tcmd.body.newOrder.strategyRef, 64); \
    rcmd.body.orderResponse.offsetFlag = tcmd.body.newOrder.offsetFlag; \
    rcmd.body.orderResponse.direction = tcmd.body.newOrder.direction; \
    rcmd.body.orderResponse.orderType = tcmd.body.newOrder.orderType; \
    rcmd.body.orderResponse.volumeTotal = tcmd.body.newOrder.volumeTotal; \
    rcmd.body.orderResponse.limitPrice = tcmd.body.newOrder.limitPrice; \
    rcmd.body.orderResponse.reduceOnly = tcmd.body.newOrder.reduceOnly; \
    rcmd.body.orderResponse.apiSourceEnum = AS_ADD_NEW_ORDER; \


#define CANCEL_ORDER_TCMD_2_RCMD(tcmd) \
    pubsub::RCommand rcmd; \
    memset(&rcmd, 0, sizeof(pubsub::RCommand)); \
    rcmd.cmdTypeEnum = pubsub::CMD_RPT_CANCEL_ORDER; \
    rcmd.body.orderResponse.exchangeTypeEnum = tcmd.body.cancelOrder.exchangeTypeEnum; \
    rcmd.body.orderResponse.instTypeEnum = tcmd.body.cancelOrder.instTypeEnum; \
    strncpy(rcmd.body.orderResponse.accountName, tcmd.body.cancelOrder.accountName, 32); \
    strncpy(rcmd.body.orderResponse.strategyId, tcmd.body.cancelOrder.strategyId, 32); \
    strncpy(rcmd.body.orderResponse.instId, tcmd.body.cancelOrder.instId, 32); \
    rcmd.body.orderResponse.clientOrderId = tcmd.body.cancelOrder.clientOrderId; \
    strncpy(rcmd.body.orderResponse.orderSysId, tcmd.body.cancelOrder.orderSysId, 64); \
    strncpy(rcmd.body.orderResponse.orderId, tcmd.body.cancelOrder.orderId, 64); \
    rcmd.body.orderResponse.apiSourceEnum = AS_CANCEL_ORDER; \


#define QUERY_ORDER_TCMD_2_RCMD(tcmd) \
    pubsub::RCommand rcmd; \
    memset(&rcmd, 0, sizeof(pubsub::RCommand)); \
    rcmd.cmdTypeEnum = pubsub::CMD_RPT_QUERY_ORDER; \
    rcmd.body.orderResponse.exchangeTypeEnum = tcmd.body.queryOrder.exchangeTypeEnum; \
    rcmd.body.orderResponse.instTypeEnum = tcmd.body.queryOrder.instTypeEnum; \
    strncpy(rcmd.body.orderResponse.accountName, tcmd.body.queryOrder.accountName, 32); \
    strncpy(rcmd.body.orderResponse.strategyId, tcmd.body.queryOrder.strategyId, 32); \
    strncpy(rcmd.body.orderResponse.instId, tcmd.body.queryOrder.instId, 32); \
    rcmd.body.orderResponse.clientOrderId = tcmd.body.queryOrder.clientOrderId; \
    strncpy(rcmd.body.orderResponse.orderSysId, tcmd.body.queryOrder.orderSysId, 64); \
    strncpy(rcmd.body.orderResponse.orderId, tcmd.body.queryOrder.orderId, 64); \
    rcmd.body.orderResponse.apiSourceEnum = AS_QUERY_ORDER; \


extern Tb2OmsRCommandInnerQueue tb2OmsRCommandInnerQueue;
extern RcmdInnerQueue rcmdInnerQueue;