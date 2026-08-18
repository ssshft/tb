#pragma once
#include "data_struct.h"
#include "crypto_errors.h"
#include "string_util.h"
#include "precision_util.h"


namespace crypto {
    inline OrderStatus get_binance_orderstatus(InstType instTypeEnum, const rapidjson::Value& rawData) {
        if (instTypeEnum == SPOT || instTypeEnum == USDT_SWAP || instTypeEnum == USDT_FUTURES || instTypeEnum == C_SWAP || instTypeEnum == C_FUTURES) {
            if (rawData.HasMember("e")) {//ws
                const std::string& status = rawData["o"]["X"].GetString();//订单的当前状态
                if (crypto::str_cmp(status.c_str(), "NEW")) {//新订单
                    return OS_NEW;
                }
                else if (crypto::str_cmp(status.c_str(),"CANCELED")) {//CANCELED 订单被取消
                    return OS_CANCELED;
                }
                else if (crypto::str_cmp(status.c_str(),"REJECTED")) {//REJECTED 新订单被拒绝
                    return OS_REJECTED;
                }
                else if (crypto::str_cmp(status.c_str(),"PARTIALLY_FILLED")) {
                    return OS_PARTFILLED;
                }
                else if (crypto::str_cmp(status.c_str(),"FILLED")) {
                    return OS_FILLED;
                }
                else if (crypto::str_cmp(status.c_str(),"EXPIRED")) {//EXPIRED 订单失效(根据订单的Time In Force参数)
                    return OS_CANCELED;
                }
                else {
                    return OS_UNKNOWN;
                }
            }
            else{//rest
                const std::string &tif = rawData["timeInForce"].GetString();
                const string &type = rawData["type"].GetString();
                const string &status = rawData["status"].GetString();
                double volumeTraded = stod(rawData["executedQty"].GetString());
                double volumeTotal = stod(rawData["origQty"].GetString());
                if(crypto::str_cmp(tif.c_str(), "IOC") == true){//IOC
                    if(crypto::is_zeronum(volumeTraded) || volumeTraded < volumeTotal){
                        return OS_CANCELED;
                    }
                    else{
                        return OS_FILLED;
                    }
                }
                else{//(tif[0] == 'G')
                    if(crypto::is_zeronum(volumeTraded)){
                        return OS_NEW;
                    }
                    else if(volumeTraded < volumeTotal){
                        return OS_PARTFILLED;
                    }
                    else{
                        return OS_FILLED;
                    }
                }
            }
        }
        else{
            return OS_UNKNOWN;
        }
    }
    inline OrderStatus get_gateio_orderstatus(InstType instTypeEnum, const rapidjson::Value &rawData){
        if(instTypeEnum == SPOT){
            const string &status = rawData["status"].GetString();
            double size = stod(rawData["amount"].GetString());
            double volumeTotal = size > 0 ? size : -size;
            // rcmd.body.newOrder.limitPrice = stod(rawData["price"].GetString());
            // rcmd.body.newOrder.tradePrice = stod(rawData["fill_price"].GetString());//stod(v.at("fill_price").as_string().c_str());
            double left = stod(rawData["left"].GetString());//v.at("left").as_double();
            left = left > 0 ? left : -left;
            double volumeTraded = volumeTotal - left;
            if(crypto::str_cmp(status.c_str(), "open")) {
                //成交数量<挂单数量 && 成交数量大于0 --> 部分成交
                if( volumeTotal > volumeTraded && volumeTraded > ZERO_NUM ){
                    return OS_PARTFILLED;
                }
                else{
                    return OS_NEW;
                }
            }
            else if(crypto::str_cmp(status.c_str(), "cancelled")){
                return OS_CANCELED;
            } else {
                string finish_as = rawData["finish_as"].GetString();
                if(crypto::str_cmp(finish_as.c_str(), "filled")){
                    return OS_FILLED;
                }
            }
            return OS_UNKNOWN;
        }
        else if(instTypeEnum == USDT_SWAP || instTypeEnum == USDT_FUTURES){
            const string &status = rawData["status"].GetString();
            double size = stod(rawData["size"].GetString());
            double volumeTotal = size > 0 ? size : -size;
            double left = stod(rawData["left"].GetString());//v.at("left").as_double();
            left = left > 0 ? left : -left;
            double volumeTraded = volumeTotal - left;
            if(crypto::str_cmp(status.c_str(), "open")){
                //成交数量<挂单数量 && 成交数量大于0 --> 部分成交
                if( volumeTotal > volumeTraded && volumeTraded > ZERO_NUM ){
                    return OS_PARTFILLED;
                }
                else{
                    return OS_NEW;
                }
            }
            else{
                string finish_as = rawData["finish_as"].GetString();
                if(crypto::str_cmp(finish_as.c_str(), "filled")){
                    return OS_FILLED;
                }
                else if(crypto::str_cmp(finish_as.c_str(), "cancelled")
                || crypto::str_cmp(finish_as.c_str(), "liquidated")//- 强制平仓撤销
                || crypto::str_cmp(finish_as.c_str(), "ioc")//未立即完全成交，因为tif设置为ioc
                || crypto::str_cmp(finish_as.c_str(), "auto_deleveraged")//自动减仓撤销
                || crypto::str_cmp(finish_as.c_str(), "reduce_only")//: 增持仓位撤销，因为设置reduce_only或平仓
                ){
                    return OS_CANCELED;
                }
                else{
                    return OS_UNKNOWN;
                }
            }
        }
        else{
            return OS_UNKNOWN;
        }
        return OS_UNKNOWN;
    }


    inline OrderStatus get_gateio_orderstatus(const char* msg) noexcept{
        using crypto::has_str;
        constexpr std::string_view rejectedPatterns[] = {
            "PRICE_TOO_DEVIATED", "ORDER_POC_IMMEDIATE", "TOO_MANY_ORDERS",
            "INVALID_PARAM_VALUE", "INVALID_PROTOCOL", "INSUFFICIENT_AVAILABLE",
            "INVALID_ARGUMENT", "POC_FILL_IMMEDIATELY", "BALANCE_NOT_ENOUGH",
            "RISK_LIMIT_EXCEEDED", "TOO_MANY_REQUESTS", "ACCOUNT_NOT_ELIGIBLE",
            "FORBIDDEN", "BORROW_FAILED", "AUTO_BORROW_TOO_MUCH", "INVALID_KEY",
            "INCREASE_POSITION"
        };

        for (const auto& pat : rejectedPatterns) {
            if (has_str(msg, pat)) return OS_REJECTED;
        }

        constexpr std::string_view unknownPatterns[] = {
            "ORDER_NOT_FOUND",
            "NOT_FOUND",
            "SERVER_ERROR"
        };

        for (const auto& pat : unknownPatterns) {
            if (has_str(msg, pat)) return OS_UNKNOWN;
        }

        return OS_UNKNOWN;
    }


    /**
     * @brief 判断oms中订单状态颠倒，比如，oms中状态是成交，部分成交，但是推送过来的状态是NEW,
     * 这时候应该判断为ws推送落后于当前的oms，返回false
     *
     * @param ot
     * @param rcmd
     * @return true
     * @return false
     */
    inline bool is_order_invert(const om::OrderTrade &ot, const pubsub::RCommand &rcmd){
        if (rcmd.body.orderResponse.orderStatus <= OS_NEW && ot.orderStatus > OS_NEW){
            return true;
        }
        return false;
    }

    inline bool is_order_finished(const OrderStatus &orderStatus){
        if (orderStatus == OS_CANCELED || orderStatus == OS_FILLED || orderStatus == OS_REJECTED) {
            return true;
        }
        return false;
    }

    inline OrderType get_bybit_ordertype(const char *timeInForce, const char *orderType) {
        if (orderType[0] == 'M') {
            return OT_MARKET;
        }
        else if (orderType[0] == 'L') {
            if (timeInForce[0] == 'I' ) {//IOC - Immediate or Cancel 无法立即成交(吃单)的部分就撤销
                return OT_IOC;
            }
            else if (timeInForce[0] == 'F') {//Fill or Kill 无法全部立即成交就撤销
                return OT_FOK;
            }
            else if (timeInForce[0] == 'P') {
                return OT_POST_ONLY;
            }
            else {//GTC 一直有效至取消
                return OT_LIMIT;
            }
        }
        else {
            return OT_MIN;
        }
    }

    inline OrderType get_binance_ordertype(const char* timeInForce, const char* orderType) {
        if (orderType[0] == 'M') {
            return OT_MARKET;
        }
        else if (orderType[0] == 'L') {
            if (timeInForce[0] == 'I') {//IOC - Immediate or Cancel 无法立即成交(吃单)的部分就撤销
                return OT_IOC;
            }
            else if (timeInForce[0] == 'F') {//Fill or Kill 无法全部立即成交就撤销
                return OT_FOK;
            }
            else if (timeInForce[0] == 'P') {
                return OT_POST_ONLY;
            }
            else {
                return OT_LIMIT;
            }
        }
        else {
            return OT_MIN;
        }
    }

    inline OrderStatus get_binance_orderstatus(const std::string& status) {
        if (status[0] == 'N') {//NEW
            return OS_NEW;
        }
        else if (status[0] == 'P') {//PARTIALLY_FILLED
            return OS_PARTFILLED;
        }
        else if (status[0] == 'F') {//FILLED
            return OS_FILLED;
        }
        else if (status[0] == 'C') {//CANCELED
            return OS_CANCELED;
        }
        else if (status[0] == 'R') {//REJECTED
            return OS_REJECTED;
        }
        else if (status[0] == 'E') {//EXPIRED
            return OS_CANCELED;
        }
        else {
            return OS_UNKNOWN;
        }
    }
    //https://www.okx.com/docs-v5/zh/#error-code-rest-public
    inline int get_okx_errorid(int code){
        switch(code){
            case 51600://查询订单的状态不存在
            case 51400://撤单失败，订单不存在
            case 51405://撤单失败，您当前没有未成交的订单
            case 51603://查询订单不存在
                return OrderNotFoundError;
            case 51401://撤单失败，订单已撤销
            case 51402://撤单失败，订单已完成
                return OrderAlreadyFinishedError;
            case 51008://委托失败，账户 {0} 可用余额不足
                return CapitalNotEnoughError;
            case 51113://TOO_MANY_ORDERS
                return TooManyOrdersError;
            default:
                return code;
        }
    }

    inline int get_binance_errorid(int code) {
        switch (code) {
            case -2011:
            case -2013:
                return OrderNotFoundError;
            case -2018:
                return PositionNotEnoughError;
            case -2019:
            case -2027:
                return CapitalNotEnoughError;
            case -1003:
            case -1015:
            case -5041:
                return TooManyOrdersError;
            case -1008:
                return OverLoadedError;
            case -4016:
                return HitUpperLowerPriceError;
            case -2010:
            case -5021:
            case -5022:
                return OrderPOCImmediateError;
            case -4164:
                return NotionalTooSmallError;
            case -5027:
                return NoNeedToModifyOrderError;
            default:
                return code;
        }

    }

    inline int get_gateio_errorid(const char *msg) {
        if(crypto::has_str(msg, "ORDER_POC_IMMEDIATE")) {
            return OrderPOCImmediateError;
        }
        else if(crypto::has_str(msg, "TOO_MANY_ORDERS") ) {
            return TooManyOrdersError;
        }
        else if(crypto::has_str(msg, "ORDER_NOT_FOUND")
        || crypto::has_str(msg, "NOT_FOUND")) {
            return OrderNotFoundError;
        }
        else if(crypto::has_str(msg, "INVALID_PARAM_VALUE")
        || crypto::has_str(msg, "INVALID_PROTOCOL")
        || crypto::has_str(msg, "INVALID_ARGUMENT")) {
            return OrderParamError;
        }
        else if(crypto::has_str(msg, "SERVER_ERROR")) {
            return NetworkError;
        }//PRICE_TOO_DEVIATED
        else{
            return UnknownError;
        }
    }

    inline int get_bybit_errorid(int code) {
        switch(code){
            case 110008:
            case 110001:
                return OrderNotFoundError;
            case 110014:
                return PositionNotEnoughError;
            case 110004:
            case 110045:
                return CapitalNotEnoughError;
            case 10018:
            case 10009://TOO_MANY_REQUESTS
            case 10006://TOO_MANY_ORDERS
                return TooManyOrdersError;
            case 110003:
                return OrderPOCImmediateError;
            default:
                return code;
        }
    }

    inline int getOrderStatusPriority(OrderStatus s) {
        switch (s) {
            case OS_PENDING_NEW: return 1;
            case OS_UNKNOWN: return 2;
            case OS_NEW: return 3;
            case OS_PARTFILLED: return 4;
            case OS_FILLED: return 5;
            case OS_CANCELED: return 6;
            case OS_REJECTED: return 7;
            default: return 0;
        }
    }

    inline bool isFinalOrderStatus(OrderStatus s) {
        return s == OS_FILLED || s == OS_CANCELED || s == OS_REJECTED;
    }
}

