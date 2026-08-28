#include "oms/AccountManager.h"


am::AccountManager::AccountManager() {
    // storage = new tb_sqlite::SqliteStorage();
}

am::AccountManager::~AccountManager() {
    // delete storage;
}

void am::AccountManager::preStart() {
    // tb_sqlite::SqliteStorage *object = tb_sqlite::SqliteStorage::get_instance();
    // storage->create_pms_table();
    // load_data();
}

void am::AccountManager::preStop() {
    // store_data();
    LOG_INFO("account manager exit");
}

bool am::AccountManager::processRcmd(pubsub::RCommand& rcmd) {
    if (rcmd.cmdTypeEnum == pubsub::CMD_RPT_BALANCE) {
        const std::string& key = crypto::get_account_balance_key(rcmd.body.balance.exchangeTypeEnum, rcmd.body.balance.instTypeEnum, rcmd.body.balance.strategyId, rcmd.body.balance.accountName, rcmd.body.balance.currency);
        auto found = balanceMap.find(key);
        if (found != balanceMap.end()) {
            pubsub::RCommand& oldRcmd = found->second;
            if (rcmd.body.balance.apiSourceEnum == AS_REST) {
                switch (rcmd.body.balance.exchangeTypeEnum) {
                    case BINANCE:
                    case OKX:
                    case BYBIT:
                    case HTX:
                    case BITGET: {
                        oldRcmd.body.balance.frozen = rcmd.body.balance.frozen;
                        oldRcmd.body.balance.total = rcmd.body.balance.total;
                        oldRcmd.body.balance.available = rcmd.body.balance.available;
                        oldRcmd.body.balance.borrowed = rcmd.body.balance.borrowed;  
                        return true;     
                    }
                    default: {
                        LOG_ERROR("balance update should not happend, {}", rcmd.getString());
                        oldRcmd.body.balance.frozen = rcmd.body.balance.frozen;
                        oldRcmd.body.balance.total = rcmd.body.balance.total;
                        oldRcmd.body.balance.available = rcmd.body.balance.available;
                        oldRcmd.body.balance.borrowed = rcmd.body.balance.borrowed;  
                        return true;
                    }
                }
            }
            else if (rcmd.body.balance.apiSourceEnum == AS_WEBSOCKET) {
                switch (rcmd.body.balance.exchangeTypeEnum) {
                    case BINANCE:
                    case OKX:
                    case BYBIT:
                    case HTX:
                    case BITGET: {
                        oldRcmd.body.balance.frozen = rcmd.body.balance.frozen;
                        oldRcmd.body.balance.total = rcmd.body.balance.total;
                        oldRcmd.body.balance.available = rcmd.body.balance.available;
                        oldRcmd.body.balance.borrowed = rcmd.body.balance.borrowed;  
                        return true;     
                    }
                    default: {
                        LOG_ERROR("balance update should not happend, {}", rcmd.getString());
                        oldRcmd.body.balance.frozen = rcmd.body.balance.frozen;
                        oldRcmd.body.balance.total = rcmd.body.balance.total;
                        oldRcmd.body.balance.available = rcmd.body.balance.available;
                        oldRcmd.body.balance.borrowed = rcmd.body.balance.borrowed;  
                        return true;
                    }
                }         
            }
            else {
                LOG_ERROR("need to be implemented! {}", rcmd.getString());
                return false;
            }
        }
        else {
            balanceMap[key] = rcmd;
            return true;
        }
    }
    else if (rcmd.cmdTypeEnum == pubsub::CMD_RPT_POSITION) {
        const std::string& key = crypto::get_account_position_key(rcmd.body.position.exchangeTypeEnum, rcmd.body.position.instTypeEnum, rcmd.body.position.strategyId, rcmd.body.position.accountName, rcmd.body.position.direction, rcmd.body.position.instId);
        if (rcmd.body.position.apiSourceEnum == AS_REST) {
            positionMap[key] = rcmd; // rest的返回字段是全的，直接覆盖（如不全，则需要单独处理）

            const std::string& positionKey = crypto::get_exch_position_key(rcmd.body.position.exchangeTypeEnum, rcmd.body.position.instTypeEnum, rcmd.body.position.accountName);
            auto foundPositionMap = currentPositionMap.find(positionKey);
            if (foundPositionMap != currentPositionMap.end()) {
                auto& positionMap = foundPositionMap->second;
                positionMap[key] = rcmd;
            }
            else {
                std::unordered_map<std::string, pubsub::RCommand> m;
                m[key] = rcmd;
                currentPositionMap[positionKey] = m;
            }

            auto foundTime = lastPubZeroPositionMap.find(positionKey);
            if (foundTime == lastPubZeroPositionMap.end()) {
                lastPubZeroPositionMap[positionKey] = crypto::getCurrentTimeSeconds();
            }

            if (rcmd.body.position.isLast) {
                int64_t currentTime = crypto::getCurrentTimeSeconds();
                int64_t& lastPubZeroPositionTime = lastPubZeroPositionMap[positionKey];
                auto& positionMap = currentPositionMap[positionKey];
                if (currentTime - lastPubZeroPositionTime > kPushZeroPositionSeconds) {
                    for (auto it = positionMap.begin(); it != positionMap.end(); ++it) {
                        auto& rc = it->second;
                        if (rc.body.position.exchangeTypeEnum == rcmd.body.position.exchangeTypeEnum && crypto::str_cmp(rc.body.position.accountName, rcmd.body.position.accountName)) {
                            Direction d = DT_LONG;
                            const std::string& keyLong = crypto::get_account_position_key(rc.body.position.exchangeTypeEnum, rc.body.position.instTypeEnum, rc.body.position.strategyId, rc.body.position.accountName, d, rc.body.position.instId);
                            d = DT_SHORT;
                            const std::string& keyShort = crypto::get_account_position_key(rc.body.position.exchangeTypeEnum, rc.body.position.instTypeEnum, rc.body.position.strategyId, rc.body.position.accountName, d, rc.body.position.instId);

                            auto foundLong = positionMap.find(keyLong);
                            auto foundShort = positionMap.find(keyShort);
                            if (foundLong == positionMap.end() && foundShort == positionMap.end()) {
                                if (fabs(rc.body.position.volume) > 0) {
                                    LOG_INFO("pub pubish 0, old rcmd: {}", rc.getString());
                                    rc.body.position.direction = DT_LONG;
                                    rc.body.position.volume = 0;
                                    rc.body.position.maintMargin = 0;
                                    rc.body.position.avgPrice = 0;
                                    rc.body.position.unrealizedPnl = 0;
                                    rc.body.position.markPrice = 0;
                                    rc.body.position.liquidPrice = 0;
                                    rc.body.position.isLast = true;
                                    rc.body.position.apiSourceEnum = AS_REST;
                                    LOG_INFO("pubish pos 0, zero rcmd: {}", rc.getString());
                                    rcmdInnerQueue.push(rc);
                                }
                            }
                        }
                    }

                    positionMap.clear();
                    lastPubZeroPositionTime = currentTime;
                }
            }
        }
        else if (rcmd.body.position.apiSourceEnum = AS_WEBSOCKET) {
            positionMap[key] = rcmd; // 暂时全覆盖，有需要单独处理
            // auto found = positionMap.find(key);
            // if (found != positionMap.end()) {

            // }
            // else {
            //     positionMap[key] = rcmd;
            // }
        }
        return true;
    }
    else {
        LOG_ERROR("need to be implemented!");
    }
    return false;
}


void am::AccountManager::load_data(){
    // auto bList = storage->get_balances();
    // for(auto& balance : bList) {
    //     // cout << "balance = " << tbSqlite.dump(balance) << endl;
    //     pubsub::RCommand rcmd;
    //     memset(&rcmd, 0, sizeof(pubsub::RCommand));
    //     rcmd.header.exchangeTypeEnum = ExchangeTypeStr2EnumMap[balance.exchangeTypeEnum];
    //     rcmd.header.instTypeEnum = InstTypeStr2EnumMap[balance.instTypeEnum];
    //     rcmd.header.cmdTime = balance.updateTime;
    //     strcpy(rcmd.header.accountName, balance.accountName.c_str());
    //     strcpy(rcmd.header.strategyId, balance.strategyId.c_str());
    //     strcpy(rcmd.body.balance.currency, balance.currency.c_str());

    //     rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_BALANCE;
    //     rcmd.body.balance.available = balance.available;
    //     rcmd.body.balance.total = balance.total;
    //     rcmd.body.balance.frozen = balance.frozen;
    //     rcmd.body.balance.unrealizedPnl = balance.unrealizedPnl;
    //     rcommandMap[balance.key] = rcmd;
    // }
    // auto pList = storage->get_positions();
    // for(auto& position : pList){
    //     pubsub::RCommand rcmd;
    //     memset(&rcmd, 0, sizeof(pubsub::RCommand));
    //     rcmd.header.exchangeTypeEnum = ExchangeTypeStr2EnumMap[position.exchangeTypeEnum];
    //     rcmd.header.instTypeEnum = InstTypeStr2EnumMap[position.instTypeEnum];
    //     rcmd.header.cmdTime = position.updateTime;
    //     strcpy(rcmd.header.accountName, position.accountName.c_str());
    //     strcpy(rcmd.header.strategyId, position.strategyId.c_str());

    //     strcpy(rcmd.body.position.instId, position.instId.c_str());
    //     rcmd.header.cmdTypeEnum = pubsub::CMD_RPT_POSITION;
    //     rcmd.body.position.volume = position.volume;
    //     rcmd.body.position.direction = DirectionStr2EnumMap[position.direction];
    //     rcmd.body.position.maintMargin = position.maintMargin;

    //     rcmd.body.position.avgPrice = position.avgPrice;
    //     rcmd.body.position.unrealizedPnl = position.unrealizedPnl;
    //     rcmd.body.position.markPrice = position.markPrice;
    //     rcmd.body.position.liquidPrice = position.liquidPrice;
    //     rcmd.body.position.adlQuantile = position.adlQuantile;
    //     // rcmd.body.position.apiSOurceEnum = position.apiSOurceEnum;
    //     rcommandMap[position.key] = rcmd;
    // }
}

void am::AccountManager::store_data(){
    // vector<tb_sqlite::BalanceSqlite> bVec;
    // vector<tb_sqlite::PositionSqlite> pVec;
    // for(auto &iter : rcommandMap){
    //     pubsub::RCommand &rcmd = iter.second;
    //     if(rcmd.header.cmdTypeEnum == pubsub::CMD_RPT_BALANCE){
    //         tb_sqlite::BalanceSqlite b;
    //         b.key = iter.first;
    //         b.exchangeTypeEnum = ExchangeTypeEnum2StrMap[rcmd.header.exchangeTypeEnum];
    //         b.instTypeEnum = InstTypeEnum2StrMap[rcmd.header.instTypeEnum];
    //         b.accountId = rcmd.header.accountId ;
    //         b.strategyId = rcmd.header.strategyId ;
    //         b.currency = rcmd.body.balance.currency;

    //         b.available = rcmd.body.balance.available;
    //         b.total = rcmd.body.balance.total;
    //         b.frozen = rcmd.body.balance.frozen;
    //         b.unrealizedPnl = rcmd.body.balance.unrealizedPnl;
    //         b.updateTime = rcmd.header.cmdTime;
    //         bVec.push_back(b);
    //     }
    //     else if(rcmd.header.cmdTypeEnum == pubsub::CMD_RPT_POSITION){
    //         tb_sqlite::PositionSqlite p;
    //         p.key = iter.first;
    //         p.exchangeTypeEnum = ExchangeTypeEnum2StrMap[rcmd.header.exchangeTypeEnum];
    //         p.instTypeEnum = InstTypeEnum2StrMap[rcmd.header.instTypeEnum];
    //         p.accountId = rcmd.header.accountId;
    //         p.strategyId = rcmd.header.strategyId;

    //         p.instId = rcmd.body.position.instId;
    //         p.direction = DirectionEnum2StrMap[rcmd.body.position.direction];
    //         p.volume = rcmd.body.position.volume;
    //         p.maintMargin = rcmd.body.position.maintMargin;
    //         p.avgPrice = rcmd.body.position.avgPrice;

    //         p.unrealizedPnl = rcmd.body.position.unrealizedPnl;
    //         p.markPrice = rcmd.body.position.markPrice;
    //         p.liquidPrice = rcmd.body.position.liquidPrice;
    //         p.adlQuantile = rcmd.body.position.adlQuantile;
    //         p.apiSourceEnum = ApiSourceEnum2StrMap[rcmd.body.position.apiSourceEnum];
    //         p.updateTime = rcmd.header.cmdTime;
    //         pVec.push_back(p);
    //     }
    //     else{
    //         LOG_ERROR("need to be implemented!");
    //     }
    // }
    
    // storage->replace_balances(bVec);
    // storage->replace_positions(pVec);
}

