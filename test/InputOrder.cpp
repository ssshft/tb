#include <iostream>
#include <csignal>
#include "log_engine.h"
#include "program_util.h"
#include "config.h"
#include "command_helper.h"
#include "crypto_exception.h"
#include "data_struct.h"
#include "key_util.h"
#include "pubsub_protocol.h"
#include "pubsub/pubsub.h"
#include "shm_global.h"
#include "concurrent_queue.h"


static void usage(void){
    fprintf(stderr, "\nusage:\n");
    fprintf(stderr, "./InputOrder json_config_file \n");
    fprintf(stderr, "\n");
    exit(-1);
}

int main(int argc, char *argv[]){
    if ( argc != 2 ){
        usage();
    }


    string tradeConfigStr = crypto::read_file("/inc/trade_config.json");
    rapidjson::Document d;
    rapidjson::Value &tradeConfig = d.Parse<rapidjson::kParseNumbersAsStringsFlag>(tradeConfigStr.c_str());

    int utrade2TbTCommandSHM = std::stoi(tradeConfig["OMS"]["Utrade2TbTCommandSHM"].GetString());
    std::string tb2UtradeRCommandSHM = tradeConfig["OMS"]["Tb2UtradeRCommandSHM"].GetString();

    om::TradeClient* tradeClient = new om::TradeClient(utrade2TbTCommandSHM);
    std::shared_ptr<pubsub::SPMCSubscriber<pubsub::RCommand>> rcmdQueue = std::make_shared<pubsub::SPMCSubscriber<pubsub::RCommand>>(tb2UtradeRCommandSHM.c_str());


    std::thread t([&]() {
        pubsub::RCommand rcmd;
        pubsub::Position position;
        pubsub::Balance balance;
        pubsub::OrderResponse orderResponse;
        pubsub::TotalAccount totalAccount;

        while (1) {
            if (rcmdQueue->pop(rcmd)) {
                if (crypto::convert_rcmd_2_ordertrade(rcmd, orderResponse)) {
                    std::cout << orderResponse.getString() << std::endl;
                }
                else if (crypto::convert_rcmd_2_balance(rcmd, balance)) {
                    std::cout << balance.getString() << std::endl;
                }
                else if (crypto::convert_rcmd_2_position(rcmd, position)) {
                    std::cout << position.getString() << std::endl;
                } else if (crypto::convert_rcmd_2_total_account(rcmd, totalAccount)) {
                    std::cout << totalAccount.getString() << std::endl;
                }
                else {
                    std::cout << "it should not happen here, please contact your developer!" << std::endl;
                }
            }
        }
    });
    t.detach();


    sleep(1);

    Config* config = Config::instance();
    config->load(argv[1]);
    string orderStr = config->get_document_str();

    rapidjson::Document d1;
    rapidjson::Value &rawData = d1.Parse<rapidjson::kParseNumbersAsStringsFlag>(orderStr.c_str());

    std::string type ;
    if(rawData.HasMember("type")) {
        type = rawData["type"].GetString();
    }
    else{
        cryptothrow("not found input order type", -1);
    }

    cout << orderStr << endl;

    if(crypto::str_cmp(type.c_str(), "add_new_order")){
        cout <<" : will add_new_order" << endl;
        string exchangeType = rawData["exchangeType"].GetString();
        string instType = rawData["instType"].GetString();
        string strategyId = rawData["strategyId"].GetString();
        string instId = rawData["instId"].GetString();
        string offsetFlag = rawData["offsetFlag"].GetString();
        string direction = rawData["direction"].GetString();
        string orderType = rawData["orderType"].GetString();
        double price = stod(rawData["price"].GetString());
        double volume = stod(rawData["volume"].GetString());
        int64_t clientOrderId = stol(rawData["clientOrderId"].GetString());
        bool reduceOnly = false;
        string strategyRef{""};
        if(rawData.HasMember("strategyRef")){
            strategyRef = rawData["strategyRef"].GetString();
        }
        if(rawData.HasMember("reduceOnly")){
            reduceOnly = rawData["reduceOnly"].GetBool();
        }

        tradeClient->add_new_order(ExchangeTypeStr2EnumMap[exchangeType] ,
                                InstTypeStr2EnumMap[instType] ,
                                strategyId.c_str(),
                                instId.c_str(),
                                OffsetFlagStr2EnumMap[offsetFlag] ,
                                DirectionStr2EnumMap[direction],
                                OrderTypeStr2EnumMap[orderType],
                                price,volume, clientOrderId,
                                reduceOnly, strategyRef.c_str());
  
    }
    else if(crypto::str_cmp(type.c_str(), "cancel_order")){
        cout << " will cancel_order" << endl;
        string exchangeType = rawData["exchangeType"].GetString();
        string instType = rawData["instType"].GetString();
        string strategyId = rawData["strategyId"].GetString();
        string instId = rawData["instId"].GetString();
        string orderId;
        int64_t clientOrderId ;
        if(rawData.HasMember("orderId")){
            orderId = rawData["orderId"].GetString();
            tradeClient->cancel_order(ExchangeTypeStr2EnumMap[exchangeType],
                                        InstTypeStr2EnumMap[instType],
                                        strategyId.c_str(),
                                        instId.c_str(),
                                        orderId.c_str(),0);

        }
        else if(rawData.HasMember("clientOrderId")){
            clientOrderId = stol(rawData["clientOrderId"].GetString());
            tradeClient->cancel_order(ExchangeTypeStr2EnumMap[exchangeType] ,
                                          InstTypeStr2EnumMap[instType] ,
                                          strategyId.c_str(),
                                          instId.c_str(), "", clientOrderId);
        }
        else{
            cryptothrow("cancel order need orderId or clientOrderId", -1);
        }
    }
    else if(crypto::str_cmp(type.c_str(), "query_order")){
        cout << "will query_order" << endl;
        string exchangeType = rawData["exchangeType"].GetString();
        string instType = rawData["instType"].GetString();
        string strategyId = rawData["strategyId"].GetString();
        string instId = rawData["instId"].GetString();
        string orderId;
        int64_t clientOrderId ;
        if(rawData.HasMember("orderId")){
            orderId = rawData["orderId"].GetString();
            tradeClient->query_order(
                ExchangeTypeStr2EnumMap[exchangeType],
                InstTypeStr2EnumMap[instType], strategyId.c_str(),
                instId.c_str(), orderId.c_str(), 0);
        }
        else if(rawData.HasMember("clientOrderId")){
            clientOrderId = stol(rawData["clientOrderId"].GetString());
            tradeClient->query_order(ExchangeTypeStr2EnumMap[exchangeType],
                InstTypeStr2EnumMap[instType], strategyId.c_str(),
                instId.c_str(), "", clientOrderId);
        }
        else{
            cryptothrow("query_order order need orderId or clientOrderId", -1);
        }
    }
    else if(crypto::str_cmp(type.c_str(), "query_account")){
        cout << "will query_account" << endl;
        string exchangeType = rawData["exchangeType"].GetString();
        string instType = rawData["instType"].GetString();
        string strategyId = rawData["strategyId"].GetString();

        tradeClient->query_account(ExchangeTypeStr2EnumMap[exchangeType],
            InstTypeStr2EnumMap[instType], strategyId.c_str());
    }
    else if(crypto::str_cmp(type.c_str(), "query_position")){
        cout << "will query_position" << endl;
        string exchangeType = rawData["exchangeType"].GetString();
        string instType = rawData["instType"].GetString();
        string strategyId = rawData["strategyId"].GetString();
        string instId = rawData["instId"].GetString();
        tradeClient->query_position(ExchangeTypeStr2EnumMap[exchangeType],
            InstTypeStr2EnumMap[instType], strategyId.c_str(), instId.c_str());
    }
    else if(crypto::str_cmp(type.c_str(), "query_balance")){
        cout << "will query_balance" << endl;
        string exchangeType = rawData["exchangeType"].GetString();
        string instType = rawData["instType"].GetString();
        string strategyId = rawData["strategyId"].GetString();
        string currency = rawData["currency"].GetString();
        tradeClient->query_balance(ExchangeTypeStr2EnumMap[exchangeType],
            InstTypeStr2EnumMap[instType], strategyId.c_str(), currency.c_str());
    }
    else {
        cryptothrow("not support your input order type except(add_new_order, cancel_order, query_order)", -1);
    }
    while(1){
        sleep(100);
    }

    return 0;
}
