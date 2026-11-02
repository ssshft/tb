cmake_minimum_required(VERSION 3.1)
project(TB)

# 设置编译器 c++17 required
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# set(CMAKE_BUILD_TYPE "Debug")
set(CMAKE_BUILD_TYPE "Release")
set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -fPIC -Wall -g -ggdb")
set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -fPIC -DABI -DNDEBUG -DOPENBLAS  -O3 -Wall -flto -fuse-linker-plugin -minline-all-stringops -malign-double -ffast-math -march=native -ffunction-sections -fdata-sections -fmessage-length=0 -msse -msse2 -msse3 -funroll-loops")

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_SOURCE_DIR}/bin)


add_definitions("-D_TURN_OFF_PLATFORM_STRING")
# add_definitions("-DUSE_BINANCE_UNIFIED")

add_definitions(-DUSE_INFO_SHM)

# 延时统计（../include/hft_metrics.h）的编译期开关。**默认关**。
#   cmake .. && make            → 关（宏展开成空，热路径零残留、不建 metrics 文件）
#   cmake -DHFT_METRICS=ON ..   → 开
#
# tb 的报文速率是**每秒几次到几十次**（只有策略请求 + 交易所私有推送，src/ 下没有任何
# 定时线程），所以这里**全部用 HFT_SCOPE 全量计时，不采样**：
#   · 采样是为了保护"每秒几百万次"的热路径；tb 没有这种路径。
#   · 1/16 采样在 5 条/秒下 = 每 3.2 秒一个样本，10 s 窗口只有 3 个样本，p99 直接等于 max。
#   · 全量计时还省掉 ×16 的换算陷阱：count 列就是真实报文数。
# 代价：HFT_SCOPE 的边际开销是恒定的 +15~20 ns（空体才看到 36.8 ns），
# 对 µs 级的报文处理是 0.8%~3.8%；按 5 条/秒算折合单核 0.00002%。
option(HFT_METRICS "编译延时统计埋点（hft_metrics.h）" OFF)
if(HFT_METRICS)
    add_definitions(-DHFT_METRICS_ENABLED=1)
else()
    add_definitions(-DHFT_METRICS_ENABLED=0)
endif()


#include
set(INC_DIR              ${CMAKE_CURRENT_SOURCE_DIR}/include)
set(SYSTEM_INC_DIR          ../include)
set(SHMQUEUE_INCLUDE_DIR        ${SYSTEM_INC_DIR}/shmqueue)


aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src       DIR_MAIN_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/base     DIR_BASE_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/oms     DIR_OMS_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/operation  DIR_OPERATION_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/binance   DIR_BINANCE_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/Gateio    DIR_GATEIO_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/okx     DIR_OKX_SRCS)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src/bybit    DIR_BYBIT_SRCS)
aux_source_directory(${SHMQUEUE_INCLUDE_DIR}            DIR_SHMQUEU_SRCS)



include_directories(
        ${INC_DIR}
        ${SYSTEM_INC_DIR}
        ${SHMQUEUE_INCLUDE_DIR}
)

link_libraries(
        pthread
        boost_system 
        crypto 
        ssl  
        z 
        rt  
        dl 
        cpp_redis 
        tacopie 
        tbb 
        stdc++fs 
        fmt
        simdjson
)

add_executable(TB
        ${DIR_MAIN_SRCS}
        ${DIR_BASE_SRCS}
        ${DIR_OMS_SRCS}
        ${DIR_OPERATION_SRCS}
        ${DIR_BINANCE_SRCS}
        ${DIR_GATEIO_SRCS}
        ${DIR_OKX_SRCS}
        ${DIR_BYBIT_SRCS}
        # ${XT_SRCS}
        # ${KUCOIN_SRCS}
        # ${BYBIT_SRCS}
        ${DIR_SHMQUEU_SRCS}
)

add_executable(InputOrder test/InputOrder.cpp ${DIR_SHMQUEU_SRCS})
