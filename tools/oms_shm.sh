#!/bin/bash
# =============================================================================
# oms_shm.sh — OMS SHM 一站式部署 / 运维脚本
#
# 用法:
#   ./oms_shm.sh build           # 编译 4 个工具 (query / bench / demo / test)
#   ./oms_shm.sh check           # 检查 /dev/shm 空间 + 权限
#   ./oms_shm.sh demo            # 跑一次完整功能演示
#   ./oms_shm.sh stats [shm]     # 打印当前 SHM stats
#   ./oms_shm.sh live  [shm]     # 列出活单
#   ./oms_shm.sh stale [shm]     # 列出卡单 (下次可能被强制回收)
#   ./oms_shm.sh bench           # 跑性能测试
#   ./oms_shm.sh test  [dir]     # 跑功能/边界/并发断言集 + 解析契约检查
#   ./oms_shm.sh reset  [shm]    # 危险: 清空 SHM (需要 CONFIRM=1)
#   ./oms_shm.sh watch  [shm]    # 持续监控 (每 5s 打印一次 stats)
#   ./oms_shm.sh doctor [shm]    # 一键健康检查, 有问题打红字
# =============================================================================

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INCLUDE_DIRS="-I${SCRIPT_DIR}/../../include -I${SCRIPT_DIR}/../include"
CXX="${CXX:-g++}"
CXXFLAGS="${CXXFLAGS:--std=c++17 -O2 -Wall -Wextra}"
DEFAULT_SHM="/dev/shm/tb_oms.dat"

RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; RESET=$'\033[0m'

log()  { echo -e "$*"; }
ok()   { echo -e "${GREEN}✓${RESET} $*"; }
warn() { echo -e "${YELLOW}⚠${RESET} $*"; }
err()  { echo -e "${RED}✗${RESET} $*" >&2; }

cmd_build() {
    log "building oms_query / oms_bench / oms_demo / oms_test ..."
    cd "$SCRIPT_DIR"
    # -pthread 的必要性只看**有没有构造 std::thread**, 与"头文件 include 了 <thread>"无关:
    #   nm -u 实测 pthread_create 引用数 = oms_query 0 / oms_bench 1 / oms_demo 1 / oms_test 1。
    #   (OmsShm.h 里那个 std::this_thread::yield() 位于 `#if defined(__x86_64__)` 的 #else 分支,
    #    x86-64 上根本没被编译 —— 四个 TU 的 this_thread 引用数都是 0。)
    # 老 glibc (< 2.34, 如 Ubuntu 20.04 / glibc 2.31) pthread 尚未并入 libc,
    # 缺 -pthread 就会 undefined reference to `pthread_create' (症状落在 oms_demo)。
    # 四行统一加: 无用无害, 且以后新增工具不会再踩一次。
    $CXX $CXXFLAGS -pthread $INCLUDE_DIRS oms_query.cpp -o oms_query \
        && ok "oms_query"  || { err "build oms_query failed"; return 1; }
    $CXX $CXXFLAGS -pthread $INCLUDE_DIRS oms_bench.cpp -o oms_bench \
        && ok "oms_bench"  || { err "build oms_bench failed"; return 1; }
    $CXX $CXXFLAGS -pthread $INCLUDE_DIRS oms_demo.cpp -o oms_demo \
        && ok "oms_demo"   || { err "build oms_demo failed"; return 1; }
    $CXX $CXXFLAGS -pthread $INCLUDE_DIRS oms_test.cpp -o oms_test \
        && ok "oms_test"   || { err "build oms_test failed"; return 1; }
}

cmd_check() {
    log "checking environment ..."
    # /dev/shm 存在且大小足够
    if [ ! -d /dev/shm ]; then
        err "/dev/shm not found — check tmpfs mount"; return 1
    fi
    local shm_size
    shm_size=$(df -m /dev/shm | tail -1 | awk '{print $2}')
    if [ "$shm_size" -lt 384 ]; then
        warn "/dev/shm only ${shm_size}MB — 100k slot shm = ~146MB (slot 100MB + 索引 48MB), consider raising"
    else
        ok "/dev/shm size = ${shm_size}MB (adequate)"
    fi
    # 是否 tmpfs (性能关键)
    local fstype
    fstype=$(stat -f -c %T /dev/shm 2>/dev/null || echo "?")
    if [ "$fstype" = "tmpfs" ]; then
        ok "/dev/shm is tmpfs"
    else
        warn "/dev/shm is $fstype (not tmpfs, expect slower access)"
    fi
    # 已有 shm 文件的权限 / 大小
    if [ -f "$DEFAULT_SHM" ]; then
        local size perms owner
        size=$(du -h "$DEFAULT_SHM" | cut -f1)
        perms=$(stat -c %a "$DEFAULT_SHM")
        owner=$(stat -c %U "$DEFAULT_SHM")
        ok "$DEFAULT_SHM exists  size=$size perms=$perms owner=$owner"
    else
        warn "$DEFAULT_SHM not created yet (tb hasn't started or different path)"
    fi
}

cmd_demo() {
    [ -x "$SCRIPT_DIR/oms_demo" ] || cmd_build
    "$SCRIPT_DIR/oms_demo" --shm=/dev/shm/tb_oms_demo.dat --reset
}

cmd_stats() {
    local shm="${1:-$DEFAULT_SHM}"
    [ -x "$SCRIPT_DIR/oms_query" ] || cmd_build
    "$SCRIPT_DIR/oms_query" --shm="$shm" --stats
}

cmd_live() {
    local shm="${1:-$DEFAULT_SHM}"
    [ -x "$SCRIPT_DIR/oms_query" ] || cmd_build
    "$SCRIPT_DIR/oms_query" --shm="$shm" --list-live
}

cmd_stale() {
    local shm="${1:-$DEFAULT_SHM}"
    [ -x "$SCRIPT_DIR/oms_query" ] || cmd_build
    "$SCRIPT_DIR/oms_query" --shm="$shm" --list-stale
}

cmd_bench() {
    [ -x "$SCRIPT_DIR/oms_bench" ] || cmd_build
    # --mixed-ttl-ms=0: mixed 相位默认把 FINISHED 的 TTL 设成 0 (立刻可回收), 否则
    #   "写入速率 × TTL" 远大于 capacity, 环 0.2 秒就满、之后每张单都失败并刷 ERROR。
    #   生产语义下可持续速率上限 = capacity / TTL, 131072 slot + 60s TTL 只有约 2185 单/秒。
    "$SCRIPT_DIR/oms_bench" --shm=/dev/shm/tb_bench.dat --capacity=131072 --iters=1000000 \
                            --readers=4 --mixed-ttl-ms=0 --reset
}

# 功能 / 边界 / 并发断言集 (oms_test) + 输出格式的解析契约检查。
#   解析契约必须在这里做: 它验的是 **oms_query 的 stdout** 与 **doctor 的解析式** 是否对得上,
#   是两个程序之间的接口, 放进 C++ 里测不到。
cmd_test() {
    local dir="${1:-/tmp/oms_test}"
    # ★ 测试命令**必须**用当前源码重新构建。只判"二进制是否存在"会让改了源码后
    #   跑到陈旧二进制 —— 测试全绿, 实际验的是旧行为, 这是最坏的一类误报。
    cmd_build || return 1
    mkdir -p "$dir"
    "$SCRIPT_DIR/oms_test" "$dir" || return 1

    log ""
    log "=== 解析契约: oms_query --stats 的数值行必须能被 doctor 的 grep 取到 ==="
    local fixture="$dir/parser_fixture.dat"
    if [ ! -f "$fixture" ]; then
        err "fixture $fixture 不存在 (oms_test 应该生成它)"; return 1
    fi
    local out
    out=$("$SCRIPT_DIR/oms_query" --shm="$fixture" --stats 2>&1) || { err "$out"; return 1; }

    local bad=0
    # 这些就是 doctor 会解析的 key。契约: 该行形如 `key[^:]*:<spaces><digits>` 且**以数字结尾**。
    for key in total_alloc_failures total_stale_live_reclaims total_alloc_slowpath \
               total_alloc_exhausted total_key_sync_failures total_alias_insert_failures \
               probe_max worst_live_run sustainable_insert_rate; do
        local line got
        line=$(echo "$out" | grep -E "^[[:space:]]*${key}([^:]*):" | head -1)
        if [ -z "$line" ]; then
            err "$key 没出现在 --stats 输出里 (doctor 会静默取到 0)"; bad=$((bad + 1)); continue
        fi
        if ! echo "$line" | grep -qE "^[[:space:]]*${key}[^:]*:[[:space:]]*[0-9]+$"; then
            err "$key 的行尾不是数字, doctor 取值会失败: $line"; bad=$((bad + 1)); continue
        fi
        # 用 doctor 完全一样的管道取值, 断言拿得到
        got=$(echo "$out" | grep -E "^[[:space:]]*${key}[^:]*:" | grep -oE '[0-9]+' | head -1)
        if [ -z "$got" ]; then
            err "$key doctor 管道取值为空: $line"; bad=$((bad + 1)); continue
        fi
        ok "$key → $got"
    done
    if [ "$bad" -eq 0 ]; then
        ok "解析契约: 全部 key 均可被 doctor 正确取值"
    else
        err "解析契约: $bad 个 key 有问题"; return 1
    fi
}

cmd_reset() {
    local shm="${1:-$DEFAULT_SHM}"
    if [ "$CONFIRM" != "1" ]; then
        err "DANGEROUS: rerun with CONFIRM=1 to wipe $shm"; return 1
    fi
    rm -f "$shm" && ok "removed $shm (tb 下次启动会重建, 活单状态丢失)"
}

cmd_watch() {
    local shm="${1:-$DEFAULT_SHM}"
    while true; do
        clear
        date
        cmd_stats "$shm" || true
        sleep 5
    done
}

cmd_doctor() {
    local shm="${1:-$DEFAULT_SHM}"
    local issues=0
    log "=== OMS SHM doctor: $shm ==="
    if [ ! -f "$shm" ]; then
        err "shm file missing"; return 1
    fi
    local out
    # doctor 之前**没有** auto-build, 别的子命令都有 —— 没编译过就只报一句
    # "No such file or directory" (被 2>&1 吞进 $out), 看不出是没编译。
    [ -x "$SCRIPT_DIR/oms_query" ] || cmd_build
    out=$("$SCRIPT_DIR/oms_query" --shm="$shm" --stats 2>&1) || { err "$out"; return 1; }
    echo "$out"
    log ""

    # 关键指标解析: 只认**以 key 开头**的那一行, 再取行内第一个数字。
    #   ① 不要用 awk '{print $NF}' —— 行尾的中文提示会被当成值, 导致
    #      [: integer expression expected 且检查静默失效 (P2-2 就是这个)。
    #   ② 也不要只 grep 裸 key —— 说明文字里出现同一个词就会误匹配到多行。
    #   ③ 累加必须写 `issues=$((issues + 1))` 而不是 `((issues++))`: 后者在
    #      issues=0 时返回 1, 在部分 bash 版本下会被 set -e 当成失败直接终止脚本。
    alloc_fail=$(echo "$out"     | grep -E '^[[:space:]]*total_alloc_failures[[:space:]]*:'      | grep -oE '[0-9]+' | head -1)
    stale_reclaims=$(echo "$out" | grep -E '^[[:space:]]*total_stale_live_reclaims[[:space:]]*:' | grep -oE '[0-9]+' | head -1)
    live_stale=$(echo "$out"     | grep "stale (卡单"                                            | grep -oE '[0-9]+' | head -1)
    worst_run=$(echo "$out"      | grep -E '^[[:space:]]*worst_live_run'                          | grep -oE '[0-9]+' | head -1)
    [ -z "$alloc_fail" ]     && alloc_fail=0
    [ -z "$stale_reclaims" ] && stale_reclaims=0
    [ -z "$live_stale" ]     && live_stale=0
    [ -z "$worst_run" ]      && worst_run=0

    # probe 上限单独一行 (数字结尾), 不靠解析 index_capacity 那行的括号内容
    probe_max=$(echo "$out" | grep -E '^[[:space:]]*probe_max[[:space:]]*:' | grep -oE '[0-9]+' | head -1)
    [ -z "$probe_max" ] && probe_max=0

    # ③ alloc 快/慢路径 + ② key 同步失败 + B4 别名失败 的专属计数 (v4/v4.1 新增)
    slowpath=$(echo "$out"      | grep -E '^[[:space:]]*total_alloc_slowpath[[:space:]]*:'    | grep -oE '[0-9]+' | head -1)
    exhausted=$(echo "$out"     | grep -E '^[[:space:]]*total_alloc_exhausted[[:space:]]*:'   | grep -oE '[0-9]+' | head -1)
    key_sync_fail=$(echo "$out" | grep -E '^[[:space:]]*total_key_sync_failures[[:space:]]*:' | grep -oE '[0-9]+' | head -1)
    alias_fail=$(echo "$out"    | grep -E '^[[:space:]]*total_alias_insert_failures[[:space:]]*:' | grep -oE '[0-9]+' | head -1)
    sustain=$(echo "$out"       | grep -E '^[[:space:]]*sustainable_insert_rate[[:space:]]*:' | grep -oE '[0-9]+' | head -1)
    [ -z "$slowpath" ]      && slowpath=0
    [ -z "$exhausted" ]     && exhausted=0
    [ -z "$key_sync_fail" ] && key_sync_fail=0
    [ -z "$alias_fail" ]    && alias_fail=0
    [ -z "$sustain" ]       && sustain=0

    if [ "$alloc_fail" -gt 0 ]; then
        err "alloc_failures = $alloc_fail  → 有单没写进 SHM! 看下面 alloc_exhausted / key_sync_failures 分辨是环满还是索引饱和"
        issues=$((issues + 1))
    else
        ok "alloc_failures = 0"
    fi
    # ③: 快路径落空但被全表扫描救回来 —— **单没丢**, 但余量已经在被消耗, 属于预警。
    if [ "$slowpath" -gt 0 ]; then
        warn "alloc_slowpath = $slowpath  → 快路径 128 步落空、被全表扫描救回 (单没丢); 在途单过于集中, 考虑加大 slot_capacity"
        issues=$((issues + 1))
    else
        ok "alloc_slowpath = 0"
    fi
    # ③: 快慢两遍都没找到可回收 slot —— 真·环满, 单确实丢了。
    if [ "$exhausted" -gt 0 ]; then
        err "alloc_exhausted = $exhausted  → 真·环满 (快慢两遍全表扫描都没找到可回收 slot), 有单丢了!"
        issues=$((issues + 1))
    else
        ok "alloc_exhausted = 0"
    fi
    # 容量给定后"能跑多快"就已经定死了 = slot_cap / min_reclaim_age。
    #   超过它必然写满丢单, **跟上层 finalize 及不及时无关** —— 把它打出来,
    #   便于和实际下单速率直接对比 (第 23 轮 bench 刷 ERROR 就是撞了这条线)。
    if [ "$sustain" -gt 0 ]; then
        log "  [i] 可持续写入速率上限 ≈ $sustain 单/秒 (= slot_cap / min_reclaim_age)"
        log "      实际下单速率超过它 → 环会写满并开始丢单; 加大 capacity 或调小 min_reclaim_age"
    else
        log "  [i] min_reclaim_age = 0 → 不限制可持续写入速率"
    fi
    # ②: key 变更时索引插入失败。已保持旧 key 不变 (副本与索引仍一致), 但订单没跟上变更。
    if [ "$key_sync_fail" -gt 0 ]; then
        err "key_sync_failures = $key_sync_fail  → key 变更时索引插入失败 (索引饱和); 已保持旧 key, 订单仍可查"
        issues=$((issues + 1))
    else
        ok "key_sync_failures = 0"
    fi
    # B4: 别名索引 (clientOrderId / orderId) 插入失败 —— 单在 SHM 里, 但按该别名查不到。
    if [ "$alias_fail" -gt 0 ]; then
        err "alias_insert_failures = $alias_fail  → 单在 SHM 里但别名索引写不进去 (策略按 clientOrderId 查活单会落空)"
        issues=$((issues + 1))
    else
        ok "alias_insert_failures = 0"
    fi
    # 探测余量 (B6): 环满后 empty 必然排干到 0, 插入能否成功只看最长 live 连续段。
    #   它 >= probe 上限 = 已经在丢单; 余量不足 1.5 倍 = 该重新评估 index_capacity 倍数了。
    if [ "$probe_max" -gt 0 ] && [ "$worst_run" -ge "$probe_max" ]; then
        err "worst_live_run = $worst_run >= probe_max = $probe_max  → 索引已饱和, 正在丢单 (看 alloc_failures)"
        issues=$((issues + 1))
    elif [ "$probe_max" -gt 0 ] && [ $((worst_run * 3)) -ge $((probe_max * 2)) ]; then
        warn "worst_live_run = $worst_run, 对 probe_max = $probe_max 余量不足 1.5 倍 → 考虑加大 index_capacity 倍数"
        issues=$((issues + 1))
    else
        ok "worst_live_run = $worst_run (probe_max = $probe_max)"
    fi
    if [ "$stale_reclaims" -gt 0 ]; then
        err "stale_live_reclaims = $stale_reclaims  → 有卡单被强制回收, 上层有 bug 导致订单不 finalize"
        issues=$((issues + 1))
    else
        ok "stale_live_reclaims = 0"
    fi
    if [ "$live_stale" -gt 0 ]; then
        warn "live_stale = $live_stale  → 目前有卡单 (24h+ 未更新), 下轮 alloc 会被回收"
        issues=$((issues + 1))
    else
        ok "live_stale = 0"
    fi

    log ""
    if [ "$issues" -eq 0 ]; then ok "healthy"; return 0
    else err "found $issues issue(s)"; return 1
    fi
}

case "${1:-help}" in
    build)  cmd_build ;;
    check)  cmd_check ;;
    demo)   cmd_demo ;;
    stats)  cmd_stats "$2" ;;
    live)   cmd_live "$2" ;;
    stale)  cmd_stale "$2" ;;
    bench)  cmd_bench ;;
    test)   cmd_test "$2" ;;
    reset)  cmd_reset "$2" ;;
    watch)  cmd_watch "$2" ;;
    doctor) cmd_doctor "$2" ;;
    *) sed -n '3,20p' "$0" | sed 's|^# ||;s|^#||' ; exit 1 ;;
esac
