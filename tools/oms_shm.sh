#!/bin/bash
# =============================================================================
# oms_shm.sh — OMS SHM 一站式部署 / 运维脚本
#
# 用法:
#   ./oms_shm.sh build           # 编译 3 个工具 (query / bench / demo)
#   ./oms_shm.sh check           # 检查 /dev/shm 空间 + 权限
#   ./oms_shm.sh demo            # 跑一次完整功能演示
#   ./oms_shm.sh stats [shm]     # 打印当前 SHM stats
#   ./oms_shm.sh live  [shm]     # 列出活单
#   ./oms_shm.sh stale [shm]     # 列出卡单 (下次可能被强制回收)
#   ./oms_shm.sh bench           # 跑性能测试
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
    log "building oms_query / oms_bench / oms_demo ..."
    cd "$SCRIPT_DIR"
    $CXX $CXXFLAGS $INCLUDE_DIRS oms_query.cpp -o oms_query \
        && ok "oms_query"  || { err "build oms_query failed"; return 1; }
    $CXX $CXXFLAGS $INCLUDE_DIRS -pthread oms_bench.cpp -o oms_bench \
        && ok "oms_bench"  || { err "build oms_bench failed"; return 1; }
    $CXX $CXXFLAGS $INCLUDE_DIRS oms_demo.cpp -o oms_demo \
        && ok "oms_demo"   || { err "build oms_demo failed"; return 1; }
}

cmd_check() {
    log "checking environment ..."
    # /dev/shm 存在且大小足够
    if [ ! -d /dev/shm ]; then
        err "/dev/shm not found — check tmpfs mount"; return 1
    fi
    local shm_size
    shm_size=$(df -m /dev/shm | tail -1 | awk '{print $2}')
    if [ "$shm_size" -lt 256 ]; then
        warn "/dev/shm only ${shm_size}MB — 100k slot shm = ~120MB, consider raising"
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
    "$SCRIPT_DIR/oms_bench" --shm=/dev/shm/tb_bench.dat --capacity=131072 --iters=1000000 --readers=4 --reset
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
    out=$("$SCRIPT_DIR/oms_query" --shm="$shm" --stats 2>&1) || { err "$out"; return 1; }
    echo "$out"
    log ""

    # 关键指标解析
    local alloc_fail stale_reclaims live_stale
    alloc_fail=$(echo "$out"    | grep total_alloc_failures       | awk '{print $NF}')
    stale_reclaims=$(echo "$out"| grep total_stale_live_reclaims  | awk '{print $NF}' | head -1)
    live_stale=$(echo "$out"    | grep "stale (卡单"              | awk '{print $NF}')
    [ -z "$alloc_fail" ]     && alloc_fail=0
    [ -z "$stale_reclaims" ] && stale_reclaims=0
    [ -z "$live_stale" ]     && live_stale=0

    if [ "$alloc_fail" -gt 0 ]; then
        err "alloc_failures = $alloc_fail  → RING EXHAUSTED, tb 无法新单! 立刻检查活单数 / 加 capacity"
        ((issues++))
    else
        ok "alloc_failures = 0"
    fi
    if [ "$stale_reclaims" -gt 0 ]; then
        err "stale_live_reclaims = $stale_reclaims  → 有卡单被强制回收, 上层有 bug 导致订单不 finalize"
        ((issues++))
    else
        ok "stale_live_reclaims = 0"
    fi
    if [ "$live_stale" -gt 0 ]; then
        warn "live_stale = $live_stale  → 目前有卡单 (24h+ 未更新), 下轮 alloc 会被回收"
        ((issues++))
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
    reset)  cmd_reset "$2" ;;
    watch)  cmd_watch "$2" ;;
    doctor) cmd_doctor "$2" ;;
    *) sed -n '3,20p' "$0" | sed 's|^# ||;s|^#||' ; exit 1 ;;
esac
