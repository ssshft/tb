#!/usr/bin/env bash
# =============================================================================
# protocol_layout.sh —— 量 pubsub_protocol.h 的真实内存布局 (本机编不了 tb 时的办法)
#
#   为什么需要: 判断"能不能往 RCommand / OrderResponse 里加字段"这类问题, 靠人肉数
#   字段 + 猜对齐**一定会错**。而本机 (macOS/arm64) 缺 fmt / rapidjson, 直接 include
#   真头文件会失败。
#
#   办法: 用**最小的 permissive 桩**顶掉缺失的第三方头 (fmt::format 变参模板 +
#   rapidjson 的 StringBuffer/Writer), 然后 include **真头文件** 量 sizeof/offsetof。
#
#   ★ 桩的合法性: fmt/rapidjson 只在**内联函数体**里被用到, 不参与任何 struct 的
#     字段类型 ⇒ 不会改变布局。这一点不是"我觉得", 而是可以**交叉验证**的:
#     OmsShm.h 的注释独立写着 `sizeof(pubsub::RCommand)` = 536, 脚本会断言它。
#     两边对上 ⇒ 桩没影响布局; 对不上 ⇒ 桩或头文件有问题, 脚本直接报错。
#
#   两个探针:
#     probe     —— pubsub_protocol.h (OrderResponse / RCommand / union 成员大小)
#     omsprobe  —— oms/OmsShm.h     (OmsSlot / OmsShmHeader / _pad1 余量)
#                   ★ 关键: `_pad1` 是**算出来**的 ⇒ RCommand 变大时 slot 仍是 1024、
#                     **段大小不变** ⇒ 旧段只能靠 kVersion 拦住 (见脚本内注释)
#
#   用法:  bash tb/tools/protocol_layout.sh
#   非生产代码, 不入 CMake, **不需要可以直接删**。
# =============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
INC="$REPO_ROOT/include"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

mkdir -p "$WORK/stub/fmt" "$WORK/stub/rapidjson"

cat > "$WORK/stub/fmt/format.h" <<'EOF'
#pragma once
#include <string>
namespace fmt {
template <typename... A> inline std::string format(const char*, A&&...) { return std::string(); }
}
EOF

cat > "$WORK/stub/rapidjson/document.h" <<'EOF'
#ifndef RAPIDJSON_STUB_H
#define RAPIDJSON_STUB_H
#include <string>
#include <cstddef>
namespace rapidjson {
struct StringBuffer {
    const char* GetString() const { return ""; }
    size_t GetSize() const { return 0; }
};
template <typename B>
struct Writer {
    explicit Writer(B&) {}
    template <typename... A> bool StartObject(A&&...) { return true; }
    template <typename... A> bool EndObject(A&&...)   { return true; }
    template <typename... A> bool StartArray(A&&...)  { return true; }
    template <typename... A> bool EndArray(A&&...)    { return true; }
    template <typename... A> bool Key(A&&...)         { return true; }
    template <typename... A> bool String(A&&...)      { return true; }
    template <typename... A> bool Int(A&&...)         { return true; }
    template <typename... A> bool Uint(A&&...)        { return true; }
    template <typename... A> bool Int64(A&&...)       { return true; }
    template <typename... A> bool Uint64(A&&...)      { return true; }
    template <typename... A> bool Double(A&&...)      { return true; }
    template <typename... A> bool Bool(A&&...)        { return true; }
    template <typename... A> bool Null(A&&...)        { return true; }
};
struct Value { };
struct Document : Value { };
}
#endif
EOF
cp "$WORK/stub/rapidjson/document.h" "$WORK/stub/rapidjson/writer.h"
cp "$WORK/stub/rapidjson/document.h" "$WORK/stub/rapidjson/stringbuffer.h"

cat > "$WORK/probe.cpp" <<'EOF'
#include "pubsub_protocol.h"
#include <cstdio>
#include <cstddef>

// 期望值。★ 这个数是**独立于本脚本**的: 2026-10-15 之前 OmsShm.h 的注释写着 536;
// 当天给 OrderResponse 加了 `double tradeSum` (成交通道融合游标) 之后变成 544。
// 两条独立来源对得上 ⇒ 桩没改变布局。改了 pubsub_protocol.h 就要同步改这里 ——
// 本脚本**故意**在数不符时失败, 就是为了让"布局悄悄变了"这件事响亮地暴露出来。
static constexpr size_t kExpectedRCommand = 544;

static int failures = 0;
static void check_eq(const char* what, size_t got, size_t want) {
    const bool ok = (got == want);
    if (!ok) ++failures;
    std::printf("  %-34s = %-5zu (期望 %-5zu) %s\n", what, got, want, ok ? "OK" : "*** 不符 ***");
}

int main() {
    using namespace pubsub;

    std::printf("各 union 成员大小 (最大者决定 CommandBody)\n");
    std::printf("  sizeof(OrderResponse)              = %zu\n", sizeof(OrderResponse));
    std::printf("  sizeof(Balance)                    = %zu\n", sizeof(Balance));
    std::printf("  sizeof(Position)                   = %zu\n", sizeof(Position));
    std::printf("  sizeof(TotalAccount)               = %zu\n", sizeof(TotalAccount));
    std::printf("  -> OrderResponse 是最大成员?        %s\n",
                sizeof(OrderResponse) == sizeof(RCommand::CommandBody) ? "是" : "否");

    std::printf("\n交叉验证 (对上才说明桩没改变布局)\n");
    check_eq("sizeof(RCommand)",        sizeof(RCommand), kExpectedRCommand);

    std::printf("\nOrderResponse 的字段布局 / 尾部填充\n");
    // ★ 尾部填充必须从**真正的最后一个字段**算起。
    //   tradeSum 在 376 —— 它后面还有 reduceOnly/errorId/originMsg/updateTime/apiSourceEnum,
    //   所以"tradeSum 之后"量出来的是"剩余字段之和 + 填充", 不是填充 (曾因此报出 152 的假数)。
    const size_t last_off = offsetof(OrderResponse, apiSourceEnum);
    const size_t last_sz  = sizeof(OrderResponse::apiSourceEnum);
    const size_t align    = alignof(OrderResponse);
    const size_t data_end = last_off + last_sz;
    const size_t tail     = sizeof(OrderResponse) - data_end;
    std::printf("  offsetof(apiSourceEnum) [末字段]    = %zu  (sizeof=%zu)\n", last_off, last_sz);
    std::printf("  offsetof(tradeSum)                 = %zu  (★ 不是末字段)\n", offsetof(OrderResponse, tradeSum));
    std::printf("  alignof(OrderResponse)             = %zu\n", align);
    std::printf("  数据末端 (末字段结束处)             = %zu\n", data_end);
    std::printf("  -> 尾部填充                        = %zu 字节\n", tail);

    // 通用: 再加一个 (sz, al) 的字段, sizeof(OrderResponse) 会变成多少
    auto sizeof_after_adding = [&](size_t sz, size_t al) -> size_t {
        size_t off = (data_end + al - 1) / al * al;     // 新字段自身对齐
        const size_t end = off + sz;
        return (end + align - 1) / align * align;       // 结构体整体对齐
    };
    auto report_add = [&](const char* name, size_t sz, size_t al) {
        const size_t ns = sizeof_after_adding(sz, al);
        const bool fits = (ns == sizeof(OrderResponse));
        // CommandBody 是 union, 最大成员恒为 OrderResponse ⇒ sizeof(RCommand) = 8 + sizeof(OrderResponse)
        std::printf("  %-20s -> OrderResponse %zu -> %zu, RCommand %zu -> %zu  %s\n",
                    name, sizeof(OrderResponse), ns, sizeof(RCommand), 8 + ns,
                    fits ? "(塞进尾部填充, **ABI 不变**)" : "(**ABI 破坏** -> payload_words 变)");
    };

    std::printf("\n结论: 再往 OrderResponse 加字段的代价 (当前尾部填充 %zu 字节)\n", tail);
    report_add("+1 字节 (bool)",      1, 1);
    report_add("+4 字节 (int/float)", 4, 4);
    report_add("+8 字节 (double)",    8, 8);
    std::printf("  ★ 当前已含 tradeSum (double): OrderResponse 528 -> 536, RCommand 536 -> 544\n"
                "    ⇒ tb + utrade_hft 必须一起重建重启 (不符时 ShmSpmcQueue 抛 kMsgHeaderMismatch)\n"
                "    ★ 注意抓住它的是 payload_words (67->68), **不是槽位字节数**:\n"
                "      align64(8+kWords*8) 在 536/544 下都是 576 —— 64 字节对齐把差异吃掉了。\n"
                "    ★ OmsShm 侧没有这个派生量 ⇒ 必须手动 bump kVersion (见上面 omsprobe)\n");

    std::printf("\n其他\n");
    std::printf("  sizeof(CommandType)                = %zu\n", sizeof(CommandType));
    std::printf("  offsetof(RCommand, body)           = %zu\n", offsetof(RCommand, body));
    std::printf("  sizeof(NewOrder)                   = %zu\n", sizeof(NewOrder));
    std::printf("  sizeof(CancelOrder)                = %zu\n", sizeof(CancelOrder));
    std::printf("  sizeof(TCommand)                   = %zu\n", sizeof(TCommand));

    std::printf("\n%s (%d 项不符)\n", failures ? "失败" : "全部通过", failures);
    return failures ? 1 : 0;
}
EOF

clang++ -std=c++17 -O1 -Wall -Wextra -I"$WORK/stub" -I"$INC" -o "$WORK/probe" "$WORK/probe.cpp"
"$WORK/probe"

# =============================================================================
# 第二个探针: OmsShm 的 slot / 段布局
#
#   为什么也要量: `OmsSlot._pad1` 是**算出来**的
#     `kSlotSize - 头部 - sizeof(pubsub::RCommand)`
#   所以 RCommand 变大 8 字节后, `_pad1` 自动缩 8, `sizeof(OmsSlot)` 仍是 1024
#   ⇒ static_assert 照样过, 而且 **/dev/shm/tb_order 的段大小一点没变**。
#   后果: size 校验拦不住旧段 ⇒ "旧段被按新布局静默错读"是可能的
#   ⇒ **必须靠 kVersion 拦住** (OmsShm.h:58 的 v4 注释), 让 open() 响亮抛错。
#   本探针把这条不变式钉在这里, 避免以后有人只看 static_assert 就以为"没事"。
# =============================================================================
cat > "$WORK/omsprobe.cpp" <<'EOF'
#include "oms/OmsShm.h"
#include <cstdio>
#include <cstddef>

int main() {
    using namespace oms::shm;

    const size_t head = 8 + 4 + 4 + 8 * 3 + 64 * 3;   // seq/state/_pad0 + 3 时间戳 + 3 key
    const size_t pad1 = kSlotSize - head - sizeof(pubsub::RCommand);

    std::printf("\n[OmsShm] slot / 段布局\n");
    std::printf("  sizeof(pubsub::RCommand)   = %zu\n", sizeof(pubsub::RCommand));
    std::printf("  slot 头部                  = %zu\n", head);
    std::printf("  offsetof(OmsSlot, order)   = %zu\n", offsetof(OmsSlot, order));
    std::printf("  _pad1 余量                 = %zu 字节\n", pad1);
    std::printf("  sizeof(OmsSlot)            = %zu  (必须 == kSlotSize = %zu)\n",
                sizeof(OmsSlot), (size_t)kSlotSize);
    std::printf("  sizeof(OmsShmHeader)       = %zu  (必须 == 4096)\n", sizeof(OmsShmHeader));
    std::printf("  ★ 段大小**没变** ⇒ 旧段不会被 size 校验拦住, 只能靠 kVersion\n");

    const bool ok = sizeof(OmsSlot) == kSlotSize
                 && sizeof(OmsShmHeader) == 4096
                 && pad1 > 0
                 && offsetof(OmsSlot, order) == head;
    std::printf("  %s\n", ok ? "OK" : "*** 不符 ***");
    return ok ? 0 : 1;
}
EOF
clang++ -std=c++17 -O1 -Wall -Wextra -I"$WORK/stub" -I"$INC" -o "$WORK/omsprobe" "$WORK/omsprobe.cpp"
"$WORK/omsprobe"
