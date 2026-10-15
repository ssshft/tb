// ============================================================================
// clordid_check.cpp —— `clOrdId` → `orderSysId` 校验规则的**可执行验证台**
//
//   目的: OkxWsTradeUnit::handleFillsUpdate() 里"能不能把 fills 回填的 clOrdId
//         当成 orderSysId 用"这个判断，是本轮(§12)定位键改动的**唯一**风险点。
//         判断错了有两种后果，且都是静默的:
//           - 太严 (把合法值拒了) ⇒ 全部走 orderId 兜底, 只是慢一点, 不致命;
//           - 太松 (把 "0"/非数字放进来) ⇒ lookup_by_orderSysId_ex 命中别人的 slot,
//                                        **把成交错记到别的单上**。
//         所以这里把那段判断**逐字**搬出来，把两边边界都钉死。
//
//   与生产代码的关系: 下面的 clordid_to_order_sysid() 是 handleFillsUpdate() 里
//   那段的**逐字拷贝**（只包了一层函数壳）。改了生产代码就要同步改这里 —— 这是本
//   验证台的**已知局限**（本机编不了 tb，只能靠"人工同步 + 逐字拷贝"）。
//
//   编译运行 (不需要 fmt / simdjson / 任何第三方库):
//     clang++ -std=c++17 -O1 -Wall -Wextra -o /tmp/clordid_check tb/tools/clordid_check.cpp
//     /tmp/clordid_check
//
//   非生产代码，不入 CMake，**不需要可以直接删**。
// ============================================================================
#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>

// ---- 逐字取自 OkxWsTrade.cpp::handleFillsUpdate ----------------------------
static bool clordid_to_order_sysid(std::string_view clOrdId_sv, std::string_view& orderSysId_sv) {
    orderSysId_sv = std::string_view{};
    if (!clOrdId_sv.empty() && !(clOrdId_sv.size() == 1 && clOrdId_sv[0] == '0')) {
        bool all_digits = true;
        for (const char c : clOrdId_sv) {
            if (c < '0' || c > '9') {
                all_digits = false;
                break;
            }
        }
        if (all_digits) {
            orderSysId_sv = clOrdId_sv;
        }
    }
    return !orderSysId_sv.empty();
}
// ---------------------------------------------------------------------------

static int failures = 0;
static int total    = 0;

static void expect(const char* what, const char* in, bool want_accept, const char* want_val) {
    ++total;
    std::string_view out;
    const bool got    = clordid_to_order_sysid(in, out);
    const bool val_ok = got ? (std::string(out) == want_val) : (want_val == nullptr);
    if (got != want_accept || !val_ok) {
        ++failures;
        std::printf("FAIL %-28s in=%-24s got=%s(%s) want=%s(%s)\n",
                    what, in,
                    got ? "accept" : "reject", got ? std::string(out).c_str() : "-",
                    want_accept ? "accept" : "reject", want_val ? want_val : "-");
    }
}

int main() {
    // ---- 接受: TSC 派生的纯数字 orderSysId ----
    //   按 3 GHz 折算, 实际取值随开机时长落在 13~19 位 (见设计方案 §12.2)。
    expect("tsc 13 digits",        "3141592653589",      true,  "3141592653589");
    expect("tsc 16 digits",        "13141592653589793",  true,  "13141592653589793");
    expect("tsc 19 digits",        "4733640000000000000",true,  "4733640000000000000");
    expect("lower bound 1",        "1",                  true,  "1");
    expect("int64 max",            "9223372036854775807",true,  "9223372036854775807");
    expect("leading zeros",        "000123",             true,  "000123");  // 非规范但合法
    expect("two zeros",            "00",                 true,  "00");      // 只有单个 '0' 是哨兵

    // ---- 拒绝: 交易所"没回填 / 不符合正整数 int64 格式"的各种形态 ----
    expect("missing field",        "",                   false, nullptr);
    expect("okx sentinel 0",       "0",                  false, nullptr);
    expect("old ok-prefixed id",   "ok71234",            false, nullptr);
    expect("binance/bitget id",    "x-71234",            false, nullptr);
    expect("gate id",              "t-71234",            false, nullptr);
    expect("bybit id",             "by71234",            false, nullptr);
    expect("negative tsc (2^63)",  "-3141592653589",     false, nullptr);
    expect("plus sign",            "+3141592653589",     false, nullptr);
    expect("leading space",        " 12345",             false, nullptr);
    expect("trailing newline",     "12345\n",            false, nullptr);
    expect("hex literal",          "0x1f",               false, nullptr);
    expect("decimal point",        "123.45",             false, nullptr);
    expect("full-width digit",     "1\xef\xbc\x92",      false, nullptr);  // 非 ASCII 数字

    // ---- OKX 对 clOrdId 的 32 字符硬限制 ----
    ++total;
    if (std::string("9223372036854775807").size() > 32) {
        ++failures;
        std::printf("FAIL int64 decimal string exceeds OKX clOrdId 32-char limit\n");
    }

    std::printf("\n%d/%d passed, %d failed\n", total - failures, total, failures);
    return failures ? 1 : 0;
}
