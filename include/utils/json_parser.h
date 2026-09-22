#pragma once

#include <simdjson.h>

inline thread_local simdjson::ondemand::parser g_parser;



// 把可能是数字也可能是字符串的字段统一读成 double
// 成功返回 true，并把结果写入 out
inline bool get_flexible_double(simdjson::ondemand::value v, double& out) {
    auto tv = v.type();
    if (tv.error()) {
        return false;
    }

    switch (tv.value()) {
        case simdjson::ondemand::json_type::number: {
            double d = 0.0;
            if (v.get_double().get(d) != simdjson::SUCCESS) {
                return false;
            }

            out = d;
            return true;
        }
        case simdjson::ondemand::json_type::string: {
            std::string_view sv;
            if (v.get_string().get(sv) != simdjson::SUCCESS) {
                return false;
            }
            
            if (!sv.empty()) {
                out = crypto::fast_atod(sv);
            }
            
            return true;
        }
        default:
            return false;
    }
}
