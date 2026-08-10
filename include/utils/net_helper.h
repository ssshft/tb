
#pragma once

#include <string>


// ------------------------------------------------------------------------
// 从 "https://api.binance.com" 或 "https://api.binance.com/" 剥出 host。
// 保持极简, 不搞完整 URL parser。 已知输入格式。
// ------------------------------------------------------------------------
inline std::string host_of(const std::string& url) {
    std::string h = url;
    auto p = h.find("://");
    if (p != std::string::npos) {
        h = h.substr(p + 3);
    }

    auto q = h.find('/');
    if (q != std::string::npos) {
        h = h.substr(0, q);
    }

    return h;
}

// Binance ws-api 消息 payload: 数字保持不 stringify (HFT 时省一次分配)
// 拼成 {"id":"<ts>","method":"userDataStream.subscribe.signature",
//       "params":{"apiKey":"...","timestamp":<ts>,"recvWindow":5000,"signature":"..."}}
inline std::string escape_json_str(const std::string& s) {
    std::string out; 
    out.reserve(s.size() + 8);
    for (char c : s) {
        switch (c) {
            case '"':  
                out += "\\\""; 
                break;
            case '\\': 
                out += "\\\\"; 
                break;
            case '\n': 
                out += "\\n";  
                break;
            case '\r': 
                out += "\\r";  
                break;
            case '\t': 
                out += "\\t";  
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    return out;
}