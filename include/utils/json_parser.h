#pragma once

#include <simdjson.h>

inline thread_local simdjson::ondemand::parser g_parser;