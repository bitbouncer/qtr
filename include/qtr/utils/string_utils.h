/*
 * copyright svante karlsson at csi dot se 2021
 */
#include <qtr/parquet/parquet_types.h>
#pragma once

namespace std {
std::string to_string(parquet::localtime32_ms);
}

// std::string timestamp_ns_to_string(int64_t);
