/*
 * copyright svante karlsson at csi dot se 2021
 */
#include "qtr/parquet/parquet_primitive_row.h"
#pragma once

namespace qtr {
namespace csv_utils {
using convert_function = std::function<qtr::parquet_primitive_value(
    std::shared_ptr<parquet::schema::PrimitiveNode> node, std::string s)>;
using column_transform = std::function<qtr::parquet_primitive_value(
    const std::vector<std::string> &csv_values)>;

// default by value convertion
parquet_primitive_value
parse_from_string(std::shared_ptr<parquet::schema::PrimitiveNode>,
                  std::string s);

struct converter_rule {
  int csvIndex;
  convert_function convertFunction;
};
} // namespace csv_utils
} // namespace qtr
