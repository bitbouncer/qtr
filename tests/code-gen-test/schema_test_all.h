/**
 * automatic generated file
 */

#include <optional>
#include <date/date.h>
#include <parquet/schema.h>
#include <parquet/stream_reader.h>
#include <parquet/stream_writer.h>
#include <nonstd/decimal.h>
#include <qtr/parquet/parquet_types.h>
#pragma once

struct schema_test_all {
  std::string a_string;
  nonstd::decimal<int32_t, 8, 4> a_decimal32;
  nonstd::decimal<int64_t, 18, 8> a_decimal64;
  date::sys_days a_date32;
  parquet::localtime32_ms a_time32_ms;
  std::chrono::milliseconds a_timestamp_ms;
  int8_t a_int8;
  int16_t a_int16;
  int32_t a_int32;
  int64_t a_int64;
  bool a_bool;
  double a_double;
  float a_float;

  schema_test_all() = default;

  static inline std::unique_ptr<schema_test_all>
  make_unique(__attribute__((unused)) std::shared_ptr<parquet::schema::GroupNode>){
    return std::make_unique<schema_test_all>();
  }

  static std::shared_ptr<parquet::schema::GroupNode> group_node();
};

void encode(const schema_test_all& obj, parquet::StreamWriter &stream);

void decode(schema_test_all& obj, parquet::StreamReader &stream);



