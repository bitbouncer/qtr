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

struct test1_v1 {
  std::string exchange;
  std::string symbol;
  std::optional<int64_t> timestamp;
  int64_t local_timestamp;
  std::string type;
  nonstd::decimal<int64_t, 18, 8> strike_price;
  int64_t expiration;
  nonstd::decimal<int64_t, 18, 8> open_interest;
  std::optional<nonstd::decimal<int64_t, 18, 8>> last_price;
  std::optional<nonstd::decimal<int64_t, 18, 8>> bid_price;
  std::optional<nonstd::decimal<int64_t, 18, 8>> bid_amount;
  std::optional<nonstd::decimal<int64_t, 18, 8>> bid_iv;
  std::optional<nonstd::decimal<int64_t, 18, 8>> ask_price;
  std::optional<nonstd::decimal<int64_t, 18, 8>> ask_amount;
  std::optional<nonstd::decimal<int64_t, 18, 8>> ask_iv;
  std::optional<nonstd::decimal<int64_t, 18, 8>> mark_price;
  std::optional<nonstd::decimal<int64_t, 18, 8>> mark_iv;
  std::string underlying_index;
  std::optional<nonstd::decimal<int64_t, 18, 8>> underlying_price;
  std::optional<double> delta;
  std::optional<double> gamma;
  std::optional<double> vega;
  std::optional<double> theta;
  std::optional<double> rho;

  test1_v1() = default;

  static inline std::unique_ptr<test1_v1>
  make_unique(__attribute__((unused)) std::shared_ptr<parquet::schema::GroupNode>){
    return std::make_unique<test1_v1>();
  }

  static std::shared_ptr<parquet::schema::GroupNode> group_node();
};

void encode(const test1_v1& obj, parquet::StreamWriter &stream);

void decode(test1_v1& obj, parquet::StreamReader &stream);



