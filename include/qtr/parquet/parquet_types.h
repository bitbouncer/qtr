/*
 * copyright svante karlsson at csi dot se 2021
 */
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <date/date.h>
#include <arrow/util/decimal.h>
#include <parquet/schema.h>
#include <nonstd/decimal.h>
#pragma once

namespace parquet {
// here goes extended types that we can persist in parquet
enum class localtime32_ms : int32_t {};
// std::chrono::milliseconds
// std::chrono::microseconds
// date::sys_days
// date::local_days

namespace internal {
parquet::schema::NodePtr make_node_impl(const std::string &, std::string name,
                                        parquet::Repetition::type repetition);

parquet::schema::NodePtr make_node_impl(const std::chrono::milliseconds &,
                                        std::string name,
                                        parquet::Repetition::type repetition);

parquet::schema::NodePtr make_node_impl(const std::chrono::microseconds &,
                                        std::string name,
                                        parquet::Repetition::type repetition);

parquet::schema::NodePtr make_node_impl(const parquet::localtime32_ms &,
                                        std::string name,
                                        parquet::Repetition::type repetition);

parquet::schema::NodePtr make_node_impl(const bool &, std::string name,
                                        parquet::Repetition::type repetition);

parquet::schema::NodePtr make_node_impl(const int32_t &, std::string name,
                                        parquet::Repetition::type repetition);

parquet::schema::NodePtr make_node_impl(const int64_t &, std::string name,
                                        parquet::Repetition::type repetition);

parquet::schema::NodePtr make_node_impl(const float &, std::string name,
                                        parquet::Repetition::type repetition);

parquet::schema::NodePtr make_node_impl(const double &, std::string name,
                                        parquet::Repetition::type repetition);

parquet::schema::NodePtr make_node_impl(const date::sys_days &dummy,
                                        std::string name,
                                        parquet::Repetition::type repetition);

parquet::schema::NodePtr make_node_impl(const date::local_days &dummy,
                                        std::string name,
                                        parquet::Repetition::type repetition);

template <typename DECIMAL,
          std::enable_if_t<
              std::is_same<DECIMAL, nonstd::decimal<int32_t, DECIMAL::precision,
                                                    DECIMAL::scale>>::value,
              bool> = true>
parquet::schema::NodePtr make_node_impl(const DECIMAL &, std::string name,
                                        parquet::Repetition::type repetition) {
  return parquet::schema::PrimitiveNode::Make(
      name, repetition,
      parquet::LogicalType::Decimal(DECIMAL::precision, DECIMAL::scale),
      parquet::Type::INT32);
}

template <typename DECIMAL,
          std::enable_if_t<
              std::is_same<DECIMAL, nonstd::decimal<int64_t, DECIMAL::precision,
                                                    DECIMAL::scale>>::value,
              bool> = true>
parquet::schema::NodePtr make_node_impl(const DECIMAL &, std::string name,
                                        parquet::Repetition::type repetition) {
  return parquet::schema::PrimitiveNode::Make(
      name, repetition,
      parquet::LogicalType::Decimal(DECIMAL::precision, DECIMAL::scale),
      parquet::Type::INT64);
}
} // namespace internal

template <class T>
parquet::schema::NodePtr make_node(const T &dummy, std::string name) {
  return internal::make_node_impl(dummy, name, parquet::Repetition::REQUIRED);
}

template <class T>
parquet::schema::NodePtr make_node(std::optional<T> dummy, std::string name) {
  return internal::make_node_impl(T(), name, parquet::Repetition::OPTIONAL);
}

} // namespace parquet

template <parquet::Type::type> struct MapToDataType_t;
template <> struct MapToDataType_t<parquet::Type::BOOLEAN> {
  using type = bool;
};
template <> struct MapToDataType_t<parquet::Type::INT32> {
  using type = int32_t;
};
template <> struct MapToDataType_t<parquet::Type::INT64> {
  using type = int64_t;
};
// template <> struct MapToDataType_t<parquet::Type::INT96> not implemented
template <> struct MapToDataType_t<parquet::Type::FLOAT> {
  using type = float;
};
template <> struct MapToDataType_t<parquet::Type::DOUBLE> {
  using type = double;
};
template <> struct MapToDataType_t<parquet::Type::BYTE_ARRAY> {
  using type = std::string;
};
// template <> struct MapToDataType_t<parquet::Type::FIXED_LEN_BYTE_ARRAY>  {
// using type = std::string; }; not implemented yet template <> struct
// MapToDataType_t<DataType::UNDEFINED> not implemented

template <parquet::Type::type T>
using MapToDataType = typename MapToDataType_t<T>::type;
