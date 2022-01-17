/*
* copyright svante karlsson at csi dot se 2021
*/
#include <qtr/parquet/parquet_types.h>

namespace parquet {
namespace internal {
parquet::schema::NodePtr make_node_impl(const std::string &, std::string name,
                                        parquet::Repetition::type repetition) {
  return parquet::schema::PrimitiveNode::Make(name, repetition,
                                              parquet::Type::BYTE_ARRAY,
                                              parquet::ConvertedType::UTF8);
}

parquet::schema::NodePtr make_node_impl(const std::chrono::milliseconds &,
                                        std::string name,
                                        parquet::Repetition::type repetition) {
  return parquet::schema::PrimitiveNode::Make(
      name, repetition, parquet::Type::INT64,
      parquet::ConvertedType::TIMESTAMP_MILLIS);
}

parquet::schema::NodePtr make_node_impl(const std::chrono::microseconds &,
                                        std::string name,
                                        parquet::Repetition::type repetition) {
  return parquet::schema::PrimitiveNode::Make(
      name, repetition, parquet::Type::INT64,
      parquet::ConvertedType::TIMESTAMP_MICROS);
}

parquet::schema::NodePtr make_node_impl(const parquet::localtime32_ms &,
                                        std::string name,
                                        parquet::Repetition::type repetition) {
  return parquet::schema::PrimitiveNode::Make(
      name, repetition,
      parquet::LogicalType::Time(false, parquet::LogicalType::TimeUnit::MILLIS),
      parquet::Type::INT32);
}

parquet::schema::NodePtr make_node_impl(const bool &, std::string name,
                                        parquet::Repetition::type repetition) {
  return parquet::schema::PrimitiveNode::Make(
      name, repetition,parquet::Type::BOOLEAN);
}

parquet::schema::NodePtr make_node_impl(const int32_t &, std::string name,
                                        parquet::Repetition::type repetition) {
  return parquet::schema::PrimitiveNode::Make(
      name, repetition, parquet::LogicalType::Int(32, true),
      parquet::Type::INT32);
}

parquet::schema::NodePtr make_node_impl(const int64_t &, std::string name,
                                        parquet::Repetition::type repetition) {
  return parquet::schema::PrimitiveNode::Make(
      name, repetition, parquet::LogicalType::Int(64, true),
      parquet::Type::INT64);
}

parquet::schema::NodePtr make_node_impl(const float &, std::string name,
                                        parquet::Repetition::type repetition) {
  return parquet::schema::PrimitiveNode::Make(
      name, repetition, parquet::Type::FLOAT, parquet::ConvertedType::NONE);
}

parquet::schema::NodePtr make_node_impl(const double &, std::string name,
                                        parquet::Repetition::type repetition) {
  return parquet::schema::PrimitiveNode::Make(
      name, repetition, parquet::Type::DOUBLE, parquet::ConvertedType::NONE);
}

parquet::schema::NodePtr make_node_impl(const date::sys_days &dummy,
                                        std::string name,
                                        parquet::Repetition::type repetition) {
  return parquet::schema::PrimitiveNode::Make(
      name, repetition, parquet::Type::INT32, parquet::ConvertedType::DATE);
}

parquet::schema::NodePtr make_node_impl(const date::local_days &dummy,
                                        std::string name,
                                        parquet::Repetition::type repetition) {
  return parquet::schema::PrimitiveNode::Make(
      name, repetition, parquet::Type::INT32, parquet::ConvertedType::DATE);
}

} // namespace internal
} // namespace parquet

