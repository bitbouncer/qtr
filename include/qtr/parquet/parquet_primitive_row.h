/*
 * copyright svante karlsson at csi dot se 2021
 */
#include <variant>
#include <parquet/schema.h>
#include <parquet/stream_reader.h>
#include <parquet/stream_writer.h>
#include <qtr/utils/parquet_schema_utils.h>
#pragma once

namespace qtr {
class parquet_primitive_value {
public:
  using physical_value_types =
      std::variant<std::monostate, bool, int32_t, int64_t, double, float,
                   std::string>;

  parquet_primitive_value(
      std::shared_ptr<parquet::schema::PrimitiveNode> schema)
      : schema_(schema) {}
  parquet_primitive_value(
      std::shared_ptr<parquet::schema::PrimitiveNode> schema,
      physical_value_types v)
      : schema_(schema), value_(v) {}

  parquet_primitive_value &operator=(const parquet_primitive_value &b) {
    assert(is_equal(schema_, b.schema_));
    value_ = b.value_;
    return *this;
  }

  inline parquet::Type::type physical_type() const {
    return schema_->physical_type();
  }
  inline parquet::LogicalType::Type::type logical_type() const {
    return schema_->logical_type()->type();
  }
  inline parquet::ConvertedType::type converted_type() const {
    return schema_->converted_type();
  }
  const std::string &name() const { return schema_->name(); }

  inline const physical_value_types &value() const { return value_; }
  inline physical_value_types &value() { return value_; }

  inline bool is_null() const { return (value_.index() == 0); }

  std::shared_ptr<parquet::schema::PrimitiveNode> schema() const {
    return schema_;
  }

  inline bool is_optional() const { return !schema_->is_required(); }

  void encode(parquet::StreamWriter &stream) const;
  parquet_primitive_value &decode(parquet::StreamReader &stream);

private:
  template <class PHYSICAL_TYPE>
  void encode_as_physical(parquet::StreamWriter &stream) const {
    if (is_optional()) {
      ::arrow::util::optional<PHYSICAL_TYPE> ov; // not the standard one...
      if (!is_null())
        ov = std::get<PHYSICAL_TYPE>(value());
      stream << ov;
    } else {
      stream << std::get<PHYSICAL_TYPE>(value());
    }
  }

  template <class PHYSICAL_TYPE>
  parquet_primitive_value &decode_as_physical(parquet::StreamReader &stream) {
    if (is_optional()) {
      ::arrow::util::optional<PHYSICAL_TYPE> val;
      stream >> val;
      if (val)
        value_ = *val;
      else
        value_ = std::monostate();
    } else {
      PHYSICAL_TYPE val;
      stream >> val;
      value_ = val;
    }
    return *this;
  }

  const std::shared_ptr<parquet::schema::PrimitiveNode> schema_;
  physical_value_types value_;
};

class parquet_primitive_row {
public:
  static std::unique_ptr<parquet_primitive_row>
      make_unique(std::shared_ptr<parquet::schema::GroupNode>);

  inline std::shared_ptr<parquet::schema::GroupNode> group_node() const {
    return group_node_;
  }

  inline std::vector<std::shared_ptr<parquet::schema::PrimitiveNode>>
  primitive_nodes() const {
    return nodes_;
  }

  inline void assign(int column, const parquet_primitive_value &v) {
    columns_[column] = v;
  }

  inline const parquet_primitive_value &value(int index) const {
    return columns_[index];
  }

  ~parquet_primitive_row() = default;

  inline size_t field_count() const { return nodes_.size(); }

  inline bool is_null(int index) const { return columns_[index].is_null(); }

  inline std::string name(int index) const { return nodes_[index]->name(); }

  std::string value_as_string(int index) const;

  inline std::shared_ptr<parquet::schema::PrimitiveNode> type(int index) const {
    return nodes_[index];
  }

  void decode(parquet::StreamReader &stream);
  void encode(parquet::StreamWriter &stream) const;

private:
  parquet_primitive_row() = default;
  parquet_primitive_row(std::shared_ptr<parquet::schema::GroupNode>);
  std::shared_ptr<parquet::schema::GroupNode> group_node_;
  std::vector<std::shared_ptr<parquet::schema::PrimitiveNode>> nodes_;
  std::vector<parquet_primitive_value> columns_;
};

inline void decode(parquet_primitive_row &row, parquet::StreamReader &stream) {
  row.decode(stream);
}

inline void encode(const parquet_primitive_row &row,
                   parquet::StreamWriter &stream) {
  row.encode(stream);
}
} // namespace qtr