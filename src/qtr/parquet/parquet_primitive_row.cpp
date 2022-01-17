/*
 * copyright svante karlsson at csi dot se 2021
 */
#include <qtr/parquet/parquet_primitive_row.h>
#include <arrow/builder.h>
#include <glog/logging.h>
#include <iomanip>
#include <nlohmann/json.hpp>
#include "qtr/utils/arrow_schema_utils.h"
#include "qtr/schema/parquet_json_schema.h"
#include <qtr/parquet/parquet_types.h>
#include <qtr/io/stream_reader_ex.h>
#include <qtr/io/stream_writer_ex.h>

namespace qtr {

void parquet_primitive_value::encode(parquet::StreamWriter &stream) const {
  if (!(is_null() ? !schema()->is_required() : true)) {
    LOG(FATAL) << "null value in required column: " << schema()->name();
  }
  if (is_null()) {
    stream.SkipColumns(1);
    return;
  }
  switch (logical_type()) {
  case parquet::LogicalType::Type::STRING:
    assert(physical_type() == parquet::Type::BYTE_ARRAY);
    stream << std::get<MapToDataType<parquet::Type::BYTE_ARRAY>>(value());
    break;
  case parquet::LogicalType::Type::DECIMAL:
    if (physical_type() == parquet::Type::INT32) {
      WriteUnsafe_INT32(stream, std::get<int32_t>(value()));
      break;
    }
    if (physical_type() == parquet::Type::INT64) {
      WriteUnsafe_INT64(stream, std::get<int64_t>(value()));
      break;
    }
    break;
  case parquet::LogicalType::Type::DATE:
    if (physical_type() == parquet::Type::INT32) {
      WriteUnsafe_INT32(stream, std::get<int32_t>(value()));
      break;
    }
    if (physical_type() == parquet::Type::INT64) {
      WriteUnsafe_INT64(stream, std::get<int64_t>(value()));
      break;
    }
    break;
  case parquet::LogicalType::Type::TIME:
    if (physical_type() == parquet::Type::INT32) {
      WriteUnsafe_INT32(stream, std::get<int32_t>(value()));
      break;
    }
    if (physical_type() == parquet::Type::INT64) {
      WriteUnsafe_INT64(stream, std::get<int64_t>(value()));
      break;
    }
    break;
  case parquet::LogicalType::Type::TIMESTAMP:
    if (physical_type() == parquet::Type::INT32) {
      WriteUnsafe_INT32(stream, std::get<int32_t>(value()));
      break;
    }
    if (physical_type() == parquet::Type::INT64) {
      WriteUnsafe_INT64(stream, std::get<int64_t>(value()));
      break;
    }
    break;
  case parquet::LogicalType::Type::INT:
    if (physical_type() == parquet::Type::INT32) {
      if (converted_type() == parquet::ConvertedType::INT_8) {
        WriteUnsafe_INT8(stream, std::get<int32_t>(value()));
        break;
      }
      if (converted_type() == parquet::ConvertedType::INT_16) {
        WriteUnsafe_INT16(stream, std::get<int32_t>(value()));
        break;
      }
      if (converted_type() == parquet::ConvertedType::INT_32) {
        WriteUnsafe_INT32(stream, std::get<int32_t>(value()));
        break;
      }
    }
    if (physical_type() == parquet::Type::INT64) {
      WriteUnsafe_INT64(stream, std::get<int64_t>(value()));
      break;
    }
    break;
  case parquet::LogicalType::Type::NONE:
    switch (physical_type()) {
    case parquet::Type::BOOLEAN:
      encode_as_physical<MapToDataType<parquet::Type::BOOLEAN>>(stream);
      break;
    case parquet::Type::INT32:
      encode_as_physical<MapToDataType<parquet::Type::INT32>>(stream);
      break;
    case parquet::Type::INT64:
      encode_as_physical<MapToDataType<parquet::Type::INT64>>(stream);
      break;
    case parquet::Type::INT96:
      // write_column<MapToDataType<parquet::Type::INT96>>(stream, columns_[i]);
      stream.SkipColumns(1);
      break;
    case parquet::Type::DOUBLE:
      encode_as_physical<MapToDataType<parquet::Type::DOUBLE>>(stream);
      break;
    case parquet::Type::FLOAT:
      encode_as_physical<MapToDataType<parquet::Type::FLOAT>>(stream);
      break;
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      // write_column<MapToDataType<parquet::Type::FIXED_LEN_BYTE_ARRAY>>(stream,
      // columns_[i]);
      stream.SkipColumns(1);
      break;
    case parquet::Type::BYTE_ARRAY:
      encode_as_physical<MapToDataType<parquet::Type::BYTE_ARRAY>>(stream);
      break;
    default:
      break;
    }
    break;

  default:
    LOG(ERROR) << "failed to write, " << schema_to_json(schema());
  }
}

parquet_primitive_value &
parquet_primitive_value::decode(parquet::StreamReader &stream) {
  // specials for int 8 & 16 (32 bytes in physical ion memory and 8 & 16 on
  // disk)
  if (logical_type() == parquet::LogicalType::Type::INT) {
    if (converted_type() == parquet::ConvertedType::INT_8) {
      std::optional<int8_t> raw;
      stream >> raw;
      if (raw)
        value_ = static_cast<MapToDataType<parquet::Type::INT32>>(*raw);
      else
        value_ = std::monostate();
      return *this;
    } else if (converted_type() == parquet::ConvertedType::INT_16) {
      std::optional<int16_t> raw;
      stream >> raw;
      if (raw)
        value_ = static_cast<MapToDataType<parquet::Type::INT32>>(*raw);
      else
        value_ = std::monostate();
      return *this;
    }
  }

  switch (physical_type()) {
  case parquet::Type::BOOLEAN:
    return decode_as_physical<MapToDataType<parquet::Type::BOOLEAN>>(stream);
  case parquet::Type::INT32:
    return decode_as_physical<MapToDataType<parquet::Type::INT32>>(stream);
  case parquet::Type::INT64:
    return decode_as_physical<MapToDataType<parquet::Type::INT64>>(stream);
  case parquet::Type::INT96:
    LOG(ERROR) << "dont know how to parse INT96, name: " << schema_->name();
    stream.SkipColumns(1);
    return *this;
  case parquet::Type::DOUBLE:
    return decode_as_physical<MapToDataType<parquet::Type::DOUBLE>>(stream);
  case parquet::Type::FLOAT:
    return decode_as_physical<MapToDataType<parquet::Type::FLOAT>>(stream);
  case parquet::Type::FIXED_LEN_BYTE_ARRAY:
    LOG(ERROR) << "dont know how to parse FIXED_LEN_BYTE_ARRAY, name: "
               << schema_->name();
    stream.SkipColumns(1);
    return *this;
    // columns_[i] =
    // read_column<MapToDataType<parquet::Type::FIXED_LEN_BYTE_ARRAY>>(stream,
    // node);
  case parquet::Type::BYTE_ARRAY:
    return decode_as_physical<MapToDataType<parquet::Type::BYTE_ARRAY>>(stream);
  case parquet::Type::UNDEFINED:
    stream.SkipColumns(1);
    value_ = std::monostate();
    return *this;
  }
  /*default:
   LOG(ERROR) << "missing conversion for item" <<
             schema_to_json(schema());
   stream.SkipColumns(1);
   return *this;
  */
  return *this; // we should never get here
}

parquet_primitive_row::parquet_primitive_row(
    std::shared_ptr<parquet::schema::GroupNode> group_node) {
  group_node_ = qtr::clone(group_node.get());
  nodes_.resize(group_node->field_count());
  columns_.reserve(group_node->field_count());
  for (auto i = 0; i < group_node->field_count(); ++i) {
    nodes_[i] = std::static_pointer_cast<parquet::schema::PrimitiveNode>(
        group_node->field(i));
    columns_.emplace_back(parquet_primitive_value(nodes_[i]));
  }
}

std::unique_ptr<parquet_primitive_row> parquet_primitive_row::make_unique(
    std::shared_ptr<parquet::schema::GroupNode> group_node) {
  return std::unique_ptr<parquet_primitive_row>(
      new parquet_primitive_row(group_node));
}

void parquet_primitive_row::decode(parquet::StreamReader &stream) {
  for (size_t i = 0; i != field_count(); ++i)
    columns_[i].decode(stream);
  stream.EndRow();
}

void parquet_primitive_row::encode(parquet::StreamWriter &stream) const {
  for (size_t i = 0; i != field_count(); ++i)
    columns_[i].encode(stream);
  stream.EndRow();
}
} // namespace qtr
