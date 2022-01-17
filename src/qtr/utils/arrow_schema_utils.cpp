/*
 * copyright svante karlsson at csi dot se 2021
 */
#include "qtr/utils/arrow_schema_utils.h"
#include <arrow/builder.h>
#include <arrow/type.h>
#include <glog/logging.h>

namespace qtr {
std::shared_ptr<parquet::schema::GroupNode>
to_group_node(std::shared_ptr<arrow::Schema> schema) {
  /*
  LOG(INFO) << "schema: " << schema->ToString();
  for (auto i : schema->fields()) {
    LOG(INFO) << "name: " << i->name() << " type: " << i->type()->name() << " "
              << i->type()->id();
  }
  */

  parquet::schema::NodeVector fields;

  for (auto i : schema->fields()) {
    // LOG(INFO) << "name: " << i->name() << " type: " << i->type()->name() << "
    // "
    //          << i->type()->id();

    bool is_optional = i->nullable();
    parquet::Repetition::type repetition = is_optional
                                               ? parquet::Repetition::OPTIONAL
                                               : parquet::Repetition::REQUIRED;

    switch (i->type()->id()) {
    /// A NULL type having no physical storage
    case arrow::Type::NA:
      assert(false);
      // how do we represent a NULL column??
      /*fields.push_back(parquet::schema::PrimitiveNode::Make(
        i->name(), parquet::Repetition::REQUIRED, parquet::Type::NULL,
        parquet::ConvertedType::UINT_8)); // the converted type seems wrong??
      */
      break;

      /// Boolean as 1 bit, LSB bit-packed ordering
    case arrow::Type::BOOL:
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::BOOLEAN,
          parquet::ConvertedType::NONE));
      break;

      /// Unsigned 8-bit little-endian integer
    case arrow::Type::UINT8:
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::INT32,
          parquet::ConvertedType::UINT_8));
      break;

      /// Signed 8-bit little-endian integer
    case arrow::Type::INT8:
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::INT32,
          parquet::ConvertedType::INT_8));
      break;

      /// Unsigned 16-bit little-endian integer
    case arrow::Type::UINT16:
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::INT32,
          parquet::ConvertedType::UINT_16));
      break;

      /// Signed 16-bit little-endian integer
    case arrow::Type::INT16:
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::INT32,
          parquet::ConvertedType::INT_16));
      break;

      /// Unsigned 32-bit little-endian integer
    case arrow::Type::UINT32:
      // is this even remotely right??
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::INT32,
          parquet::ConvertedType::UINT_32));
      break;

      /// Signed 32-bit little-endian integer
    case arrow::Type::INT32:
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::INT32,
          parquet::ConvertedType::INT_32));
      break;

      /// Unsigned 64-bit little-endian integer
    case arrow::Type::UINT64:
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::INT64,
          parquet::ConvertedType::UINT_64));
      break;

      /// Signed 64-bit little-endian integer
    case arrow::Type::INT64:
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::INT64,
          parquet::ConvertedType::INT_64));
      break;

      /// 2-byte floating point value
    case arrow::Type::HALF_FLOAT:
      // is converted type right?
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::FLOAT,
          parquet::ConvertedType::NONE));
      break;

      /// 4-byte floating point value
    case arrow::Type::FLOAT:
      // is converted type right?
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::FLOAT,
          parquet::ConvertedType::NONE));
      break;

      /// 8-byte floating point value
    case arrow::Type::DOUBLE:
      // is converted type right?
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::DOUBLE,
          parquet::ConvertedType::NONE));
      break;

      /// UTF8 variable-length string as List<Char>
    case arrow::Type::STRING:
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::BYTE_ARRAY,
          parquet::ConvertedType::UTF8));
      break;

      /// Variable-length bytes (no guarantee of UTF8-ness)
    case arrow::Type::BINARY:
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::BYTE_ARRAY,
          parquet::ConvertedType::UTF8));
      break;

      /// Fixed-size binary. Each value occupies the same number of bytes
    case arrow::Type::FIXED_SIZE_BINARY:
      assert(false);
      // fields.push_back(parquet::schema::PrimitiveNode::Make(
      //  i->name(), parquet::Repetition::REQUIRED,
      //  parquet::Type::FIXED_LEN_BYTE_ARRAY, parquet::ConvertedType::NONE,
      //  i->type()->layout()->));
      // how do we get length of field here???
      break;

      /// int32_t days since the UNIX epoch
    case arrow::Type::DATE32:
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::INT32,
          parquet::ConvertedType::DATE));

      /*      fields.push_back(parquet::schema::PrimitiveNode::Make(
                i->name(), repetition, parquet::Type::INT32,
                parquet::ConvertedType::INT_32));
      */
      break;

      /// int64_t milliseconds since the UNIX epoch
    case arrow::Type::DATE64:
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::INT64,
          parquet::ConvertedType::DATE));
      break;

      /// Exact timestamp encoded with int64 since UNIX epoch
      /// Default unit millisecond
    case arrow::Type::TIMESTAMP:
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::INT64,
          parquet::ConvertedType::TIMESTAMP_MILLIS));
      break;

      /// Time as signed 32-bit integer, representing either seconds or
      /// milliseconds since midnight
    case arrow::Type::TIME32:
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::INT32,
          parquet::ConvertedType::TIME_MILLIS));
      break;

      /// Time as signed 64-bit integer, representing either microseconds or
      /// nanoseconds since midnight
    case arrow::Type::TIME64:
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition, parquet::Type::INT64,
          parquet::ConvertedType::TIME_MILLIS)); // millis is probably wrong
                                                 // here
      break;

    case arrow::Type::INTERVAL_MONTH_DAY_NANO:
    case arrow::Type::INTERVAL_MONTHS:
    case arrow::Type::INTERVAL_DAY_TIME:
      assert(false);
      break;

      /// YEAR_MONTH or DAY_TIME interval in SQL style
      // case arrow::Type::INTERVAL:
      //  assert(false);
      //  break;

      /// Precision- and scale-based decimal type. Storage type depends on the
      /// parameters.
    case arrow::Type::DECIMAL: {
      arrow::DecimalType *dt =
          static_cast<arrow::DecimalType *>(i->type().get());
      int precision = dt->precision();
      int scale = dt->scale();
      int physical_size = dt->bit_width();
      parquet::Type::type primitive_type =
          (physical_size == 32) ? parquet::Type::INT32 : parquet::Type::INT64;
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          i->name(), repetition,
          parquet::LogicalType::Decimal(precision, scale), primitive_type));
    } break;
      /// A list of some logical data type
    case arrow::Type::LIST:
      assert(false);
      break;
      /// Struct of logical types
    case arrow::Type::STRUCT:
      assert(false);
      break;

    /// Unions of logical types
    case arrow::Type::SPARSE_UNION:
      assert(false);
      break;

    case arrow::Type::DENSE_UNION:
      assert(false);
      break;

      /// Dictionary-encoded type, also called "categorical" or "factor"
      /// in other programming languages. Holds the dictionary value
      /// type but not the dictionary itself, which is part of the
      /// ArrayData struct
    case arrow::Type::DICTIONARY:
      assert(false);
      break;
      /// Map, a repeated struct logical type
    case arrow::Type::MAP:
      assert(false);
      break;
      /// Custom data type, implemented by user
    case arrow::Type::EXTENSION:
      assert(false);
      break;
      /// Fixed size list of some logical type
    case arrow::Type::FIXED_SIZE_LIST:
      assert(false);
      break;
      /// Measure of elapsed time in either seconds, milliseconds, microseconds
      /// or nanoseconds.
    case arrow::Type::DURATION:
      assert(false);
      break;
      /// Like STRING, but with 64-bit offsets
    case arrow::Type::LARGE_STRING:
      assert(false);
      break;
      /// Like BINARY, but with 64-bit offsets
    case arrow::Type::LARGE_BINARY:
      assert(false);
      break;
      /// Like LIST, but with 64-bit offsets
    case arrow::Type::LARGE_LIST:
      assert(false);
      break;

    case arrow::Type::MAX_ID:
      assert(false);
      break;

    case arrow::Type::DECIMAL256:
      assert(false);
      break;
    }
  }

  return std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED,
                                       fields));
}

std::shared_ptr<::parquet::schema::GroupNode>
clone(const ::parquet::schema::GroupNode *src) {
  parquet::schema::NodeVector fields;
  for (int i = 0; i != src->field_count(); ++i) {
    int field_id = i + 1;
    auto node =
        std::static_pointer_cast<parquet::schema::PrimitiveNode>(src->field(i));
    // vi tappar time32 här.... TODO
    // parquet::LogicalType
    auto x = node->logical_type();
    if (x->is_decimal()) {
      parquet::schema::DecimalMetadata metadata;
      x->ToConvertedType(&metadata);
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          node->name(), node->repetition(), node->physical_type(),
          node->converted_type(), -1, metadata.precision, metadata.scale,
          field_id));

    } else if (x->is_time()) { // a bit hardcoded...
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          node->name(), node->repetition(),
          parquet::LogicalType::Time(false,
                                     parquet::LogicalType::TimeUnit::MILLIS),
          parquet::Type::INT32, node->type_length(), field_id));
    } else if (x->is_date()) {
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          node->name(), node->repetition(), parquet::Type::INT32,
          parquet::ConvertedType::DATE, node->type_length(), -1, -1, field_id));
    } else if (x->is_timestamp()) {
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          node->name(), node->repetition(), parquet::Type::INT64,
          parquet::ConvertedType::TIMESTAMP_MILLIS, node->type_length(), -1, -1,
          field_id));
    } else {
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          node->name(), node->repetition(), node->physical_type(),
          node->converted_type(), node->type_length(), -1, -1, field_id));
    }
  }

  auto group_node = std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED,
                                       fields));
  return group_node;
}
} // namespace qtr