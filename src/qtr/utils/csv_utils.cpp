/*
 * copyright svante karlsson at csi dot se 2021
 */
#include "qtr/utils/csv_utils.h"
#include <glog/logging.h>
#include <qtr/parquet/parquet_types.h>

using namespace date;
using namespace std::chrono_literals;

namespace qtr {
namespace csv_utils {
parquet_primitive_value
parse_from_string(std::shared_ptr<parquet::schema::PrimitiveNode> node,
                  std::string s) {
  // if (s.size()==0 && !node->is_required())
  //     return parquet_primitive_value(node, std::monostate());

  if (s.size() == 0)
    return parquet_primitive_value(node, std::monostate());

  try {
    /*LOG(INFO)
        << ", logical type: " << node->logical_type()->ToString()
        << ", converted type: "
        << ConvertedTypeToString(node->converted_type()) << ", "
        << ", physical type: " <<
        TypeToString(node->physical_type())
        << ", name: " << node->name();
    */

    /*
    enum type {
      BOOLEAN = 0,
      INT32 = 1,
      INT64 = 2,
      INT96 = 3,
      FLOAT = 4,
      DOUBLE = 5,
      BYTE_ARRAY = 6,
      FIXED_LEN_BYTE_ARRAY = 7,
      // Should always be last element.
      UNDEFINED = 8
    */

    auto la = node->logical_type();
    if (la && la->is_valid()) {
      // LOG(INFO) << la->ToString();
      switch (la->type()) {
      case parquet::LogicalType::Type::STRING:
        assert(node->physical_type() == parquet::Type::BYTE_ARRAY);
        return parquet_primitive_value(
            node, static_cast<MapToDataType<parquet::Type::BYTE_ARRAY>>(s));
      case parquet::LogicalType::Type::DECIMAL: {
        parquet::schema::DecimalMetadata metadata;
        la->ToConvertedType(&metadata);
        if (node->physical_type() == parquet::Type::INT32) {
          auto d = nonstd::decimal32(metadata.precision, metadata.scale,
                                     atof(s.c_str()));
          return parquet_primitive_value(
              node, static_cast<MapToDataType<parquet::Type::INT32>>(d.value_));
        } else if (node->physical_type() == parquet::Type::INT64) {
          auto d = nonstd::decimal64(metadata.precision, metadata.scale,
                                     atof(s.c_str()));
          return parquet_primitive_value(node, d.value_);
        }
      } break;
      case parquet::LogicalType::Type::DATE: {
        // a bit naive since it only supports YYYY-MM-DD format
        date::sys_days tp;
        std::stringstream ss{s};
        ss >> date::parse("%Y-%m-%d", tp);
        if (tp == sys_days(January / 1 / 1970)) {
          std::string err = "bad date_given: " + s;
          throw std::runtime_error(err);
        }
        if (node->physical_type() == parquet::Type::INT32)
          return parquet_primitive_value(
              node, static_cast<MapToDataType<parquet::Type::INT32>>(
                        tp.time_since_epoch().count()));
        if (node->physical_type() == parquet::Type::INT64)
          return parquet_primitive_value(
              node, static_cast<MapToDataType<parquet::Type::INT64>>(
                        tp.time_since_epoch().count()));
      } break;
      case parquet::LogicalType::Type::TIME:
        switch (node->converted_type()) {
        case parquet::ConvertedType::TIME_MILLIS:
          LOG(ERROR) << "NOT IMPLEMENTED, logical type: "
                     << node->logical_type()->ToString() << ", converted type: "
                     << ConvertedTypeToString(node->converted_type()) << ", "
                     << ", physical type: "
                     << TypeToString(node->physical_type())
                     << ", name: " << node->name();
          // columns_[column] =
          //     read_column<parquet::localtime32_ms>(stream, node.get());
          assert(false);
          break;
        case parquet::ConvertedType::NONE: // is this right??
          if (node->physical_type() == parquet::Type::INT32)
            // columns_[column] =
            //     read_column<parquet::localtime32_ms>(stream, node.get());
            LOG(ERROR) << "NOT IMPLEMENTED, logical type: "
                       << node->logical_type()->ToString()
                       << ", converted type: "
                       << ConvertedTypeToString(node->converted_type()) << ", "
                       << ", physical type: "
                       << TypeToString(node->physical_type())
                       << ", name: " << node->name();

          assert(false);
          break;
        default:
          LOG(ERROR) << "NOT IMPLEMENTED, logical type: "
                     << node->logical_type()->ToString() << ", converted type: "
                     << ConvertedTypeToString(node->converted_type()) << ", "
                     << ", physical type: "
                     << TypeToString(node->physical_type())
                     << ", name: " << node->name();
          // not implemented
          // stream.SkipColumns(1);
          break;
        }
        break; // switch la-type()
      case parquet::LogicalType::Type::TIMESTAMP:
        if (node->physical_type() == parquet::Type::INT32)
          return parquet_primitive_value(
              node, static_cast<MapToDataType<parquet::Type::INT32>>(
                        atoll(s.c_str())));
        if (node->physical_type() == parquet::Type::INT64)
          return parquet_primitive_value(
              node, static_cast<MapToDataType<parquet::Type::INT64>>(
                        atoll(s.c_str())));
        break;

      case parquet::LogicalType::Type::INT:
        if (node->physical_type() == parquet::Type::INT32)
          return parquet_primitive_value(
              node, static_cast<MapToDataType<parquet::Type::INT32>>(
                        atoi(s.c_str())));
        if (node->physical_type() == parquet::Type::INT64)
          return parquet_primitive_value(
              node, static_cast<MapToDataType<parquet::Type::INT64>>(
                        atoll(s.c_str())));
        break;

      case parquet::LogicalType::Type::NONE:
        switch (node->physical_type()) {
        case parquet::Type::BOOLEAN:
          return parquet_primitive_value(
              node, static_cast<MapToDataType<parquet::Type::BOOLEAN>>(
                        s == "1" || s == "TRUE" || s == "true"));
          break;
        case parquet::Type::INT32:
          return parquet_primitive_value(
              node, static_cast<MapToDataType<parquet::Type::INT32>>(
                        atoi(s.c_str())));
        case parquet::Type::INT64:
          return parquet_primitive_value(
              node, static_cast<MapToDataType<parquet::Type::INT64>>(
                        atoll(s.c_str())));
        case parquet::Type::DOUBLE:
          return parquet_primitive_value(
              node, static_cast<MapToDataType<parquet::Type::DOUBLE>>(
                        atof(s.c_str())));
        case parquet::Type::FLOAT:
          return parquet_primitive_value(
              node, static_cast<MapToDataType<parquet::Type::FLOAT>>(
                        atof(s.c_str())));
        case parquet::Type::FIXED_LEN_BYTE_ARRAY:
          break;
          // return primitive_value(node,
          // static_cast<MapToDataType<parquet::Type::FIXED_LEN_BYTE_ARRAY>>(s));
        case parquet::Type::BYTE_ARRAY:
          return parquet_primitive_value(
              node, static_cast<MapToDataType<parquet::Type::BYTE_ARRAY>>(s));
        default:
          LOG(ERROR) << "missing parser for type "
                     << ConvertedTypeToString(node->converted_type()) << ", "
                     << TypeToString(node->physical_type());
          break;
        }
        break; // switch la-type()

      case parquet::LogicalType::Type::MAP:
      case parquet::LogicalType::Type::LIST:
      case parquet::LogicalType::Type::ENUM:
      case parquet::LogicalType::Type::INTERVAL:
      case parquet::LogicalType::Type::NIL: // Thrift NullType
      case parquet::LogicalType::Type::JSON:
      case parquet::LogicalType::Type::BSON:
      case parquet::LogicalType::Type::UUID:
      case parquet::LogicalType::Type::UNDEFINED:
        // not implemented
        // stream.SkipColumns(1);
        break;
      }
    } else {
      // unexpected stuff in file
      LOG(ERROR) << "missing parser"
                 << ", logical type: " << node->logical_type()->ToString()
                 << ", converted type: "
                 << ConvertedTypeToString(node->converted_type()) << ", "
                 << ", physical type: " << TypeToString(node->physical_type())
                 << ", name: " << node->name();
      // stream.SkipColumns(1);
    }
  } catch (std::exception &e) {
    LOG(ERROR) << "failed to read "
               << ", logical type: " << node->logical_type()->ToString()
               << ", converted type: "
               << ConvertedTypeToString(node->converted_type()) << ", "
               << ", physical type: " << TypeToString(node->physical_type())
               << ", name: " << node->name();
    throw;
  }
  // we should never get here...

  return parquet_primitive_value(node, std::monostate());
}
} // namespace csv_utils
} // namespace qtr
