#include "type_conv.h"
#include <glog/logging.h>

std::string inner_cpp_type(parquet::schema::NodePtr nodePtr) {
  auto node = std::static_pointer_cast<parquet::schema::PrimitiveNode>(nodePtr);
  auto la = node->logical_type();
  if (la && la->is_valid() && !la->is_none()) {
    // LOG(INFO) << la->ToString();
    switch (la->type()) {
    case parquet::LogicalType::Type::STRING:
      return "std::string";
    case parquet::LogicalType::Type::MAP:
      return "#error";
    case parquet::LogicalType::Type::LIST:
      return "#error";
    case parquet::LogicalType::Type::ENUM:
      return "#error";
    case parquet::LogicalType::Type::DECIMAL:
      if (node->physical_type() == parquet::Int32Type::type_num) {
        return "nonstd::decimal<int32_t, " +
               std::to_string(node->decimal_metadata().precision) + ", " +
               std::to_string(node->decimal_metadata().scale) + ">";
      } else if (node->physical_type() == parquet::Int64Type::type_num) {
        return "nonstd::decimal<int64_t, " +
               std::to_string(node->decimal_metadata().precision) + ", " +
               std::to_string(node->decimal_metadata().scale) + ">";
      }
      LOG(ERROR) << "skipping unknown decimal, physical type:: "
                 << TypeToString(node->physical_type());
      return "#error";

    case parquet::LogicalType::Type::DATE:
      return "date::sys_days";
    case parquet::LogicalType::Type::TIME:
      switch (node->converted_type()) {
      case parquet::ConvertedType::TIME_MILLIS:
        return "parquet::localtime32_ms";
      case parquet::ConvertedType::NONE:
        if (node->physical_type() == parquet::Type::INT32)
          return "parquet::localtime32_ms";
      default:
        return "#error";
      }
    case parquet::LogicalType::Type::TIMESTAMP:
      switch (node->converted_type()) {
      case parquet::ConvertedType::TIMESTAMP_MILLIS:
        return "std::chrono::milliseconds";
      default:
        return "#error";
      }
    case parquet::LogicalType::Type::INTERVAL:
      return "#error";
    case parquet::LogicalType::Type::INT:
      switch (node->converted_type()) {
      case parquet::ConvertedType::INT_8:
        return "int8_t";
      case parquet::ConvertedType::INT_16:
        return "int16_t";
      case parquet::ConvertedType::INT_32:
        return "int32_t";
      case parquet::ConvertedType::INT_64:
        return "int64_t";
      default:
        return "#error";
      }
    case parquet::LogicalType::Type::NIL: // Thrift NullType
      return "#error";
    case parquet::LogicalType::Type::JSON:
      return "#error";
    case parquet::LogicalType::Type::BSON:
      return "#error";
    case parquet::LogicalType::Type::UUID:
      return "#error";
    case parquet::LogicalType::Type::NONE:
      break;
    case parquet::LogicalType::Type::UNDEFINED:
      break;
    }
  }

  if (node->converted_type() == parquet::ConvertedType::NONE) {
    switch (node->physical_type()) {
    case parquet::Type::BOOLEAN:
      return "bool";
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      return "#error";
    case parquet::Type::BYTE_ARRAY:
      return "#error";
    case parquet::Type::INT32:
      // is this right???
      return "int32_t";
    case parquet::Type::INT64:
      return "int64_t";
    case parquet::Type::DOUBLE:
      return "double";
    case parquet::Type::FLOAT:
      return "float";
    default:
      return "#error";
    }
  }
  LOG(ERROR) << "missing parser for type "
             << ConvertedTypeToString(node->converted_type()) << ", "
             << TypeToString(node->physical_type());
  return "#error";
}

std::string to_arrow_data_type(parquet::schema::NodePtr nodePtr) {
  auto node = std::static_pointer_cast<parquet::schema::PrimitiveNode>(nodePtr);
  //parquet::schema::PrimitiveNode *p2 = node.get();
  /*
  auto x1 = p2->converted_type();
  auto x2 = p2->physical_type();
  auto x3 = p2->logical_type();
  // auto x4 = x3->ToThrift();
  */
  auto la = node->logical_type();
  if (la && la->is_valid() && !la->is_none()) {
    // LOG(INFO) << la->ToString();
    switch (la->type()) {
    case parquet::LogicalType::Type::STRING:
      return "arrow::StringType";
    case parquet::LogicalType::Type::MAP:
      return "#error";
    case parquet::LogicalType::Type::LIST:
      return "#error";
    case parquet::LogicalType::Type::ENUM:
      return "#error";
    case parquet::LogicalType::Type::DECIMAL:
      return "arrow::Decimal128Type";
    case parquet::LogicalType::Type::DATE:
      return "arrow::Date32Type";
    case parquet::LogicalType::Type::TIME:
      return "arrow::Time32Type";
    case parquet::LogicalType::Type::TIMESTAMP:
      return "arrow::TimestampType";
    case parquet::LogicalType::Type::INTERVAL:
      return "#error";
    case parquet::LogicalType::Type::INT:
      switch (node->converted_type()) {
      case parquet::ConvertedType::INT_8:
        return "arrow::Int8Type";
      case parquet::ConvertedType::INT_16:
        return "arrow::Int16Type";
      case parquet::ConvertedType::INT_32:
        return "arrow::Int32Type";
      case parquet::ConvertedType::INT_64:
        return "arrow::Int64Type";
      default:
        return "#error";
      }
    case parquet::LogicalType::Type::NIL: // Thrift NullType
      return "#error";
    case parquet::LogicalType::Type::JSON:
      return "#error";
    case parquet::LogicalType::Type::BSON:
      return "#error";
    case parquet::LogicalType::Type::UUID:
      return "#error";
    case parquet::LogicalType::Type::NONE:
      break;
    case parquet::LogicalType::Type::UNDEFINED:
      break;
    }
  }

  if (node->converted_type() == parquet::ConvertedType::NONE) {
    switch (node->physical_type()) {
    case parquet::Type::BOOLEAN:
      return "arrow::BooleanType";
      break;
      // case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      //
      // case parquet::Type::BYTE_ARRAY:

    case parquet::Type::INT32:
      return "arrow::Int32Type";
    case parquet::Type::INT64:
      return "arrow::Int64Type";
    case parquet::Type::DOUBLE:
      return "arrow::DoubleType";
    case parquet::Type::FLOAT:
      return "arrow::FloatType";
    default:
      return "#error";
    }
  }

  LOG(ERROR) << "missing parser for type "
             << ConvertedTypeToString(node->converted_type()) << ", "
             << TypeToString(node->physical_type());
  return "#error";
}

std::string to_arrow_type(parquet::schema::NodePtr nodePtr) {
  auto node = std::static_pointer_cast<parquet::schema::PrimitiveNode>(nodePtr);
  auto la = node->logical_type();
  if (la && la->is_valid() && !la->is_none()) {
    // LOG(INFO) << la->ToString();
    switch (la->type()) {
    case parquet::LogicalType::Type::STRING:
      return "arrow::utf8()";
    case parquet::LogicalType::Type::MAP:
      return "#error";
    case parquet::LogicalType::Type::LIST:
      return "#error";
    case parquet::LogicalType::Type::ENUM:
      return "#error";
    case parquet::LogicalType::Type::DECIMAL:
      return "arrow::decimal(" +
             std::to_string(node->decimal_metadata().precision) + ", " +
             std::to_string(node->decimal_metadata().scale) + ")";
    case parquet::LogicalType::Type::DATE:
      return "arrow::date32()";
    case parquet::LogicalType::Type::TIME:
      switch (node->converted_type()) {
      case parquet::ConvertedType::TIME_MILLIS:
        return "arrow::time32(arrow::TimeUnit::MILLI)";
      case parquet::ConvertedType::NONE:
        if (node->physical_type() == parquet::Type::INT32)
          return "arrow::time32(arrow::TimeUnit::MILLI)";
      default:
        return "#error";
      }
    case parquet::LogicalType::Type::TIMESTAMP:
      switch (node->converted_type()) {
      case parquet::ConvertedType::TIMESTAMP_MILLIS:
        return "arrow::timestamp(arrow::TimeUnit::MILLI)";
      default:
        return "#error";
      }
    case parquet::LogicalType::Type::INTERVAL:
      return "#error";
    case parquet::LogicalType::Type::INT:
      switch (node->converted_type()) {
      case parquet::ConvertedType::INT_8:
        return "arrow::int8()";
      case parquet::ConvertedType::INT_16:
        return "arrow::int16()";
      case parquet::ConvertedType::INT_32:
        return "arrow::int32()";
      case parquet::ConvertedType::INT_64:
        return "arrow::int64()";
      default:
        return "#error";
      }
    case parquet::LogicalType::Type::NIL: // Thrift NullType
      return "#error";
    case parquet::LogicalType::Type::JSON:
      return "#error";
    case parquet::LogicalType::Type::BSON:
      return "#error";
    case parquet::LogicalType::Type::UUID:
      return "#error";
    case parquet::LogicalType::Type::NONE:
      break;
    case parquet::LogicalType::Type::UNDEFINED:
      break;
    }
  }

  if (node->converted_type() == parquet::ConvertedType::NONE) {
    switch (node->physical_type()) {
    case parquet::Type::BOOLEAN:
      return "arrow::boolean()";
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      return "#error";
    case parquet::Type::BYTE_ARRAY:
      return "#error";
    case parquet::Type::INT32:
      // is this right???
      return "arrow::int32()";
    case parquet::Type::INT64:
      return "arrow::int64()";
    case parquet::Type::DOUBLE:
      return "arrow::float64()";
    case parquet::Type::FLOAT:
      return "arrow::float32()";
    default:
      return "#error";
    }
  }
  LOG(ERROR) << "missing parser for type "
             << ConvertedTypeToString(node->converted_type()) << ", "
             << TypeToString(node->physical_type());
  return "#error";
}

std::string to_cpp_type(parquet::schema::NodePtr nodePtr) {
  if (nodePtr->is_optional()) {
    return "std::optional<" + inner_cpp_type(nodePtr) + ">";
  }
  return inner_cpp_type(nodePtr);
}

std::string to_cpp_name(parquet::schema::NodePtr nodePtr) {
  return nodePtr->name();
}

// optional conv for types that cannot be directly constructed by arrow equiv
std::string to_arrow_conv(parquet::schema::NodePtr nodePtr, std::string name) {
  auto node = std::static_pointer_cast<parquet::schema::PrimitiveNode>(nodePtr);
  auto la = node->logical_type();
  if (la && la->is_valid() && !la->is_none()) {
    switch (la->type()) {
    case parquet::LogicalType::Type::DECIMAL:
      return "to_arrow(" + name + ")";
    case parquet::LogicalType::Type::DATE:
      return name + ".time_since_epoch().count()";
    case parquet::LogicalType::Type::TIME:
      switch (node->converted_type()) {
      case parquet::ConvertedType::TIME_MILLIS:
        return "(int32_t) " + name;
      case parquet::ConvertedType::NONE:
        if (node->physical_type() == parquet::Type::INT32)
          return "(int32_t) " + name;
      default:
        return name;
      }
    case parquet::LogicalType::Type::TIMESTAMP:
      switch (node->converted_type()) {
      case parquet::ConvertedType::TIMESTAMP_MILLIS:
        return name + ".count()";
      default:
        return name;
      }
    default:
      return name;
    }
  }
  return name;
}
