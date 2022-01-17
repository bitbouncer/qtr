/*
 * copyright svante karlsson at csi dot se 2021
 */
#include "qtr/schema/parquet_json_schema.h"
#include <iostream>
#include <fstream>
#include <filesystem>
#include <glog/logging.h>

using json = nlohmann::json;

namespace qtr {
static nlohmann::json to_json(parquet::schema::NodePtr nodePtr) {
  json result;
  result["name"] = nodePtr->name();
  result["optional"] = nodePtr->is_optional();

  auto node = std::static_pointer_cast<parquet::schema::PrimitiveNode>(nodePtr);
  auto la = node->logical_type();
  if (la && la->is_valid() && !la->is_none()) {
    // LOG(INFO) << la->ToString();
    switch (la->type()) {
    case parquet::LogicalType::Type::STRING:
      result["type"] = "string";
      return result;
    case parquet::LogicalType::Type::MAP:
      result["type"] = "map";
      return result;
    case parquet::LogicalType::Type::LIST:
      result["type"] = "list";
      return result;
    case parquet::LogicalType::Type::ENUM:
      result["type"] = "enum";
      return result;
    case parquet::LogicalType::Type::DECIMAL: {
      json t;
      result["type"] = "decimal";
      if (node->physical_type() == parquet::Int32Type::type_num)
        result["physical_size"] = 32;
      else if (node->physical_type() == parquet::Int64Type::type_num)
        result["physical_size"] = 64;
      result["precision"] = node->decimal_metadata().precision;
      result["scale"] = node->decimal_metadata().scale;
      return result;
    }
    case parquet::LogicalType::Type::DATE:
      result["type"] = "date32";
      return result;
    case parquet::LogicalType::Type::TIME:
      result["type"] = "time32_ms";
      return result;
    case parquet::LogicalType::Type::TIMESTAMP:
      if (node->converted_type() == parquet::ConvertedType::TIMESTAMP_MILLIS)
        result["type"] = "timestamp_ms";
      return result;
    case parquet::LogicalType::Type::INTERVAL:
      result["type"] = "interval";
      return result;
    case parquet::LogicalType::Type::INT:
      switch (node->converted_type()) {
      case parquet::ConvertedType::INT_8:
        result["type"] = "int8";
        return result;
      case parquet::ConvertedType::INT_16:
        result["type"] = "int16";
        return result;
      case parquet::ConvertedType::INT_32:
        result["type"] = "int32";
        return result;
      case parquet::ConvertedType::INT_64:
        result["type"] = "int64";
        return result;
      default:
        result["type"] = "unknown";
        return result;
      }
    case parquet::LogicalType::Type::NIL: // Thrift NullType
      result["type"] = "nil";
      return result;
    case parquet::LogicalType::Type::JSON:
      result["type"] = "json";
      return result;
    case parquet::LogicalType::Type::BSON:
      result["type"] = "bson";
      return result;
    case parquet::LogicalType::Type::UUID:
      result["type"] = "uuid";
      return result;
    case parquet::LogicalType::Type::NONE:
      break;
    case parquet::LogicalType::Type::UNDEFINED:
      break;
    }
  }

  if (node->converted_type() == parquet::ConvertedType::NONE) {
    switch (node->physical_type()) {
    case parquet::Type::BOOLEAN:
      result["type"] = "bool";
      return result;
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      result["type"] = "array";
      result["more"] = "more";
      return result;
    case parquet::Type::BYTE_ARRAY:
      result["type"] = "bytes";
      result["more"] = "more";
      return result;
    case parquet::Type::INT32:
      result["type"] = "int32";
      return result;
    case parquet::Type::INT64:
      result["type"] = "int64";
      return result;
    case parquet::Type::DOUBLE:
      result["type"] = "double";
      return result;
    case parquet::Type::FLOAT:
      result["type"] = "float";
      return result;
    default:
      result["type"] = "unknown";
      return result;
    }
  }
  result["type"] = "unknown";
  return result;
}

nlohmann::json
to_json_schema(std::string class_name,
               const parquet::SchemaDescriptor *schemaDescriptor) {
  auto group_node = schemaDescriptor->group_node();
  json j;
  j["type"] = "record";
  j["name"] = class_name;
  for (int i = 0; i != group_node->field_count(); ++i) {
    j["fields"].push_back(to_json(group_node->field(i)));
  }
  return j;
}

nlohmann::json
to_json_schema(std::string class_name,
               std::shared_ptr<const ::parquet::schema::GroupNode> group_node) {
  json j;
  j["type"] = "record";
  j["name"] = class_name;
  for (int i = 0; i != group_node->field_count(); ++i) {
    j["fields"].push_back(to_json(group_node->field(i)));
  }
  return j;
}

std::shared_ptr<::parquet::schema::GroupNode>
json_to_group_node(nlohmann::json j) {
  parquet::schema::NodeVector fields;

  std::string name = j["name"];
  std::string type = j["type"];
  nlohmann::json jfields = j["fields"];
  // iterate the array
  for (json::iterator it = jfields.begin(); it != jfields.end(); ++it) {
    // std::cout << *it << '\n';
    //  if ((*it).find("name") == jfields.end())
    //   throw...
    //  if ((*it).find("type") == jfields.end())
    //   throw...
    std::string name = (*it)["name"];
    std::string type = (*it)["type"];
    bool is_optional = (*it).value("optional", false);
    parquet::Repetition::type repetition = is_optional
                                               ? parquet::Repetition::OPTIONAL
                                               : parquet::Repetition::REQUIRED;
    if (type == "string") {
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          name, repetition, parquet::Type::BYTE_ARRAY,
          parquet::ConvertedType::UTF8));
    } else if (type == "bool") {
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          name, repetition, parquet::Type::BOOLEAN,
          parquet::ConvertedType::NONE));
    } else if (type == "int8") {
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          name, repetition, parquet::Type::INT32,
          parquet::ConvertedType::INT_8));
    } else if (type == "int16") {
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          name, repetition, parquet::Type::INT32,
          parquet::ConvertedType::INT_16));
    } else if (type == "int32") {
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          name, repetition, parquet::Type::INT32,
          parquet::ConvertedType::INT_32));
    } else if (type == "int64") {
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          name, repetition, parquet::Type::INT64,
          parquet::ConvertedType::INT_64));
    } else if (type == "float") {
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          name, repetition, parquet::Type::FLOAT,
          parquet::ConvertedType::NONE));
    } else if (type == "double") {
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          name, repetition, parquet::Type::DOUBLE,
          parquet::ConvertedType::NONE));
    } else if (type == "timestamp_ms") {
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          name, repetition, parquet::Type::INT64,
          parquet::ConvertedType::TIMESTAMP_MILLIS));
    } else if (type == "timestamp_us") {
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          name, repetition, parquet::Type::INT64,
          parquet::ConvertedType::TIMESTAMP_MICROS));
    } else if (type == "time32_ms") {
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          name, repetition,
          parquet::LogicalType::Time(false,
                                     parquet::LogicalType::TimeUnit::MILLIS),
          parquet::Type::INT32));
    } else if (type == "date32") {
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          name, repetition, parquet::Type::INT32,
          parquet::ConvertedType::DATE));
    } else if (type == "decimal") {
      // if ((*it).find("precision") == jfields.end())
      //  throw...
      // if ((*it).find("scale") == jfields.end())
      //  throw...
      // if ((*it).find("physical_size") == jfields.end())
      //  throw...
      int precision = (*it)["precision"];
      int scale = (*it)["scale"];
      int physical_size = (*it)["physical_size"];
      assert(physical_size == 32 || physical_size == 64); // throw
      parquet::Type::type primitive_type =
          (physical_size == 32) ? parquet::Type::INT32 : parquet::Type::INT64;
      fields.push_back(parquet::schema::PrimitiveNode::Make(
          name, repetition, parquet::LogicalType::Decimal(precision, scale),
          primitive_type));
    } else {
      std::cerr << "+++ missing +++  for name " << name << std::endl;
      // throw;
    }
  }

  return std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED,
                                       fields));
}

std::shared_ptr<::parquet::schema::GroupNode>
load_parquet_schema(std::string filename) {
  if (!std::filesystem::exists(filename)) {
    LOG(ERROR) << "cannot open " << filename << std::endl;
    return nullptr;
  }
  std::ifstream i(filename);
  json j;
  i >> j;
  std::shared_ptr<parquet::schema::GroupNode> parquet_schema =
      qtr::json_to_group_node(j);
  return parquet_schema;
}

nlohmann::json
schema_to_json(std::shared_ptr<parquet::schema::PrimitiveNode> node) {
  using json = nlohmann::json;
  json result;
  result["name"] = node->name();
  result["optional"] = node->is_optional();
  if (node->is_repeated())
    result["repeated"] = true;
  result["logical_type"] = node->logical_type()->ToString();
  result["converted_type"] = ConvertedTypeToString(node->converted_type());
  result["physical_type"] = TypeToString(node->physical_type());
  return result;
}
} // namespace qtr