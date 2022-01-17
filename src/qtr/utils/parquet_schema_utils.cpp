/*
 * copyright svante karlsson at csi dot se 2021
 */
#include "qtr/schema/parquet_json_schema.h"
#include "qtr/utils/parquet_schema_utils.h"
#include <glog/logging.h>

namespace qtr {
bool is_equal(std::shared_ptr<::parquet::schema::GroupNode> a,
              std::shared_ptr<::parquet::schema::GroupNode> b) {
  // arrow seems to have a strange equals
  // return a->Equals(static_cast<parquet::schema::Node *>(b.get()));
  if (a->field_count() != b->field_count())
    return false;

  for (int i = 0; i != a->field_count(); ++i) {
    if (!is_equal(a->field(i), b->field(i)))
      return false;
  }
  return true;
}

bool is_equal(std::shared_ptr<::parquet::schema::Node> a,
              std::shared_ptr<::parquet::schema::Node> b) {
  if (a->name() != b->name())
    return false;
  if (a->is_primitive() != b->is_primitive())
    return false;
  if (a->is_repeated() != b->is_repeated())
    return false;
  if (a->is_required() != b->is_required())
    return false;
  if (a->logical_type()->type() != b->logical_type()->type())
    return false;
  if (a->converted_type() != b->converted_type())
    return false;

  if (a->is_primitive()) {
    auto primitive_a = static_cast<parquet::schema::PrimitiveNode *>(a.get());
    auto primitive_b = static_cast<parquet::schema::PrimitiveNode *>(b.get());

    if (primitive_a->physical_type() != primitive_b->physical_type())
      return false;
  }
  return true;
}

bool is_equal(std::shared_ptr<::parquet::schema::PrimitiveNode> a,
              std::shared_ptr<::parquet::schema::PrimitiveNode> b) {
  if (a->name() != b->name())
    return false;
  if (a->is_repeated() != b->is_repeated()) // primitive??
    return false;
  if (a->is_required() != b->is_required())
    return false;
  if (a->logical_type()->type() != b->logical_type()->type())
    return false;
  if (a->converted_type() != b->converted_type())
    return false;
  if (a->physical_type() != b->physical_type())
    return false;
  return true;
}

static void
debug_log_output_node(std::shared_ptr<::parquet::schema::Node> node) {
  if (node->is_primitive()) {
    auto primitive_node =
        static_cast<parquet::schema::PrimitiveNode *>(node.get());
    LOG(INFO) << "name: " << node->name()
              << ", logical type: " << node->logical_type()->ToString()
              << ", converted type: "
              << ConvertedTypeToString(node->converted_type());
    TypeToString(primitive_node->physical_type());
  } else {
    LOG(INFO) << "name: " << node->name()
              << ", logical type: " << node->logical_type()->ToString()
              << ", converted type: "
              << ConvertedTypeToString(node->converted_type());
  }
}

bool verify_equal(std::shared_ptr<::parquet::schema::GroupNode> a,
                  std::shared_ptr<::parquet::schema::GroupNode> b) {
  if (a->field_count() != b->field_count())
    return false;

  for (int i = 0; i != a->field_count(); ++i) {
    if (!is_equal(a->field(i), b->field(i))) {
      auto from_a = a->field(i);
      auto from_b = b->field(i);
      debug_log_output_node(from_a);
      debug_log_output_node(from_b);
      return false;
    }
  }
  return true;
}

// pretty print lohman json?

std::string to_string_v1(std::shared_ptr<::parquet::schema::GroupNode> schema) {
  auto json = to_json_schema(schema->name(), schema);
  return to_string(json);
}

std::string to_string_v2(std::shared_ptr<::parquet::schema::GroupNode> schema) {
  std::string s;
  s += "{ \"name\" : " + schema->name() + ",\"fields\":[";
  for (int i = 0; i != schema->field_count(); ++i) {
    const auto &node = schema->field(i);
    s += "{\"name\":" + node->name();
    s += ",\"logical type\":" + node->logical_type()->ToString();
    s += ",\"converted type\":" + ConvertedTypeToString(node->converted_type());
    if (node->is_primitive()) {
      auto primitive_node =
          static_cast<parquet::schema::PrimitiveNode *>(node.get());
      s += ",\"physical type\": ";
      s += TypeToString(primitive_node->physical_type());
    }
    s += "},"; // todo fix trailing ,
  }
  s += "]}";
  return s;
}
} // namespace qtr