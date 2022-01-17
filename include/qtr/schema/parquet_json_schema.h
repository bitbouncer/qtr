/*
 * copyright svante karlsson at csi dot se 2021
 */
#include <parquet/arrow/schema.h>
#include <nlohmann/json.hpp>
#pragma once

namespace qtr {

// this is qtr "own" schema - todo rename
nlohmann::json to_json_schema(std::string class_name,
                              const parquet::SchemaDescriptor *);
nlohmann::json
to_json_schema(std::string class_name,
               std::shared_ptr<const ::parquet::schema::GroupNode> group_node);

std::shared_ptr<::parquet::schema::GroupNode>
    json_to_group_node(nlohmann::json);

std::shared_ptr<::parquet::schema::GroupNode>
load_parquet_schema(std::string filename);

// full parquet schema for a primitive node)
nlohmann::json
schema_to_json(std::shared_ptr<parquet::schema::PrimitiveNode> node);

} // namespace qtr
