/*
* copyright svante karlsson at csi dot se 2021
*/
#include <parquet/arrow/schema.h>
#pragma once

namespace qtr {
 bool is_equal(std::shared_ptr<::parquet::schema::GroupNode> a, std::shared_ptr<::parquet::schema::GroupNode> b);

 bool is_equal(std::shared_ptr<::parquet::schema::Node> a, std::shared_ptr<::parquet::schema::Node> b);

 bool is_equal(std::shared_ptr<::parquet::schema::PrimitiveNode> a, std::shared_ptr<::parquet::schema::PrimitiveNode> b);

 // logging to help debugging
 bool verify_equal(std::shared_ptr<::parquet::schema::GroupNode> a, std::shared_ptr<::parquet::schema::GroupNode> b);

 // simplified json
 std::string to_string_v1(std::shared_ptr<::parquet::schema::GroupNode>);
 // deep arrow
 std::string to_string_v2(std::shared_ptr<::parquet::schema::GroupNode>);
}
