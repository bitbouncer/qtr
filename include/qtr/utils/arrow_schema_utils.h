/*
 * copyright svante karlsson at csi dot se 2021
 */
#include <parquet/arrow/schema.h>
#pragma once

namespace qtr {
std::shared_ptr<::parquet::schema::GroupNode>
    to_group_node(std::shared_ptr<arrow::Schema>);

std::shared_ptr<::parquet::schema::GroupNode>
clone(const ::parquet::schema::GroupNode *);

} // namespace qtr