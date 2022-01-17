#include <parquet/schema.h>
#pragma once

std::string gen_header(std::string class_name,
                       const parquet::schema::GroupNode* group_node);

std::string gen_body(std::string class_name,
                       const parquet::schema::GroupNode* group_node);

