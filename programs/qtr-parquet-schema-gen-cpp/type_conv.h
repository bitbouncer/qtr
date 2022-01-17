#include <parquet/schema.h>
#pragma once

std::string inner_cpp_type(parquet::schema::NodePtr nodePtr);
std::string to_cpp_type(parquet::schema::NodePtr nodePtr);
std::string to_cpp_name(parquet::schema::NodePtr nodePtr);

std::string to_arrow_type(parquet::schema::NodePtr nodePtr);
std::string to_arrow_data_type(parquet::schema::NodePtr nodePtr);
std::string to_arrow_conv(parquet::schema::NodePtr nodePtr, std::string name);