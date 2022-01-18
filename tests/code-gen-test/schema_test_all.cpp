/**
 * automatic generated file
 */

#include "schema_test_all.h"
#include <arrow/type_traits.h>
#include <arrow/builder.h>
#include <qtr/io/stream_writer_ex.h>
#include <qtr/io/stream_reader_ex.h>

static std::shared_ptr<parquet::schema::GroupNode> make_group_node(){
  using namespace parquet;
  using namespace parquet::schema;
  NodeVector nv;
  nv.push_back(make_node(decltype(schema_test_all::a_string)(), "a_string"));
  nv.push_back(make_node(decltype(schema_test_all::a_decimal32)(), "a_decimal32"));
  nv.push_back(make_node(decltype(schema_test_all::a_decimal64)(), "a_decimal64"));
  nv.push_back(make_node(decltype(schema_test_all::a_date32)(), "a_date32"));
  nv.push_back(make_node(decltype(schema_test_all::a_time32_ms)(), "a_time32_ms"));
  nv.push_back(make_node(decltype(schema_test_all::a_timestamp_ms)(), "a_timestamp_ms"));
  nv.push_back(make_node(decltype(schema_test_all::a_int8)(), "a_int8"));
  nv.push_back(make_node(decltype(schema_test_all::a_int16)(), "a_int16"));
  nv.push_back(make_node(decltype(schema_test_all::a_int32)(), "a_int32"));
  nv.push_back(make_node(decltype(schema_test_all::a_int64)(), "a_int64"));
  nv.push_back(make_node(decltype(schema_test_all::a_bool)(), "a_bool"));
  nv.push_back(make_node(decltype(schema_test_all::a_double)(), "a_double"));
  nv.push_back(make_node(decltype(schema_test_all::a_float)(), "a_float"));
  return std::static_pointer_cast<GroupNode>(GroupNode::Make("schema", Repetition::REQUIRED, nv));
}

std::shared_ptr<parquet::schema::GroupNode> schema_test_all::group_node(){
  static std::shared_ptr<parquet::schema::GroupNode> group_node_s = make_group_node();
  return group_node_s;
}

void encode(const schema_test_all& obj, parquet::StreamWriter &stream){
  stream << obj.a_string;
  stream << obj.a_decimal32;
  stream << obj.a_decimal64;
  stream << obj.a_date32;
  stream << obj.a_time32_ms;
  stream << obj.a_timestamp_ms;
  stream << obj.a_int8;
  stream << obj.a_int16;
  stream << obj.a_int32;
  stream << obj.a_int64;
  stream << obj.a_bool;
  stream << obj.a_double;
  stream << obj.a_float;
  stream.EndRow();
}

void decode(schema_test_all& obj, parquet::StreamReader &stream){
  stream >> obj.a_string;
  stream >> obj.a_decimal32;
  stream >> obj.a_decimal64;
  stream >> obj.a_date32;
  stream >> obj.a_time32_ms;
  stream >> obj.a_timestamp_ms;
  stream >> obj.a_int8;
  stream >> obj.a_int16;
  stream >> obj.a_int32;
  stream >> obj.a_int64;
  stream >> obj.a_bool;
  stream >> obj.a_double;
  stream >> obj.a_float;
  stream.EndRow();
}


