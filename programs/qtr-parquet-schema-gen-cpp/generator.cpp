#include "generator.h"
#include "type_conv.h"
#include <glog/logging.h>

std::string gen_header(std::string class_name,
                       const parquet::schema::GroupNode* group_node) {
  std::stringstream s;
  s << "/**\n";
  s << " * automatic generated file\n";
  s << " */\n";
  s << "\n";
  s << "#include <optional>\n";
  s << "#include <date/date.h>\n";
  s << "#include <parquet/schema.h>\n";
  s << "#include <parquet/stream_reader.h>\n";
  s << "#include <parquet/stream_writer.h>\n";
  s << "#include <nonstd/decimal.h>\n";
  s << "#include <qtr/parquet/parquet_types.h>\n";
  s << "#pragma once\n";
  s << "\n";
  s << "struct " << class_name << " {\n";
  for (int i = 0; i != group_node->field_count(); ++i)
    s << "  " << to_cpp_type(group_node->field(i)) << " "
      << to_cpp_name(group_node->field(i)) << ";\n";
  s << "\n";
  s << "  " << class_name << "() = default;\n";
  //s << "\n";
  //s << "  static std::shared_ptr<arrow::Schema> arrow_schema();\n";
  s << "\n";
  s << "  static inline std::unique_ptr<" << class_name << ">\n";
  s << "  make_unique(__attribute__((unused)) std::shared_ptr<parquet::schema::GroupNode>){\n";
  s << "    return std::make_unique<" << class_name << ">();\n";
  s << "  }\n";
  s << "\n";
  s << "  static std::shared_ptr<parquet::schema::GroupNode> group_node();\n";
  s << "};\n"; // end of class definition
  s << "\n";
  s << "void encode(const " << class_name
    << "& obj, parquet::StreamWriter &stream);\n";
  s << "\n";
  s << "void decode(" << class_name
    << "& obj, parquet::StreamReader &stream);\n";
  s << "\n";
  s << "\n";
  return s.str();
}


std::string gen_body(std::string class_name,
                       const parquet::schema::GroupNode* group_node){
  std::stringstream s;
  s << "/**\n";
  s << " * automatic generated file\n";
  s << " */\n";
  s << "\n";
  s << "#include \"" << class_name << ".h\"\n";
  s << "#include <arrow/type_traits.h>\n";
  s << "#include <arrow/builder.h>\n";
  s << "#include <qtr/io/stream_writer_ex.h>\n";
  s << "#include <qtr/io/stream_reader_ex.h>\n";
  //s << "#include <arrow/table.h>\n";

  s << "\n";
  s << "static std::shared_ptr<parquet::schema::GroupNode> make_group_node(){\n";
  s << "  using namespace parquet;\n";
  s << "  using namespace parquet::schema;\n";
  s << "  NodeVector nv;\n";
  for (int i = 0; i != group_node->field_count(); ++i)
    s << "  nv.push_back(make_node(decltype("
      << class_name << "::" << to_cpp_name(group_node->field(i)) << ")(), \""
      << group_node->field(i)->name() << "\"));\n";
  s << "  return std::static_pointer_cast<GroupNode>(GroupNode::Make(\"schema\", "
       "Repetition::REQUIRED, nv));\n";
  s << "}\n";
  s << "\n";
  s << "std::shared_ptr<parquet::schema::GroupNode> " << class_name << "::group_node(){\n";
  s << "  static std::shared_ptr<parquet::schema::GroupNode> group_node_s = make_group_node();\n";
  s << "  return group_node_s;\n";
  s << "}\n";
  s << "\n";
  /*
   s << "std::shared_ptr<arrow::Schema> " << class_name << "::arrow_schema(){\n";
  s << "  static std::shared_ptr<arrow::Schema> arrow_schema_s = arrow::schema({\n";
  for (int i = 0; i != group_node->field_count(); ++i) {
    s << "      arrow::field(\"" << group_node->field(i)->name() << "\", "
      << to_arrow_type(group_node->field(i)) << ", "
      << std::to_string(group_node->field(i)->is_optional()) << ")";
    if (i != group_node->field_count() - 1)
      s << ",\n";
    else
      s << "\n";
  }
  s << "  });\n";
  s << "  return arrow_schema_s;\n";
  s << "}\n"; // end of arrow_schema()
  s << "\n";
  */

  s << "void encode(const " << class_name
    << "& obj, parquet::StreamWriter &stream){\n";
  for (int i = 0; i != group_node->field_count(); ++i)
    s << "  stream << obj." << to_cpp_name(group_node->field(i)) << ";\n";
  s << "  stream.EndRow();\n";
  s << "}\n";
  s << "\n";
  s << "void decode(" << class_name
    << "& obj, parquet::StreamReader &stream){\n";
  for (int i = 0; i != group_node->field_count(); ++i)
    s << "  stream >> obj." << to_cpp_name(group_node->field(i)) << ";\n";
  s << "  stream.EndRow();\n";
  s << "}\n";

  s << "\n";
  return s.str();
}