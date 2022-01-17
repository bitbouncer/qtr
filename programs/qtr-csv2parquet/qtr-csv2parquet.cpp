#include <iostream>
#include <filesystem>
#include <qtr/parquet/parquet_primitive_row.h>
#include "qtr/utils/csv_utils.h"
#include "qtr/schema/parquet_json_schema.h"
#include "qtr/io/parquet_file_sink.h"
#include "qtr/io/csv_file_source.h"
using json = nlohmann::json;

int main(int argc, char *argv[]) {
  if (argc<4){
    std::cerr << "usage: " << argv[0] << " schema source destination [converter]" << std::endl;
    return -1;
  }
  std::string schema_json =  argv[1];
  std::string src_filename = argv [2];
  std::string dst_filename = argv [3];

  std::shared_ptr<parquet::schema::GroupNode> parquet_schema = qtr::load_parquet_schema(schema_json);
  LOG(INFO) << qtr::to_json_schema("dummy", parquet_schema);


  qtr::csv_file_source csv_source(src_filename);
  auto src_column_names = csv_source.column_names();

  auto sink = qtr::parquet_file_sink<qtr::parquet_primitive_row>(dst_filename, parquet_schema);

  // calc mapping between src and dst names
  std::vector<int> src_index;
  auto dummy = qtr::parquet_primitive_row::make_unique(parquet_schema);

  for (size_t i=0; i!=dummy->field_count(); ++i){
    //std::cout << dummy->name(i) << std::endl;

    int index=0;
    bool found = false;
    for (std::vector<std::string>::const_iterator j= src_column_names.begin(); j!=src_column_names.end(); ++j, ++index){
      if (dummy->name(i) == *j){
        src_index.push_back(index);
        found = true;
        break;
      }
    }
    if (!found){
      std::cerr << "could not find column: " << dummy->name(i) << " in source, exiting " << std::endl;
      return -1;
    }
  }

  //for (size_t i=0; i!=dummy->field_count(); ++i){
  //  std::cout << "column: " << dummy->name(i) << ", from index " << src_index[i] << std::endl;
  //}
  //std::cout << std::endl;

  std::vector<std::shared_ptr<parquet::schema::PrimitiveNode>> primitive_nodes = dummy->primitive_nodes();

  while(!csv_source.eof()) {
    auto csv_row = csv_source.get_row();
    auto row = qtr::parquet_primitive_row::make_unique(parquet_schema);
    for (size_t i=0; i!=row->field_count(); ++i){
      auto v = qtr::csv_utils::parse_from_string(primitive_nodes[i], csv_row[src_index[i]]); // todo move to generic parquet???
      row->assign(i, v);
    }
    sink.push_back(std::move(row));
  }
  sink.flush();
  return 0;
}
