#include <fstream>
#include <iostream>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <filesystem>
#include <qtr/parquet/parquet_primitive_row.h>
#include "qtr/schema/parquet_json_schema.h"
#include "qtr/io/parquet_file_sink.h"
#include "qtr/io/parquet_file_source.h"

using json = nlohmann::json;

int main(int argc, char *argv[]) {
  std::shared_ptr<parquet::schema::GroupNode> dst_parquet_schema = qtr::load_parquet_schema("/home/saka/source/crypto-tools-cpp/schemas/tardis_option_options_chain_v1.parquet-schema.json");
  //LOG(INFO)  << to_string(dst_parquet_schema);
  std::string src_filename = "/mnt/lab0/dataset-raw/tardis-parquet/deribit/OPTIONS/options_chain/2020/2020-01-01-deribit.OPTIONS.options_chain.parquet";
  /*
  if (!std::filesystem::exists(src_filename)){
    std::cerr << "cannot find " << src_filename << std::endl;
    return -1;
  }
  */
  auto source = qtr::parquet_file_source<qtr::parquet_primitive_row>(src_filename);
  auto sink = qtr::parquet_file_sink<qtr::parquet_primitive_row>("/tmp/tardis_option_options_chain.parquet", dst_parquet_schema);
  auto group_node = source.group_node();

  std::vector<std::string> src_column_names;
  for (int i =0; i!= group_node->field_count(); ++i){
    src_column_names.emplace_back(group_node->field(i)->name());
  }
  std::vector<int> src_index;

  auto dummy = qtr::parquet_primitive_row::make_unique(dst_parquet_schema);

  for (size_t i=0; i!=dummy->field_count(); ++i){
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
  std::vector<std::shared_ptr<parquet::schema::PrimitiveNode>> columns = dummy->primitive_nodes();

  while(auto src_row = source.get_next()) {
    auto dst_row = qtr::parquet_primitive_row::make_unique(dst_parquet_schema);

    for (size_t i=0; i!=dst_row->field_count(); ++i){
      auto item = src_row->value(src_index[i]);
      dst_row->assign(i, item);
    }
    sink.push_back(std::move(dst_row));
  }
  //Cleanup
  //source.close(); todo
  sink.flush();
  return 0;
}
