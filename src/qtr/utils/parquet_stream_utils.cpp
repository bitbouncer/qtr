/*
 * copyright svante karlsson at csi dot se 2021
 */
#include "qtr/utils/parquet_stream_utils.h"
#include <arrow/io/file.h>
#include <glog/logging.h>
#include "qtr/utils/arrow_fs_utils.h"
#include "qtr/utils/arrow_schema_utils.h"

std::shared_ptr<parquet::StreamWriter> create_parquet_output_stream(
    std::string uri, std::shared_ptr<parquet::schema::GroupNode> group_node) {
  auto dst_res = qtr::get_filesystem_from_uri(uri);
  auto dst_fs = std::get<0>(dst_res);
  std::string dst_path = std::get<1>(dst_res);

  std::shared_ptr<arrow::io::OutputStream> output_file;
  PARQUET_ASSIGN_OR_THROW(output_file, dst_fs->OpenOutputStream(dst_path));

  /*
  : pool_(::arrow::default_memory_pool()),
      dictionary_pagesize_limit_(DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT),
      write_batch_size_(DEFAULT_WRITE_BATCH_SIZE),
      max_row_group_length_(DEFAULT_MAX_ROW_GROUP_LENGTH),
      pagesize_(kDefaultDataPageSize),
      version_(ParquetVersion::PARQUET_1_0),
      data_page_version_(ParquetDataPageVersion::V1),
  */
  // enable_dictionary(true);
  parquet::WriterProperties::Builder builder;
  builder.compression(parquet::Compression::SNAPPY);
  // builder.compression(parquet::Compression::ZSTD);
  // builder.compression(parquet::Compression::LZ4);
  //  builder.write_batch_size(parquet::DEFAULT_WRITE_BATCH_SIZE);
  //  builder.max_row_group_length(parquet::DEFAULT_MAX_ROW_GROUP_LENGTH);
  builder.max_row_group_length(1000000);
  // builder.data_pagesize(parquet::kDefaultDataPageSize);
  // builder.data_page_version(parquet::ParquetDataPageVersion::V2);
  // builder.version(parquet::ParquetVersion::PARQUET_2_0);
  auto file_writer = parquet::ParquetFileWriter::Open(output_file, group_node,
                                                      builder.build());
  // PrintSchema(group_node.get(), std::cout);
  return std::make_shared<parquet::StreamWriter>(std::move(file_writer));
}

std::tuple<std::shared_ptr<parquet::StreamReader>,
           std::shared_ptr<parquet::schema::GroupNode>>
create_parquet_input_stream(std::string uri) {
  auto src_res = qtr::get_filesystem_from_uri(uri);
  auto src_fs = std::get<0>(src_res);
  std::string src_path = std::get<1>(src_res);
  std::shared_ptr<arrow::io::RandomAccessFile> input_file;
  PARQUET_ASSIGN_OR_THROW(input_file, src_fs->OpenInputFile(src_path));
  auto file_reader = parquet::ParquetFileReader::Open(input_file);

  std::stringstream s;
  PrintSchema(file_reader->metadata()->schema()->schema_root().get(), s);
  LOG(INFO) << uri << " opened, schema: " << s.str();

  auto schema_descriptor = file_reader->metadata()->schema();
  auto group_node = qtr::clone(schema_descriptor->group_node());
  return {std::make_shared<parquet::StreamReader>(std::move(file_reader)),
          group_node};
}
