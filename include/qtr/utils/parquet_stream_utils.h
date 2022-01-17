/*
 * copyright svante karlsson at csi dot se 2021
 */
#include <parquet/stream_reader.h>
#include <parquet/stream_writer.h>
#pragma once

std::shared_ptr<parquet::StreamWriter> create_parquet_output_stream(
    std::string uri,
    std::shared_ptr<parquet::schema::GroupNode> parquet_schema);

std::tuple<std::shared_ptr<parquet::StreamReader>,
           std::shared_ptr<parquet::schema::GroupNode>>
create_parquet_input_stream(std::string uri);
