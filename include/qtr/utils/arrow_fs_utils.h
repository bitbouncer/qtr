/*
 * copyright svante karlsson at csi dot se 2021
 */
#include <tuple>
#include <arrow/api.h>
#include <arrow/filesystem/filesystem.h>
#pragma once

namespace qtr {
std::string get_absolute_path(std::string uri);

std::tuple<std::shared_ptr<arrow::fs::FileSystem>, std::string>
get_filesystem_from_uri(std::string uri);
} // namespace qtr
