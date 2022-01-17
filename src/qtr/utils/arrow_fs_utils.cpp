/*
 * copyright svante karlsson at csi dot se 2021
 */
#include "qtr/utils/arrow_fs_utils.h"
#include <filesystem>
#include <iostream>
#include <glog/logging.h>
#include <arrow/filesystem/s3fs.h>

// fixme
#define S3_REGION "us-east-1"

namespace qtr {

/* Get absolute path with workaround for missing files */
std::string get_absolute_path(std::string uri) {
  if (uri.size()) {
    std::filesystem::path p(uri);
    if (!std::filesystem::exists(p)) {
      std::filesystem::path base = std::filesystem::current_path();
      std::filesystem::path path = std::filesystem::absolute(base / p);
      path = std::filesystem::canonical(path.remove_filename()) / p.filename();
      return path.string();
    } else {
      return std::filesystem::canonical(p).string();
    }
  }
  return "";
}

std::tuple<std::shared_ptr<arrow::fs::FileSystem>, std::string>
get_filesystem_from_uri(std::string uri) {
  std::string filename_part;
  if (uri.find_first_of(':') != std::string::npos) {
    std::string endpoint_name = uri.substr(0, uri.find_first_of(':'));
    std::string expected_env = "qtr_ENDPOINT_" + endpoint_name;
    LOG(INFO) << "expected_env: " << expected_env;
    const char *env = getenv(expected_env.c_str());
    if (env) {
      // LOG(INFO) << "found : " << env;
      std::string s(env);
      std::string scheme = s.substr(0, s.find_first_of(':'));
      s = s.substr(scheme.size() + 3);
      std::string access_key = s.substr(0, s.find_first_of(':'));
      s = s.substr(access_key.size() + 1);
      std::string secret_key = s.substr(0, s.find_first_of('@'));
      s = s.substr(secret_key.size() + 1);
      std::string endpoint = s;

      if (scheme.size() && access_key.size() && secret_key.size() &&
          endpoint.size()) {
        LOG(INFO) << "found env match for endoint - overriding s3 fs";
        arrow::fs::EnsureS3Initialized();
        std::shared_ptr<arrow::fs::S3FileSystem> s3fs;
        arrow::fs::S3Options options;
        options = arrow::fs::S3Options::FromAccessKey(access_key, secret_key);
        options.endpoint_override = endpoint;
        options.scheme = scheme;
        options.region = S3_REGION; // we dont really care for this -or ??
        s3fs = arrow::fs::S3FileSystem::Make(options).ValueOrDie();

        std::string stripped_url = uri.substr(endpoint_name.size() + 1);
        std::string bucket =
            stripped_url.substr(0, stripped_url.find_first_of('/'));
        std::string rest_of_path =
            uri.substr(endpoint_name.size() + 1 + bucket.size() + 1);
        filename_part = rest_of_path;
        return {std::make_shared<arrow::fs::SubTreeFileSystem>(bucket, s3fs),
                filename_part};
      }
    }
  }

  if (uri.find_first_of(':') == std::string::npos)
    uri = "file://" + get_absolute_path(uri);

  auto fs = arrow::fs::FileSystemFromUri(uri, &filename_part).ValueOrDie();
  return {fs, filename_part};
}
} // namespace qtr
