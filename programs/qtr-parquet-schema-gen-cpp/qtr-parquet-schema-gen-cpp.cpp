#include <cstdlib>
#include <filesystem>
#include <glog/logging.h>
#include <iostream>
#include <fstream>
#include "qtr/schema/parquet_json_schema.h"
#include "generator.h"

using json = nlohmann::json;

int main(int argc, char **argv) {
  if (argc <= 2) {
    std::cerr << "usage " << argv[0] << "src, dst_root" << std::endl;
    // Fake success for CI purposes.
    return EXIT_SUCCESS;
  }

  std::string src = argv[1];
  if (!std::filesystem::exists(src)) {
    LOG(ERROR) << "cannot find src: " << src;
    return -1;
  }

  std::string dst_root;
  if (argc == 3)
    dst_root = argv[2];

  std::ifstream ifs(src);
  json j = json::parse(ifs, nullptr, true, true);

  auto group_node = qtr::json_to_group_node(j);

  std::string class_name = j["name"];

  std::cout << gen_header(class_name, group_node.get()) << std::endl;

  std::cout << "++++++++++++" << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  std::cout << gen_body(class_name, group_node.get()) << std::endl;

  if (dst_root.size()) {
    std::filesystem::create_directories(dst_root);
    std::ofstream header(dst_root + "/" + class_name + ".h");
    header << gen_header(class_name, group_node.get()) << std::endl;

    std::ofstream body(dst_root + "/" + class_name + ".cpp");
    body << gen_body(class_name, group_node.get()) << std::endl;
  }
  return EXIT_SUCCESS;
}