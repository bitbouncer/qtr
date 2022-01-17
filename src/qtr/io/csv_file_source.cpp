/*
 * copyright svante karlsson at csi dot se 2021
 */
#include "qtr/io/csv_file_source.h"
#include <filesystem>
#include <fstream>
#include <boost/tokenizer.hpp>
#include <glog/logging.h>

namespace qtr {

typedef boost::tokenizer<boost::escaped_list_separator<char>,
                         std::string::const_iterator, std::string>
    Tokenizer;

inline bool ends_with(std::string const &value, std::string const &ending) {
  if (ending.size() > value.size())
    return false;
  return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
}

// TODO fix so we can use arrow fs stuff ie a s3 bucket
csv_file_source::csv_file_source(std::string filename)
    : seps_('\\', ',', '\"') {
  if (!std::filesystem::exists(filename)) {
    LOG(ERROR) << "cannot find " << filename << std::endl;
    throw std::runtime_error("cannot find " + filename);
  }

  if (ends_with(filename, ".gz")) {
    // Read from the first command line argument, assume it's gzipped
    file_ = std::make_unique<std::ifstream>(
        filename, std::ios_base::in | std::ios_base::binary);
    inbuf_ = std::make_unique<
        boost::iostreams::filtering_streambuf<boost::iostreams::input>>();
    inbuf_->push(boost::iostreams::gzip_decompressor());
    inbuf_->push(*file_.get());
    // Convert streambuf to istream
    instream_ = std::make_unique<std::istream>(inbuf_.get());
  } else if (ends_with(filename, ".csv")) {
    instream_ = std::make_unique<std::ifstream>(
        filename, std::ios_base::in | std::ios_base::binary);
  } else {
    throw std::runtime_error("dont know how to parse " + filename);
  }
  // get column names
  // tokenizer

  // consume 1st line (ie header)
  std::string line;
  std::getline(*instream_, line);
  Tokenizer tok(line, seps_);
  for (auto i : tok)
    column_names_.emplace_back(i);

  // read first line
  read_row_();
}

csv_file_source::~csv_file_source() {}

std::vector<std::string> csv_file_source::get_row() {
  std::vector<std::string> v = std::move(next_row_);
  read_row_();
  return v;
}

void csv_file_source::read_row_() {
  std::string line;
  if (std::getline(*instream_, line)) {
    Tokenizer tok(line, seps_);
    next_row_.clear();
    for (auto i : tok)
      next_row_.emplace_back(i);
  } else {
    eof_ = true;
  }
}

} // namespace qtr
