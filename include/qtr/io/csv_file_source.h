/*
* copyright svante karlsson at csi dot se 2021
*/
#include <string>
#include <vector>
#include <memory>
#include <fstream>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/token_functions.hpp>
#pragma once

namespace qtr {
class csv_file_source {
public:
  // should probably have some parsing clues here like line separator etc
  csv_file_source(std::string uri);
  ~csv_file_source();
  const std::vector<std::string>& column_names() const {
    return column_names_;
  }

  bool eof() const {
    return eof_;
  }

  std::vector<std::string> get_row();

private:
  void read_row_();
  // TODO those replace with arrow stuff - this only handles gzip'ed csv's on normal fs
  // maybe this should go to an impl class instead
  std::unique_ptr<std::ifstream> file_;
  std::unique_ptr<std::istream> instream_;
  std::unique_ptr<boost::iostreams::filtering_streambuf<boost::iostreams::input>> inbuf_;
  boost::escaped_list_separator<char> seps_;


      bool eof_=false;
  std::vector<std::string> column_names_;
  std::vector<std::string> next_row_;
};

} // namespace qtr
