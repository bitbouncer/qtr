/*
 * copyright svante karlsson at csi dot se 2021
 */
#include <qtr/utils/string_utils.h>
#include <iomanip>
#include <strstream>

namespace std {
std::string to_string(parquet::localtime32_ms t) {
  std::string s;
  int hour = static_cast<int32_t>(t) / 3600000;
  int minute = (static_cast<int32_t>(t) - (hour * 3600000)) / 60000;
  int second =
      (static_cast<int32_t>(t) - (hour * 3600000) - minute * 60000) / 1000;
  int ms = (static_cast<int32_t>(t) - (hour * 3600000) - minute * 60000 -
            second * 1000);
  char buf[128];
  sprintf(buf, "%02d:%02d:%02d.%03d", hour, minute, second, ms);
  return buf;
}
} // namespace std

using namespace std;
using namespace chrono;

std::string timestamp_ns_to_string(int64_t ts_ns) {
  int64_t ts_ms = ts_ns / 1000000;
  const auto durationSinceEpoch = std::chrono::milliseconds(ts_ms);
  const time_point<system_clock> tp_after_duration(durationSinceEpoch);
  time_t time_after_duration = system_clock::to_time_t(tp_after_duration);
  std::tm tm = *std::gmtime(&time_after_duration);
  // std::tm *formattedTime = std::localtime(&time_after_duration);
  // long long int milliseconds_remainder = ts_ms % 1000;
  std::stringstream s;
  s << put_time(&tm, "%Y-%m-%d %H:%M:%S"); // we drop ms here??
  // s << std::put_time(&tm, "%c %Z");
  return s.str();
}
