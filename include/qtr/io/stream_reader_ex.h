/*
 * copyright svante karlsson at csi dot se 2021
 */
#include <date/date.h>
#include <nonstd/decimal.h>
#include <parquet/stream_reader.h>
#include <qtr/parquet/parquet_types.h>
#pragma once

namespace parquet {
class UnsafeStreamReader : public StreamReader {
  explicit UnsafeStreamReader() = delete;

public:
  using StreamReader::CheckColumn;
  using StreamReader::Read;
  using StreamReader::ReadOptional;
};

inline StreamReader &operator>>(StreamReader &stream, nonstd::decimal32 &v) {
  static_cast<UnsafeStreamReader *>(&stream)->CheckColumn(
      Type::INT32, ConvertedType::DECIMAL);
  int32_t tmp;
  static_cast<UnsafeStreamReader *>(&stream)->Read<Int32Reader>(&tmp);
  v.value_ = tmp;
  return stream;
}

inline StreamReader &operator>>(StreamReader &stream, nonstd::decimal64 &v) {
  static_cast<UnsafeStreamReader *>(&stream)->CheckColumn(
      Type::INT64, ConvertedType::DECIMAL);
  int64_t tmp;
  static_cast<UnsafeStreamReader *>(&stream)->Read<Int64Reader>(&tmp);
  v.value_ = tmp;
  return stream;
}

inline StreamReader &operator>>(StreamReader &stream, localtime32_ms &v) {
  // CheckColumn(Type::INT32,ConvertedType::TIME_MILLIS); todo check out if
  // "NONE" is right
  // CheckColumn(Type::INT32,ConvertedType::ConvertedType::NONE);
  int32_t tmp;
  static_cast<UnsafeStreamReader *>(&stream)->Read<Int32Reader>(&tmp);
  v = static_cast<localtime32_ms>(tmp);
  return stream;
}

inline StreamReader &operator>>(StreamReader &stream, date::local_days &v) {
  static_cast<UnsafeStreamReader *>(&stream)->CheckColumn(Type::INT32,
                                                          ConvertedType::DATE);
  int32_t tmp;
  static_cast<UnsafeStreamReader *>(&stream)->Read<Int32Reader>(&tmp);
  v = date::local_days{} + date::days{tmp};
  return stream;
}

inline StreamReader &operator>>(StreamReader &stream, date::sys_days &v) {
  static_cast<UnsafeStreamReader *>(&stream)->CheckColumn(Type::INT32,
                                                          ConvertedType::DATE);
  int32_t tmp;
  static_cast<UnsafeStreamReader *>(&stream)->Read<Int32Reader>(&tmp);
  v = date::sys_days{} + date::days{tmp};
  return stream;
}

template <unsigned int precision, unsigned int scale>
inline PARQUET_EXPORT StreamReader &
operator>>(StreamReader &stream,
           nonstd::decimal<int32_t, precision, scale> &v) {
  static_cast<UnsafeStreamReader *>(&stream)->CheckColumn(
      Type::INT32, ConvertedType::DECIMAL);
  int32_t tmp = 0;
  static_cast<UnsafeStreamReader *>(&stream)->Read<Int32Reader>(&tmp);
  v = nonstd::decimal<int32_t, precision, scale>::make_from_rep(tmp);
  return stream;
}

template <unsigned int precision, unsigned int scale>
inline PARQUET_EXPORT StreamReader &
operator>>(StreamReader &stream,
           nonstd::decimal<int64_t, precision, scale> &v) {
  static_cast<UnsafeStreamReader *>(&stream)->CheckColumn(
      Type::INT64, ConvertedType::DECIMAL);
  int64_t tmp = 0;
  static_cast<UnsafeStreamReader *>(&stream)->Read<Int64Reader>(&tmp);
  v = nonstd::decimal<int64_t, precision, scale>::make_from_rep(tmp);
  return stream;
}

template <unsigned int precision, unsigned int scale>
inline PARQUET_EXPORT StreamReader &
operator>>(StreamReader &stream,
           nonstd::optional<nonstd::decimal<int32_t, precision, scale>> &v) {
  static_cast<UnsafeStreamReader *>(&stream)->CheckColumn(
      Type::INT32, ConvertedType::DECIMAL);
  nonstd::optional<int32_t> tmp = 0;
  static_cast<UnsafeStreamReader *>(&stream)
      ->ReadOptional<Int32Reader, int32_t>(&tmp);
  if (tmp)
    v = nonstd::decimal<int32_t, precision, scale>::make_from_rep(*tmp);
  else
    v.reset();
  return stream;
}

template <unsigned int precision, unsigned int scale>
inline PARQUET_EXPORT StreamReader &
operator>>(StreamReader &stream,
           nonstd::optional<nonstd::decimal<int64_t, precision, scale>> &v) {
  static_cast<UnsafeStreamReader *>(&stream)->CheckColumn(
      Type::INT64, ConvertedType::DECIMAL);
  nonstd::optional<int64_t> tmp = 0;
  static_cast<UnsafeStreamReader *>(&stream)
      ->ReadOptional<Int64Reader, int64_t>(&tmp);
  if (tmp)
    v = nonstd::decimal<int64_t, precision, scale>::make_from_rep(*tmp);
  else
    v.reset();
  return stream;
}

template <class T>
StreamReader &operator>>(StreamReader &stream, std::optional<T> &v) {
  nonstd::optional<T> tmp;
  stream >> tmp;
  if (tmp)
    v = *tmp;
  else
    v.reset();
  return stream;
}

} // namespace parquet
