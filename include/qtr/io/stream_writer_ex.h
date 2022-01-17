/*
 * copyright svante karlsson at csi dot se 2021
 */
#include <date/date.h>
#include <parquet/stream_writer.h>
#include "nonstd/decimal.h"
#include <qtr/parquet/parquet_types.h>
#pragma once

namespace parquet {
class UnsafeStreamWriter : public StreamWriter {
  explicit UnsafeStreamWriter() = delete;

public:
  using StreamWriter::CheckColumn;
  using StreamWriter::SkipOptionalColumn;
  using StreamWriter::Write;

public:
};

inline StreamWriter &operator<<(StreamWriter &stream, const localtime32_ms &v) {
  // CheckColumn(Type::INT32,ConvertedType::TIME_MILLIS); //todo check out if
  // "NONE" is right CheckColumn(Type::INT32,ConvertedType::NONE);
  static_cast<UnsafeStreamWriter *>(&stream)->Write<Int32Writer>(
      static_cast<int32_t>(v));
  return stream;
}

inline StreamWriter &operator<<(StreamWriter &stream, const date::sys_days &v) {
  static_cast<UnsafeStreamWriter *>(&stream)->CheckColumn(Type::INT32,
                                                          ConvertedType::DATE);
  static_cast<UnsafeStreamWriter *>(&stream)->Write<Int32Writer>(
      static_cast<int32_t>(v.time_since_epoch().count())); // verify saka
  return stream;
}

inline StreamWriter &operator<<(StreamWriter &stream,
                                const date::local_days &v) {
  static_cast<UnsafeStreamWriter *>(&stream)->CheckColumn(Type::INT32,
                                                          ConvertedType::DATE);
  static_cast<UnsafeStreamWriter *>(&stream)->Write<Int32Writer>(
      static_cast<int32_t>(v.time_since_epoch().count())); // verify saka utc !?
  return stream;
}

inline StreamWriter &operator<<(StreamWriter &stream,
                                const nonstd::decimal32 &v) {
  static_cast<UnsafeStreamWriter *>(&stream)->CheckColumn(
      Type::INT32, ConvertedType::DECIMAL);
  static_cast<UnsafeStreamWriter *>(&stream)->Write<Int32Writer>(v.value_);
  return stream;
}

inline StreamWriter &operator<<(StreamWriter &stream,
                                const nonstd::decimal64 &v) {
  static_cast<UnsafeStreamWriter *>(&stream)->CheckColumn(
      Type::INT64, ConvertedType::DECIMAL);
  static_cast<UnsafeStreamWriter *>(&stream)->Write<Int64Writer>(v.value_);
  return stream;
}

template <unsigned int precision, unsigned int scale>
inline PARQUET_EXPORT StreamWriter &
operator<<(StreamWriter &stream,
           const nonstd::decimal<int32_t, precision, scale> &v) {
  static_cast<UnsafeStreamWriter *>(&stream)->CheckColumn(
      Type::INT32, ConvertedType::DECIMAL);
  static_cast<UnsafeStreamWriter *>(&stream)
      ->Write<TypedColumnWriter<PhysicalType<Type::INT32>>>(v.get_rep());
  return stream;
}

template <unsigned int precision, unsigned int scale>
inline PARQUET_EXPORT StreamWriter &
operator<<(StreamWriter &stream,
           const nonstd::decimal<int64_t, precision, scale> &v) {
  static_cast<UnsafeStreamWriter *>(&stream)->CheckColumn(
      Type::INT64, ConvertedType::DECIMAL);
  static_cast<UnsafeStreamWriter *>(&stream)->Write<Int64Writer>(v.get_rep());
  return stream;
}

inline void WriteUnsafe_INT8(StreamWriter &stream, const int32_t &val) {
  stream << ((int8_t)val);
}

inline void WriteUnsafe_INT16(StreamWriter &stream, const int32_t &val) {
  stream << ((int16_t)val);
}

inline void WriteUnsafe_INT32(StreamWriter &stream, const int32_t &val) {
  static_cast<UnsafeStreamWriter *>(&stream)
      ->Write<TypedColumnWriter<PhysicalType<Type::INT32>>>(val);
}

inline void WriteUnsafe_INT64(StreamWriter &stream, const int64_t &val) {
  static_cast<UnsafeStreamWriter *>(&stream)
      ->Write<TypedColumnWriter<PhysicalType<Type::INT64>>>(val);
}

// conversion between std::optional -> nonstd::optional
template <class T>
parquet::StreamWriter &operator<<(parquet::StreamWriter &stream,
                                  const std::optional<T> &v) {
  if (v) {
    return stream << (*v);
  }
  static_cast<UnsafeStreamWriter *>(&stream)->SkipOptionalColumn();
  return stream;
}
} // namespace parquet
