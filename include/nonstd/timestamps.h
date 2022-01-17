/*
 * copyright svante karlsson at csi dot se 2021
 */
#include <stdint.h>
#include <chrono>
#pragma once

namespace nonstd {
  class ns_duration_t {
  public:
    constexpr inline ns_duration_t() = default;

    inline ~ns_duration_t() = default;

    constexpr inline explicit ns_duration_t(int64_t duration) : value_(duration) {}

    constexpr inline ns_duration_t(const std::chrono::nanoseconds &b) : value_(b.count()) {}

    constexpr inline ns_duration_t(const std::chrono::microseconds &b) : value_(b.count() * 1000L) {}

    constexpr inline ns_duration_t(const std::chrono::milliseconds &b) : value_(b.count() * 1000000L) {}

    constexpr inline ns_duration_t(const std::chrono::seconds &b) : value_(b.count() * 1000000000L) {}

    constexpr explicit operator std::chrono::nanoseconds() { return std::chrono::nanoseconds(value_); }

    constexpr explicit operator std::chrono::microseconds() { return std::chrono::microseconds(value_ / 1000L); }

    constexpr explicit operator std::chrono::milliseconds() { return std::chrono::milliseconds(value_ / 1000000L); }

    constexpr explicit operator std::chrono::seconds() { return std::chrono::seconds(value_ / 1000000000L); }

    constexpr inline bool operator>(ns_duration_t b) const { return value_ > b.value_; }

    constexpr inline bool operator>=(ns_duration_t b) const { return value_ >= b.value_; }

    constexpr inline bool operator<(ns_duration_t b) const { return value_ < b.value_; }

    constexpr inline bool operator<=(ns_duration_t b) const { return value_ <= b.value_; }

    constexpr inline bool operator==(ns_duration_t b) const { return value_ == b.value_; }

    constexpr inline bool operator!=(ns_duration_t b) const { return value_ != b.value_; }

    constexpr inline ns_duration_t operator+(ns_duration_t duration) const {
      return ns_duration_t(value() + duration.value());
    }

    constexpr inline ns_duration_t operator-(ns_duration_t duration) const {
      return ns_duration_t(value() - duration.value());
    }

    constexpr inline ns_duration_t operator*(double val) const {
      return ns_duration_t(value() * val);
    }

    constexpr inline ns_duration_t operator/(double val) const {
      return ns_duration_t(value() / val);
    }

    constexpr inline int64_t value() const { return value_; }

  private:
    int64_t value_ = 0;
  };


  class utc_ns_since_epoch_t {
  public:
    constexpr inline utc_ns_since_epoch_t() = default;

    inline ~utc_ns_since_epoch_t() = default;

    constexpr inline explicit utc_ns_since_epoch_t(int64_t ts) : value_(ts) {}

    constexpr inline explicit utc_ns_since_epoch_t(const std::chrono::nanoseconds &b) : value_(b.count()) {}

    constexpr inline explicit utc_ns_since_epoch_t(const std::chrono::microseconds &b) : value_(b.count() * 1000L) {}

    constexpr inline explicit utc_ns_since_epoch_t(const std::chrono::milliseconds &b) : value_(b.count() * 1000000L) {}

    constexpr inline explicit utc_ns_since_epoch_t(const std::chrono::seconds &b) : value_(b.count() * 1000000000L) {}

    constexpr inline explicit operator std::chrono::nanoseconds() { return std::chrono::nanoseconds(value_); }

    constexpr inline explicit operator std::chrono::microseconds() { return std::chrono::microseconds(value_ / 1000L); }

    constexpr inline explicit operator std::chrono::milliseconds() {
      return std::chrono::milliseconds(value_ / 1000000L);
    }

    constexpr inline explicit operator std::chrono::seconds() { return std::chrono::seconds(value_ / 1000000000L); }

    constexpr inline bool operator>(utc_ns_since_epoch_t b) const { return value_ > b.value_; }

    constexpr inline bool operator>=(utc_ns_since_epoch_t b) const { return value_ >= b.value_; }

    constexpr inline bool operator<(utc_ns_since_epoch_t b) const { return value_ < b.value_; }

    constexpr inline bool operator<=(utc_ns_since_epoch_t b) const { return value_ <= b.value_; }

    constexpr inline bool operator==(utc_ns_since_epoch_t b) const { return value_ == b.value_; }

    constexpr inline bool operator!=(utc_ns_since_epoch_t b) const { return value_ != b.value_; }

    constexpr inline utc_ns_since_epoch_t operator+(ns_duration_t duration) const {
      return utc_ns_since_epoch_t(value() + duration.value());
    }

    constexpr inline utc_ns_since_epoch_t operator-(ns_duration_t duration) const {
      return utc_ns_since_epoch_t(value() - duration.value());
    }

    constexpr inline utc_ns_since_epoch_t &operator+=(ns_duration_t duration) {
      value_ += duration.value();
      return *this;
    }

    constexpr inline utc_ns_since_epoch_t &operator-=(ns_duration_t duration) {
      value_ -= duration.value();
      return *this;
    }

    constexpr inline int64_t value() const { return value_; }

  private:
    int64_t value_ = 0;
  };

  class localtime_ns_since_midnight_t {
  public:
    constexpr inline localtime_ns_since_midnight_t(const localtime_ns_since_midnight_t &b) : value_(b.value_) {}

    constexpr inline explicit localtime_ns_since_midnight_t(int64_t ts) : value_(ts) {}

    constexpr inline explicit localtime_ns_since_midnight_t(const std::chrono::nanoseconds &b) : value_(b.count()) {}

    constexpr inline explicit localtime_ns_since_midnight_t(const std::chrono::microseconds &b) : value_(
        b.count() * 1000L) {}

    constexpr inline explicit localtime_ns_since_midnight_t(const std::chrono::milliseconds &b) : value_(
        b.count() * 1000000L) {}

    constexpr inline explicit localtime_ns_since_midnight_t(const std::chrono::seconds &b) : value_(
        b.count() * 1000000000L) {}

    constexpr inline explicit operator std::chrono::nanoseconds() { return std::chrono::nanoseconds(value_); }

    constexpr inline explicit operator std::chrono::microseconds() { return std::chrono::microseconds(value_ / 1000L); }

    constexpr inline explicit operator std::chrono::milliseconds() {
      return std::chrono::milliseconds(value_ / 1000000L);
    }

    constexpr inline explicit operator std::chrono::seconds() { return std::chrono::seconds(value_ / 1000000000L); }


    constexpr inline bool operator>(localtime_ns_since_midnight_t b) const { return value_ > b.value_; }

    constexpr inline bool operator>=(localtime_ns_since_midnight_t b) const { return value_ >= b.value_; }

    constexpr inline bool operator<(localtime_ns_since_midnight_t b) const { return value_ < b.value_; }

    constexpr inline bool operator<=(localtime_ns_since_midnight_t b) const { return value_ <= b.value_; }

    constexpr inline bool operator==(localtime_ns_since_midnight_t b) const { return value_ == b.value_; }

    constexpr inline bool operator!=(localtime_ns_since_midnight_t b) const { return value_ != b.value_; }

    constexpr inline localtime_ns_since_midnight_t operator+(ns_duration_t duration) const {
      return localtime_ns_since_midnight_t(value() + duration.value());
    }

    constexpr inline localtime_ns_since_midnight_t operator-(ns_duration_t duration) const {
      return localtime_ns_since_midnight_t(value() - duration.value());
    }

    constexpr inline localtime_ns_since_midnight_t &operator+=(ns_duration_t duration) {
      value_ += duration.value();
      return *this;
    }

    constexpr inline localtime_ns_since_midnight_t &operator-=(ns_duration_t duration) {
      value_ -= duration.value();
      return *this;
    }


    constexpr inline int64_t value() const { return value_; }

  private:
    int64_t value_ = 0;
  };
} // namespace
