/*
 * copyright svante karlsson at csi dot se 2021
 */
#include <chrono>
#include <arrow/type.h>
#include <glog/logging.h>
#include "moodycamel/concurrentqueue.h"
#include "qtr/utils/parquet_stream_utils.h"
#pragma once

namespace qtr {
template <class V> class parquet_file_sink {
public:
  parquet_file_sink(std::string filename) : thread_([this]() { thread(); }) {
    impl_ = create_parquet_output_stream(filename, V::group_node());
    run_ = true;
  }

  parquet_file_sink(std::string filename,
                    std::shared_ptr<parquet::schema::GroupNode> group_node)
      : thread_([this]() { thread(); }) {
    impl_ = create_parquet_output_stream(filename, group_node);
    run_ = true;
  }

  ~parquet_file_sink() {
    flush();
    exit_ = true;
    thread_.join();
    LOG(INFO) << "closing, total_msg: " << input_count_
              << ", remaining: " << queue_.size_approx();
  }

  inline size_t queue_size() const { return queue_.size_approx(); }

  inline void push_back(std::unique_ptr<const V> row) {
    queue_.enqueue(std::move(row));
    input_count_ += 1;
  }

  inline void push_back(std::vector<std::unique_ptr<const V>> &v) {
    queue_.enqueue_bulk(v.begin(), v.size());
    input_count_ += v.size();
  }

  void flush() {
    using namespace std::chrono_literals;
    LOG(INFO) << "flushing";
    while (queue_size()) {
      std::this_thread::sleep_for(100ms);
    }
  }

private:
  void thread() {
    using namespace std::chrono_literals;
    while (!run_) {
      std::this_thread::sleep_for(100ms);
    }
    while (!exit_) {
      std::unique_ptr<const V> p;
      // todo try bulk dequeue
      bool result = queue_.try_dequeue(p);
      if (!result) {
        std::this_thread::sleep_for(10ms);
        continue;
      }
      encode(*p, *impl_);
    }
  }
  bool run_ = false;
  bool exit_ = false;
  moodycamel::ConcurrentQueue<std::unique_ptr<const V>> queue_;
  size_t input_count_ = 0;
  std::thread thread_;
  std::shared_ptr<parquet::StreamWriter> impl_;
};
} // namespace qtr
