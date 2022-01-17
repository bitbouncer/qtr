/*
 * copyright svante karlsson at csi dot se 2021
 */
#include <glog/logging.h>
#include <moodycamel/concurrentqueue.h>
#include <qtr/utils/arrow_schema_utils.h>
#include <qtr/utils/parquet_stream_utils.h>
#pragma once

// todo figure out how to push s3 select range queries on timestamps
namespace qtr {
template <class V> class parquet_file_source {
public:
  parquet_file_source(std::string uri)
      : thread_([this]() { thread(); }), queue_(), uri_(uri) {
    std::tuple<std::shared_ptr<parquet::StreamReader>,
               std::shared_ptr<parquet::schema::GroupNode>>
        tuple = create_parquet_input_stream(uri);
    stream_reader_ = std::get<0>(tuple);
    group_node_ = std::get<1>(tuple);
    assert(stream_reader_ != nullptr);
    assert(group_node_ != nullptr);
    run_ = true;
  }

  ~parquet_file_source() {
    exit_ = true;
    thread_.join();
    LOG(INFO) << "closing " << uri_ << ", read: " << nr_of_rows_;
  }

  std::shared_ptr<parquet::schema::GroupNode> group_node() const {
    return group_node_;
  }

  inline int64_t next_event_time() const { return queue_.next_event_time(); }

  inline bool eof() const {
    return (queue_.size_approx() == 0 && stream_reader_->eof());
  }

  bool need_to_wait() const {
    return (queue_.empty() && !stream_reader_->eof());
  }

  std::unique_ptr<const V> get_next() {
    using namespace std::chrono_literals;
    while (true) {
      std::unique_ptr<V> p;
      bool result = queue_.try_dequeue(p);
      if (!result) {
        if (eof())
          return nullptr;
        std::this_thread::sleep_for(10ms);
        continue;
      }
      return p;
    }
  };

private:
  void thread() {
    using namespace std::chrono_literals;
    while (!run_) {
      std::this_thread::sleep_for(100ms);
    }

    nr_of_rows_ = 0;
    while (!stream_reader_->eof()) {
      auto row = V::make_unique(group_node_);
      decode(*row, *stream_reader_);
      queue_.enqueue(std::move(row));
      ++nr_of_rows_;
      if (exit_)
        break;
    }

    // bulk mode todo
    /*
    //std::vector<std::unique_ptr<const V>> buffer;
    for (nr_of_rows_ = 0; !stream_reader_->eof(); ++nr_of_rows_) {
      auto row = std::make_unique<V>();
      decode(*row, *stream_reader_);
      buffer.push_back(row);
      if (buffer.size() > 100) {
        queue_.push_back(buffer);
        buffer.clear();
        buffer.reserve(100);
      }
      while (!exit_ && queue_.size() > max_in_queue_) {
        std::this_thread::sleep_for(10ms);
      }

      if (exit_)
        break;
    }
    */
    // stream_reader_->close();
    // stream_reader_->.reset();
  }

  bool run_ = false;
  bool exit_ = false;
  std::thread thread_;
  moodycamel::ConcurrentQueue<std::unique_ptr<V>> queue_;
  std::string uri_;
  std::shared_ptr<parquet::StreamReader> stream_reader_;
  std::shared_ptr<parquet::schema::GroupNode> group_node_;
  size_t nr_of_rows_ = 0;
  // size_t max_in_queue_ = 100000;
};

} // namespace qtr
