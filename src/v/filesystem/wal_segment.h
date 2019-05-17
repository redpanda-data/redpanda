#pragma once

#include <numeric>

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/sstring.hh>
#include <smf/log.h>
#include <smf/macros.h>


/// \brief modeled after seastar/core/fstream.cc:file_data_sink_impl
class wal_segment {
 public:
  static constexpr const int64_t kChunkSize = 4096 * 31;
  class chunk {
   public:
    SMF_DISALLOW_COPY_AND_ASSIGN(chunk);

    explicit chunk(const std::size_t alignment)
      : buf_(seastar::allocate_aligned_buffer<char>(kChunkSize, alignment)) {}
    inline bool
    is_full() const {
      return pos_ == kChunkSize;
    }
    inline std::size_t
    space_left() const {
      return kChunkSize - pos_;
    }
    inline std::size_t
    pos() const {
      return pos_;
    }
    inline std::size_t
    append(const char *src, std::size_t sz) {
      sz = sz > space_left() ? space_left() : sz;
      std::memcpy(buf_.get() + pos_, src, sz);
      pos_ += sz;
      return sz;
    }
    inline const char *
    data() const {
      return buf_.get();
    }

   private:
    std::unique_ptr<char[], seastar::free_deleter> buf_;
    std::size_t pos_{0};
  };

  SMF_DISALLOW_COPY_AND_ASSIGN(wal_segment);
  wal_segment(seastar::sstring filename, const seastar::io_priority_class &prio,
              int64_t max_file_size, int32_t max_unflushed_bytes);
  ~wal_segment();

  seastar::future<> open();
  seastar::future<> close();
  seastar::future<> flush();
  seastar::future<> append(const char *buf, const std::size_t n);
  inline int64_t
  current_size() const {
    auto sz = flushed_pages_ * kChunkSize;
    return std::accumulate(
      chunks_.begin(), chunks_.end(), sz,
      [](auto acc, auto &next) { return acc + next->pos(); });
  }

  const seastar::sstring filename;
  const seastar::io_priority_class &pclass;
  const int64_t max_file_size;
  const int32_t max_unflushed_bytes;

 private:
  /// \brief unsafe. use flush()
  /// assumes that we hold the sem/lock for flushing
  /// dequeues all the full pages, and copies the last one!
  ///
  seastar::future<> do_flush();

 private:
  bool closed_{false};
  bool is_fully_flushed_{true};
  int64_t bytes_pending_{0};
  seastar::semaphore append_sem_{1};

  std::deque<std::unique_ptr<chunk>> chunks_;
  seastar::lw_shared_ptr<seastar::file> f_;
  // cache of f_->disk_write_dma_alignment()
  int32_t dma_write_alignment_{0};
  // \brief - only modify this variable under the semaphore
  //
  int64_t flushed_pages_{0};
};

