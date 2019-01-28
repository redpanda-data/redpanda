#include "wal_segment.h"

#include <boost/iterator/counting_iterator.hpp>
#include <seastar/core/align.hh>
#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>
#include <smf/human_bytes.h>
#include <smf/log.h>

#include "hbadger/hbadger.h"

namespace rp {

wal_segment::wal_segment(seastar::sstring name,
                         const seastar::io_priority_class &prio,
                         int64_t max_file_bytes, int32_t unflushed_bytes,
                         int32_t concurrent_writes)
  : filename(name), pclass(prio), max_file_size(max_file_bytes),
    max_unflushed_bytes(unflushed_bytes), concurrency(concurrent_writes) {
  append_sem_.ensure_space_for_waiters(1);
}

wal_segment::~wal_segment() {
  LOG_ERROR_IF(!closed_, "***POSSIBLE DATA LOSS*** file was not closed. {}",
               filename);
}

seastar::future<>
wal_segment::open() {
  HBADGER(filesystem, wal_segment::open);
  return file_exists(filename).then([this](bool filename_exists) {
    LOG_THROW_IF(filename_exists,
                 "File: {} already exists. Cannot re-open for writes",
                 filename);
    auto flags = seastar::open_flags::rw | seastar::open_flags::create;
    return seastar::open_file_dma(filename, flags)
      .then([this](seastar::file f) {
        f_ = seastar::make_lw_shared(std::move(f));
        dma_write_alignment_ = f_->disk_write_dma_alignment();
        auto const max_size = seastar::align_up<int64_t>(
          max_file_size, f_->disk_write_dma_alignment());
        DLOG_TRACE("fallocating: {} for log segment",
                   smf::human_bytes(max_size));
        return f_->allocate(0, max_size);
      });
  });
}
seastar::future<>
wal_segment::close() {
  DLOG_THROW_IF(closed_, "File lease already closed. Bug: {}", filename);
  closed_ = true;
  return flush()
    .then([this] {
      DLOG_TRACE("Truncating at offset: {}", current_size());
      return f_->truncate(current_size());
    })
    .then([f = f_] { return f->close().finally([f] {}); });
}
/// \brief
seastar::future<>
wal_segment::do_flush() {
  HBADGER(filesystem, wal_segment::do_flush);
  std::vector<std::unique_ptr<chunk>> to_flush;
  to_flush.reserve(chunks_.size());
  for (uint32_t i = 0u, max = chunks_.rbegin()->get()->is_full()
                                ? chunks_.size()
                                : chunks_.size() - 1;
       i < max; ++i) {
    to_flush.push_back(std::move(chunks_.front()));
    chunks_.pop_front();
  }
  // check if we need to copy last page
  if (!chunks_.empty()) {
    auto dst = std::make_unique<chunk>(dma_write_alignment_);
    auto src = chunks_.rbegin()->get();
    dst->append(src->data(), src->pos());
    to_flush.push_back(std::move(dst));
  }
  return seastar::do_with(
    seastar::semaphore(concurrency), std::move(to_flush),
    [this](auto &limit, auto &pages_to_flush) {
      using iter_t = decltype(pages_to_flush.begin());
      return seastar::do_for_each(
               std::move_iterator<iter_t>(pages_to_flush.begin()),
               std::move_iterator<iter_t>(pages_to_flush.end()),
               [this, &limit](auto page) mutable {
                 return seastar::with_semaphore(
                   limit, 1, [this, p = std::move(page)]() mutable {
                     bool is_full = p->is_full();
                     auto xoffset = flushed_pages_ * dma_write_alignment_;
                     DLOG_TRACE("dma_offset:{}, page:{}, size:{}", xoffset,
                                flushed_pages_, p->pos());
                     DLOG_THROW_IF(
                       (xoffset & (dma_write_alignment_ - 1)) != 0,
                       "Start offset is not page aligned. Severe bug");

                     // after the logs
                     flushed_pages_++;

                     // send in background - safely!!
                     //
                     auto dptr = p->data();
                     auto dsz = dma_write_alignment_;
                     f_->dma_write(xoffset, dptr, dsz, pclass)
                       .then([this, is_full, p = std::move(p),
                              xoffset](int64_t written_bytes) {
                         DLOG_THROW_IF(
                           written_bytes != dma_write_alignment_,
                           "Wrote incorrect number of bytes written: {}",
                           written_bytes);

                         if (!is_full) {
                           // Note: this does not need a lock because by this
                           // time we have already dispatched all writes, since
                           // this can only be possibly the last write dispatch
                           flushed_pages_--;
                         }
                         return seastar::make_ready_future<>();
                       });
                   });
               })
        .then([this, &limit] {
          return limit.wait(concurrency).then([this] { return f_->flush(); });
        });
    });
}

seastar::future<>
wal_segment::flush() {
  HBADGER(filesystem, wal_segment::flush);
  if (is_fully_flushed_ || chunks_.empty()) {
    return seastar::make_ready_future<>();
  }
  return seastar::with_semaphore(append_sem_, 1,
                                 [this] {
                                   // need to double check
                                   if (chunks_.empty()) {
                                     return seastar::make_ready_future<>();
                                   }
                                   // do the real flush
                                   return do_flush();
                                 })
    .finally([this] {
      bytes_pending_ = 0;
      is_fully_flushed_ = true;
    });
}

seastar::future<>
wal_segment::append(const char *buf, const std::size_t origi_sz) {
  HBADGER(filesystem, wal_segment::append);
  is_fully_flushed_ = false;
  if (chunks_.empty()) {
    // NOTE: we need to be careful about getting memory and use a semaphore
    chunks_.push_back(std::make_unique<chunk>(dma_write_alignment_));
  }
  std::size_t n = origi_sz;
  while (n > 0) {
    auto it = chunks_.rbegin()->get();
    if (it->is_full()) {
      chunks_.push_back(std::make_unique<chunk>(dma_write_alignment_));
      continue;
    }
    n -= it->append(buf + (origi_sz - n), n);
  }
  bytes_pending_ += origi_sz;

  if (bytes_pending_ < max_unflushed_bytes) {
    return seastar::make_ready_future<>();
  }
  return flush();
}

}  // namespace rp
