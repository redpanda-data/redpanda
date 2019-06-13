#include "filesystem/wal_segment.h"

#include "hbadger/hbadger.h"

#include <seastar/core/align.hh>
#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>

#include <smf/human_bytes.h>
#include <smf/log.h>

#include <boost/iterator/counting_iterator.hpp>

static constexpr inline int64_t chunk_size(int64_t alignment) {
    // 126.98KB
    return /*alignment*/ 4096 * 31;
}

wal_segment::wal_segment(
  seastar::sstring name,
  const seastar::io_priority_class& prio,
  int64_t max_file_bytes,
  int32_t unflushed_bytes)
  : filename(name)
  , pclass(prio)
  , max_file_size(max_file_bytes)
  , max_unflushed_bytes(unflushed_bytes) {
    append_sem_.ensure_space_for_waiters(1);
}

wal_segment::~wal_segment() {
    LOG_ERROR_IF(
      !_closed, "***POSSIBLE DATA LOSS*** file was not closed. {}", filename);
}

seastar::future<> wal_segment::open() {
    HBADGER(filesystem, wal_segment::open);
    return file_exists(filename).then([this](bool filename_exists) {
        LOG_THROW_IF(
          filename_exists,
          "File: {} already exists. Cannot re-open for writes",
          filename);
        auto flags = seastar::open_flags::wo | seastar::open_flags::create;
        seastar::file_open_options open_opts;
        // sets xfs extent size
        // attr.fsx_xflags |= XFS_XFLAG_EXTSIZE;
        // attr.fsx_extsize = options.extent_allocation_size_hint;
        open_opts.extent_allocation_size_hint = max_file_size;
        return seastar::open_file_dma(filename, flags, std::move(open_opts))
          .then([this](seastar::file f) {
              _f = seastar::make_lw_shared(std::move(f));
              dma_write_alignment_ = _f->disk_write_dma_alignment();
              auto const max_size = seastar::align_up<int64_t>(
                max_file_size, dma_write_alignment_);
              DLOG_TRACE(
                "fallocating: {} for log segment", smf::human_bytes(max_size));
              return _f->allocate(0, max_size);
          });
    });
}
seastar::future<> wal_segment::close() {
    DLOG_THROW_IF(_closed, "File lease already closed. Bug: {}", filename);
    _closed = true;
    return flush()
      .then([this] {
          DLOG_TRACE("Truncating at offset: {}", current_size());
          return _f->truncate(current_size());
      })
      .then([this, f = _f] {
          auto sz = current_size();
          return f->close().finally([f, sz, name = filename] {
              if (sz == 0) {
                  LOG_INFO("Removing empty segment: {} ", name);
                  /// launch in the background
                  return seastar::remove_file(name).then_wrapped(
                    [](auto _) { /*ignore*/ });
              }
              return seastar::make_ready_future<>();
          });
      });
}
/// \brief
// NOTE: using _disk*READ*_dma_alignment is on purpose.
// it minimizes the wasted IO on the device, up to 40%
// wasted otherwise
// auto dsz =
//   seastar::align_up<int64_t>(p->pos(), _f->disk_read_dma_alignment());
seastar::future<> wal_segment::do_flush() {
    HBADGER(filesystem, wal_segment::do_flush);

    auto verify_fn_gen = [](int64_t expected_write_bytes) {
        return [=](int64_t real_write_bytes) {
            DLOG_THROW_IF(
              expected_write_bytes != real_write_bytes,
              "Wrote incorrect number of bytes. Expected:{},  Written:{}",
              expected_write_bytes,
              real_write_bytes);
            return seastar::make_ready_future<>();
        };
    };
    auto max_waiters = _chunks.size();
    auto flush_after = seastar::make_lw_shared<seastar::semaphore>(max_waiters);
    for (int32_t i = 0, max = _chunks.size(); i < max; ++i) {
        auto const xoffset = flushed_pages_ * kChunkSize;
        auto const position = _chunks.front()->pos();
        DLOG_THROW_IF(
          (xoffset & (dma_write_alignment_ - 1)) != 0,
          "Start offset is not page aligned. Severe bug");
        DLOG_TRACE(
          "dma_offset:{}, page:{}, size:{}", xoffset, flushed_pages_, position);
        auto dsz = seastar::align_up<int64_t>(
          position, _f->disk_read_dma_alignment());

        if (!_chunks.front()->is_full()) {
            auto dptr = _chunks.front()->data();
            flush_after->wait(1).then(
              [this, xoffset, dptr, dsz, verify_fn_gen, flush_after] {
                  return _f->dma_write(xoffset, dptr, dsz, pclass)
                    .then(verify_fn_gen(dsz))
                    .finally([flush_after] { flush_after->signal(1); });
              });
        } else {
            auto p = std::move(_chunks.front());
            _chunks.pop_front();
            flushed_pages_++;
            auto dptr = p->data();
            // Background future
            flush_after->wait(1).then([this,
                                       xoffset,
                                       dptr,
                                       dsz,
                                       p = std::move(p),
                                       verify_fn_gen,
                                       flush_after]() mutable {
                return _f->dma_write(xoffset, dptr, dsz, pclass)
                  .then(verify_fn_gen(dsz))
                  .finally([p = std::move(p), flush_after] {
                      flush_after->signal(1);
                  });
            });
        }
    }
    return flush_after->wait(max_waiters).then([this] { return _f->flush(); });
}

seastar::future<> wal_segment::flush() {
    HBADGER(filesystem, wal_segment::flush);
    if (is_fully_flushed_ || _chunks.empty() || bytes_pending_ == 0) {
        return seastar::make_ready_future<>();
    }
    return seastar::with_semaphore(append_sem_, 1, [this] {
        // need to double check
        if (_chunks.empty() || bytes_pending_ == 0) {
            return seastar::make_ready_future<>();
        }
        // do the real flush
        return do_flush().finally([this] {
            bytes_pending_ = 0;
            is_fully_flushed_ = true;
        });
    });
}

seastar::future<>
wal_segment::append(const char* buf, const std::size_t origi_sz) {
    HBADGER(filesystem, wal_segment::append);
    is_fully_flushed_ = false;
    if (_chunks.empty()) {
        // NOTE: we need to be careful about getting memory and use a semaphore
        _chunks.push_back(std::make_unique<chunk>(dma_write_alignment_));
    }
    std::size_t n = origi_sz;
    while (n > 0) {
        auto it = _chunks.rbegin()->get();
        if (it->is_full()) {
            _chunks.push_back(std::make_unique<chunk>(dma_write_alignment_));
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
