#include "page_cache.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/reactor.hh>

#include "hashing/xx.h"
#include "prometheus/prometheus_sanitize.h"
// generated by tools/generate_page_table.py
#include "page_cache.generated.h"

using page_range_ptr = page_cache::page_range_ptr;

static inline constexpr int64_t
rounded_limit(double ratio, int64_t memory_lcore) {
  return seastar::align_up<int64_t>(static_cast<int64_t>(ratio * memory_lcore),
                                    page_cache_buffer_manager::kBufferSize);
}

page_cache &
page_cache::get() {
  static thread_local page_cache c(
    rounded_limit(0.3, seastar::memory::stats().total_memory()),
    rounded_limit(0.6, seastar::memory::stats().total_memory()));
  return c;
}
page_cache::~page_cache() { DLOG_INFO("{}", _stats); }
page_cache::page_cache(int64_t min_reserve, int64_t max_limit)
  : _mngr(min_reserve, max_limit),
    _reclaimer([this] { return reclaim_region(); },
               seastar::memory::reclaimer_scope::sync) {
  // set metrics
  namespace sm = seastar::metrics;
  _metrics.add_group(
    prometheus_sanitize::metrics_name("page_cache::"),
    {sm::make_derive("disk_bytes_read", _stats.disk_bytes_read,
                     sm::description("bytes read from disk")),
     sm::make_derive("served_bytes", _stats.served_bytes,
                     sm::description("bytes returned to user-space")),
     sm::make_derive("cache_hit", _stats.cache_hit,
                     sm::description("Number of cache hits")),
     sm::make_derive("cache_miss", _stats.cache_miss,
                     sm::description("Number of cache misses")),
     sm::make_gauge(
       "disk_latency_ema", [this] { return _stats.disk_latency.get(); },
       sm::description("exponential moving average<10> disk latency")),
     sm::make_derive(
       "stalls", _stats.stalls,
       sm::description("Number times we stalled waiting for read"))});
}

page_cache_file_idx *
page_cache::index(uint32_t file) {
  auto it = _files.find(file);
  if (it == _files.end()) {
    it =
      _files.emplace(file, std::make_unique<page_cache_file_idx>(file)).first;
  }
  return it->second.get();
}

/// \brief ONLY use if you want to auto-accounting for clearing page bits
page_cache_result *
page_cache::try_get(page_cache_request r) {
  auto ptr = index(r.file_id);
  if (auto ret = ptr->range(r.begin_pageno); ret) {
    if (ret->pending_reads > 0) {
      // NOTE: This is a bug/improvement to be made.
      // pending_reads should be tracked if and only if we have
      // seen the request_id for this cache request (some form of session)
      //
      // as is, this is just a soft number, still useful but needs improvement
      ret->pending_reads--;
    }
    return ret;
  }
  return nullptr;
}

void
page_cache::cache(uint32_t fileid, page_range_ptr ptr) {
  index(fileid)->cache(std::move(ptr));
}

seastar::future<>
page_cache::prefetch(page_cache_request r, page_cache_result::priority prio) {
  auto clamp = page_cache_table_clamp_page(r.begin_pageno);
  return seastar::with_semaphore(_mngr.lock(r.file_id, clamp), 1, [=] {
    return _mngr.allocate(clamp.first, prio)
      .then([this, r, clamp](page_range_ptr range_ptr) {
        if (auto ptr = try_get(r); ptr != nullptr) {
          ptr->pending_reads++;
          return seastar::make_ready_future<>();
        }
        ++_stats.prefetches;
        auto ptr = range_ptr.get();
        const int32_t page_offset = clamp.first * 4096;
        return r.fptr
          ->dma_read(page_offset, ptr->data.data(), ptr->data.size(), r.pc)
          .then([this, r, range_ptr = std::move(range_ptr),
                 m = _stats.disk_latency.measure()](std::size_t size) mutable {
            if (SMF_UNLIKELY(size == 0)) {
              LOG_ERROR("Got size 0 when reading page: {}. Usually it means "
                        "you are reading past the end of a physical file. Off "
                        "by one errors. Check the caller.",
                        size);
              return seastar::make_ready_future<>();
            }
            _stats.disk_bytes_read += size;
            ++_stats.prefetches;
            range_ptr->pending_reads++;
            range_ptr->data = {range_ptr->data.data(), int64_t(size)};
            cache(r.file_id, std::move(range_ptr));
            return seastar::make_ready_future<>();
          });
      });
  });
}
/// used by the wal_reader_node.cc: to remove a file from all caches
seastar::future<>
page_cache::remove_file(uint32_t fileid) {
  auto it = _files.find(fileid);
  if (it == _files.end()) return seastar::make_ready_future<>();
  _files.erase(it);
  return seastar::make_ready_future<>();
}

// ensures that the next set of pages are prefetched
// sets the next page as high priority, the one after as medium, the rest as
// evictable
void
page_cache::prefetch_next(page_cache_request r) {

  constexpr const int32_t kStridesOf256KB = 4; /*1MB of fetches*/

  auto next_first_page = [](int32_t pageno) {
    return pageno + (page_cache_buffer_manager::kBufferSize / 4096);
  };

  auto ptr = index(r.file_id);
  auto prio = page_cache_result::priority::high;

  for (int i = 0; i < kStridesOf256KB; ++i) {
    r.begin_pageno = next_first_page(r.begin_pageno);
    if (i != 0) { prio = page_cache_result::priority::low; }
    if (r.begin_pageno >= r.hint_file_last_pageno) { break; }
    if (auto range = ptr->range(r.begin_pageno); range) {
      range->prio = prio;
    } else {
      prefetch(r, prio);
    }
  }
}

// returns the usable bytes
seastar::future<page_cache_result_lease>
page_cache::read(page_cache_request r) {
  using ret_t = page_cache_result_lease;
  if (r.begin_pageno > r.end_pageno) {
    LOG_WARN("Asked for more data than request: {}", r);
    return seastar::make_ready_future<ret_t>(nullptr);
  }
  prefetch_next(r);

  if (auto ptr = try_get(r); ptr != nullptr) {
    ++_stats.cache_hit;
    _stats.served_bytes += ptr->data.size();
    return seastar::make_ready_future<ret_t>(ptr);
  }
  // not in cache, have to wait for it.
  ++_stats.cache_miss;

  auto clamp = page_cache_table_clamp_page(r.begin_pageno);
  return seastar::with_semaphore(_mngr.lock(r.file_id, clamp), 1, [=] {
    return _mngr.allocate(clamp.first, page_cache_result::priority::low)
      .then([this, r, clamp](page_range_ptr range_ptr) {
        if (auto ptr = try_get(r); ptr != nullptr) {
          _stats.served_bytes += ptr->data.size();
          return seastar::make_ready_future<ret_t>(ptr);
        }
        ++_stats.stalls;
        auto ptr = range_ptr.get();
        const int32_t page_offset = clamp.first * 4096;
        return r.fptr
          ->dma_read(page_offset, ptr->data.data(), ptr->data.size(), r.pc)
          .then([this, r, ptr, range_ptr = std::move(range_ptr),
                 m = _stats.disk_latency.measure()](auto size) mutable {
            if (SMF_UNLIKELY(size == 0)) {
              LOG_ERROR("Got size 0 when reading page: {}. Usually it means "
                        "you are reading past the end of a physical file. Off "
                        "by one errors. Check the caller.",
                        size);
              return seastar::make_ready_future<ret_t>(nullptr);
            }
            _stats.disk_bytes_read += size;
            // Update the span
            range_ptr->data = {range_ptr->data.data(), int64_t(size)};
            cache(r.file_id, std::move(range_ptr));
            return seastar::make_ready_future<ret_t>(ptr);
          });
      });
  });
}

/// \brief called during low memory pressure
seastar::memory::reclaiming_result
page_cache::reclaim_region() {
  if (_mngr.total_alloc_bytes() <= _mngr.min_memory_reserved) {
    // always keep at least 30% of memory used
    return seastar::memory::reclaiming_result::reclaimed_nothing;
  }
  _mngr.decrement_buffers();
  auto it = _files.begin();
  std::advance(it, _rng() % _files.size());
  for (uint32_t i = 0, max = _files.size(); i < max; ++i) {
    if (it == _files.end()) { it = _files.begin(); }
    auto opt = it->second->try_evict();
    if (opt) { return seastar::memory::reclaiming_result::reclaimed_something; }
    ++it;
  }
  return seastar::memory::reclaiming_result::reclaimed_nothing;
}

void
page_cache::evict_pages(uint32_t fileid, std::set<int32_t> pages) {
  if (_files.empty()) { return; };
  auto it = _files.find(fileid);
  if (it == _files.end()) { return; }
  it->second->evict_pages(std::move(pages));
}
