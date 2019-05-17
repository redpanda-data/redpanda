#include "wal_disk_pager.h"

#include <seastar/core/prefetch.hh>
#include <smf/log.h>

// filesystem
#include "page_cache.h"


wal_disk_pager::wal_disk_pager(page_cache_request r) : req_(r) {}

wal_disk_pager::wal_disk_pager(wal_disk_pager &&o) noexcept
  : req_(std::move(o.req_)), lease_(std::move(o.lease_)) {}

wal_disk_pager::~wal_disk_pager() {}

seastar::future<const page_cache_result *>
wal_disk_pager::fetch_next() {
  if (lease_ && lease_.result) {
    req_.begin_pageno =
      std::min<int32_t>(lease_.result->end_pageno(), req_.end_pageno);
  }
  return page_cache::get().read(req_).then([this](auto range) {
    LOG_THROW_IF(!range, "Got an empty range");
    DLOG_THROW_IF(!range.result->is_page_in_range(req_.begin_pageno),
                  "Bad fetch from page_cache. req:{} - not in range - {}", req_,
                  *range.result);
    lease_ = range;
    // ask the prefetcher to start to bring the data for page
    // has ~16-20% ins/sec improvement
    // this has no temporal locality. remove after use via 0
    seastar::prefetch<const char, 0>(lease_.result->data.data());
    return seastar::make_ready_future<const page_cache_result *>(lease_.result);
  });
}
