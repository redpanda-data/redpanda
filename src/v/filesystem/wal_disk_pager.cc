#include "filesystem/wal_disk_pager.h"

#include "filesystem/page_cache.h"

#include <seastar/core/prefetch.hh>

#include <smf/log.h>

wal_disk_pager::wal_disk_pager(page_cache_request r)
  : _req(r) {
}

wal_disk_pager::wal_disk_pager(wal_disk_pager&& o) noexcept
  : _req(std::move(o._req))
  , _lease(std::move(o._lease)) {
}

wal_disk_pager::~wal_disk_pager() {
}

seastar::future<const page_cache_result*> wal_disk_pager::fetch_next() {
    if (_lease && _lease.result) {
        _req.begin_pageno = std::min<int32_t>(
          _lease.result->end_pageno(), _req.end_pageno);
    }
    return page_cache::get().read(_req).then([this](auto range) {
        LOG_THROW_IF(!range, "Got an empty range");
        DLOG_THROW_IF(
          !range.result->is_page_in_range(_req.begin_pageno),
          "Bad fetch from page_cache. req:{} - not in range - {}",
          _req,
          *range.result);
        _lease = range;
        // ask the prefetcher to start to bring the data for page
        // has ~16-20% ins/sec improvement
        // this has no temporal locality. remove after use via 0
        seastar::prefetch<const char, 0>(_lease.result->data.data());
        return seastar::make_ready_future<const page_cache_result*>(
          _lease.result);
    });
}
