#pragma once

// filesystem
#include "page_cache.h"

class wal_disk_pager {
 public:
  explicit wal_disk_pager(page_cache_request r);
  ~wal_disk_pager();
  wal_disk_pager(wal_disk_pager &&o) noexcept;
  SMF_DISALLOW_COPY_AND_ASSIGN(wal_disk_pager);

  const page_cache_request &
  request() const {
    return req_;
  }

  inline const page_cache_result *
  range() const {
    return lease_.result;
  }
  inline bool
  is_page_in_result_range(int32_t pageno) const {
    return lease_ && lease_.result->is_page_in_range(pageno);
  }
  inline bool
  is_page_in_request_range(int32_t pageno) const {
    return pageno >= req_.begin_pageno && pageno <= req_.end_pageno;
  }
  inline bool
  is_page_in_range(int32_t pageno) const {
    return is_page_in_request_range(pageno);
  }
  inline seastar::future<const page_cache_result *>
  fetch(int32_t pageno) {
    DLOG_THROW_IF(!is_page_in_request_range(pageno), "Buggy page: {}", pageno);
    if (is_page_in_result_range(pageno)) {
      return seastar::make_ready_future<const page_cache_result *>(
        lease_.result);
    }
    // split between hot code and cold code paths
    return fetch_next();
  }

 private:
  /// \brief fetches from page cache
  /// usually the cold path
  seastar::future<const page_cache_result *> fetch_next();

 private:
  page_cache_request req_;
  page_cache_result_lease lease_;
};
