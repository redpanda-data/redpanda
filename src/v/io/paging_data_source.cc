/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "io/paging_data_source.h"

#include "base/vassert.h"
#include "io/pager.h"

#include <seastar/core/coroutine.hh>

namespace experimental::io {

paging_data_source::paging_data_source(pager* pager, config config)
  : pager_(pager)
  , offset_(config.offset)
  , remaining_(config.length) {}

paging_data_source::paging_data_source(
  std::vector<seastar::lw_shared_ptr<page>> pages, config config)
  : pager_(nullptr)
  , offset_(config.offset)
  , remaining_(config.length)
  , pages_(std::move(pages)) {
    std::reverse(pages_.begin(), pages_.end());
}

seastar::future<seastar::temporary_buffer<char>>
paging_data_source::get() noexcept {
    if (remaining_ == 0) {
        co_return seastar::temporary_buffer<char>();
    }

    /*
     * Hydrate the set of pages from the pager.
     */
    if (pages_.empty()) {
        if (pager_ != nullptr) {
            pages_ = co_await pager_->read({offset_, remaining_});
            std::reverse(pages_.begin(), pages_.end());
        }
        if (pages_.empty()) {
            co_return seastar::temporary_buffer<char>();
        }
    }

    /*
     * pages are reversed since erasing the front of a vector is expensive. we
     * could also track the index or change pager iterface to provide something
     * like a std::deque which is more efficient.
     */
    pages_.back()->cache_hook.touch();
    const auto page = pages_.back();
    pages_.pop_back();

    auto buf = page->data().clone();
    vassert(
      !buf.empty(),
      "Attempted to access evicted page at offset {}",
      page->offset());

    buf.trim_front(offset_ - page->offset());
    if (buf.size() > remaining_) {
        buf.trim(remaining_);
    }

    offset_ += buf.size();
    remaining_ -= buf.size();

    co_return buf;
}

} // namespace experimental::io
