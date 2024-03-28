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
#include "io/pager.h"

#include "base/vassert.h"
#include "io/page.h"
#include "io/page_cache.h"
#include "io/scheduler.h"

#include <seastar/core/align.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <span>

namespace experimental::io {

pager::pager(
  std::filesystem::path file,
  size_t size,
  persistence* storage,
  page_cache* cache,
  scheduler* sched)
  : file_(std::move(file))
  , size_(size)
  , cache_(cache)
  , sched_(sched)
  , queue_(
      storage, file_, [](page& page) noexcept { handle_completion(page); }) {
    sched_->add_queue(&queue_);
}

seastar::future<> pager::close() noexcept {
    co_await scheduler::remove_queue(&queue_);
    for (const auto& page : pages_) {
        cache_->remove(*page);
    }
}

seastar::lw_shared_ptr<page>
pager::alloc_page(uint64_t offset, std::optional<cache_hook> hook) noexcept {
    auto buf = seastar::temporary_buffer<char>::aligned(page_size, page_size);
    if (hook.has_value()) {
        return seastar::make_lw_shared<page>(
          offset, std::move(buf), hook.value());
    }
    return seastar::make_lw_shared<page>(offset, std::move(buf));
}

seastar::future<std::vector<seastar::lw_shared_ptr<page>>>
pager::read(const read_config cfg) noexcept {
    // the logical size that is visible to the reader
    const auto file_size = size_;

    std::vector<seastar::lw_shared_ptr<page>> pages;

    if (cfg.length == 0 || cfg.offset >= file_size) {
        co_return pages;
    }

    size_t total = 0;
    uint64_t offset = cfg.offset;
    while (true) {
        /*
         * this will either be a cache hit, or the miss will be handled
         * synchronously. this is roughly the location to implement read-ahead
         * where by a cache miss will continue reading, but return the pages
         * that were available in the cache.
         */
        auto page = co_await get_page(offset);

        pages.emplace_back(page);
        total += page->size();

        if (total >= cfg.length) {
            break;
        }

        offset = page->offset() + page->size();
        if (offset >= file_size) {
            break;
        }
    }

    co_return pages;
}

seastar::future<> pager::append(seastar::temporary_buffer<char> data) noexcept {
    /*
     * helper to copy up to size bytes from the input data into a page at a
     * specific offset. the number of bytes copied is returned.
     */
    const auto copy = [this, &data](size_t size, page* page, size_t off) {
        const std::span src(data.get(), std::min(data.size(), size));
        auto dst = std::span(page->get_write(), page->size()).subspan(off);
        std::copy_n(src.begin(), src.size(), dst.begin());
        data.trim_front(src.size());
        /*
         * the scheduler does the right thing if pages are marked more than once
         * as dirty and resubmitted for write-back.
         */
        page->set_flag(page::flags::dirty);
        sched_->submit_write(&queue_, page);
        return src.size();
    };

    /*
     * handle the page mapped by the append position (ie the file size). if the
     * file size is aligned, then no page is mapped. otherwise, the page may
     * need to be paged in for read-modify-write.
     */
    auto offset = size_; // offset of the first byte being appended
    if ((offset % page_size) != 0) {
        auto page = co_await get_page(offset);
        const auto written = offset - page->offset();
        const auto capacity = page->size() - written;
        offset += copy(capacity, page.get(), written);
    }

    /*
     * the remaining pages are new and will be filled, except for the last page
     * created by this loop which may be partially filled.
     */
    while (!data.empty()) {
        auto page = alloc_page(offset);
        offset += copy(page->size(), page.get(), 0);
        const auto res = pages_.insert(page);
        vassert(
          res.second,
          "Page already cached offset {} size {}",
          page->offset(),
          page->size());
        cache_->insert(*page);
    }

    size_ = offset;
}

seastar::future<seastar::lw_shared_ptr<page>>
pager::get_page(uint64_t offset) noexcept {
    /*
     * find the page mapped by the target offset and return (page, true) if the
     * page exists and has not yet been evicted. in all other cases, allocate a
     * new page and return (page, false).
     */
    auto find_or_alloc_page = [this, offset] {
        if (auto it = pages_.find(offset); it != pages_.end()) {
            auto page = *it;
            if (!page->data().empty()) {
                return std::make_pair(std::move(page), true);
            }
            /*
             * reusing the page is technically possible, but isn't yet done,
             * in favor of the simplicity of re-creating the page with a
             * specific known states. the cache hook is transferred so that we
             * do not lose the cache statistics.
             */
            pages_.erase(it);
            return std::make_pair(
              alloc_page(page->offset(), page->cache_hook), false);
        }
        return std::make_pair(
          alloc_page(seastar::align_down(offset, page_size)), false);
    };

    while (true) {
        auto [page, found] = find_or_alloc_page();
        if (found) {
            /*
             * the page may be in the cache index, but faulting. in this case we
             * can wait until the fault is finished and try again.
             */
            if (page->test_flag(page::flags::faulting)) {
                page::waiter waiter;
                page->add_waiter(waiter);
                auto fut = waiter.ready.get_future();
                co_await std::move(fut);
                continue;
            }
            vassert(
              page->size() > 0, "Empty page at offset {}", page->offset());
            co_return page;
        }

        /*
         * the page was not found in the index, so we need to fault it in.
         *
         * NOTE: due to a not yet known issue, when waiter is allocated on the
         * stack, co_await on the waiter triggers clang-tidy check
         * StackAddressEscape. this seems like a false positive, so allocate on
         * the heap for now to trick it.
         */
        auto waiter = std::make_unique<page::waiter>();
        page->set_flag(page::flags::faulting);
        page->add_waiter(*waiter);

        cache_->insert(*page);

        auto res = pages_.insert(page);
        vassert(
          res.second,
          "Page already in index offset {} size {}",
          page->offset(),
          page->size());

        sched_->submit_read(&queue_, page.get());
        co_await waiter->ready.get_future();
    }
}

void pager::handle_completion(page& page) noexcept {
    if (page.test_flag(page::flags::faulting)) {
        vassert(page.test_flag(page::flags::read), "Expected read page");
        page.clear_flag(page::flags::faulting);
        page.signal_waiters();
    } else if (page.test_flag(page::flags::dirty)) {
        vassert(page.test_flag(page::flags::write), "Expected write page");
        page.clear_flag(page::flags::dirty);
    }
}

} // namespace experimental::io
