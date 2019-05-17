#include <iostream>

#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>

#include "page_cache_buffer_manager.h"
#include "page_cache_file_idx.h"


seastar::future<>
tests(page_cache_buffer_manager *mngr_, page_cache_file_idx *idx_) {
  LOG_THROW_IF(nullptr != idx_->range(66), "Empty state failed");

  return mngr_->allocate(66, page_cache_result::priority::low)
    .then([=](auto range_ptr) {
      idx_->cache(std::move(range_ptr));
      return seastar::make_ready_future<>();
    })
    .then([=] {
      LOG_INFO("Basic sanity tests");
      auto x = idx_->range(66);
      LOG_THROW_IF(x == nullptr, "bug: page should exist");
      LOG_THROW_IF(x->prio != page_cache_result::priority::low,
                   "invalid priority");
      LOG_THROW_IF(x->locks != 0, "range should not be default-locked");
      LOG_THROW_IF(x->marked_for_eviction != false, "should not be evictable");
      LOG_THROW_IF(!x->is_page_in_range(66), "why is page not in range?");
      LOG_THROW_IF(!idx_->try_evict(), "could not evict an evictable range");
      LOG_THROW_IF(idx_->range(66) != nullptr,
                   "eviction not working, range should be nil");
      return seastar::make_ready_future<>();
    })
    .then([=] {
      LOG_INFO("high priority sanity tests");
      return mngr_->allocate(1401, page_cache_result::priority::high)
        .then([=](auto range_ptr) {
          idx_->cache(std::move(range_ptr));
          return seastar::make_ready_future<>();
        })
        .then([=] {
          LOG_INFO("Locking/unlocking 1401 page");
          auto x = idx_->range(1401);
          x->locks++;
          LOG_THROW_IF(!x->is_page_in_range(1401), "why is page not in range?");
          LOG_THROW_IF(!!idx_->try_evict(),
                       "could not evict an HIGH priority range");
          x->locks--;

          LOG_THROW_IF(!idx_->try_evict(), "cleanup failed");
          return seastar::make_ready_future<>();
        });
    })
    .then([=] {
      LOG_INFO("idx::evict_pages(<set>) tests");
      return mngr_->allocate(1401, page_cache_result::priority::high)
        .then([=](auto range_ptr) {
          idx_->cache(std::move(range_ptr));
          return seastar::make_ready_future<>();
        })
        .then([=] {
          auto x = idx_->range(1401);
          idx_->evict_pages({1401});
          LOG_THROW_IF(idx_->range(1401) != nullptr,
                       "should return false for evicted pages in range");
          return seastar::make_ready_future<>();
        });
    });
}

int
main(int argc, char **argv) {
  std::cout.setf(std::ios::unitbuf);
  seastar::app_template app;

  return app.run(argc, argv, [&] {
    smf::app_run_log_level(seastar::log_level::trace);
    // 512MB
    return seastar::do_with(
      std::make_unique<page_cache_buffer_manager>(1 << 29, 1 << 29),
      std::make_unique<page_cache_file_idx>(42 /*fileid*/),
      [](auto &mngr_, auto &idx_) mutable {
        return tests(mngr_.get(), idx_.get());
      });
  });
}
