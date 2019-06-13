#include "filesystem/page_cache_buffer_manager.h"
#include "filesystem/page_cache_file_idx.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>

#include <iostream>

seastar::future<>
tests(page_cache_buffer_manager* _mngr, page_cache_file_idx* _idx) {
    LOG_THROW_IF(nullptr != _idx->range(66), "Empty state failed");

    return _mngr->allocate(66, page_cache_result::priority::low)
      .then([=](auto range_ptr) {
          _idx->cache(std::move(range_ptr));
          return seastar::make_ready_future<>();
      })
      .then([=] {
          LOG_INFO("Basic sanity tests");
          auto x = _idx->range(66);
          LOG_THROW_IF(x == nullptr, "bug: page should exist");
          LOG_THROW_IF(
            x->prio != page_cache_result::priority::low, "invalid priority");
          LOG_THROW_IF(x->locks != 0, "range should not be default-locked");
          LOG_THROW_IF(
            x->marked_for_eviction != false, "should not be evictable");
          LOG_THROW_IF(!x->is_page_in_range(66), "why is page not in range?");
          LOG_THROW_IF(
            !_idx->try_evict(), "could not evict an evictable range");
          LOG_THROW_IF(
            _idx->range(66) != nullptr,
            "eviction not working, range should be nil");
          return seastar::make_ready_future<>();
      })
      .then([=] {
          LOG_INFO("high priority sanity tests");
          return _mngr->allocate(1401, page_cache_result::priority::high)
            .then([=](auto range_ptr) {
                _idx->cache(std::move(range_ptr));
                return seastar::make_ready_future<>();
            })
            .then([=] {
                LOG_INFO("Locking/unlocking 1401 page");
                auto x = _idx->range(1401);
                x->locks++;
                LOG_THROW_IF(
                  !x->is_page_in_range(1401), "why is page not in range?");
                LOG_THROW_IF(
                  !!_idx->try_evict(),
                  "could not evict an HIGH priority range");
                x->locks--;

                LOG_THROW_IF(!_idx->try_evict(), "cleanup failed");
                return seastar::make_ready_future<>();
            });
      })
      .then([=] {
          LOG_INFO("idx::evict_pages(<set>) tests");
          return _mngr->allocate(1401, page_cache_result::priority::high)
            .then([=](auto range_ptr) {
                _idx->cache(std::move(range_ptr));
                return seastar::make_ready_future<>();
            })
            .then([=] {
                auto x = _idx->range(1401);
                _idx->evict_pages({1401});
                LOG_THROW_IF(
                  _idx->range(1401) != nullptr,
                  "should return false for evicted pages in range");
                return seastar::make_ready_future<>();
            });
      });
}

int main(int argc, char** argv) {
    std::cout.setf(std::ios::unitbuf);
    seastar::app_template app;

    return app.run(argc, argv, [&] {
        smf::app_run_log_level(seastar::log_level::trace);
        // 512MB
        return seastar::do_with(
          std::make_unique<page_cache_buffer_manager>(1 << 29, 1 << 29),
          std::make_unique<page_cache_file_idx>(42 /*fileid*/),
          [](auto& _mngr, auto& _idx) mutable {
              return tests(_mngr.get(), _idx.get());
          });
    });
}
