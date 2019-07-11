#pragma once

#include "seastarx.h"
#include "filesystem/page_cache_result.h"
#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/semaphore.hh>

#include <smf/log.h>
#include <cstdint>
#include <deque>
#include <vector>

// FIXME(agallego): create a central component for system-wide reservations.
class page_cache_buffer_manager {
public:
    SMF_DISALLOW_COPY_AND_ASSIGN(page_cache_buffer_manager);
    constexpr static const int32_t kBufferSize = 1 << 18;
    using buf_ptr_t = std::unique_ptr<char[], free_deleter>;
    struct page_cache_result_dtor {
        page_cache_result_dtor(char* s, page_cache_buffer_manager* m)
          : semb(s)
          , mngr(m) {
        }
        char* semb;
        page_cache_buffer_manager* mngr;

        void operator()(page_cache_result* r) {
            mngr->update_free_list(semb);
            delete r;
        }
    };

    using page_cache_result_ptr
      = std::unique_ptr<page_cache_result, page_cache_result_dtor>;

    page_cache_buffer_manager(int64_t mem_reserved, int64_t max_limit)
      : min_memory_reserved(mem_reserved)
      , max_memory_limit(max_limit) {
        for (int64_t i = 0, max = min_memory_reserved / kBufferSize; i < max;
             ++i) {
            grow_by_one();
            _locks.push_back(semaphore(1));
        }
    }

    const int64_t min_memory_reserved;
    const int64_t max_memory_limit;

    future<page_cache_result_ptr>
    allocate(int32_t begin_page, page_cache_result::priority prio) {
        if (!free_list_.empty()) {
            return make_ready_future<page_cache_result_ptr>(
              do_allocate(begin_page, prio));
        }
        if (total_alloc_bytes() < max_memory_limit) {
            grow_by_one();
            return make_ready_future<page_cache_result_ptr>(
              do_allocate(begin_page, prio));
        }
        return with_semaphore(
          no_free_pages_, 1, [=] { return allocate(begin_page, prio); });
    }

    semaphore&
    lock(uint32_t file_id, std::pair<int32_t, int32_t> clamp) {
        uint32_t id = xxhash_32(
          std::array{static_cast<int32_t>(file_id), clamp.first});
        return _locks[jump_consistent_hash(id, _locks.size())];
    }

    int64_t total_alloc_bytes() const {
        return _buffers.size() * kBufferSize;
    }
    void decrement_buffers() {
        if (total_alloc_bytes() < min_memory_reserved) {
            // minimum is reserved
            return;
        }
        if (!free_list_.empty()) {
            auto front = free_list_.front();
            free_list_.pop_front();
            // NOTE: slow - linear.
            // linear? maybe not too bad? not sure.
            auto it = std::find_if(
              _buffers.begin(), _buffers.end(), [=](auto& buf) {
                  return front == buf.get();
              });
            std::swap(*it, _buffers.back());
            _buffers.pop_back();
            return;
        }
        // acquire lock as soon as it can & free that buffer
        // prepare clean up in background future
        with_semaphore(
          no_free_pages_, 1, [=] { decrement_buffers(); });
    }

private:
    friend page_cache_result_dtor;
    void update_free_list(char* b) {
        DLOG_THROW_IF(b == nullptr, "Updated a free buffer with null!");
        free_list_.push_back(b);
        if (no_free_pages_.waiters() > 0) {
            no_free_pages_.signal(1);
        }
    }
    page_cache_result_ptr
    do_allocate(int32_t begin_page, page_cache_result::priority prio) {
        auto front = free_list_.front();
        free_list_.pop_front();
        return page_cache_result_ptr(
          new page_cache_result(begin_page, {front, kBufferSize}, prio),
          page_cache_result_dtor(front, this));
    }
    void grow_by_one() {
        _buffers.push_back(
          allocate_aligned_buffer<char>(kBufferSize, 4096));
        free_list_.push_back(_buffers.back().get());
    }

private:
    std::vector<semaphore> _locks;
    std::vector<buf_ptr_t> _buffers;
    std::deque<char*> free_list_;
    semaphore no_free_pages_{1};
};
