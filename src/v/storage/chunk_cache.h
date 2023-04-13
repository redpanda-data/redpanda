#pragma once
#include "resource_mgmt/memory_groups.h"
#include "seastarx.h"
#include "ssx/semaphore.h"
#include "storage/logger.h"
#include "storage/segment_appender_chunk.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/later.hh>

#include <boost/iterator/counting_iterator.hpp>

namespace storage::internal {

class chunk_cache {
    using chunk = segment_appender_chunk;
    using chunk_ptr = ss::lw_shared_ptr<chunk>;

public:
    /**
     * The chunk cache serves all segment files, which individually may have
     * different alignment requirements (e.g. different file systems or
     * devices). However current restrictions mean that all segments share the
     * same device and file system. For convenience we use the fail safe size
     * specified by seastar, and dynamically verify compatibility for each file.
     */
    static constexpr const alignment alignment{4_KiB};

    chunk_cache() noexcept
      : _size_target(memory_groups::chunk_cache_min_memory())
      , _size_limit(memory_groups::chunk_cache_max_memory())
      , _chunk_size(config::shard_local_cfg().append_chunk_size()) {}

    chunk_cache(chunk_cache&&) = delete;
    chunk_cache& operator=(chunk_cache&&) = delete;
    chunk_cache(const chunk_cache&) = delete;
    chunk_cache& operator=(const chunk_cache&) = delete;
    ~chunk_cache() noexcept = default;

    ss::future<> start() {
        const auto num_chunks = memory_groups::chunk_cache_min_memory()
                                / _chunk_size;
        return ss::do_for_each(
          boost::counting_iterator<size_t>(0),
          boost::counting_iterator<size_t>(num_chunks),
          [this](size_t) {
              auto c = ss::make_lw_shared<chunk>(_chunk_size, alignment);
              _size_total += _chunk_size;
              add(c);
          });
    }

    void add(const chunk_ptr& chunk) {
        if (_size_available >= _size_target) {
            _size_total -= _chunk_size;
            return;
        }
        _chunks.push_back(chunk);
        _size_available += _chunk_size;
        if (_sem.waiters()) {
            _sem.signal();
        }
    }

    ss::future<chunk_ptr> get() {
        // don't steal if there are waiters
        if (!_sem.waiters()) {
            return do_get();
        }
        return ss::get_units(_sem, 1).then(
          [this](ssx::semaphore_units) { return do_get(); });
    }

    size_t chunk_size() const { return _chunk_size; }

private:
    ss::future<chunk_ptr> do_get() {
        if (auto c = pop_or_allocate(); c) {
            return ss::make_ready_future<chunk_ptr>(c);
        }
        return ss::get_units(_sem, 1).then(
          [this](ssx::semaphore_units) { return do_get(); });
    }

    chunk_ptr pop_or_allocate() {
        if (!_chunks.empty()) {
            auto c = _chunks.front();
            _chunks.pop_front();
            _size_available -= _chunk_size;
            c->reset();
            return c;
        }
        if (_size_total < _size_limit) {
            try {
                auto c = ss::make_lw_shared<chunk>(_chunk_size, alignment);
                _size_total += _chunk_size;
                return c;
            } catch (const std::bad_alloc& e) {
                vlog(stlog.debug, "chunk allocation failed: {}", e);
            }
        }
        return nullptr;
    }

    ss::chunked_fifo<chunk_ptr> _chunks;
    ssx::semaphore _sem{0, "s/chunk-cache"};
    size_t _size_available{0};
    size_t _size_total{0};
    const size_t _size_target;
    const size_t _size_limit;

    const size_t _chunk_size{0};
};

inline chunk_cache& chunks() {
    static thread_local chunk_cache cache;
    return cache;
}

} // namespace storage::internal
