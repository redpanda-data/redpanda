/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/seastarx.h"
#include "transform/memory_limiter.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/semaphore.hh>

#include <algorithm>
#include <numeric>
#include <optional>

namespace transform {

template<typename T>
concept MemoryMeasurable = requires(const T v) {
    { v.memory_usage() } -> std::convertible_to<size_t>;
};

constexpr size_t default_items_per_chunk = 128;

/**
 * A single producer single consumer queue for transfering variable sized
 * entries between fibers.
 *
 * If needing a fixed number of elements (or have entries with fixed memory
 * requirements) ss::queue is a better option. This queue limits based on the
 * presence of a `memory_usage()` method. Note that this limit is a soft limit
 * and we prefer making progress over keeping the limit. Concretely that means
 * that if this queue is empty, `push` will always succeed.
 *
 * All public methods in the queue can be aborted using an existing
 * `ss::abort_source`.
 *
 */
template<MemoryMeasurable T, size_t items_per_chunk = default_items_per_chunk>
class transfer_queue {
public:
    /**
     * Construct a transfer queue with `max_memory_usage` being the soft limit
     * at which to limit in the queue.
     */
    explicit transfer_queue(memory_limiter* ml)
      : _memory_limiter(ml) {}

    /**
     * Push an entry into the queue, waiting for there to be available memory.
     *
     * NOTE: in the case of an empty queue, this operation always succeeds as
     * the memory limit is soft and we prioritize making progress.
     *
     * If the abort source fires, we will noop the push and drop the entry on
     * the floor.
     */
    ss::future<> push(T entry, ss::abort_source* as) noexcept {
        // We want to be able to always insert at least one item, so cap memory
        // usage by _max_memory so we don't overload our semaphore and so we
        // can't get stuck.
        size_t mem = std::min(
          entry.memory_usage(), _memory_limiter->max_memory());
        try {
            co_await _memory_limiter->acquire(mem, as);
        } catch (...) {
            co_return;
        }
        _entries.push_back(std::move(entry));
        _cond_var.signal();
    }

    /**
     * Take a single element out of the queue waiting until there is one.
     *
     * If the provided about_source is aborted, then this method will return
     * `std::nullopt`.
     */
    ss::future<std::optional<T>> pop_one(ss::abort_source* as) noexcept {
        co_await wait_for_non_empty(as);
        if (as->abort_requested() || _entries.empty()) {
            co_return std::nullopt;
        }
        T entry = std::move(_entries.front());
        _entries.pop_front();
        _memory_limiter->release(
          std::min(entry.memory_usage(), _memory_limiter->max_memory()));
        _cond_var.signal();
        co_return entry;
    }

    /**
     * Extract all entries from this queue as soon as it is non-empty.
     *
     * If the abort_source is aborted, then this method will return an empty
     * container.
     */
    ss::future<ss::chunked_fifo<T, items_per_chunk>>
    pop_all(ss::abort_source* as) noexcept {
        co_await wait_for_non_empty(as);
        if (as->abort_requested()) {
            co_return ss::chunked_fifo<T, items_per_chunk>{};
        }
        _memory_limiter->release(memory_usage());
        _cond_var.signal();
        co_return std::exchange(_entries, {});
    }

    /**
     * Remove all entries from this queue.
     */
    void clear() noexcept {
        _memory_limiter->release(memory_usage());
        _entries.clear();
    }

private:
    ss::future<> wait_for_non_empty(ss::abort_source* as) noexcept {
        auto sub = as->subscribe([this]() noexcept { _cond_var.signal(); });
        if (sub) {
            co_await _cond_var.wait([this, as] {
                return as->abort_requested() || !_entries.empty();
            });
        }
    }

    size_t memory_usage() const {
        size_t sum = 0;
        for (const auto& entry : _entries) {
            sum += entry.memory_usage();
        }
        return sum;
    }

    ss::chunked_fifo<T, items_per_chunk> _entries;
    ss::condition_variable _cond_var;
    memory_limiter* _memory_limiter;
};

} // namespace transform
