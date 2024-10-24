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

#pragma once

#include "base/seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/util/later.hh>

#include <list>

namespace transform {

/**
 * A semaphore for limiting memory usage.
 *
 * This construct should be used to limit memory in a system. Units allow RAII
 * usage like so:
 *
 * ```
 * {
 *    auto units = co_await limiter.wait(100, &_as);
 *    auto foo = load_foo();
 * }
 * // 100 bytes of memory returned to `limiter`
 * ```
 *
 * We have this construct over seastar::semaphore because at the time of writing
 * there is a stack-use-after-return issue with
 * `seastar::semaphore::wait(ss::about_source&, size_t)`. Instead we manually
 * implement a semaphore using a queue.
 *
 * Users of this class must wait for all outstanding waiters to complete before
 * destructing this object.
 */
class memory_limiter {
public:
    explicit memory_limiter(size_t limit);
    memory_limiter(const memory_limiter&) = delete;
    memory_limiter(memory_limiter&&) = delete;
    memory_limiter& operator=(const memory_limiter&) = delete;
    memory_limiter& operator=(memory_limiter&&) = delete;
    ~memory_limiter();

    /**
     * A holder of memory acquired from the limiter that automatically releases
     * the memory when destructed.
     *
     * Memory can be manually released using `release_all`.
     *
     * Move-only.
     */
    class [[nodiscard]] units {
    public:
        units(memory_limiter* limiter, size_t amount);
        units(const units&) = delete;
        units& operator=(const units&) = delete;
        units(units&& other) noexcept;
        units& operator=(units&& other) noexcept;
        void release_all();
        ~units() noexcept;

    private:
        memory_limiter* _limiter;
        size_t _amount;
    };

    /**
     * Wait for `amount` of memory to be available and return units that release
     * the memory when destructed.
     *
     * @param `amount` **must** be less than the maximum memory available to
     * this limiter.
     *
     * @returns a failed future if the abort source fired before acquiring the
     * memory.
     */
    [[nodiscard]] ss::future<units> wait(size_t amount, ss::abort_source* as);

    /**
     * Acquire `amount` bytes of memory from the limiter. This API requires
     * manually calling `release` with the same `amount`. It is recommended that
     * `wait` is used instead.
     *
     * @param `amount` **must** be less than the maximum memory available to
     * this limiter.
     *
     * @returns a failed future if the abort source fired before acquiring the
     * memory.
     */
    [[nodiscard]] ss::future<> acquire(size_t amount, ss::abort_source* as);

    /**
     * Return `amount` of memory to the limiter, freeing any waiters needing
     * this memory.
     */
    void release(size_t amount);

    // The maximum memory that this limiter can provide access too.
    size_t max_memory() const;
    // The amount of currently used memory for this limiter.
    size_t used_memory() const;
    // The remaining memory available for this limiter to hand out.
    size_t available_memory() const;

private:
    bool has_available(size_t amount);

    void notify();

    struct waiter {
        using memory_acquired = ss::bool_class<struct memory_acquired_tag>;
        size_t amount;
        std::optional<memory_acquired> state;
        ss::promise<> prom;
    };

    std::list<waiter> _waiters;
    size_t _available;
    size_t _max;
};
} // namespace transform
