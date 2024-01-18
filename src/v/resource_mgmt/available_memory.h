/*
 * Copyright 2022 Redpanda Data, Inc.
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
#include "container/intrusive_list_helpers.h"
#include "metrics/metrics.h"

#include <seastar/core/future.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/noncopyable_function.hh>

#include <memory>
#include <vector>

namespace resources {

class available_memory;

/**
 * @brief Allows determining the available memory on a shard.
 *
 * Available memory is free memory (e.g., as reported by the
 * seastar allocator) *plus* memory which can be reclaimed.
 * That is, it is the amount of memory that new allocations can
 * reasonably expect to be able to use.
 */
class available_memory final {
    // type erased function to report available memory for a
    // registered reclaimer
    using afn = ss::noncopyable_function<size_t()>;

    struct reporter {
        friend available_memory;
        ss::sstring name;
        afn avail_fn;
        intrusive_list_hook hook;
    };

public:
    using deregister_holder = std::unique_ptr<reporter>;

    // does not make sense to move or cpoy this object
    available_memory();
    available_memory(available_memory&) = delete;
    available_memory& operator=(const available_memory&) = delete;

    /**
     * @brief Return the current available memory value.
     *
     * Available memory is free memory, plus all the memory that can be in
     * princple reclaimed from subsystems which have registered a reclaim hook.
     *
     * Effectively, this is reclaimable() + ss::memory::stats().free_memory()
     *
     * @return size_t the estimated amount of available memory in the system
     */
    size_t available() const;

    /**
     * This method calculates the current reclaimable by polling all the
     * registered reclaimers for how much reclaimable memory they hold, and
     * returning the sum of all of these amounts.
     *
     * The returned value is the assumed amount of memory that could be released
     * back to the allocator if all reclaimers did an exhaustive reclaim.
     */
    size_t reclaimable() const;

    /**
     * @brief Register associated metric(s) for available memory.
     *
     * This call registers an "available_memory" metric in the default
     * metric groups, which reports the get_available() call when it is
     * polled. Without this call, the class still works fine, exposing
     * its API to callers, but no metrics are created.
     */
    void register_metrics();

    /**
     * @brief The low-water mark for available memory.
     *
     * This returns the lowest available memory value observed by this object.
     * This object only observes the available memory when
     * update_low_water_mark() is called, either implicitly (e.g., by calling
     * this method) or explicitly.
     *
     *
     * This call makes a call to update_low_water_mark() immediately before
     * returning its result, so this always includes the current memory state
     * in the LWM calculatoin.
     *
     * @return size_t the available low-water mark as described above
     */
    size_t available_low_water_mark();

    /**
     * @brief Update the low-water mark if applicable.
     *
     * Samples the currently available memory (as-if by calling available() and
     * update the low-water mark if the currnet value is lower than any seen so
     * far.
     *
     * Calls to this method should be inserted at places where memory is likely
     * to reach new lows: inside the reclaimer (if one is registered) would be
     * a suitable place.
     */
    void update_low_water_mark();

    /**
     * @brief Register an available memory reporter.
     *
     * A reporter is a component that can reclaim memory, and so needs to
     * report the amount of reclaimable memory it holds.
     *
     * This method returns a deregister_holder, which when destroyed
     * deregisters this reporter. Typically, this object will be stored as
     * a memory of the object to which the reporter function points so that
     * the function does not access a dangling reference after it is destroyed.
     *
     * @param name name of the reporter
     * @param reporter_fn a function which returns the amount of reclaimable
     * memory held at any moment in time by this reporter
     * @return an opaque object that deregisters the reporter
     */
    template<typename F>
    [[nodiscard("You need to hold the returned object to maintain "
                "registration")]] deregister_holder
    register_reporter(const ss::sstring& name, F reporter_fn) {
        return inner_register(name, std::move(reporter_fn));
    }

    /**
     * @brief Get a reference to the shard-global available_memory instance.
     *
     * @return The global available_memory object for this shard.
     */
    static available_memory& local() { return _local_instance; }

private:
    deregister_holder inner_register(const ss::sstring& name, afn&& fn);

    static thread_local available_memory
      _local_instance; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

    intrusive_list<reporter, &reporter::hook> _reporters;
    std::optional<metrics::all_metrics_groups> _metrics;
    size_t _lwm;
};

} // namespace resources
