/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "ssx/semaphore.h"

/**
 * This class is extension of ss::semaphore to fit the needs
 * of the storage_resources class's tracking of byte/concurrency
 * allowances.
 *
 * Callers may use this class as either a soft or hard quota.  In
 * the hard case, regular async-waiting semaphore calls (ss::get_units)
 * may be used.  In the soft case, the take() function will allow the
 * semaphore count to go negative, but return a `checkpoint_hint` field
 * that prompts the holder of the units to release some.
 *
 * This is 'adjustable' in that:
 * - Regular sempahores are just a counter: they have no
 *   memory of their intended capacity.  In order to enable runtime
 *   changes to the max units in a semaphore, we must keep an extra
 *   record of the capacity.
 * - This enables runtime configuration changes to parameters that
 *   control the capacity of a semaphore.
 */
class adjustable_semaphore {
public:
    explicit adjustable_semaphore(uint64_t capacity)
      : adjustable_semaphore(capacity, "s/allowance") {}
    adjustable_semaphore(uint64_t capacity, const ss::sstring& sem_name)
      : _sem(capacity, sem_name)
      , _capacity(capacity) {}

    void set_capacity(uint64_t capacity) noexcept {
        if (capacity > _capacity) {
            _sem.signal(capacity - _capacity);
        } else if (capacity < _capacity) {
            _sem.consume(_capacity - capacity);
        }

        _capacity = capacity;
    }

    /**
     * When a consumer wants some units, it gets them unconditionally, but
     * gets a hint as to whether it exceeded the capacity.  That is the hint
     * to e.g. the offset translator that now is the time to checkpoint
     * because there are too many dirty bytes.
     */
    struct take_result {
        ssx::semaphore_units units;
        bool checkpoint_hint{false};
    };

    /**
     * Non-blocking consume of units, may send the semaphore negative.
     *
     * Includes a hint in the response if the semaphore has gone negative,
     * to induce the caller to release some units when they can.
     */
    take_result take(size_t units) {
        take_result result = {
          .units = ss::consume_units(_sem, units),
          .checkpoint_hint = _sem.current() <= 0};

        return result;
    }

    /**
     * Blocking get units: will block until units are available.
     */
    ss::future<ssx::semaphore_units> get_units(size_t units) {
        return ss::get_units(_sem, units);
    }

    /**
     * Blocking get units: will block until units are available or until abort
     * source is triggered.
     */
    ss::future<ssx::semaphore_units>
    get_units(size_t units, ss::abort_source& as) {
        return ss::get_units(_sem, units, as);
    }

    /**
     * Attempts to immediately get units from the semaphore, returning
     * std::nullopt if no units exist.
     */
    std::optional<ssx::semaphore_units> try_get_units(size_t units) {
        return ss::try_get_units(_sem, units);
    }

    size_t current() const noexcept { return _sem.current(); }
    ssize_t available_units() const noexcept { return _sem.available_units(); }

    /**
     * Since we know our expected total capacity, we may calculate how many
     * units are currently leant out.
     *
     * If capacity was recently adjusted we might have more units outstanding
     * than the total capacity (i.e. when available_units() is negative)
     */
    size_t outstanding() const noexcept {
        return _capacity - available_units();
    }

    void broken() noexcept { _sem.broken(); }

    uint64_t capacity() { return _capacity; }

private:
    ssx::semaphore _sem;

    uint64_t _capacity{0};
};
