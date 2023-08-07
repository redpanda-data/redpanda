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

#pragma once

#include "ssx/fiber_local_storage.h"
#include "utils/string_switch.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/semaphore.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/util/log.hh>

#include <map>
#include <stdexcept>

namespace ssx {

inline ss::logger dd_log("deadlock-detector");

class deadlock_detected : public std::runtime_error {
public:
    deadlock_detected()
      : std::runtime_error("deadlock detected") {}
    explicit deadlock_detected(const char* s)
      : std::runtime_error(s) {}
    explicit deadlock_detected(const std::string& s)
      : std::runtime_error(s) {}
};

static constexpr uint32_t deadlock_detector_max_depth = 32;

namespace details {

class failstop_monitor {
public:
    void
    report_deadlock(const seastar::sstring& lhs, const seastar::sstring& rhs) {
        // Crash redpanda and rely on generated backtrace
        vassert(false, "Deadlock between {} and {} detected", lhs, rhs);
    }
    void report_recursion(const seastar::sstring& name) {
        // The recursion can happen if for instance we're acquiring several
        // segments in a row.
        vlog(dd_log.debug, "Recursion on {} detected", name);
    }
};

/// Deadlock detector implementation.
///
/// \param DeadlockMonitor is an object which is used to report detected
///        deadlocks. It should be default constructible and implement the
///        'report' method.
template<class DeadlockMonitor = failstop_monitor>
class deadlock_detector {
    enum class causality {
        after,
        before,
        recursion,
    };
    struct transition_row {
        std::map<seastar::sstring, causality> deps;
    };

    friend std::ostream& operator<<(std::ostream& s, causality c) {
        switch (c) {
        case causality::after:
            s << "after";
            break;
        case causality::before:
            s << "before";
            break;
        case causality::recursion:
            s << "resursion";
            break;
        };
        return s;
    }

public:
    deadlock_detector() = default;

    struct lock_vector {
        seastar::sstring prev;
        seastar::sstring curr;

        std::tuple<causality, seastar::sstring, seastar::sstring>
        get_tuple() const {
            int cmp = std::strcmp(curr.c_str(), prev.c_str());
            if (cmp > 0) {
                return std::make_tuple(causality::after, curr, prev);
            } else if (cmp < 0) {
                return std::make_tuple(causality::before, prev, curr);
            }
            return std::make_tuple(causality::recursion, curr, curr);
        }
    };

    /// Set size limit for the internal data structure.
    /// The state will be reset when the limit is reached. This means
    /// fewer detections but also lower memory reqirements.
    void set_size_limit(size_t size) { _size_limit = size; }

    void on_lock(lock_vector vec) {
        // The locks in the tuple are sorted by name but the
        // 'c' variable caputures causality. This is needed to make sure
        // that two semaphores with the same name will hit the same row
        // in the '_transitions' table no matter in which order they're
        // acquired.
        auto [c, x, y] = vec.get_tuple();
        if (c == causality::recursion) {
            _monitor.report_recursion(x);
        }
        auto row = _transitions.find(x);
        if (row == _transitions.end()) {
            // the row is empty, the sempahore with such name was never used
            // before
            auto [it, ok] = _transitions.insert(
              std::make_pair(x, transition_row{}));
            vassert(ok, "Row {} is already inserted", x);
            row = it;
        }
        auto transition = row->second.deps.find(y);
        if (transition == row->second.deps.end()) {
            // transition was never observed before
            auto [it, ok] = row->second.deps.insert(std::make_pair(y, c));
            vassert(ok, "Value for {} is already inserted", y);
            transition = it;
            _count++;
        }
        if (transition->second != c) {
            print_row(row, x, y, c);
            _monitor.report_deadlock(x, y);
        }
        // Avoid growing beyond the size limit.
        maybe_reset();
    }

    const auto& get_monitor() const { return _monitor; }

    /// This method can be called to free up space to avoid hogging
    /// too much memory. Also, when the deadlock is detected it is possible
    /// that the transitions table contains incorrect transition that caused
    /// deadlock and all subsequent attempts to lock in the right order will
    /// cause the same deadlock detection. To avoid this we can clear the
    /// table after the deadlock is detected.
    void reset() {
        _transitions.clear();
        _monitor = DeadlockMonitor();
        _count = 0;
    }

    void print_row(
      std::map<seastar::sstring, transition_row>::const_iterator it,
      const seastar::sstring& from,
      const seastar::sstring& to,
      causality dir) const {
        vlog(
          dd_log.info,
          "Trying to acquire {} {} {}, acquisition history for {}",
          from,
          dir,
          to,
          from);
        const transition_row& row = it->second;
        for (const auto& d : row.deps) {
            vlog(dd_log.info, "-- Acquired {} {} {}", from, d.second, d.first);
        }
    }

private:
    void maybe_reset() {
        if (_size_limit > 0 && _count > _size_limit) {
            reset();
        }
    }
    std::map<seastar::sstring, transition_row> _transitions;
    DeadlockMonitor _monitor;
    size_t _count{0};
    /// Limits thte size of the transitions map, 0 means no limit
    size_t _size_limit{0};
};

template<class Monitor>
class deadlock_detector_waiter_state {
public:
    explicit deadlock_detector_waiter_state(seastar::sstring name)
      : _name(std::move(name))
      , _fiber_state(std::ref(*this)) {
        on_lock();
    }
    ~deadlock_detector_waiter_state() = default;
    deadlock_detector_waiter_state(deadlock_detector_waiter_state&&) noexcept
      = default;
    deadlock_detector_waiter_state&
    operator=(deadlock_detector_waiter_state&&) noexcept
      = default;

    deadlock_detector_waiter_state(const deadlock_detector_waiter_state&)
      = delete;
    deadlock_detector_waiter_state&
    operator=(const deadlock_detector_waiter_state&)
      = delete;

    const auto& get_monitor() const { return _detector.get_monitor(); }

    static deadlock_detector<Monitor>& get_deadlock_detector() {
        return _detector;
    }

    static void set_size_limit(size_t size) { _detector.set_size_limit(size); }

private:
    void on_lock() const {
        for (const auto& ref : _fiber_state) {
            if (&ref.get().get() == this) {
                continue;
            }
            typename deadlock_detector<Monitor>::lock_vector vec{
              .prev = ref.get().get()._name,
              .curr = _name,
            };
            _detector.on_lock(vec);
        }
    }

    using fls_t = fiber_local<
      struct deadlock_detector_waiter_state_tag,
      std::reference_wrapper<deadlock_detector_waiter_state<Monitor>>>;

    seastar::sstring _name;
    fls_t _fiber_state;
    static thread_local deadlock_detector<Monitor> _detector;
};

template<class Monitor>
thread_local deadlock_detector<Monitor>
  deadlock_detector_waiter_state<Monitor>::_detector;

} // namespace details

} // namespace ssx
