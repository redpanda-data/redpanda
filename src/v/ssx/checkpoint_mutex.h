/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/vassert.h"
#include "ssx/sformat.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sstring.hh>

#include <utility>

namespace ssx {

template<typename Clock = seastar::timer<>::clock>
struct basic_mutex_checkpoint {
    seastar::sstring name;
    Clock::time_point time;
};

template<class Clock>
class basic_mutex_units;

namespace detail {
struct checkpoint_mutex_locker;
}

/// Checkpoint mutex allows the user to store some additional information
/// about the waiter, such as the name of the checkpoint and the time when it
/// was awaited. In the event of a timeout the exception will contain the
/// name of the checkpoint that holds the units.
template<typename Clock = seastar::timer<>::clock>
class basic_checkpoint_mutex {
    /// Stores information about the checkpoint that acquired units last.
    struct checkpoint_store {
        bool has_checkpoint() const noexcept { return _current.has_value(); }
        // Get the name of the checkpoint that acquired units last.
        seastar::sstring checkpoint_name() const noexcept {
            return _current ? _current->name.data() : "unavailable";
        }
        Clock::time_point checkpoint_time() const noexcept {
            return _current ? _current->time : Clock::time_point::min();
        }
        seastar::sstring
        format_checkpoint(const seastar::sstring& name) const noexcept {
            if (has_checkpoint()) {
                return ssx::sformat(
                  "{}: {} at {}",
                  name,
                  checkpoint_name(),
                  checkpoint_time().time_since_epoch().count());
            }
            return ssx::sformat("{}: no checkpoint", name);
        }
        void
        add_checkpoint(seastar::sstring name, Clock::time_point time) noexcept {
            _current = basic_mutex_checkpoint<Clock>{std::move(name), time};
        }
        void release() noexcept { _current.reset(); }
        std::optional<basic_mutex_checkpoint<Clock>> _current;
    };

    template<class Base>
    class derived_checkpoint_exception : public Base {
    public:
        explicit derived_checkpoint_exception(
          seastar::sstring name, const checkpoint_store* cs) noexcept
          : _msg(cs->format_checkpoint(name))
          , _checkpoint(cs->_current) {}

        const char* what() const noexcept override { return _msg.c_str(); }

        const auto& get_checkpoint() const noexcept { return _checkpoint; }

    private:
        seastar::sstring _msg;
        std::optional<basic_mutex_checkpoint<Clock>> _checkpoint;
    };

    using checkpoint_mutex_timed_out
      = derived_checkpoint_exception<seastar::semaphore_timed_out>;
    using checkpoint_mutex_broken
      = derived_checkpoint_exception<seastar::broken_semaphore>;
    using checkpoint_mutex_aborted
      = derived_checkpoint_exception<seastar::semaphore_aborted>;

    struct checkpoint_mutex_exception_factory {
        explicit checkpoint_mutex_exception_factory(
          seastar::sstring name, const checkpoint_store* cs) noexcept
          : _name(std::move(name))
          , _cs(cs) {}
        checkpoint_mutex_timed_out timeout() const noexcept {
            return checkpoint_mutex_timed_out("Mutex timed out, " + _name, _cs);
        }
        checkpoint_mutex_broken broken() const noexcept {
            return checkpoint_mutex_broken("Broken mutex, " + _name, _cs);
        }
        checkpoint_mutex_aborted aborted() const noexcept {
            return checkpoint_mutex_aborted("Mutex aborted, " + _name, _cs);
        }

        seastar::sstring _name;
        // The checkpoint_store is owned by the basic_checkpoint_mutex instance.
        const checkpoint_store* _cs;
    };

public:
    explicit basic_checkpoint_mutex(seastar::sstring name)
      : _sem(
          1,
          checkpoint_mutex_exception_factory{
            std::move(name), &_checkpoint_store}) {}

    void broken() noexcept { _sem.broken(); }

    bool has_units() const noexcept { return _sem.available_units() > 0; }

    basic_mutex_checkpoint<Clock> get_blocking_checkpoint() const noexcept {
        if (_checkpoint_store.has_checkpoint()) {
            return _checkpoint_store._current.value();
        }
        return basic_mutex_checkpoint<Clock>{
          "unavailable", Clock::time_point::min()};
    }

private:
    seastar::future<> lock(seastar::sstring checkpoint_name) {
        return _sem.wait(1).then(
          [this, checkpoint_name = std::move(checkpoint_name)]() mutable {
              // The checkpoint store is updated when the semaphore is acquired.
              _checkpoint_store.add_checkpoint(
                std::move(checkpoint_name), Clock::now());
          });
    }

    seastar::future<>
    lock(seastar::sstring checkpoint_name, typename Clock::duration timeout) {
        return _sem.wait(timeout, 1)
          .then([this, checkpoint_name = std::move(checkpoint_name)]() mutable {
              _checkpoint_store.add_checkpoint(
                std::move(checkpoint_name), Clock::now());
          });
    }

    seastar::future<> lock(
      seastar::sstring checkpoint_name, typename Clock::time_point deadline) {
        return _sem.wait(deadline, 1)
          .then([this, checkpoint_name = std::move(checkpoint_name)]() mutable {
              _checkpoint_store.add_checkpoint(
                std::move(checkpoint_name), Clock::now());
          });
    }

    seastar::future<>
    lock(seastar::sstring checkpoint_name, seastar::abort_source& as) {
        return _sem.wait(as, 1).then(
          [this, checkpoint_name = std::move(checkpoint_name)]() mutable {
              _checkpoint_store.add_checkpoint(
                std::move(checkpoint_name), Clock::now());
          });
    }

    void release() noexcept {
        _sem.signal(1);
        // Clear the checkpoint store when the semaphore is released.
        _checkpoint_store._current.reset();
    }

    friend class basic_mutex_units<Clock>;
    friend struct detail::checkpoint_mutex_locker;

    // The checkpoint store is used to keep track of the last checkpoint that
    // was able to acquire units.
    checkpoint_store _checkpoint_store;
    seastar::basic_semaphore<checkpoint_mutex_exception_factory, Clock> _sem;
};

template<class Clock = seastar::timer<>::clock>
class [[nodiscard]] basic_mutex_units {
public:
    basic_mutex_units() noexcept = default;
    explicit basic_mutex_units(basic_checkpoint_mutex<Clock>& mut)
      : _mut(&mut) {
        // This c-tor assumes that the units are already acquired.
        vassert(!_mut->has_units(), "The mutex is not acquired");
    }
    ~basic_mutex_units() noexcept {
        // Release the mutex if it is being held.
        release();
    }
    basic_mutex_units(const basic_mutex_units&) = delete;
    basic_mutex_units& operator=(const basic_mutex_units&) = delete;
    basic_mutex_units(basic_mutex_units&& other) noexcept
      : _mut(std::exchange(other._mut, nullptr)) {
        // Move constructor transfers ownership of the mutex.
        // The moved-from object will not hold any mutex.
        if (_mut) {
            vassert(!_mut->has_units(), "The mutex is not acquired");
        }
    }
    basic_mutex_units& operator=(basic_mutex_units&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        _mut = std::exchange(other._mut, nullptr);
        if (_mut) {
            vassert(!_mut->has_units(), "The mutex is not acquired");
        }
        return *this;
    }
    void release() noexcept {
        if (_mut) {
            _mut->release();
            _mut = nullptr;
        }
    }

private:
    basic_checkpoint_mutex<Clock>* _mut;
};

namespace detail {
struct checkpoint_mutex_locker {
    template<typename Clock>
    static seastar::future<basic_mutex_units<Clock>>
    lock(basic_checkpoint_mutex<Clock>& mut, seastar::sstring checkpoint_name) {
        return mut.lock(std::move(checkpoint_name)).then([&mut]() {
            return basic_mutex_units<Clock>(mut);
        });
    }
    template<typename Clock>
    static seastar::future<basic_mutex_units<Clock>> lock(
      basic_checkpoint_mutex<Clock>& mut,
      typename Clock::time_point deadline,
      seastar::sstring checkpoint_name) {
        return mut.lock(std::move(checkpoint_name), deadline).then([&mut]() {
            return basic_mutex_units<Clock>(mut);
        });
    }
    template<typename Clock>
    static seastar::future<basic_mutex_units<Clock>> lock(
      basic_checkpoint_mutex<Clock>& mut,
      typename Clock::duration timeout,
      seastar::sstring checkpoint_name) {
        return mut.lock(std::move(checkpoint_name), timeout).then([&mut]() {
            return basic_mutex_units<Clock>(mut);
        });
    }
    template<typename Clock>
    static seastar::future<basic_mutex_units<Clock>> lock(
      basic_checkpoint_mutex<Clock>& mut,
      seastar::abort_source& as,
      seastar::sstring checkpoint_name) {
        return mut.lock(std::move(checkpoint_name), as).then([&mut]() {
            return basic_mutex_units<Clock>(mut);
        });
    }
};

} // namespace detail

/// Acquire mutex and create checkpoint.
template<typename Clock = seastar::timer<>::clock>
seastar::future<basic_mutex_units<Clock>> get_units_with_checkpoint(
  basic_checkpoint_mutex<Clock>& mut, seastar::sstring checkpoint_name) {
    return detail::checkpoint_mutex_locker::lock(
      mut, std::move(checkpoint_name));
}
template<typename Clock = seastar::timer<>::clock>
seastar::future<basic_mutex_units<Clock>> get_units_with_checkpoint(
  basic_checkpoint_mutex<Clock>& mut,
  seastar::abort_source& as,
  seastar::sstring checkpoint_name) {
    return detail::checkpoint_mutex_locker::lock(
      mut, as, std::move(checkpoint_name));
}
template<typename Clock = seastar::timer<>::clock>
seastar::future<basic_mutex_units<Clock>> get_units_with_checkpoint(
  basic_checkpoint_mutex<Clock>& mut,
  typename Clock::time_point deadline,
  seastar::sstring checkpoint_name) {
    return detail::checkpoint_mutex_locker::lock(
      mut, deadline, std::move(checkpoint_name));
}
template<typename Clock = seastar::timer<>::clock>
seastar::future<basic_mutex_units<Clock>> get_units_with_checkpoint(
  basic_checkpoint_mutex<Clock>& mut,
  typename Clock::duration timeout,
  seastar::sstring checkpoint_name) {
    return detail::checkpoint_mutex_locker::lock(
      mut, timeout, std::move(checkpoint_name));
}

using checkpoint_mutex = basic_checkpoint_mutex<>;
using checkpoint_mutex_units = basic_mutex_units<>;

} // namespace ssx
