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

#include "base/seastarx.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <array>
#include <optional>

namespace ssx {

// Policy for handling messages when the mailbox is full
enum class overflow_policy : uint8_t {
    // Block the sender until space is available
    block,
    // Drop the oldest message in the mailbox to make room
    drop_oldest,
};

namespace detail {

// Fixed-size ring buffer with no heap allocation
template<typename T, size_t Capacity>
class ring_buffer {
public:
    void push_back(T val) noexcept {
        new (&_data[_tail].data) T(std::move(val));
        _tail = (_tail + 1) % Capacity;
        ++_size;
    }

    T pop_front() noexcept {
        auto val = std::move(_data[_head].data);
        (&_data[_head].data)->~T();
        _head = (_head + 1) % Capacity;
        --_size;
        return val;
    }

    [[nodiscard]] bool empty() const { return _size == 0; }
    [[nodiscard]] size_t size() const { return _size; }

private:
    // This is a trick that seastar uses for `circular_buffer_fixed_capacity`
    // to allow uninitialized storage.
    union maybe_storage {
        T data;
        maybe_storage() noexcept {}
        ~maybe_storage() {}
    };
    std::array<maybe_storage, Capacity> _data;
    size_t _head{0};
    size_t _tail{0};
    size_t _size{0};
};

// Primary template: use ring_buffer
template<typename T, size_t MaxSize>
struct mailbox_storage {
    using type = ring_buffer<T, MaxSize>;

    static bool is_full(const type& m, size_t max) { return m.size() >= max; }
    static bool is_empty(const type& m) { return m.empty(); }
    static void push(type& m, T val) { m.push_back(std::move(val)); }
    static T pop(type& m) { return m.pop_front(); }
};

// Specialization for MaxSize=1: use std::optional
template<typename T>
struct mailbox_storage<T, 1> {
    using type = std::optional<T>;

    static bool is_full(const type& m, size_t) { return m.has_value(); }
    static bool is_empty(const type& m) { return !m.has_value(); }
    static void push(type& m, T val) { m.emplace(std::move(val)); }
    static T pop(type& m) {
        auto v = std::move(*m);
        m.reset();
        return v;
    }
};

} // namespace detail

/**
 * A base class for implementing actors with a message mailbox.
 *
 * Derive from this class and implement the process() method to handle
 * messages of type T. The actor processes messages sequentially on a
 * single fiber.
 *
 * @tparam T The message type
 * @tparam MaxQueueSize Maximum number of messages in the mailbox
 * @tparam OverflowPolicy How to handle messages when mailbox is full
 */
template<
  typename T,
  size_t MaxQueueSize,
  overflow_policy OverflowPolicy = overflow_policy::block>
class actor {
public:
    actor() = default;
    actor(const actor&) = delete;
    actor& operator=(const actor&) = delete;
    actor(actor&&) = delete;
    actor& operator=(actor&&) = delete;
    virtual ~actor() = default;

    // Start the actor's processing loop
    virtual ss::future<> start() {
        ssx::background = ss::with_gate(_gate, [this] { return run(); });
        return ss::now();
    }

    // Stop the actor, waiting for current message processing to complete
    virtual ss::future<> stop() {
        _as.request_abort();
        _cv.broadcast();
        co_await _gate.close();
    }

    // Send a message. It waits until space is available.
    ss::future<> tell(T msg)
    requires(OverflowPolicy == overflow_policy::block)
    {
        if (_as.abort_requested()) {
            co_return;
        }
        auto holder = _gate.hold();
        // Wait for space if mailbox is full
        while (!_as.abort_requested() && is_full()) {
            co_await _cv.wait();
        }
        if (_as.abort_requested()) {
            co_return;
        }
        storage_traits::push(_mailbox, std::move(msg));
        _cv.broadcast();
    }

    // Send a message. It drops the oldest message to make room (never blocks)
    void tell(T msg)
    requires(OverflowPolicy == overflow_policy::drop_oldest)
    {
        if (_as.abort_requested()) {
            return;
        }
        auto holder = _gate.hold();
        // Drop oldest if full
        if (is_full()) {
            storage_traits::pop(_mailbox);
        }
        storage_traits::push(_mailbox, std::move(msg));
        _cv.broadcast();
    }

    // Try to send a message without blocking, returns false if mailbox is full.
    // Only available when OverflowPolicy is block (with drop_oldest, tell()
    // never blocks so try_tell is unnecessary).
    bool try_tell(T msg)
    requires(OverflowPolicy == overflow_policy::block)
    {
        if (_as.abort_requested() || is_full()) {
            return false;
        }
        storage_traits::push(_mailbox, std::move(msg));
        _cv.broadcast();
        return true;
    }

protected:
    // Override this to process messages
    virtual ss::future<> process(T msg) = 0;

    // Override this to handle exceptions thrown by process()
    virtual void on_error(std::exception_ptr) noexcept = 0;

private:
    using storage_traits = detail::mailbox_storage<T, MaxQueueSize>;
    using mailbox_type = typename storage_traits::type;

    bool is_full() const {
        return storage_traits::is_full(_mailbox, MaxQueueSize);
    }

    bool is_empty() const { return storage_traits::is_empty(_mailbox); }

    ss::future<> run() {
        while (true) {
            while (!_as.abort_requested() && is_empty()) {
                co_await _cv.wait();
            }
            if (_as.abort_requested()) {
                co_return;
            }
            auto msg = storage_traits::pop(_mailbox);
            // Signal waiters that space is available
            _cv.signal();
            try {
                co_await process(std::move(msg));
            } catch (...) {
                on_error(std::current_exception());
            }
        }
    }

protected:
    ss::abort_source _as;
    ss::gate _gate;

private:
    ss::condition_variable _cv;
    mailbox_type _mailbox;
};

} // namespace ssx
