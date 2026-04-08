/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/common/producer_queue.h"

#include "config/configuration.h"
#include "container/chunked_hash_map.h"
#include "model/record.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

namespace cloud_topics::l0 {

namespace {
class ticket_impl;
}

using producer_state_map = chunked_hash_map<model::producer_id, ticket_impl*>;

class producer_ticket::impl {
public:
    virtual ~impl() = default;
    virtual ss::future<> redeem() = 0;
    virtual void release() = 0;
};

namespace {

class nopid_ticket_impl final : public producer_ticket::impl {
public:
    explicit nopid_ticket_impl(ss::semaphore& s)
      : _s(s) {}
    ~nopid_ticket_impl() override = default;

    ss::future<> redeem() override {
        try {
            _units = co_await ss::get_units(_s, 1, _as);
        } catch (const ss::abort_requested_exception&) {
            // release() was called before the semaphore was acquired;
            // return normally so the caller can propagate the abort.
        }
    }

    void release() override {
        _units = {};
        _as.request_abort();
    }

    ss::semaphore& _s;
    ss::semaphore_units<> _units;
    ss::abort_source _as;
};

/// A real ticket that enforces ordering by chaining futures.
class ticket_impl final : public producer_ticket::impl {
public:
    ticket_impl(
      ticket_impl* prev,
      model::producer_id pid,
      ss::lw_shared_ptr<producer_state_map> map)
      : _prev(prev)
      , _pid(pid)
      , _map(std::move(map)) {
        if (_prev != nullptr) {
            // If we're waiting for a previous ticket to be released
            // tell it who to notify next.
            _prev->_next = this;
        } else {
            // Otherwise we're at the head of the queue we can resolve ourself.
            _p.set_value();
        }
    }

    ~ticket_impl() override { release(); }

    ss::future<> redeem() override { return _p.get_future(); }

    void release() override {
        // Use the null map to signify a released ticket.
        if (!_map) {
            return;
        }
        // If we are waiting on something, then change it to be waiting on the
        // next element instead.
        if (_prev != nullptr) {
            _prev->_next = _next;
            // We also resolve our own future if we were waiting on something
            // but were released early.
            _p.set_value();
        }
        if (_next != nullptr) {
            // If we have no previous node, then we can set the next as
            // resolved.
            if (_prev == nullptr) {
                _next->_p.set_value();
            }
            _next->_prev = _prev;
        } else if (_prev != nullptr) {
            // If we have no next node, but there is a previous node,
            // update it to be the tail of the list
            _map->insert_or_assign(_pid, _prev);
        } else {
            // There is no next or previous state, we can cleanup.
            _map->erase(_pid);
        }
        _map = nullptr;
        _prev = nullptr;
        _next = nullptr;
    }

private:
    ticket_impl* _prev;
    ticket_impl* _next = nullptr;

    model::producer_id _pid;
    ss::lw_shared_ptr<producer_state_map> _map;
    ss::promise<> _p;
};

} // namespace

class producer_queue::impl {
public:
    impl()
      : _no_pid(
          config::shard_local_cfg().cloud_topics_produce_no_pid_concurrency()) {
    }

    producer_ticket reserve(model::producer_id pid) {
        if (pid == model::no_producer_id) {
            return producer_ticket{
              std::make_unique<nopid_ticket_impl>(_no_pid)};
        }
        auto it = _producer_states->find(pid);
        ticket_impl* prev = nullptr;
        if (it != _producer_states->end()) {
            prev = it->second;
        }
        auto ticket = std::make_unique<ticket_impl>(
          prev, pid, _producer_states);
        _producer_states->insert_or_assign(it, pid, ticket.get());
        return producer_ticket{std::move(ticket)};
    }

    size_t size() const { return _producer_states->size(); }

private:
    ss::lw_shared_ptr<producer_state_map> _producer_states
      = ss::make_lw_shared<producer_state_map>();

    ss::semaphore _no_pid;
};

producer_ticket::producer_ticket() = default;

producer_ticket::producer_ticket(std::unique_ptr<impl> ticket_impl)
  : _impl(std::move(ticket_impl)) {}

producer_ticket::~producer_ticket() = default;

producer_ticket::producer_ticket(producer_ticket&&) noexcept = default;

producer_ticket&
producer_ticket::operator=(producer_ticket&&) noexcept = default;

ss::future<> producer_ticket::redeem() { return _impl->redeem(); }
ss::future<> producer_ticket::redeem(ss::abort_source& as) {
    std::exception_ptr ep;
    auto sub = as.subscribe(
      [this, &as, &ep](const std::optional<std::exception_ptr>& e) noexcept {
          ep = e.value_or(as.get_default_exception());
          _impl->release(); // this will trigger the waiting future to resolve
      });
    if (!sub) {
        co_await ss::coroutine::return_exception_ptr(
          as.abort_requested_exception_ptr());
    }
    co_await _impl->redeem();
    if (ep) {
        co_await ss::coroutine::return_exception_ptr(ep);
    }
}

void producer_ticket::release() { _impl->release(); }

// producer_queue wrapper implementation
producer_queue::producer_queue()
  : _impl(std::make_unique<impl>()) {}

producer_queue::~producer_queue() = default;

producer_queue::producer_queue(producer_queue&&) noexcept = default;

producer_queue& producer_queue::operator=(producer_queue&&) noexcept = default;

producer_ticket producer_queue::reserve(model::producer_id pid) {
    return _impl->reserve(pid);
}

size_t producer_queue::size() const { return _impl->size(); }

} // namespace cloud_topics::l0
