/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "utils/retry_chain_node.h"

#include "base/vassert.h"
#include "ssx/sformat.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/ragel.hh>

#include <fmt/chrono.h>
#include <fmt/format.h>

#include <iterator>
#include <limits>
#include <variant>

using namespace std::chrono_literals;

/// Jitter
thread_local static uint32_t fiber_count = 0;
static constexpr size_t max_retry_chain_depth = 8;
static constexpr uint16_t max_retry_count = std::numeric_limits<uint16_t>::max()
                                            - 1;

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(ss::abort_source& as)
  : _id(fiber_count++) // generate new head id
  , _backoff{0}
  , _deadline{time_point::min()}
  , _parent(&as) {}

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(
  ss::abort_source& as,
  time_point deadline,
  duration backoff)
  : _id(fiber_count++) // generate new head id
  , _backoff{std::chrono::duration_cast<std::chrono::milliseconds>(backoff)}
  , _deadline{deadline}
  , _parent(&as) {
    vassert(
      backoff <= milliseconds_uint16_t::max(),
      "Initial backoff {} is too large",
      backoff);
}

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(
  ss::abort_source& as,
  time_point deadline,
  duration backoff,
  retry_strategy retry_strategy)
  : basic_retry_chain_node(as, deadline, backoff) {
    _retry_strategy = retry_strategy;
}

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(
  ss::abort_source& as, duration timeout, duration backoff)
  : basic_retry_chain_node(as, Clock::now() + timeout, backoff) {}

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(
  ss::abort_source& as,
  duration timeout,
  duration backoff,
  retry_strategy retry_strategy)
  : basic_retry_chain_node(as, timeout, backoff) {
    _retry_strategy = retry_strategy;
}

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(
  basic_retry_chain_node* parent)
  : _retry_strategy{parent->_retry_strategy}
  , _id(parent->add_child())
  , _backoff{parent->_backoff}
  , _deadline{parent->_deadline}
  , _parent{parent} {
    auto len = get_len();
    vassert(
      len < max_retry_chain_depth, "Retry chain is too deep, {} >= 8", len);
}

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(
  retry_strategy retry_strategy, basic_retry_chain_node* parent)
  : basic_retry_chain_node(parent) {
    _retry_strategy = retry_strategy;
}

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(
  duration backoff, basic_retry_chain_node* parent)
  : _retry_strategy{parent->_retry_strategy}
  , _id(parent->add_child())
  , _backoff{std::chrono::duration_cast<std::chrono::milliseconds>(backoff)}
  , _deadline{parent->_deadline}
  , _parent{parent} {
    vassert(
      backoff <= milliseconds_uint16_t::max(),
      "Initial backoff {} is too large",
      backoff);
    auto len = get_len();
    vassert(
      len < max_retry_chain_depth, "Retry chain is too deep, {} >= 8", len);
}

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(
  duration backoff,
  retry_strategy retry_strategy,
  basic_retry_chain_node* parent)
  : basic_retry_chain_node(backoff, parent) {
    _retry_strategy = retry_strategy;
}

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(
  time_point deadline, duration backoff, basic_retry_chain_node* parent)
  : _retry_strategy{parent->_retry_strategy}
  , _id(parent->add_child())
  , _backoff{std::chrono::duration_cast<std::chrono::milliseconds>(backoff)}
  , _deadline{deadline}
  , _parent{parent} {
    vassert(
      backoff <= milliseconds_uint16_t::max(),
      "Initial backoff {} is too large",
      backoff);

    if (auto parent = get_parent();
        parent != nullptr && parent->_deadline != time_point::min()) {
        _deadline = std::min(_deadline, parent->_deadline);
    }
    auto len = get_len();
    vassert(
      len < max_retry_chain_depth, "Retry chain is too deep, {} >= 8", len);
}

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(
  time_point deadline,
  duration backoff,
  retry_strategy retry_strategy,
  basic_retry_chain_node* parent)
  : basic_retry_chain_node(deadline, backoff, parent) {
    _retry_strategy = retry_strategy;
}

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(
  duration timeout, duration backoff, basic_retry_chain_node* parent)
  : basic_retry_chain_node(Clock::now() + timeout, backoff, parent) {}

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(
  duration timeout,
  duration backoff,
  retry_strategy retry_strategy,
  basic_retry_chain_node* parent)
  : basic_retry_chain_node(timeout, backoff, parent) {
    _retry_strategy = retry_strategy;
}

template<class Clock>
basic_retry_chain_node<Clock>* basic_retry_chain_node<Clock>::get_parent() {
    if (std::holds_alternative<basic_retry_chain_node*>(_parent)) {
        return std::get<basic_retry_chain_node*>(_parent);
    }
    return nullptr;
}

template<class Clock>
const basic_retry_chain_node<Clock>*
basic_retry_chain_node<Clock>::get_parent() const {
    if (std::holds_alternative<basic_retry_chain_node*>(_parent)) {
        return std::get<basic_retry_chain_node*>(_parent);
    }
    return nullptr;
}

template<class Clock>
ss::abort_source* basic_retry_chain_node<Clock>::get_abort_source() {
    if (std::holds_alternative<ss::abort_source*>(_parent)) {
        return std::get<ss::abort_source*>(_parent);
    }
    return nullptr;
}

template<class Clock>
const ss::abort_source*
basic_retry_chain_node<Clock>::get_abort_source() const {
    if (std::holds_alternative<ss::abort_source*>(_parent)) {
        return std::get<ss::abort_source*>(_parent);
    }
    return nullptr;
}

template<class Clock>
basic_retry_chain_node<Clock>::~basic_retry_chain_node() {
    vassert(
      _num_children == 0,
      "{} Fiber stopped before its dependencies, num children {}",
      (*this)(),
      _num_children);
    if (auto parent = get_parent(); parent != nullptr) {
        parent->rem_child();
    }
}

template<class Clock>
ss::sstring basic_retry_chain_node<Clock>::operator()() const {
    fmt::memory_buffer buf;
    auto bii = std::back_insert_iterator(buf);
    bii = '[';
    format(bii);
    bii = ']';
    return ss::sstring(buf.data(), buf.size());
}

template<class Clock>
const basic_retry_chain_node<Clock>*
basic_retry_chain_node<Clock>::get_root() const {
    auto it = this;
    auto root = it;
    while (it) {
        root = it;
        it = it->get_parent();
    }
    return root;
}

template<class Clock>
bool basic_retry_chain_node<Clock>::same_root(
  const basic_retry_chain_node<Clock>& other) const {
    return get_root() == other.get_root();
}

template<class Clock>
retry_permit basic_retry_chain_node<Clock>::retry() {
    auto& as = root_abort_source();
    as.check();

    auto now = Clock::now();
    if (
      _deadline < now || _deadline == time_point::min()
      || _retry == max_retry_count) {
        // deadline is not set or _retry counter is about to overflow (which
        // will lead to 0ms backoff time) retries are not allowed
        return {.is_allowed = false, .abort_source = &as, .delay = 0ms};
    }

    if (_retry_strategy == retry_strategy::disallow && _retry != 0) {
        return {.is_allowed = false, .abort_source = &as, .delay = 0ms};
    }

    auto required_delay = [this]() -> duration {
        switch (_retry_strategy) {
        case retry_strategy::backoff:
            return get_backoff();
        case retry_strategy::polling:
            return get_poll_interval();
        case retry_strategy::disallow:
            return 0ms;
        }
    }();

    _retry++;
    return {
      .is_allowed = (now + required_delay) < _deadline,
      .abort_source = &as,
      .delay = required_delay};
}

template<class Clock>
typename Clock::duration basic_retry_chain_node<Clock>::get_backoff() const {
    duration backoff(_backoff * (1UL << _retry));
    duration jitter(fast_prng_source() % backoff.count());
    return backoff + jitter;
}

template<class Clock>
typename Clock::duration
basic_retry_chain_node<Clock>::get_poll_interval() const {
    duration jitter(fast_prng_source() % _backoff.count());
    return _backoff + jitter;
}

template<class Clock>
typename Clock::duration basic_retry_chain_node<Clock>::get_timeout() const {
    auto now = Clock::now();
    return now < _deadline ? _deadline - now : 0ms;
}

template<class Clock>
typename Clock::time_point basic_retry_chain_node<Clock>::get_deadline() const {
    return _deadline;
}

template<class Clock>
uint16_t basic_retry_chain_node<Clock>::get_len() const {
    uint16_t len = 1;
    auto next = get_parent();
    while (next) {
        len++;
        next = next->get_parent();
    }
    return len;
}

template<class Clock>
void basic_retry_chain_node<Clock>::format(
  std::back_insert_iterator<fmt::memory_buffer>& bii) const {
    std::array<uint16_t, max_retry_chain_depth> ids{_id};
    int ids_len = 1;
    auto next = get_parent();
    while (next) {
        ids.at(ids_len) = next->_id;
        ids_len++;
        next = next->get_parent();
    }
    int ix = 0;
    for (auto id = ids.rbegin() + (max_retry_chain_depth - ids_len);
         id != ids.rend();
         ix++, id++) {
        if (ix == 0) {
            fmt::format_to(bii, "fiber{}", *id);
        } else {
            fmt::format_to(bii, "~{}", *id);
        }
    }
    if (_deadline != time_point::min()) {
        auto now = Clock::now();
        duration time_budget{0ms};
        if (now < _deadline) {
            time_budget = _deadline - now;
        }
        // [fiber42~0~4|2|100ms]
        fmt::format_to(
          bii,
          "|{}|{}",
          _retry,
          std::chrono::duration_cast<std::chrono::milliseconds>(time_budget));
    }
}

template<class Clock>
uint16_t basic_retry_chain_node<Clock>::add_child() {
    _num_children++;
    return _fanout_id++;
}

template<class Clock>
void basic_retry_chain_node<Clock>::rem_child() {
    _num_children--;
}

template<class Clock>
void basic_retry_chain_node<Clock>::request_abort() {
    // Follow the links until the root node will be found
    auto it = this;
    auto root = it;
    while (it) {
        root = it;
        it = it->get_parent();
    }
    auto as = root->get_abort_source();
    if (!as) {
        throw std::logic_error("Abort source not set");
    }
    as->request_abort();
}

template<class Clock>
void basic_retry_chain_node<Clock>::check_abort() const {
    auto it = this;
    auto root = it;
    while (it) {
        root = it;
        it = it->get_parent();
    }
    auto as = root->get_abort_source();
    if (as) {
        as->check();
    }
}

template<class Clock>
ss::abort_source& basic_retry_chain_node<Clock>::root_abort_source() {
    auto it = this;
    auto root = it;
    while (it) {
        root = it;
        it = it->get_parent();
    }
    auto as_ptr = root->get_abort_source();

    // This should never happen: our destructor asserts that all children
    // are destroyed before the parent.
    vassert(as_ptr != nullptr, "Root of retry_chain_node has no abort source!");
    return *as_ptr;
}

template<class Clock>
void basic_retry_chain_logger<Clock>::do_log(
  ss::log_level lvl,
  ss::noncopyable_function<void(ss::logger&, ss::log_level)> fn) const {
    fn(_log, lvl);
}

template class basic_retry_chain_node<ss::lowres_clock>;
template class basic_retry_chain_node<ss::manual_clock>;
template class basic_retry_chain_logger<ss::lowres_clock>;
template class basic_retry_chain_logger<ss::manual_clock>;
