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

#include "ssx/sformat.h"
#include "vassert.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
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

retry_chain_node::retry_chain_node(ss::abort_source& as)
  : _id(fiber_count++) // generate new head id
  , _backoff{0}
  , _deadline{ss::lowres_clock::time_point::min()}
  , _parent(&as) {}

retry_chain_node::retry_chain_node(
  ss::abort_source& as,
  ss::lowres_clock::time_point deadline,
  ss::lowres_clock::duration backoff)
  : _id(fiber_count++) // generate new head id
  , _backoff{std::chrono::duration_cast<std::chrono::milliseconds>(backoff)}
  , _deadline{deadline}
  , _parent(&as) {
    vassert(
      backoff <= milliseconds_uint16_t::max(),
      "Initial backoff {} is too large",
      backoff);
}

retry_chain_node::retry_chain_node(
  ss::abort_source& as,
  ss::lowres_clock::duration timeout,
  ss::lowres_clock::duration backoff)
  : retry_chain_node(as, ss::lowres_clock::now() + timeout, backoff) {}

retry_chain_node::retry_chain_node(retry_chain_node* parent)
  : _id(parent->add_child())
  , _backoff{parent->_backoff}
  , _deadline{parent->_deadline}
  , _parent{parent} {
    auto len = get_len();
    vassert(
      len < max_retry_chain_depth, "Retry chain is too deep, {} >= 8", len);
}

retry_chain_node::retry_chain_node(
  ss::lowres_clock::duration backoff, retry_chain_node* parent)
  : _id(parent->add_child())
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

retry_chain_node::retry_chain_node(
  ss::lowres_clock::time_point deadline,
  ss::lowres_clock::duration backoff,
  retry_chain_node* parent)
  : _id(parent->add_child())
  , _backoff{std::chrono::duration_cast<std::chrono::milliseconds>(backoff)}
  , _deadline{deadline}
  , _parent{parent} {
    vassert(
      backoff <= milliseconds_uint16_t::max(),
      "Initial backoff {} is too large",
      backoff);

    if (auto parent = get_parent();
        parent != nullptr
        && parent->_deadline != ss::lowres_clock::time_point::min()) {
        _deadline = std::min(_deadline, parent->_deadline);
    }
    auto len = get_len();
    vassert(
      len < max_retry_chain_depth, "Retry chain is too deep, {} >= 8", len);
}
retry_chain_node::retry_chain_node(
  ss::lowres_clock::duration timeout,
  ss::lowres_clock::duration backoff,
  retry_chain_node* parent)
  : retry_chain_node(ss::lowres_clock::now() + timeout, backoff, parent) {}

retry_chain_node* retry_chain_node::get_parent() {
    if (std::holds_alternative<retry_chain_node*>(_parent)) {
        return std::get<retry_chain_node*>(_parent);
    }
    return nullptr;
}

const retry_chain_node* retry_chain_node::get_parent() const {
    if (std::holds_alternative<retry_chain_node*>(_parent)) {
        return std::get<retry_chain_node*>(_parent);
    }
    return nullptr;
}

ss::abort_source* retry_chain_node::get_abort_source() {
    if (std::holds_alternative<ss::abort_source*>(_parent)) {
        return std::get<ss::abort_source*>(_parent);
    }
    return nullptr;
}

const ss::abort_source* retry_chain_node::get_abort_source() const {
    if (std::holds_alternative<ss::abort_source*>(_parent)) {
        return std::get<ss::abort_source*>(_parent);
    }
    return nullptr;
}

retry_chain_node::~retry_chain_node() {
    vassert(
      _num_children == 0,
      "{} Fiber stopped before its dependencies, num children {}",
      (*this)(),
      _num_children);
    if (auto parent = get_parent(); parent != nullptr) {
        parent->rem_child();
    }
}
ss::sstring retry_chain_node::operator()() const {
    fmt::memory_buffer buf;
    auto bii = std::back_insert_iterator(buf);
    bii = '[';
    format(bii);
    bii = ']';
    return ss::sstring(buf.data(), buf.size());
}

const retry_chain_node* retry_chain_node::get_root() const {
    auto it = this;
    auto root = it;
    while (it) {
        root = it;
        it = it->get_parent();
    }
    return root;
}

bool retry_chain_node::same_root(const retry_chain_node& other) const {
    return get_root() == other.get_root();
}

retry_permit retry_chain_node::retry(retry_strategy st) {
    auto& as = root_abort_source();
    as.check();

    auto now = ss::lowres_clock::now();
    if (
      _deadline < now || _deadline == ss::lowres_clock::time_point::min()
      || _retry == max_retry_count) {
        // deadline is not set or _retry counter is about to overflow (which
        // will lead to 0ms backoff time) retries are not allowed
        return {.is_allowed = false, .abort_source = &as, .delay = 0ms};
    }
    auto required_delay = st == retry_strategy::backoff ? get_backoff()
                                                        : get_poll_interval();
    _retry++;
    return {
      .is_allowed = (now + required_delay) < _deadline,
      .abort_source = &as,
      .delay = required_delay};
}

ss::lowres_clock::duration retry_chain_node::get_backoff() const {
    ss::lowres_clock::duration backoff(_backoff * (1UL << _retry));
    ss::lowres_clock::duration jitter(fast_prng_source() % backoff.count());
    return backoff + jitter;
}

ss::lowres_clock::duration retry_chain_node::get_poll_interval() const {
    ss::lowres_clock::duration jitter(fast_prng_source() % _backoff.count());
    return _backoff + jitter;
}

ss::lowres_clock::duration retry_chain_node::get_timeout() const {
    auto now = ss::lowres_clock::now();
    return now < _deadline ? _deadline - now : 0ms;
}

ss::lowres_clock::time_point retry_chain_node::get_deadline() const {
    return _deadline;
}

uint16_t retry_chain_node::get_len() const {
    uint16_t len = 1;
    auto next = get_parent();
    while (next) {
        len++;
        next = next->get_parent();
    }
    return len;
}

void retry_chain_node::format(
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
    if (_deadline != ss::lowres_clock::time_point::min()) {
        auto now = ss::lowres_clock::now();
        ss::lowres_clock::duration time_budget{0ms};
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

uint16_t retry_chain_node::add_child() {
    _num_children++;
    return _fanout_id++;
}

void retry_chain_node::rem_child() { _num_children--; }

void retry_chain_node::request_abort() {
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

void retry_chain_node::check_abort() const {
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

ss::abort_source& retry_chain_node::root_abort_source() {
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

void retry_chain_logger::do_log(
  ss::log_level lvl,
  ss::noncopyable_function<void(ss::logger&, ss::log_level)> fn) const {
    fn(_log, lvl);
}
