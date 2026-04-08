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
#include "random/generators.h"
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
namespace detail {
rtc_circular_buffer::rtc_circular_buffer(size_t capacity)
  : _data()
  , _capacity(capacity) {}

void rtc_circular_buffer::push_back(char value) {
    if (_size < _capacity) {
        _data.push_back(value);
        ++_size;
    } else {
        _data[_start] = value;
        _start = (_start + 1) % _capacity;
    }
    _total_added++;
}

void rtc_circular_buffer::push_back(std::string_view sv) {
    if (sv.empty()) {
        return;
    }

    if (sv.size() >= _capacity) {
        // String is larger than or equal to capacity, only keep the last
        // part
        auto offset = sv.size() - _capacity;
        _data.assign(sv.begin() + offset, sv.end());
        _size = _capacity;
        _start = 0;
        _total_added += sv.size();
        return;
    }

    // String fits within capacity
    size_t bytes_to_copy = sv.size();

    if (_size < _capacity) {
        // Buffer not full, append normally
        size_t space_available = _capacity - _size;
        size_t n = std::min(space_available, bytes_to_copy);
        std::copy(
          sv.begin(), std::next(sv.begin(), n), std::back_inserter(_data));
        std::copy(std::next(sv.begin(), n), sv.end(), _data.begin());
        _size += n;
        _start = (bytes_to_copy - n) % _capacity;
    } else {
        // Buffer is full, overwrite using circular logic
        size_t space_to_end = _capacity - _start;
        if (bytes_to_copy <= space_to_end) {
            // Case one: enough space from _start to end of buffer
            std::memcpy(_data.data() + _start, sv.data(), bytes_to_copy);
            _start = (_start + bytes_to_copy) % _capacity;
        } else {
            // Case two: not enough space, need two memcpy calls
            std::memcpy(_data.data() + _start, sv.data(), space_to_end);
            size_t remaining = bytes_to_copy - space_to_end;
            std::memcpy(_data.data(), sv.data() + space_to_end, remaining);
            _start = remaining;
        }
    }

    _total_added += bytes_to_copy;
}

/// Access element at logical index (0 is the oldest element)
rtc_circular_buffer::reference rtc_circular_buffer::operator[](size_t index) {
    return _data[(_start + index) % _capacity];
}

/// Access element at logical index (0 is the oldest element)
rtc_circular_buffer::const_reference
rtc_circular_buffer::operator[](size_t index) const {
    return _data[(_start + index) % _capacity];
}

/// Access element at logical index with bounds checking
rtc_circular_buffer::reference rtc_circular_buffer::at(size_t index) {
    if (index >= _size) {
        throw std::out_of_range("circular_buffer::at");
    }
    return _data[(_start + index) % _capacity];
}

/// Access element at logical index with bounds checking
rtc_circular_buffer::const_reference
rtc_circular_buffer::at(size_t index) const {
    if (index >= _size) {
        throw std::out_of_range("circular_buffer::at");
    }
    return _data[(_start + index) % _capacity];
}

/// Get the first (oldest) element
rtc_circular_buffer::reference rtc_circular_buffer::front() {
    return _data[_start];
}

/// Get the first (oldest) element
rtc_circular_buffer::const_reference rtc_circular_buffer::front() const {
    return _data[_start];
}

/// Get the last (newest) element
rtc_circular_buffer::reference rtc_circular_buffer::back() {
    if (_size < _capacity) {
        return _data[_size - 1];
    } else {
        return _data[(_start + _size - 1) % _capacity];
    }
}

/// Get the last (newest) element
rtc_circular_buffer::const_reference rtc_circular_buffer::back() const {
    if (_size < _capacity) {
        return _data[_size - 1];
    } else {
        return _data[(_start + _size - 1) % _capacity];
    }
}

/// Number of elements currently in the buffer
size_t rtc_circular_buffer::size() const noexcept { return _size; }

/// Maximum capacity of the buffer
size_t rtc_circular_buffer::capacity() const noexcept { return _capacity; }

/// Check if the buffer is empty
bool rtc_circular_buffer::empty() const noexcept { return _size == 0; }

/// Check if the buffer is at full capacity
bool rtc_circular_buffer::full() const noexcept { return _size == _capacity; }

/// Get the number of elements that were added to the buffer and
/// then overwritten and lost. The count is reset if 'clear' is
/// called.
size_t rtc_circular_buffer::overwritten() const noexcept {
    return _total_added > _capacity ? _total_added - _capacity : 0;
}

/// Clear all elements from the buffer
void rtc_circular_buffer::clear() noexcept {
    _size = 0;
    _start = 0;
    _total_added = 0;
    _data.clear();
    _data.shrink_to_fit();
}
} // namespace detail

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(
  basic_retry_chain_context<Clock>& ctx)
  : _id(fiber_count++) // generate new head id
  , _backoff{0}
  , _deadline{time_point::min()}
  , _parent(&ctx) {}

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(
  basic_retry_chain_context<Clock>& ctx,
  time_point deadline,
  duration backoff)
  : _id(fiber_count++) // generate new head id
  , _backoff{std::chrono::duration_cast<std::chrono::milliseconds>(backoff)}
  , _deadline{deadline}
  , _parent(&ctx) {
    vassert(
      backoff <= milliseconds_uint16_t::max(),
      "Initial backoff {} is too large",
      backoff);
}

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(
  basic_retry_chain_context<Clock>& ctx,
  time_point deadline,
  duration backoff,
  retry_strategy retry_strategy)
  : basic_retry_chain_node(ctx, deadline, backoff) {
    _retry_strategy = retry_strategy;
}

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(
  basic_retry_chain_context<Clock>& ctx, duration timeout, duration backoff)
  : basic_retry_chain_node(ctx, Clock::now() + timeout, backoff) {}

template<class Clock>
basic_retry_chain_node<Clock>::basic_retry_chain_node(
  basic_retry_chain_context<Clock>& ctx,
  duration timeout,
  duration backoff,
  retry_strategy retry_strategy)
  : basic_retry_chain_node(ctx, timeout, backoff) {
    _retry_strategy = retry_strategy;
}

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

    if (
      auto parent = get_parent();
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
    } else if (std::holds_alternative<context_t*>(_parent)) {
        return &std::get<context_t*>(_parent)->as();
    }
    return nullptr;
}

template<class Clock>
const ss::abort_source*
basic_retry_chain_node<Clock>::get_abort_source() const {
    if (std::holds_alternative<ss::abort_source*>(_parent)) {
        return std::get<ss::abort_source*>(_parent);
    } else if (std::holds_alternative<context_t*>(_parent)) {
        return &std::get<context_t*>(_parent)->as();
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
    duration jitter(random_generators::global().get_int(backoff.count() - 1));
    return backoff + jitter;
}

template<class Clock>
typename Clock::duration
basic_retry_chain_node<Clock>::get_poll_interval() const {
    duration jitter(random_generators::global().get_int(_backoff.count() - 1));
    return _backoff + jitter;
}

template<class Clock>
bool basic_retry_chain_node<Clock>::has_retry_chain_context() const {
    auto root = get_root();
    return std::holds_alternative<basic_retry_chain_context<Clock>*>(
      root->_parent);
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
void basic_retry_chain_node<Clock>::maybe_add_trace(
  const ss::sstring& s) const {
    auto root = get_root();
    if (!root->has_retry_chain_context()) {
        return;
    }
    std::get<context_t*>(root->_parent)->add_trace(s);
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
