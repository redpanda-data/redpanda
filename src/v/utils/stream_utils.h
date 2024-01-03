/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "base/vassert.h"
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>

#include <exception>

namespace detail {

/// Helper template that wraps the input stream and consumes
/// it to the cyclic buffer
template<class Ch>
class input_stream_fanout final {
    struct data_item {
        ss::temporary_buffer<Ch> buf;
        ssx::semaphore_units units;
        unsigned mask;
    };
    using ring_buffer = ss::circular_buffer<data_item>;

public:
    /// C-tor
    ///
    /// \param i is a source input stream
    /// \param num_clients is a number of consumers (fanout ratio)
    /// \param read_ahead is a number of buffers that can be in-flight at the
    /// same time
    ///        (should be relatively low, e.g. 10)
    /// \param max_size is an optional size limit for in-flight data
    input_stream_fanout(
      ss::input_stream<Ch> i,
      size_t num_clients,
      size_t read_ahead,
      std::optional<size_t> max_size)
      : _num_clients(num_clients)
      , _bitmask(0)
      , _max_size(max_size ? *max_size / read_ahead : 0)
      , _in(std::move(i))
      , _sem(read_ahead, "stream-fanout") {
        vassert(
          num_clients <= 10 && num_clients >= 2,
          "input_stream_fanout can have up to 10 clients, {} were given",
          num_clients);
        vassert(
          read_ahead != 0,
          "input_stream_fanaout can't have zero readahead value");
    }

    /// Start consuming the input stream
    void start() {
        // Run produce in the background until stopped
        // or produced everything.
        (void)produce();
    }

    /// Stop consuming the input stream
    ss::future<> stop() {
        _pcond.broadcast();
        co_await _gate.close();
        co_await _in.close();
    }

    /// Detach client with 'index' from the fanout
    ///
    /// The remaining clients can proceed as usual.
    /// If the client is the last one, the stop method will be invoked.
    ss::future<> detach(unsigned index) {
        auto m = 1U << index;
        _bitmask |= m;
        if (_bitmask == (1U << _num_clients) - 1) {
            // This is the last client, call stop
            co_await stop();
        }

        // update all in-flight buffers
        auto all_bits = (1U << _num_clients) - 1;
        auto cleanup_mark = _buffer.begin();
        for (auto it = _buffer.begin(); it != _buffer.end(); ++it) {
            it->mask |= m;
            if (unlikely(it->mask == all_bits)) {
                vassert(
                  it == cleanup_mark,
                  "there are non contiguous buffers eligible for cleanup: {}",
                  std::distance(cleanup_mark, it));
                cleanup_mark++;
            }
        }

        // Erase all elements from the buffer which have all bits set before we
        // start scanning it. This will usually happen if some clients have read
        // the buffers and others exited early, resulting in a mask of all_bits.
        _buffer.erase(_buffer.begin(), cleanup_mark);
    }

    /// Get next buffer from original input stream
    ///
    /// \param index is an index of the consumer
    /// \return buffer
    ss::future<ss::temporary_buffer<Ch>> get(unsigned index) {
        auto g = _gate.hold();
        while (!_gate.is_closed()) {
            auto gen = _cnt;
            if (_producer_error) {
                std::rethrow_exception(_producer_error);
            }
            if (auto ob = maybe_get(index); ob.has_value()) {
                co_return std::move(ob.value());
            }
            // We need to wait for the data in the following scenarios:
            // - The consumer is outrunning other consumers and the producer.
            // - There is not data in the buffer because consumers are faster
            // then the producer.
            try {
                co_await _pcond.wait(
                  [this, gen] { return _cnt != gen || _producer_error; });
            } catch (const ss::broken_condition_variable&) {
            }
        }
        co_return ss::temporary_buffer<Ch>();
    }

private:
    /// Get next buffer from original input stream
    ///
    /// \return buffer or std::nullopt if the consumer need to repeat
    std::optional<ss::temporary_buffer<Ch>> maybe_get(unsigned index) {
        vassert(
          index < _num_clients,
          "Consumer index {} is too large. Only {} consumers allowed.",
          index,
          _num_clients);
        unsigned mask = 1U << index;
        std::optional<ss::temporary_buffer<Ch>> buf = std::nullopt;

        auto next = _buffer.end();
        for (auto it = _buffer.begin(); it != _buffer.end(); it++) {
            if ((it->mask & mask) == 0) {
                it->mask |= mask;
                next = it;
                break;
            }
        }

        auto all_bits = (1U << _num_clients) - 1;
        if (next != _buffer.end()) {
            if (next->mask == all_bits) {
                // The item is consumed by all clients
                buf = std::move(next->buf);
                vassert(
                  next == _buffer.begin(),
                  "broken input_stream_fanout invariant");
                _buffer.pop_front();
            } else {
                // Other consumers will read this item
                buf = next->buf.share();
            }
        }
        return buf;
    }
    /// Constantly pull data from input stream and produce
    /// the resulting buffers to clients
    ss::future<> produce() {
        auto g = _gate.hold();
        try {
            while (!_in.eof() && !_gate.is_closed()) {
                auto units = co_await ss::get_units(_sem, 1);
                ss::temporary_buffer<Ch> buf;
                if (_max_size == 0) {
                    buf = co_await _in.read();
                } else {
                    buf = co_await _in.read_up_to(_max_size);
                }
                ++_cnt;
                _buffer.push_back(data_item{
                  .buf = std::move(buf),
                  .units = std::move(units),
                  .mask = _bitmask});
                _pcond.broadcast();
            }
        } catch (...) {
            _producer_error = std::current_exception();
            _pcond.broken();
        }
    }

    const size_t _num_clients;
    unsigned _bitmask;
    const size_t _max_size;
    ss::input_stream<Ch> _in;
    ssx::semaphore _sem;
    ss::condition_variable _pcond;
    ring_buffer _buffer;
    ss::gate _gate;
    uint64_t _cnt{0};
    std::exception_ptr _producer_error{nullptr};
};

template<class Ch>
struct fanout_data_source final : ss::data_source_impl {
    fanout_data_source(
      ss::lw_shared_ptr<input_stream_fanout<Ch>> i, size_t source_id)
      : _isf(std::move(i))
      , _id{source_id} {}

    ss::future<> close() final { co_await _isf->detach(_id); }

    ss::future<ss::temporary_buffer<char>> skip(uint64_t) final {
        vassert(false, "Not supported");
    }
    ss::future<ss::temporary_buffer<char>> get() final {
        co_return co_await _isf->get(_id);
    }
    ss::lw_shared_ptr<input_stream_fanout<Ch>> _isf;
    size_t _id;
};

template<typename Ch, size_t... ix>
auto construct_data_sources(
  ss::lw_shared_ptr<input_stream_fanout<Ch>> fanout,
  std::integer_sequence<size_t, ix...>) {
    return (
      std::make_tuple(std::make_unique<fanout_data_source<Ch>>(fanout, ix)...));
}

} // namespace detail

template<size_t N, typename Ch>
auto input_stream_fanout(
  ss::input_stream<Ch> in,
  size_t read_ahead,
  std::optional<size_t> memory_limit = std::nullopt) {
    static_assert(N >= 2 && N <= 10, "N should be in 2..10 range");
    auto f = ss::make_lw_shared<detail::input_stream_fanout<Ch>>(
      std::move(in), N, read_ahead, memory_limit);
    f->start();
    auto ixseq = std::make_index_sequence<N>{};
    auto fds = detail::construct_data_sources(f, ixseq);
    auto is = std::apply(
      [](auto&&... ds) {
          return (std::make_tuple(
            ss::input_stream<Ch>(ss::data_source(std::move(ds)))...));
      },
      fds);
    return is;
}
