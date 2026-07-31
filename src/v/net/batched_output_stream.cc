// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "net/batched_output_stream.h"

#include "base/likely.h"
#include "base/vassert.h"
#include "ssx/semaphore.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

namespace net {

batched_output_stream::batched_output_stream(
  ss::output_stream<char> o, size_t cache)
  : _out(std::move(o))
  , _cache_size(cache)
  , _write_sem(std::make_unique<ssx::semaphore>(1, "net/batch-ostream")) {
    // Size zero reserved for identifying default-initialized
    // instances in stop()
    vassert(_cache_size > 0, "Size must be > 0");
}

[[gnu::cold]] static ss::future<bool>
already_closed_error(scattered_buffer& bufs) {
    return ss::make_exception_future<bool>(
      batched_output_stream_closed(iobuf::scattered_size(bufs)));
}

ss::future<bool> batched_output_stream::write(scattered_buffer bufs) {
    if (unlikely(_closed)) {
        co_return co_await ss::coroutine::without_preemption_check(
          already_closed_error(bufs));
    }
    // Keep the write and waiter-based flush decision in one task turn when the
    // underlying futures are ready, matching the continuation implementation.
    auto units = co_await ss::coroutine::without_preemption_check(
      ss::get_units(*_write_sem, 1));
    if (unlikely(_closed)) {
        co_return co_await ss::coroutine::without_preemption_check(
          already_closed_error(bufs));
    }
    const size_t bytes = iobuf::scattered_size(bufs);
    co_await ss::coroutine::without_preemption_check(
      _out.write(std::span{bufs}));
    _unflushed_bytes += bytes;
    if (_write_sem->waiters() == 0 || _unflushed_bytes >= _cache_size) {
        co_await ss::coroutine::without_preemption_check(do_flush());
        co_return true;
    }
    co_return false;
}
ss::future<> batched_output_stream::do_flush() {
    if (_unflushed_bytes == 0) {
        return ss::make_ready_future<>();
    }
    _unflushed_bytes = 0;
    return _out.flush();
}
ss::future<> batched_output_stream::flush() {
    return ss::with_semaphore(*_write_sem, 1, [this] { return do_flush(); });
}
ss::future<> batched_output_stream::stop() {
    if (_closed) {
        return ss::make_ready_future<>();
    }
    _closed = true;

    if (_cache_size == 0) {
        // A default-initialized batched_output_stream has a default
        // initialized output_stream, which has a default initialized
        // data_sink, which has a null pimpl pointer, and will segfault if
        // any methods (including flush or close) are called on it.
        return ss::make_ready_future();
    }

    return ss::with_semaphore(*_write_sem, 1, [this] {
        return do_flush().finally([this] { return _out.close(); });
    });
}

} // namespace net
