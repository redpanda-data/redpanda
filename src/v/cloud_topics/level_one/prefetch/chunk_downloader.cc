/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/prefetch/chunk_downloader.h"

#include "base/vassert.h"
#include "cloud_topics/level_one/common/object.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/util/defer.hh>

namespace cloud_topics::prefetch {

static ss::logger cdlog("chunk_downloader");

chunk_downloader::chunk_downloader(
  l1::io* io,
  l1::object_id oid,
  cloud_io::group_id group,
  std::optional<std::reference_wrapper<ss::abort_source>> abort_source,
  std::optional<size_t> start_position)
  : _io(io)
  , _oid(oid)
  , _group(group)
  , _abort_source_opt(abort_source)
  , _cursor(start_position)
  // An explicit run start fixes the cursor: out-of-order dispatch (including a
  // chunk preceding the run start) must not move it.
  , _cursor_locked(start_position.has_value()) {}

void chunk_downloader::_drain_reorder() {
    if (_reorder_buf.empty()) {
        return;
    }
    // If cursor not yet locked, pull it down to the current minimum buffered
    // position. This handles the case where a later-dispatched chunk arrives
    // before an earlier-dispatched chunk has been registered in _cursor.
    if (!_cursor_locked) {
        size_t min_pos = _reorder_buf.begin()->first;
        if (!_cursor.has_value() || min_pos < *_cursor) {
            _cursor = min_pos;
        }
    }

    vassert(_cursor.has_value(), "cursor must be set before draining");

    while (!_reorder_buf.empty()) {
        auto it = _reorder_buf.begin();
        if (it->first != *_cursor) {
            // Gap: wait for the missing chunk.
            break;
        }
        // Lock the cursor: once we start draining it must only advance.
        _cursor_locked = true;
        size_t chunk_size = it->second.size_bytes();
        auto batches = _reassembler.feed(std::move(it->second));
        _reorder_buf.erase(it);
        *_cursor += chunk_size;
        for (auto& b : batches) {
            _ready.push_back(std::move(b));
        }
    }
}

ss::future<> chunk_downloader::dispatch(size_t position, size_t size) {
    // Update the decode cursor to the minimum of all dispatched positions, but
    // only while the cursor is still unlocked (before the first chunk drains).
    // Once decoding has started the cursor must only advance, never retreat.
    if (!_cursor_locked) {
        if (!_cursor.has_value() || position < *_cursor) {
            _cursor = position;
        }
    }

    // Use C++23 deducing-this on the lambda to move captures into the coroutine
    // frame, preventing use-after-free when the lambda object is freed while
    // the coroutine is still suspended (see CLAUDE.md § Lambda coroutines).
    //
    // ss::try_with_gate returns a disengaged optional when the gate is already
    // closed, avoiding a synchronous gate_closed_exception throw. _in_flight is
    // incremented inside the gated body so it is always balanced: if the gate
    // is closed we never increment, and on any error path the finally()
    // decrement ensures the counter returns to zero.
    auto gate_fut = co_await ss::coroutine::as_future(
      ss::try_with_gate(
        _gate, [this, position, size](this auto) -> ss::future<> {
            ++_in_flight;
            auto decrement = ss::defer([this] { --_in_flight; });

            // Choose which abort_source to pass to the IO layer.
            ss::abort_source* as = _abort_source_opt ? &_abort_source_opt->get()
                                                     : &_internal_abort;

            l1::object_extent extent{
              .id = _oid,
              .position = position,
              .size = size,
            };

            auto fut = co_await ss::coroutine::as_future(
              _io->download_object_as_iobuf(extent, as, _group));

            if (fut.failed()) {
                auto ex = fut.get_exception();
                vlog(
                  cdlog.warn,
                  "chunk GET failed for object {} pos={} size={}: {}",
                  _oid,
                  position,
                  size,
                  ex);
                if (!_poison_pos || position < *_poison_pos) {
                    _poison_pos = position;
                    _poison_ex = ex;
                }
                co_return;
            }

            auto result = std::move(fut).get();
            if (!result.has_value()) {
                vlog(
                  cdlog.warn,
                  "chunk GET error for object {} pos={} size={}: {}",
                  _oid,
                  position,
                  size,
                  result.error());
                auto ex = std::make_exception_ptr(
                  std::runtime_error(
                    fmt::format(
                      "chunk GET failed for object {}: {}",
                      _oid,
                      std::to_underlying(result.error()))));
                if (!_poison_pos || position < *_poison_pos) {
                    _poison_pos = position;
                    _poison_ex = ex;
                }
                co_return;
            }

            _reorder_buf.emplace(position, std::move(result).value());
            _drain_reorder();
        }));
    if (gate_fut.failed()) {
        // gate_closed_exception: gate closed before/during dispatch; ignore.
        vlog(
          cdlog.debug,
          "dispatch for object {} pos={} size={} skipped: gate closed",
          _oid,
          position,
          size);
    }
}

chunked_vector<model::record_batch> chunk_downloader::take_ready() {
    // Surface the poison error once the decode cursor reaches the failed chunk.
    if (_poison_pos && _cursor.has_value() && *_cursor >= *_poison_pos) {
        vassert(
          _poison_ex.has_value(),
          "poison_pos set but poison_ex missing for object {}",
          _oid);
        std::rethrow_exception(*_poison_ex);
    }
    return std::exchange(_ready, {});
}

ss::future<l1::footer>
chunk_downloader::fetch_footer(size_t footer_pos, size_t object_size) {
    size_t footer_total_size = object_size - footer_pos;

    l1::object_extent extent{
      .id = _oid,
      .position = footer_pos,
      .size = footer_total_size,
    };

    ss::abort_source* as = _abort_source_opt ? &_abort_source_opt->get()
                                             : &_internal_abort;

    auto read_fut = co_await ss::coroutine::as_future(
      _io->download_object_as_iobuf(extent, as, _group));

    if (read_fut.failed()) {
        auto ex = read_fut.get_exception();
        vlog(
          cdlog.error,
          "Exception reading footer from object {} (pos={} size={}): {}",
          _oid,
          footer_pos,
          object_size,
          ex);
        std::rethrow_exception(ex);
    }

    auto read_result = std::move(read_fut).get();
    if (!read_result.has_value()) {
        throw std::runtime_error(
          fmt::format(
            "Failed to read footer from object {} (pos={} size={}): {}",
            _oid,
            footer_pos,
            object_size,
            std::to_underlying(read_result.error())));
    }

    auto footer_result = co_await l1::footer::read(
      std::move(read_result).value());

    if (!std::holds_alternative<l1::footer>(footer_result)) {
        throw std::runtime_error(
          fmt::format(
            "Failed to parse footer from object {} (pos={} size={})",
            _oid,
            footer_pos,
            object_size));
    }

    co_return std::get<l1::footer>(std::move(footer_result));
}

ss::future<> chunk_downloader::close() {
    _internal_abort.request_abort();
    auto fut = co_await ss::coroutine::as_future(_gate.close());
    if (fut.failed()) {
        // Swallow exceptions from close() — the gate's close() shouldn't
        // throw but the spec requires we swallow any errors.
        auto ex = fut.get_exception();
        vlog(
          cdlog.debug,
          "Exception during chunk_downloader::close() for object {}: {}",
          _oid,
          ex);
    }
}

} // namespace cloud_topics::prefetch
