// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_index.h"

#include "model/timestamp.h"
#include "serde/serde.h"
#include "storage/index_state.h"
#include "storage/logger.h"
#include "storage/segment_utils.h"
#include "vassert.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>

#include <bits/stdint-uintn.h>
#include <boost/container/container_fwd.hpp>
#include <fmt/format.h>

#include <algorithm>

namespace storage {

static inline segment_index::entry translate_index_entry(
  const index_state& s,
  std::tuple<uint32_t, offset_time_index, uint64_t> entry) {
    auto [relative_offset, relative_time, filepos] = entry;
    return segment_index::entry{
      .offset = model::offset(relative_offset + s.base_offset()),
      .timestamp = model::timestamp(relative_time() + s.base_timestamp()),
      .filepos = filepos,
    };
}

segment_index::segment_index(
  segment_full_path path,
  model::offset base,
  size_t step,
  ss::sharded<features::feature_table>& feature_table,
  debug_sanitize_files sanitize)
  : _path(std::move(path))
  , _step(step)
  , _feature_table(std::ref(feature_table))
  , _state(index_state::make_empty_index(
      storage::internal::should_apply_delta_time_offset(_feature_table)))
  , _sanitize(sanitize) {
    _state.base_offset = base;
}

segment_index::segment_index(
  segment_full_path path,
  ss::file mock_file,
  model::offset base,
  size_t step,
  ss::sharded<features::feature_table>& feature_table)
  : _path(std::move(path))
  , _step(step)
  , _feature_table(std::ref(feature_table))
  , _state(index_state::make_empty_index(
      storage::internal::should_apply_delta_time_offset(_feature_table)))
  , _mock_file(mock_file) {
    _state.base_offset = base;
}

ss::future<ss::file> segment_index::open() {
    if (_mock_file) {
        // Unit testing hook
        return ss::make_ready_future<ss::file>(_mock_file.value());
    }

    return internal::make_handle(
      _path, ss::open_flags::create | ss::open_flags::rw, {}, _sanitize);
}

void segment_index::reset() {
    auto base = _state.base_offset;
    _state = index_state::make_empty_index(
      storage::internal::should_apply_delta_time_offset(_feature_table));
    _state.base_offset = base;

    _acc = 0;
}

void segment_index::swap_index_state(index_state&& o) {
    _needs_persistence = true;
    _acc = 0;
    std::swap(_state, o);
}

void segment_index::maybe_track(
  const model::record_batch_header& hdr, size_t filepos) {
    _acc += hdr.size_bytes;

    _state.update_batch_timestamps_are_monotonic(
      hdr.max_timestamp >= _last_batch_max_timestamp);
    _last_batch_max_timestamp = std::max(
      hdr.first_timestamp, hdr.max_timestamp);

    if (_state.maybe_index(
          _acc,
          _step,
          filepos,
          hdr.base_offset,
          hdr.last_offset(),
          hdr.first_timestamp,
          hdr.max_timestamp,
          path().is_internal_topic()
            || hdr.type == model::record_batch_type::raft_data)) {
        _acc = 0;
    }
    _needs_persistence = true;
}

std::optional<segment_index::entry>
segment_index::find_nearest(model::timestamp t) {
    if (t < _state.base_timestamp) {
        return std::nullopt;
    }
    if (_state.empty()) {
        return std::nullopt;
    }

    const auto delta = t - _state.base_timestamp;
    const auto entry = _state.find_entry(delta);
    if (!entry) {
        return std::nullopt;
    }

    return translate_index_entry(_state, *entry);
}

std::optional<segment_index::entry>
segment_index::find_nearest(model::offset o) {
    if (o < _state.base_offset || _state.empty()) {
        return std::nullopt;
    }
    const uint32_t needle = o() - _state.base_offset();
    auto it = std::lower_bound(
      std::begin(_state.relative_offset_index),
      std::end(_state.relative_offset_index),
      needle,
      std::less<uint32_t>{});
    if (it == _state.relative_offset_index.end()) {
        it = std::prev(it);
    }
    // make it signed so it can be negative
    int i = std::distance(_state.relative_offset_index.begin(), it);
    do {
        if (_state.relative_offset_index[i] <= needle) {
            return translate_index_entry(_state, _state.get_entry(i));
        }
    } while (i-- > 0);

    return std::nullopt;
}

ss::future<>
segment_index::truncate(model::offset o, model::timestamp new_max_timestamp) {
    if (o < _state.base_offset) {
        co_return;
    }
    const uint32_t i = o() - _state.base_offset();
    auto it = std::lower_bound(
      std::begin(_state.relative_offset_index),
      std::end(_state.relative_offset_index),
      i,
      std::less<uint32_t>{});

    if (it != _state.relative_offset_index.end()) {
        _needs_persistence = true;
        int remove_back_elems = std::distance(
          it, _state.relative_offset_index.end());
        while (remove_back_elems-- > 0) {
            _state.pop_back();
        }
    }

    if (o < _state.max_offset) {
        _needs_persistence = true;
        if (_state.empty()) {
            _state.max_timestamp = _state.base_timestamp;
            _state.max_offset = _state.base_offset;
        } else {
            _state.max_timestamp = new_max_timestamp;
            _state.max_offset = o;
        }
    }

    co_return co_await flush();
}

/**
 *
 * @return true if decoded without errors, false on a serialization error
 *         while loading.  On all other types of error (e.g. IO), throw.
 */
ss::future<bool> segment_index::materialize_index() {
    return ss::with_file(open(), [this](ss::file f) {
        return materialize_index_from_file(std::move(f));
    });
}

ss::future<bool> segment_index::materialize_index_from_file(ss::file f) {
    auto size = co_await f.size();
    auto buf = co_await f.dma_read_bulk<char>(0, size);
    if (buf.empty()) {
        co_return false;
    }
    iobuf b;
    b.append(std::move(buf));
    try {
        _state = serde::from_iobuf<index_state>(std::move(b));
        co_return true;
    } catch (const serde::serde_exception& ex) {
        vlog(
          stlog.info,
          "Rebuilding index_state after decoding failure: {}",
          ex.what());
        co_return false;
    }
}

ss::future<> segment_index::drop_all_data() {
    reset();
    return ss::with_file(open(), [](ss::file f) { return f.truncate(0); });
}

ss::future<> segment_index::flush() {
    if (!_needs_persistence) {
        return ss::now();
    }
    _needs_persistence = false;
    return with_file(open(), [this](ss::file backing_file) {
        return flush_to_file(std::move(backing_file));
    });
}

ss::future<> segment_index::flush_to_file(ss::file backing_file) {
    co_await backing_file.truncate(0);
    auto out = co_await ss::make_file_output_stream(std::move(backing_file));

    auto b = serde::to_iobuf(_state.copy());
    for (const auto& f : b) {
        co_await out.write(f.get(), f.size());
    }
    co_await out.flush();
}

std::ostream& operator<<(std::ostream& o, const segment_index& i) {
    return o << "{file:" << i.path() << ", offsets:" << i.base_offset()
             << ", index:" << i._state << ", step:" << i._step
             << ", needs_persistence:" << i._needs_persistence << "}";
}
std::ostream& operator<<(std::ostream& o, const segment_index_ptr& i) {
    if (i) {
        return o << "{ptr=" << *i << "}";
    }
    return o << "{ptr=nullptr}";
}
std::ostream&
operator<<(std::ostream& o, const std::optional<segment_index::entry>& e) {
    if (e) {
        return o << *e;
    }
    return o << "{empty segment_index::entry}";
}
std::ostream& operator<<(std::ostream& o, const segment_index::entry& e) {
    return o << "{offset:" << e.offset << ", time:" << e.timestamp
             << ", filepos:" << e.filepos << "}";
}

} // namespace storage
