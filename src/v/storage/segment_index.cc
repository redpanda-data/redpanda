// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/segment_index.h"

#include "base/vassert.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "storage/index_state.h"
#include "storage/logger.h"
#include "storage/segment_utils.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>

#include <bits/stdint-uintn.h>
#include <boost/container/container_fwd.hpp>
#include <fmt/format.h>

#include <algorithm>

namespace storage {

segment_index::segment_index(
  segment_full_path path,
  model::offset base,
  size_t step,
  ss::sharded<features::feature_table>& feature_table,
  std::optional<ntp_sanitizer_config> sanitizer_config,
  std::optional<model::timestamp> broker_timestamp,
  std::optional<model::timestamp> clean_compact_timestamp,
  bool may_have_tombstone_records)
  : _path(std::move(path))
  , _step(step)
  , _feature_table(std::ref(feature_table))
  , _state(index_state::make_empty_index(
      storage::internal::should_apply_delta_time_offset(_feature_table)))
  , _sanitizer_config(std::move(sanitizer_config)) {
    _state.base_offset = base;
    _state.broker_timestamp = broker_timestamp;
    _state.clean_compact_timestamp = clean_compact_timestamp;
    _state.may_have_tombstone_records = may_have_tombstone_records;
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
      _path,
      ss::open_flags::create | ss::open_flags::rw,
      {},
      _sanitizer_config);
}

void segment_index::reset() {
    // Persist the base offset, clean compaction timestamp, and tombstones
    // identifier through a reset.
    auto base = _state.base_offset;
    auto clean_compact_timestamp = _state.clean_compact_timestamp;
    auto may_have_tombstone_records = _state.may_have_tombstone_records;

    _state = index_state::make_empty_index(
      storage::internal::should_apply_delta_time_offset(_feature_table));

    _state.base_offset = base;
    _state.clean_compact_timestamp = clean_compact_timestamp;
    _state.may_have_tombstone_records = may_have_tombstone_records;

    _acc = 0;
}

void segment_index::swap_index_state(index_state&& o) {
    _needs_persistence = true;
    _acc = 0;
    std::swap(_state, o);
}

// helper for segment_index::maybe_track, converts betwen optional-wrapped
// broker_timestamp_t and model::timestamp
constexpr auto to_optional_model_timestamp(std::optional<broker_timestamp_t> in)
  -> std::optional<model::timestamp> {
    if (unlikely(!in.has_value())) {
        return std::nullopt;
    }
    // conversion from broker_timestamp_t to system_clock in this way it's
    // possible because they share the same epoch
    return model::to_timestamp(
      std::chrono::system_clock::time_point{in->time_since_epoch()});
}

void segment_index::maybe_track(
  const model::record_batch_header& hdr,
  std::optional<broker_timestamp_t> new_broker_ts,
  size_t filepos) {
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
          to_optional_model_timestamp(new_broker_ts),
          path().is_internal_topic()
            || hdr.type == model::record_batch_type::raft_data,
          internal::is_compactible(hdr) ? hdr.record_count : 0)) {
        _acc = 0;
    }
    _needs_persistence = true;
}

std::optional<segment_index::entry>
segment_index::find_nearest(model::timestamp t) {
    return _state.find_nearest(t);
}

std::optional<segment_index::entry>
segment_index::find_above_size_bytes(size_t distance) {
    return _state.find_above_size_bytes(distance);
}

std::optional<segment_index::entry>
segment_index::find_below_size_bytes(size_t distance) {
    return _state.find_below_size_bytes(distance);
}

std::optional<segment_index::entry>
segment_index::find_nearest(model::offset o) {
    return _state.find_nearest(o);
}

ss::future<> segment_index::truncate(
  model::offset new_max_offset, model::timestamp new_max_timestamp) {
    _needs_persistence = _state.truncate(new_max_offset, new_max_timestamp);
    if (_needs_persistence) {
        co_await flush();
    }
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
    clear_cached_disk_usage();
    return ss::with_file(open(), [](ss::file f) { return f.truncate(0); });
}

ss::future<> segment_index::flush() {
    if (!_needs_persistence) {
        return ss::now();
    }
    _needs_persistence = false;
    clear_cached_disk_usage();

    // Flush is usually called when we either shrunk the index (truncate)
    // or when we're no longer going to append (close): in either case,
    // it is a good time to free speculatively allocated memory.
    _state.shrink_to_fit();

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

ss::future<size_t> segment_index::disk_usage() {
    if (!_disk_usage_size.has_value()) {
        try {
            _disk_usage_size = co_await ss::file_size(path().string());
        } catch (const std::filesystem::filesystem_error& e) {
            const auto level = e.code() == std::errc::no_such_file_or_directory
                                 ? ss::log_level::trace
                                 : ss::log_level::info;
            vlogl(stlog, level, "could not query file size {}: {}", path(), e);
            co_return 0;
        } catch (const std::exception& e) {
            vlog(stlog.info, "could not query file size {}: {}", path(), e);
            co_return 0;
        }
    }
    co_return _disk_usage_size.value();
}

std::optional<model::timestamp>
segment_index::find_highest_timestamp_before(model::timestamp t) const {
    if (_state.base_timestamp > t || _state.empty()) {
        return std::nullopt;
    }

    auto relative_t = (t - _state.base_timestamp).value();

    for (int i = _state.size() - 1; i >= 0; --i) {
        auto [relative_offset, offset_time, position] = _state.get_entry(i);
        if (offset_time() < relative_t) {
            return model::timestamp(
              _state.base_timestamp.value() + offset_time());
        }
    }

    return std::nullopt;
}

} // namespace storage
