/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/access_time_tracker.h"

#include "base/units.h"
#include "bytes/iobuf_parser.h"
#include "bytes/iostream.h"
#include "serde/peek.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/rw.h"
#include "serde/rw/scalar.h"
#include "serde/rw/sstring.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/defer.hh>

#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>

#include <exception>
#include <ranges>
#include <variant>

namespace absl {

template<class Key, class Value>
void read_nested(
  iobuf_parser& in,
  btree_map<Key, Value>& btree,
  const size_t bytes_left_limit) {
    using serde::read_nested;
    uint64_t sz;
    read_nested(in, sz, bytes_left_limit);
    for (auto i = 0UL; i < sz; i++) {
        Key key;
        Value value;
        read_nested(in, key, bytes_left_limit);
        read_nested(in, value, bytes_left_limit);
        btree.insert({key, value});
    }
}

} // namespace absl

namespace cloud_storage {

// For async serialization/deserialization, the fixed-size content
// is defined in a header struct, and then the main body of the encoding
// is defined by hand in the read/write methods so that it can be done
// with streaming.
struct table_header
  : serde::envelope<table_header, serde::version<1>, serde::compat_version<0>> {
    size_t table_size{0};
    tracker_version version{tracker_version::v1};

    auto serde_fields() { return std::tie(table_size, version); }
};

ss::future<> access_time_tracker::write(
  ss::output_stream<char>& out, tracker_version version) {
    // This lock protects us from the _table being mutated while we
    // are iterating over it and yielding during the loop.
    auto lock_guard = co_await ss::get_units(_table_lock, 1);

    _dirty = false;

    const table_header h{.table_size = _table.size(), .version = version};
    iobuf header_buf;
    serde::write(header_buf, h);
    co_await write_iobuf_to_output_stream(std::move(header_buf), out);

    // How many items to serialize per stream write()
    constexpr size_t chunk_count = 2048;

    size_t i = 0;
    iobuf serialize_buf;
    for (const auto& [path, metadata] : _table) {
        serde::write(serialize_buf, path);
        serde::write(serialize_buf, metadata.atime_sec);
        serde::write(serialize_buf, metadata.size);
        ++i;
        if (i % chunk_count == 0 || i == _table.size()) {
            iobuf chunk_size;
            serde::write(chunk_size, serialize_buf.size_bytes());
            co_await write_iobuf_to_output_stream(std::move(chunk_size), out);
            for (const auto& f : serialize_buf) {
                co_await out.write(f.get(), f.size());
            }
            serialize_buf.clear();
        }
    }

    lock_guard.return_all();
    on_released_table_lock();
}

bool access_time_tracker::should_track(std::string_view key) const {
    if (
      key.ends_with(".tx") || key.ends_with(".index")
      || key.ends_with(cache_tmp_file_extension)) {
        return false;
    }

    return true;
}

void access_time_tracker::on_released_table_lock() {
    // When dropping lock, drain any pending upserts that came in via
    // add/remove calls while we were locking the main table.
    if (!_pending_upserts.empty()) {
        // We will drain some deferred updates, this will dirty the table.
        _dirty = true;
    }

    for (const auto& [hash, ts] : _pending_upserts) {
        if (ts.has_value()) {
            _table[hash] = ts.value();
        } else {
            _table.erase(hash);
        }
    }
    _pending_upserts.clear();
}

ss::future<> access_time_tracker::read(ss::input_stream<char>& in) {
    auto lock_guard = co_await ss::get_units(_table_lock, 1);

    _table.clear();
    _dirty = false;

    // Accumulate a serialized table_header in this buffer
    iobuf header_buf;

    // Read serde envelope header
    auto envelope_header_tmp = co_await in.read_exactly(
      serde::envelope_header_size);
    header_buf.append(envelope_header_tmp.get(), envelope_header_tmp.size());

    // Peek at the size of the header's serde body
    iobuf envelope_header_buf = header_buf.copy();
    auto peek_parser = iobuf_parser(std::move(envelope_header_buf));
    auto header_size = serde::peek_body_size(peek_parser);

    // Read the rest of the header + decode it
    auto tmp = co_await in.read_exactly(header_size);
    header_buf.append(tmp.get(), tmp.size());
    auto h_parser = iobuf_parser(std::move(header_buf));
    auto h = serde::read_nested<table_header>(h_parser, 0);

    auto defer = ss::defer([&] {
        lock_guard.return_all();
        // Drop writes accumulated while reading
        _pending_upserts.clear();
    });

    // Skip loading data for older version
    if (h.version == tracker_version::v1) {
        co_return;
    }

    while (!in.eof()) {
        auto chunk_sz_buf = co_await in.read_exactly(sizeof(size_t));
        if (chunk_sz_buf.empty() && in.eof()) {
            break;
        }

        iobuf chunk_sz;
        chunk_sz.append(std::move(chunk_sz_buf));
        auto chunk_sz_parser = iobuf_parser{std::move(chunk_sz)};
        auto chunk_size = serde::read<size_t>(chunk_sz_parser);
        auto tmp_buf = co_await in.read_exactly(chunk_size);
        iobuf items_buf;
        items_buf.append(std::move(tmp_buf));
        auto parser = iobuf_parser(std::move(items_buf));
        while (parser.bytes_left() > 0) {
            auto path = serde::read_nested<ss::sstring>(parser, 0);
            auto atime = serde::read_nested<uint32_t>(parser, 0);
            auto size = serde::read_nested<uint64_t>(parser, 0);
            _table.emplace(
              path, file_metadata{.atime_sec = atime, .size = size});
        }
    }

    vassert(
      _table.size() == h.table_size,
      "unexpected tracker size, loaded {} items, expected {} items",
      _table.size(),
      h.table_size);
}

void access_time_tracker::add(
  ss::sstring path, std::chrono::system_clock::time_point atime, size_t size) {
    if (!should_track(path)) {
        return;
    }

    uint32_t seconds = std::chrono::time_point_cast<std::chrono::seconds>(atime)
                         .time_since_epoch()
                         .count();

    auto units = seastar::try_get_units(_table_lock, 1);
    if (units.has_value()) {
        // Got lock, update main table
        _table[path] = {.atime_sec = seconds, .size = size};
        _dirty = true;
    } else {
        // Locked during serialization, defer write
        _pending_upserts[path] = {.atime_sec = seconds, .size = size};
    }
}

void access_time_tracker::remove(std::string_view key) noexcept {
    try {
        ss::sstring k{key.data(), key.size()};
        auto units = seastar::try_get_units(_table_lock, 1);
        if (units.has_value()) {
            // Unlocked, update main table
            _table.erase(k);
            _dirty = true;
        } else {
            // Locked during serialization, defer write
            _pending_upserts[k] = std::nullopt;
        }
    } catch (...) {
        vassert(
          false,
          "Can't remove key {} from access_time_tracker, exception: {}",
          key,
          std::current_exception());
    }
}

ss::future<> access_time_tracker::sync(
  const fragmented_vector<file_list_item>& existent,
  add_entries_t add_entries) {
    absl::btree_set<ss::sstring> paths;
    for (const auto& i : existent) {
        paths.insert(i.path);
    }

    auto lock_guard = co_await ss::get_units(_table_lock, 1);

    table_t tmp;

    for (const auto& it : _table) {
        if (paths.contains(it.first)) {
            tmp.insert(it);
        }
        co_await ss::maybe_yield();
    }

    if (add_entries) {
        auto should_add = [this, &tmp](const auto& e) {
            return should_track(e.path) && !tmp.contains(e.path);
        };
        for (const auto& entry : existent | std::views::filter(should_add)) {
            _dirty = true;
            tmp.insert(
              {entry.path,
               {static_cast<uint32_t>(
                  std::chrono::time_point_cast<std::chrono::seconds>(
                    entry.access_time)
                    .time_since_epoch()
                    .count()),
                entry.size}});
        }
    }

    if (_table.size() != tmp.size()) {
        // We dropped one or more entries, therefore mutated the table.
        _dirty = true;
    }
    _table = std::move(tmp);

    lock_guard.return_all();
    on_released_table_lock();
}

std::optional<file_metadata>
access_time_tracker::get(const std::string& key) const {
    if (auto it = _table.find(key); it != _table.end()) {
        return it->second;
    }
    return std::nullopt;
}

bool access_time_tracker::is_dirty() const { return _dirty; }

fragmented_vector<file_list_item> access_time_tracker::lru_entries() const {
    fragmented_vector<file_list_item> items;
    items.reserve(_table.size());
    for (const auto& [path, metadata] : _table) {
        items.emplace_back(metadata.time_point(), path, metadata.size);
    }
    std::ranges::sort(
      items, {}, [](const auto& item) { return item.access_time; });
    return items;
}

std::chrono::system_clock::time_point file_metadata::time_point() const {
    return std::chrono::system_clock::time_point{
      std::chrono::seconds{atime_sec}};
}

} // namespace cloud_storage
