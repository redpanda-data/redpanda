// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/spill_key_index.h"

#include "bytes/bytes.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/compacted_index_writer.h"
#include "storage/logger.h"
#include "utils/vint.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future-util.hh>

#include <fmt/ostream.h>

namespace storage::internal {
using namespace storage; // NOLINT

// use *exactly* 1 write-behind buffer for keys sice it will be an in memory
// workload for most workloads. No need to waste memory here
//
spill_key_index::spill_key_index(
  ss::sstring name,
  ss::file index_file,
  ss::io_priority_class p,
  size_t max_memory)
  : compacted_index_writer::impl(std::move(name))
  , _appender(
      std::move(index_file),
      segment_appender::options(
        p, 1, config::shard_local_cfg().segment_fallocation_step.bind()))
  , _max_mem(max_memory) {}

spill_key_index::~spill_key_index() {
    vassert(
      _midx.empty(),
      "must drain all keys before destroy spill_key_index, keys left:{}",
      _midx.size());
}

ss::future<>
spill_key_index::index(bytes_view v, model::offset base_offset, int32_t delta) {
    if (auto it = _midx.find(v); it != _midx.end()) {
        auto& pair = it->second;
        if (base_offset > pair.base_offset) {
            pair.base_offset = base_offset;
            pair.delta = delta;
        }
        return ss::now();
    }
    // not found
    return add_key(bytes(v), value_type{base_offset, delta});
}

ss::future<> spill_key_index::add_key(bytes b, value_type v) {
    auto f = ss::now();
    auto const key_size = b.size();
    auto const expected_size = idx_mem_usage() + _keys_mem_usage + key_size;

    if (expected_size >= _max_mem) {
        f = ss::do_until(
          [this, key_size] {
              // stop condition
              return _midx.empty()
                     || idx_mem_usage() + _keys_mem_usage + key_size < _max_mem;
          },
          [this] {
              /**
               * Evict first entry, we use hash function that guarante good
               * randomness so evicting first entry is actually evicting a
               * pseudo random elemnent
               */
              auto node = _midx.extract(_midx.begin());

              return ss::do_with(
                node.key(),
                node.mapped(),
                [this](const bytes& k, value_type o) {
                    _keys_mem_usage -= k.size();
                    return spill(compacted_index::entry_type::key, k, o);
                });
          });
    }

    return f.then([this, b = std::move(b), v]() mutable {
        // convert iobuf to key
        _keys_mem_usage += b.size();
        _midx.insert({std::move(b), v});
    });
}

ss::future<>
spill_key_index::index(bytes&& b, model::offset base_offset, int32_t delta) {
    if (auto it = _midx.find(b); it != _midx.end()) {
        auto& pair = it->second;
        // must use both base+delta, since we only want to keep the latest
        // which might be inserted into the batch multiple times by client
        const auto record = base_offset + model::offset(delta);
        const auto current = pair.base_offset + model::offset(pair.delta);
        if (record > current) {
            pair.base_offset = base_offset;
            pair.delta = delta;
        }
        return ss::now();
    }
    // not found
    return add_key(std::move(b), value_type{base_offset, delta});
}
ss::future<> spill_key_index::index(
  const iobuf& key, model::offset base_offset, int32_t delta) {
    return index(
      iobuf_to_bytes(key), // makes a copy, but we need deterministic keys
      base_offset,
      delta);
}

/// format is:
/// INT16 BYTE VINT VINT []BYTE
///
ss::future<> spill_key_index::spill(
  compacted_index::entry_type type, bytes_view b, value_type v) {
    constexpr size_t size_reservation = sizeof(uint16_t);
    ++_footer.keys;
    iobuf payload;
    // INT16
    auto ph = payload.reserve(size_reservation);
    // BYTE
    payload.append(reinterpret_cast<const uint8_t*>(&type), 1);
    // VINT
    {
        auto x = vint::to_bytes(v.base_offset);
        payload.append(x.data(), x.size());
    }
    // VINT
    {
        auto x = vint::to_bytes(v.delta);
        payload.append(x.data(), x.size());
    }
    // []BYTE
    {
        size_t key_size = std::min(max_key_size, b.size());

        payload.append(b.data(), key_size);
    }
    const size_t size = payload.size_bytes() - size_reservation;
    const size_t size_le = ss::cpu_to_le(size); // downcast
    ph.write(reinterpret_cast<const char*>(&size_le), size_reservation);

    // update internal state
    _footer.size += payload.size_bytes();
    for (auto& f : payload) {
        // NOLINTNEXTLINE
        _crc.extend(reinterpret_cast<const uint8_t*>(f.get()), f.size());
    }
    vassert(
      payload.size_bytes() <= compacted_index::max_entry_size,
      "Entries cannot be bigger than uint16_t::max(): {}",
      payload);
    // Append to the file
    co_await _appender.append(payload);
}

ss::future<> spill_key_index::append(compacted_index::entry e) {
    return ss::do_with(std::move(e), [this](compacted_index::entry& e) {
        return spill(e.type, e.key, value_type{e.offset, e.delta});
    });
}

ss::future<> spill_key_index::drain_all_keys() {
    return ss::do_until(
      [this] {
          // stop condition
          return _midx.empty();
      },
      [this] {
          auto node = _midx.extract(_midx.begin());
          _keys_mem_usage -= node.key().size();
          return ss::do_with(
            node.key(), node.mapped(), [this](const bytes& k, value_type o) {
                return spill(compacted_index::entry_type::key, k, o);
            });
      });
}

void spill_key_index::set_flag(compacted_index::footer_flags f) {
    _footer.flags |= f;
}

ss::future<> spill_key_index::truncate(model::offset o) {
    set_flag(compacted_index::footer_flags::truncation);
    return drain_all_keys().then([this, o] {
        static constexpr std::string_view compacted_key = "compaction";
        return spill(
          compacted_index::entry_type::truncation,
          bytes_view(
            // NOLINTNEXTLINE
            reinterpret_cast<const uint8_t*>(compacted_key.data()),
            compacted_key.size()),
          // this is actually the base_offset + max_delta so everything upto and
          // including this offset must be ignored during self compaction
          value_type{o, 0});
    });
}

ss::future<> spill_key_index::close() {
    co_await drain_all_keys();

    vassert(
      _keys_mem_usage == 0,
      "Failed to drain all keys, {} bytes left",
      _keys_mem_usage);
    _footer.crc = _crc.value();

    auto footer_buf = reflection::to_iobuf(_footer);
    vassert(
      footer_buf.size_bytes() == compacted_index::footer_size,
      "Footer is bigger than expected: {}",
      footer_buf);

    co_await _appender.append(footer_buf);
    co_await _appender.close();
}

void spill_key_index::print(std::ostream& o) const { o << *this; }

std::ostream& operator<<(std::ostream& o, const spill_key_index& k) {
    fmt::print(
      o,
      "{{name:{}, max_mem:{}, key_mem_usage:{}, persisted_entries:{}, "
      "in_memory_entries:{}, file_appender:{}}}",
      k.filename(),
      k._max_mem,
      k._keys_mem_usage,
      k._footer.keys,
      k._midx.size(),
      k._appender);
    return o;
}

} // namespace storage::internal

namespace storage {
compacted_index_writer make_file_backed_compacted_index(
  ss::sstring name, ss::file f, ss::io_priority_class p, size_t max_memory) {
    return compacted_index_writer(std::make_unique<internal::spill_key_index>(
      std::move(name), std::move(f), p, max_memory));
}
} // namespace storage
