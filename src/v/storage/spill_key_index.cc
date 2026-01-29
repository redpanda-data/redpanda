// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/spill_key_index.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "bytes/bytes.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "ssx/async_algorithm.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_writer.h"
#include "storage/compaction_key.h"
#include "storage/logger.h"
#include "storage/segment_utils.h"
#include "utils/vint.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future-util.hh>

#include <fmt/ostream.h>

#include <exception>
using namespace std::chrono_literals;

namespace storage::internal {
using namespace storage; // NOLINT

// use *exactly* 1 write-behind buffer for keys sice it will be an in memory
// workload for most workloads. No need to waste memory here
//
spill_key_index::spill_key_index(
  ss::sstring name,
  bool truncate,
  storage_resources& resources,
  std::optional<ntp_sanitizer_config> sanitizer_config)
  : compacted_index_writer(std::move(name))
  , _sanitizer_config(std::move(sanitizer_config))
  , _resources(resources)
  , _truncate(truncate) {}

/**
 * This constructor is only for unit tests, which pre-construct a ss::file
 * rather than relying on spill_key_index to open it on-demand.
 */
spill_key_index::spill_key_index(
  ss::sstring name,
  ss::file dummy_file,
  size_t max_mem,
  storage_resources& resources)
  : compacted_index_writer(std::move(name))
  , _resources(resources)
  , _appender(
      storage::segment_appender(
        std::move(dummy_file),
        segment_appender::options(std::nullopt, _resources, nullptr)))
  , _max_mem(max_mem) {}

spill_key_index::~spill_key_index() {
    vassert(
      _midx.empty(),
      "must drain all keys before destroy spill_key_index, keys left:{} {}",
      _midx.size(),
      filename());
}

ss::future<> spill_key_index::index(
  const compaction::compaction_key& v,
  model::offset base_offset,
  int32_t delta) {
    return ss::try_with_gate(_gate, [this, &v, base_offset, delta]() {
        if (auto it = _midx.find(v); it != _midx.end()) {
            auto& pair = it->second;
            if (base_offset > pair.base_offset) {
                pair.base_offset = base_offset;
                pair.delta = delta;
            }
            return ss::now();
        }
        // not found
        return add_key(v, value_type{base_offset, delta});
    });
}

ss::future<>
spill_key_index::spill_some(size_t entry_size, size_t min_index_size) {
    auto stop_cond = [this, entry_size, min_index_size] {
        size_t total_mem = idx_mem_usage() + _keys_mem_usage + entry_size;

        // Instance-local capacity check
        bool local_ok = total_mem < _max_mem;

        // Shard-wide capacity check
        bool global_ok = _resources.compaction_index_bytes_available()
                         || total_mem < min_index_size;

        // Stop condition: none of our size thresholds must be violated
        return _midx.empty() || (local_ok && global_ok);
    };

    /*
     * Collect keys to spill in a buffer until the stop condition is met and
     * then dump all of them at once into the backing file. A periodic yield is
     * introduced to reduce the chance of a reactor stall.
     *
     * In order to avoid creating hot erase spots we start removal at the
     * location immediately after the last inserted key. The exact last inserted
     * key is skipped to maintain good hit locality, and the next key should not
     * have any temporal correlation to recent inserts.
     *
     * The amount of randomness this introduces is highly dependent on the
     * workload, but it is better than always erasing at begin(). One may want
     * to explore storing the last N inserted unique keys and randomly selecting
     * from this set.
     */
    ssize_t yield_count = 0;
    spill_payload payload;

    auto it = _midx.find(_last_key_indexed);
    if (it == _midx.end() || (++it == _midx.end())) {
        it = _midx.begin();
    }

    while (!stop_cond()) {
        // `next` might be end(), so don't dereference
        auto next = std::next(it);
        release_entry_memory(it->first);
        append_to_spill_payload(
          payload, compacted_index::entry_type::key, it->first, it->second);
        _midx.erase(it);
        if (next == _midx.end()) {
            next = _midx.begin();
        }
        // `it` is guaranteed to not be end() provided that _midx is not empty.
        // if it is empty, then stop_cond() will stop the next loop iteration.
        it = next;
        if (++yield_count >= ssx::async_algo_traits::interval) {
            co_await ss::coroutine::maybe_yield();
            yield_count = 0;
        }
    }

    if (!payload.data.empty()) {
        co_await spill(std::move(payload));
    }
}

ss::future<>
spill_key_index::add_key(compaction::compaction_key b, value_type v) {
    auto f = ss::now();
    const auto entry_size = entry_mem_usage(b);
    const auto expected_size = idx_mem_usage() + _keys_mem_usage + entry_size;

    auto take_result = _resources.compaction_index_take_bytes(entry_size);
    if (_mem_units.count() == 0) {
        _mem_units = std::move(take_result.units);
    } else {
        _mem_units.adopt(std::move(take_result.units));
    }

    // Don't spill unless we're at least this big.  Prevents a situation
    // where some other index has used up the memory allowance, and we
    // would end up spilling on every key.
    const size_t min_index_size = std::min(32_KiB, _max_mem);

    if (
      (take_result.checkpoint_hint && expected_size > min_index_size)
      || expected_size >= _max_mem) {
        f = spill_some(entry_size, min_index_size);
    }

    return f.then([this, entry_size, b = std::move(b), v]() mutable {
        // convert iobuf to key
        _keys_mem_usage += entry_size;

        // No update to _mem_units here: we already took units at top
        // of add_key before starting the write.

        _last_key_indexed = b;
        _midx.insert({std::move(b), v});
    });
}

ss::future<> spill_key_index::index(
  model::record_batch_type batch_type,
  bool is_control_batch,
  bytes&& b,
  model::offset base_offset,
  int32_t delta) {
    return ss::try_with_gate(
      _gate,
      [this,
       batch_type,
       is_control_batch,
       b = std::move(b),
       base_offset,
       delta]() {
          auto key = enhance_key(batch_type, is_control_batch, b);
          if (auto it = _midx.find(key); it != _midx.end()) {
              auto& pair = it->second;
              // must use both base+delta, since we only want to keep the
              // latest which might be inserted into the batch multiple times
              // by client
              const auto record = base_offset + model::offset(delta);
              const auto current = pair.base_offset + model::offset(pair.delta);
              if (record > current) {
                  pair.base_offset = base_offset;
                  pair.delta = delta;
              }
              return ss::now();
          }
          // not found
          return add_key(std::move(key), value_type{base_offset, delta});
      });
}
ss::future<> spill_key_index::index(
  model::record_batch_type batch_type,
  bool is_control_batch,
  const iobuf& key,
  model::offset base_offset,
  int32_t delta) {
    return index(
      batch_type,
      is_control_batch,
      iobuf_to_bytes(key), // makes a copy, but we need deterministic keys
      base_offset,
      delta);
}

ss::future<> spill_key_index::spill(
  compacted_index::entry_type type, bytes_view b, value_type v) {
    spill_payload payload;
    append_to_spill_payload(payload, type, b, v);
    co_await spill(std::move(payload));
}

ss::future<> spill_key_index::spill(spill_payload payload) {
    _footer.keys += payload.keys;
    _footer.keys_deprecated += payload.keys;
    _footer.size += payload.data.size_bytes();
    _footer.size_deprecated = _footer.size;
    for (auto& f : payload.data) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        _crc.extend(reinterpret_cast<const uint8_t*>(f.get()), f.size());
    }
    // Append to the file
    co_await maybe_open();
    co_await _appender->append(payload.data);
}

/// format is:
/// INT16 BYTE VINT VINT []BYTE
///
void spill_key_index::append_to_spill_payload(
  spill_payload& payload,
  compacted_index::entry_type type,
  bytes_view b,
  value_type v) {
    constexpr size_t size_reservation = sizeof(uint16_t);
    ++payload.keys;
    const auto payload_start_size = payload.data.size_bytes();
    // INT16
    auto ph = payload.data.reserve(size_reservation);
    // BYTE
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    payload.data.append(reinterpret_cast<const uint8_t*>(&type), 1);
    // VINT
    {
        auto x = vint::to_bytes(v.base_offset);
        payload.data.append(x.data(), x.size());
    }
    // VINT
    {
        auto x = vint::to_bytes(v.delta);
        payload.data.append(x.data(), x.size());
    }
    // []BYTE
    {
        size_t key_size = std::min(max_key_size, b.size());
        payload.data.append(b.data(), key_size);
    }
    const auto payload_size = payload.data.size_bytes() - payload_start_size;
    const size_t size = payload_size - size_reservation;
    const size_t size_le = ss::cpu_to_le(size); // downcast
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    ph.write(reinterpret_cast<const char*>(&size_le), size_reservation);
    vassert(
      payload_size <= compacted_index::max_entry_size,
      "Entries cannot be bigger than uint16_t::max(): {}",
      payload.data);
}

ss::future<> spill_key_index::append(compacted_index::entry e) {
    return ss::try_with_gate(_gate, [this, e = std::move(e)]() mutable {
        return ss::do_with(std::move(e), [this](compacted_index::entry& e) {
            return spill(e.type, e.key, value_type{e.offset, e.delta});
        });
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
          release_entry_memory(node.key());
          return ss::do_with(
            node.key(), node.mapped(), [this](const bytes& k, value_type o) {
                return spill(compacted_index::entry_type::key, k, o);
            });
      });
}

void spill_key_index::set_flag(compacted_index::footer_flags f) {
    _footer.flags |= f;
}

ss::future<> spill_key_index::maybe_open() {
    if (!_appender.has_value()) {
        co_await open();
    }
}

/**
 * Open file and initialize _appender
 */
ss::future<> spill_key_index::open() {
    auto index_file = co_await make_writer_handle(
      std::filesystem::path(filename()), _sanitizer_config, _truncate);

    _appender.emplace(
      storage::segment_appender(
        std::move(index_file),
        segment_appender::options(std::nullopt, _resources, nullptr)));
}

ss::future<> spill_key_index::close() {
    co_await _gate.close();
    // ::close includes a flush, which can fail, but we must catch the
    // exception to avoid potentially leaving an open handle in _appender
    std::exception_ptr ex;
    try {
        co_await maybe_open();

        co_await drain_all_keys();

        vassert(
          _keys_mem_usage == 0,
          "Failed to drain all keys, {} bytes left",
          _keys_mem_usage);

        _footer.crc = _crc.value();
        auto footer_buf = reflection::to_iobuf(_footer);
        vassert(
          footer_buf.size_bytes() == compacted_index::footer::footer_size,
          "Footer is bigger than expected: {}",
          footer_buf);

        co_await _appender->append(footer_buf);
    } catch (...) {
        ex = std::current_exception();
    }

    // Even if the flush failed, make sure we are closing any open file
    // handle.
    if (_appender.has_value()) {
        co_await _appender->close();
    }

    if (ex) {
        vlog(stlog.error, "error flushing index during close: {} ", ex);

        // Drop any dirty state that we couldn't flush: otherwise our
        // destructor will assert out.  This is valid because future reads of
        // a compaction index can detect invalid content and regenerate.
        _midx.clear();

        std::rethrow_exception(ex);
    }
}

void spill_key_index::print(std::ostream& o) const { o << *this; }

std::ostream& operator<<(std::ostream& o, const spill_key_index& k) {
    fmt::print(
      o,
      "{{name:{}, key_mem_usage:{}, persisted_entries:{}, "
      "in_memory_entries:{}, file_appender:{}}}",
      k.filename(),
      k._keys_mem_usage,
      k._footer.keys,
      k._midx.size(),
      k._appender);
    return o;
}

size_t spill_key_index::size_bytes() const {
    if (_appender.has_value()) {
        return _appender->size_bytes();
    }
    return 0;
}

} // namespace storage::internal

namespace storage {
std::unique_ptr<compacted_index_writer> make_file_backed_compacted_index(
  ss::sstring name,
  bool truncate,
  storage_resources& resources,
  std::optional<ntp_sanitizer_config> sanitizer_config) {
    return std::make_unique<internal::spill_key_index>(
      std::move(name), truncate, resources, std::move(sanitizer_config));
}
} // namespace storage
