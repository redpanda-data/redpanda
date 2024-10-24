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

#pragma once
#include "bytes/bytes.h"
#include "hashing/crc32c.h"
#include "hashing/xx.h"
#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "ssx/semaphore.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_writer.h"
#include "storage/segment_appender.h"
#include "storage/storage_resources.h"
#include "storage/types.h"
#include "utils/vint.h"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>

#include <absl/container/node_hash_map.h>
#include <absl/hash/hash.h>
namespace storage::internal {
using namespace storage; // NOLINT
class spill_key_index final : public compacted_index_writer::impl {
public:
    struct value_type {
        model::offset base_offset;
        int32_t delta{0};
    };
    static constexpr auto value_sz = sizeof(value_type);
    static constexpr size_t max_key_size = compacted_index::max_entry_size
                                           - (2 * vint::max_length);
    using underlying_t = absl::node_hash_map<
      compaction_key,
      value_type,
      bytes_hasher<uint64_t, xxhash_64>,
      bytes_type_eq>;

    spill_key_index(
      ss::sstring filename,
      ss::io_priority_class,
      bool truncate,
      storage_resources&,
      std::optional<ntp_sanitizer_config> sanitizer_config);

    spill_key_index(
      ss::sstring name,
      ss::file dummy_file,
      size_t max_mem,
      storage_resources&);

    spill_key_index(const spill_key_index&) = delete;
    spill_key_index& operator=(const spill_key_index&) = delete;
    spill_key_index(spill_key_index&&) noexcept = default;
    spill_key_index& operator=(spill_key_index&&) noexcept = delete;
    ~spill_key_index() override;

    // public
    ss::future<> index(
      model::record_batch_type,
      bool is_control_batch,
      const iobuf& key,
      model::offset,
      int32_t) final;
    ss::future<> index(const compaction_key& b, model::offset, int32_t) final;
    ss::future<> index(
      model::record_batch_type,
      bool is_control_batch,
      bytes&&,
      model::offset,
      int32_t) final;
    ss::future<> truncate(model::offset) final;
    ss::future<> append(compacted_index::entry) final;
    ss::future<> close() final;
    void print(std::ostream&) const final;
    void set_flag(compacted_index::footer_flags) final;

private:
    /**
     * returns memory usage of the map structures itself. In order to get total
     * memory usage of spill key index we have to do:
     *
     *    idx_mem_usage() + _keys_mem_usage
     */
    size_t idx_mem_usage() {
        using debug = absl::container_internal::hashtable_debug_internal::
          HashtableDebugAccess<underlying_t>;
        return debug::AllocatedByteSize(_midx);
    }

    size_t entry_mem_usage(const bytes& k) const {
        // One entry in a node hash map: key and value
        // are allocated together, and the key is a basic_sstring with
        // internal buffer that may be spilled if key was longer.
        auto is_external = k.size() > bytes_inline_size;
        return (is_external ? sizeof(k) + k.size() : sizeof(k)) + value_sz;
    }

    void release_entry_memory(const bytes& k) {
        auto entry_memory = entry_mem_usage(k);
        _keys_mem_usage -= entry_memory;

        // Handle the case of a double-release, in case this comes up
        // during retries/exception handling.
        auto release_units = std::min(entry_memory, _mem_units.count());
        _mem_units.return_units(release_units);
    }

    ss::future<> maybe_open();
    ss::future<> open();
    ss::future<> drain_all_keys();
    ss::future<> add_key(compaction_key, value_type);
    ss::future<> spill(compacted_index::entry_type, bytes_view, value_type);

    std::optional<ntp_sanitizer_config> _sanitizer_config;
    storage_resources& _resources;
    ss::io_priority_class _pc;
    bool _truncate;
    std::optional<segment_appender> _appender;
    underlying_t _midx;

    // Max memory we'll use for _midx, although we may spill earlier
    // if hinted to by storage_resources
    size_t _max_mem{512_KiB};

    // Units handed out by storage_resources to track our consumption
    // of the per-shard compaction index memory allowance.
    ssx::semaphore_units _mem_units;

    size_t _keys_mem_usage{0};
    compacted_index::footer _footer;
    crc::crc32c _crc;
    ss::gate _gate;

    friend std::ostream& operator<<(std::ostream&, const spill_key_index&);
};

} // namespace storage::internal
