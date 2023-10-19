// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "storage/compacted_index.h"
#include "utils/tracking_allocator.h"

#include <seastar/core/future.hh>

#include <absl/container/btree_map.h>

namespace storage {

/**
 * Map containing the latest offsets of each key.
 */
class key_offset_map {
public:
    key_offset_map() = default;
    key_offset_map(const key_offset_map&) = delete;
    key_offset_map& operator=(const key_offset_map&) = delete;
    key_offset_map(key_offset_map&&) noexcept = default;
    key_offset_map& operator=(key_offset_map&&) noexcept = default;
    virtual ~key_offset_map() = default;

    /**
     * Associate \p offset with the given \p key. If \p key already exists then
     * the new mapping with override the existing mapping provided that \p
     * offset is larger than the existing offset associated with the key.
     */
    virtual seastar::future<bool>
    put(const compaction_key& key, model::offset offset) = 0;

    /**
     * Return the offset for the given \p key.
     */
    virtual seastar::future<std::optional<model::offset>>
    get(const compaction_key& key) const = 0;

    /**
     * Return the highest inserted offset.
     */
    virtual model::offset max_offset() const = 0;
};

/**
 * A key_offset_map that stores the entire key.
 */
class simple_key_offset_map final : public key_offset_map {
public:
    static constexpr size_t default_key_limit = 1000;

    /**
     * Construct a new simple_key_offset_map with \p max_key maximum number of
     * keys.
     */
    explicit simple_key_offset_map(size_t max_keys = default_key_limit);

    seastar::future<bool>
    put(const compaction_key& key, model::offset offset) override;

    seastar::future<std::optional<model::offset>>
    get(const compaction_key& key) const override;

    model::offset max_offset() const override;

private:
    ss::shared_ptr<util::mem_tracker> _memory_tracker;
    util::mem_tracked::map_t<absl::btree_map, compaction_key, model::offset>
      _map;
    model::offset _max_offset;
    size_t _max_keys;
};

} // namespace storage
