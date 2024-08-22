// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "container/fragmented_vector.h"
#include "hashing/secure.h"
#include "model/fundamental.h"
#include "storage/compaction.h"
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

    /**
     * Return the number of keys in the map.
     */
    virtual size_t size() const = 0;

    /**
     * Returns the number of entries the map has space allocated for.
     */
    virtual size_t capacity() const = 0;
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
    explicit simple_key_offset_map(
      std::optional<size_t> max_keys = std::nullopt);

    seastar::future<bool>
    put(const compaction_key& key, model::offset offset) override;

    seastar::future<std::optional<model::offset>>
    get(const compaction_key& key) const override;

    model::offset max_offset() const override;

    size_t size() const override;
    size_t capacity() const override;

private:
    ss::shared_ptr<util::mem_tracker> _memory_tracker;
    util::mem_tracked::map_t<absl::btree_map, compaction_key, model::offset>
      _map;
    model::offset _max_offset;
    size_t _max_keys;
};

/**
 * A key_offset_map in which the key space is mapped to sha256(key).
 *
 * This container does not auto-grow on insert, and a default initialized
 * instance has zero capacity. To add capacity call `reset(size_bytes)`. This is
 * futurized to avoid reactor stalls when allocating a large amount of memory
 * for the container.
 *
 * For large containers call `co_await reset(0)` to clean up before the
 * destructor is run.
 */
class hash_key_offset_map : public key_offset_map {
    static constexpr double max_load_factor = 0.95;

public:
    seastar::future<std::optional<model::offset>>
    get(const compaction_key& key) const override;

    seastar::future<bool>
    put(const compaction_key& key, model::offset offset) override;

    model::offset max_offset() const override;

    size_t size() const override;
    size_t capacity() const override;

    /**
     * Reset the map to have capacity \p size bytes. A call to reset() will
     * destroy all existing data in the map and reset the size to zero.
     *
     * A call to `reset(0)` is useful when destroying a large instance of this
     * container which may otherwise incur reactor stalls when freed as part of
     * the destructor cleanup.
     */
    seastar::future<> initialize(size_t size_bytes);

    /**
     * Initialize prepares the container for use in a new context by resetting
     * every element to an initial state. This call does not change the size of
     * the container.
     */
    seastar::future<> reset();

    /**
     * The ratio of hash table searches (e.g. one get or put) to the number of
     * underlying accesses made into the hash table.
     */
    double hit_rate() const;

private:
    using hash_type = hash_sha256;

    /**
     * hash table entry.
     */
    struct entry {
        hash_type::digest_type digest{};
        model::offset offset;
        bool empty() const;
    };

    /**
     * Uses successive chunks of sizeof(index_type) bytes taken from hash(key)
     * as probes into the hash table. When `next()` returns null then the caller
     * should switch to linear probing.
     */
    struct probe {
        using index_type = uint32_t;
        static_assert(sizeof(index_type) <= hash_type::digest_size);

        explicit probe(const hash_type::digest_type&);

        std::optional<index_type> next();

        hash_type::digest_type::const_pointer iter;
        hash_type::digest_type::const_pointer end;
    };

    /**
     * hash the compaction key. this helper will catch exceptions and reset the
     * hashing object which is reused to avoid reinitialization of OpenSSL
     * state.
     */
    hash_type::digest_type hash_key(const compaction_key&) const;

    mutable hash_type hasher_;
    chunked_vector<entry> entries_;

    size_t size_{0};
    model::offset max_offset_;
    size_t capacity_{0};
    mutable size_t search_count_{0};
    mutable size_t probe_count_{0};
};

} // namespace storage
