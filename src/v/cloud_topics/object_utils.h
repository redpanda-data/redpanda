/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/format_to.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/types.h"
#include "container/chunked_vector.h"

#include <expected>

namespace cloud_topics {

/*
 * Utilities for working with the object storage paths.
 */
class object_path_factory {
public:
    /*
     * Generate the path of a level-zero object.
     */
    static cloud_storage_clients::object_key level_zero_path(object_id id);

    /*
     * Generate a level-zero object path prefix
     */
    static cloud_storage_clients::object_key
    level_zero_path_prefix(std::string_view ext);

    /*
     * Level-zero object data root directory. Contains a trailing "/".
     */
    static cloud_storage_clients::object_key level_zero_data_dir();

    /*
     * Extract the epoch from an L0 object key.
     */
    static std::expected<cluster_epoch, std::string>
      level_zero_path_to_epoch(std::string_view);

    static std::expected<object_id::prefix_t, std::string>
      level_zero_path_to_prefix(std::string_view);
};

struct prefix_range_inclusive {
    using T = object_id::prefix_t;
    static_assert(std::is_unsigned_v<T>);
    static constexpr T t_max = object_id::prefix_max;
    T min;
    T max;
    prefix_range_inclusive(T min, T max);
    bool contains(T v) const;
    bool operator==(const prefix_range_inclusive& other) const;
    fmt::iterator format_to(fmt::iterator it) const;
};

/**
 * @brief A trie for compressing a contiguous range of 3-digit numeric prefixes
 * into a minimal set of string prefixes that cover the same range.
 *
 * Object IDs use a 3-digit prefix (000-999) for partitioning data across
 * object storage. When listing objects within a range of prefixes, issuing
 * one list request per prefix is inefficient. This trie compresses the range
 * into the smallest set of prefix strings that exactly covers it.
 *
 * The compression exploits the decimal structure: a prefix like "1" covers
 * all values 100-199, "05" covers 050-059, and "042" covers only 042.
 *
 * Usage:
 *   1. insert() a prefix_range_inclusive - populates the trie with all
 *      3-digit strings in the range
 *   2. prune() - collapses saturated subtrees into shorter prefixes
 *   3. collect() - returns the minimal covering prefix set
 *
 * Examples (after insert + prune):
 *   [0, 12]     -> {"00", "010", "011", "012"}
 *   [100, 199]  -> {"1"}
 *   [89, 300]   -> {"089", "09", "1", "2", "300"}
 *   [0, 999]    -> {""}  (empty string matches everything)
 *
 * Before pruning, collect() returns all values in the range as 3-digit
 * zero-padded strings. After pruning, it returns the compressed set.
 * Both insert() and prune() are idempotent on their own, and collect()
 * is always idempotent.
 *
 * clear() resets the trie to its initial empty state.
 */
class trie {
private:
    struct node;

public:
    trie();
    ~trie();
    trie(const trie&) = delete;
    trie& operator=(const trie&) = delete;
    trie(trie&&) = delete;
    trie& operator=(trie&&) = delete;

    /**
     * @brief Insert all 3-digit zero-padded strings in the given range.
     */
    void insert(prefix_range_inclusive prefixes);

    /**
     * @brief Collapse saturated subtrees into shorter prefixes. A subtree is
     * saturated when it contains all possible values for its depth:
     * - depth 1: 100 values (e.g., "1" covers 100-199)
     * - depth 2: 10 values (e.g., "05" covers 050-059)
     * - depth 3: 1 value (leaf node)
     */
    void prune();

    /**
     * @brief Return the current set of prefix strings. Before prune(), this
     * returns all inserted values. After prune(), this returns the minimal
     * covering set. Returns empty if nothing has been inserted.
     */
    chunked_vector<ss::sstring> collect() const;

    /**
     * @brief Reset the trie to its initial empty state.
     */
    void clear();

    fmt::iterator format_to(fmt::iterator it) const;

private:
    std::unique_ptr<node> root;
};

/**
 * @brief Stateful iterator over compressed prefix keys for object listing.
 *
 * Wraps a trie to provide incremental consumption of compressed prefixes.
 * Maintains iteration state across calls to consume_prefix().
 *
 * Iteration behavior:
 * - consume_prefix() returns prefixes one at a time as object keys
 * - Returns nullopt when exhausted, then wraps around on the next call
 * - Useful for round-robin distribution of list operations across iterations
 *
 * Range updates:
 * - set_range(same_range) is a no-op; iteration position preserved
 * - set_range(different_range) resets iteration to the beginning
 * - set_range(nullopt) clears all state; consume returns nullopt
 *
 * compressed_key_prefixes() returns all prefixes without affecting iteration.
 */
struct prefix_compressor {
    prefix_compressor() = default;

    /**
     * @brief Set the prefix range. No-op if range unchanged. Resets
     * iteration if range differs. Clears state if nullopt.
     */
    void set_range(std::optional<prefix_range_inclusive>);

    /**
     * @brief Return all compressed prefixes as object keys (non-consuming).
     */
    chunked_vector<cloud_storage_clients::object_key>
    compressed_key_prefixes() const;

    /**
     * @brief Return the next prefix, or nullopt if exhausted. Wraps around on
     * subsequent call after exhaustion.
     */
    std::optional<cloud_storage_clients::object_key> consume_prefix();

private:
    std::optional<prefix_range_inclusive> range_;
    trie trie_;
    chunked_vector<ss::sstring> raw_prefixes_;
    std::optional<size_t> prefix_idx_;
};

} // namespace cloud_topics
