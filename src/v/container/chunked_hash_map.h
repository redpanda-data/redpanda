/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "container/fragmented_vector.h"

#include <absl/hash/hash.h>
#include <ankerl/unordered_dense.h>

#include <type_traits>

namespace detail {

template<typename T>
concept has_absl_hash = requires(T val) {
    { AbslHashValue(std::declval<absl::HashState>(), val) };
};

/// Wrapper around absl::Hash that disables the extra hash mixing in
/// unordered_dense
template<typename T>
struct avalanching_absl_hash {
    // absl always hash mixes itself so no need to do it again
    using is_avalanching = void;

    auto operator()(const T& x) const noexcept -> uint64_t {
        return absl::Hash<T>()(x);
    }
};

} // namespace detail

/**
 * @brief A hash map that uses a chunked vector as the underlying storage.
 *
 * Use when the hash map is expected to have a large number of elements (e.g.:
 * scales with partitions or topics). Performance wise it's equal to the abseil
 * hashmaps.
 *
 * NB: References and iterators are not stable across insertions and deletions.
 *
 * Both std::hash and abseil's AbslHashValue are supported. We dispatch to the
 * latter if available. Given AbslHashValue also supports std::hash we could
 * also unconditionally dispatch to it. However, absl's hash mixing seems more
 * extensive (and hence less performant) so we only do that when needed.
 *
 * For more info please see
 * https://github.com/martinus/unordered_dense/?tab=readme-ov-file#1-overview
 */
template<
  typename Key,
  typename Value,
  typename Hash = std::conditional_t<
    detail::has_absl_hash<Key>,
    detail::avalanching_absl_hash<Key>,
    ankerl::unordered_dense::hash<Key>>,
  typename EqualTo = std::equal_to<Key>>
using chunked_hash_map = ankerl::unordered_dense::segmented_map<
  Key,
  Value,
  Hash,
  EqualTo,
  chunked_vector<std::pair<Key, Value>>,
  ankerl::unordered_dense::bucket_type::standard,
  chunked_vector<ankerl::unordered_dense::bucket_type::standard>>;

/**
 * @brief A set counterpart of chunked_hash_map (uses a chunked vector as the
 * underlying storage).
 */
template<
  typename Key,
  typename Hash = std::conditional_t<
    detail::has_absl_hash<Key>,
    detail::avalanching_absl_hash<Key>,
    ankerl::unordered_dense::hash<Key>>,
  typename EqualTo = std::equal_to<Key>>
using chunked_hash_set = ankerl::unordered_dense::segmented_set<
  Key,
  Hash,
  EqualTo,
  chunked_vector<Key>,
  ankerl::unordered_dense::bucket_type::standard,
  chunked_vector<ankerl::unordered_dense::bucket_type::standard>>;
