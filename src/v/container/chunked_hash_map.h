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

#include "absl/hash/hash.h"
#include "container/chunked_vector.h"

#include <ankerl/unordered_dense.h>
#include <fmt/format.h>

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

namespace detail {
template<typename Range>
struct chunked_hash_map_from_range_impl {
    using value_t = std::ranges::range_value_t<std::decay_t<Range>>;
    using first_t = typename value_t::first_type;
    using second_t = typename value_t::second_type;
    using ret_t = chunked_hash_map<first_t, second_t>;
};
} // namespace detail

// reserves if range size is known
template<typename Range>
typename detail::chunked_hash_map_from_range_impl<Range>::ret_t
chunked_hash_map_from_range(Range&& range) {
    size_t size = 0;
    if constexpr (std::ranges::sized_range<Range>) {
        size = std::ranges::size(range);
    }
    return {std::ranges::begin(range), std::ranges::end(range), size};
};

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

template<typename K, typename V>
std::ostream& operator<<(std::ostream& o, const chunked_hash_map<K, V>& r) {
    o << "{";
    bool first = true;
    for (const auto& [k, v] : r) {
        if (!first) {
            o << ", ";
        }
        o << "{" << k << " -> " << v << "}";
        first = false;
    }
    o << "}";
    return o;
}
template<typename K, typename V>
struct fmt::formatter<chunked_hash_map<K, V>> {
    using type = chunked_hash_map<K, V>;

    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    typename FormatContext::iterator
    format(const type& map, FormatContext& ctx) const {
        // Map formatting is broken until version 11:
        // https://github.com/fmtlib/fmt/issues/3685
        auto out = ctx.out();
        out = fmt::format_to(out, "[");
        auto it = map.begin();
        if (it != map.end()) {
            out = fmt::format_to(out, "{{{} -> {}}}", it->first, it->second);
            for (++it; it != map.end(); ++it) {
                out = fmt::format_to(
                  out, ", {{{} -> {}}}", it->first, it->second);
            }
        }
        return fmt::format_to(out, "]");
    }
};

/// Returns a lower bound on the memory currently being held by `m`.
template<
  typename K,
  typename V,
  typename Hash = std::conditional_t<
    detail::has_absl_hash<K>,
    detail::avalanching_absl_hash<K>,
    ankerl::unordered_dense::hash<K>>,
  typename EqualTo = std::equal_to<K>>
size_t
memory_usage_lower_bound(const chunked_hash_map<K, V, Hash, EqualTo>& m) {
    return m.bucket_count()
             * sizeof(typename chunked_hash_map<K, V>::bucket_type)
           + m.values().capacity() * sizeof(m.values()[0]);
}

template<typename K>
struct fmt::formatter<chunked_hash_set<K>> {
    using type = chunked_hash_set<K>;

    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    typename FormatContext::iterator
    format(const type& set, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "[{}]", fmt::join(set, ","));
    }
};
