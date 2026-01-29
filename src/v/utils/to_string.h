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

#include "absl/container/btree_map.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_map.h"
#include "base/seastarx.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/print.hh>

#include <fmt/core.h>

#include <optional>
#include <ostream>
#include <variant>

namespace std {

template<typename T>
std::ostream& operator<<(std::ostream& os, const std::optional<T>& opt) {
    if (opt) {
        fmt::print(os, "{{{}}}", *opt);
        return os;
    }
    return os << "{nullopt}";
}

template<typename... T>
requires(sizeof...(T) > 0)
std::ostream& operator<<(std::ostream& os, const std::variant<T...>& v) {
    std::visit([&os](auto& arg) { fmt::print(os, "{{{}}}", arg); }, v);
    return os;
}

} // namespace std

template<typename T, size_t chunk_size>
struct fmt::formatter<ss::chunked_fifo<T, chunk_size>> {
    using type = ss::chunked_fifo<T, chunk_size>;

    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    typename FormatContext::iterator
    format(const type& fifo, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "[{}]", fmt::join(fifo, ", "));
    }
};

namespace absl {

template<typename E>
std::ostream& operator<<(std::ostream& o, const absl::flat_hash_set<E>& s) {
    return o << fmt::format("{{{}}}", fmt::join(s, ","));
}

template<typename K, typename V>
std::ostream& operator<<(std::ostream& o, const absl::flat_hash_map<K, V>& r) {
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
std::ostream& operator<<(std::ostream& o, const absl::node_hash_map<K, V>& r) {
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
std::ostream& operator<<(std::ostream& o, const absl::btree_map<K, V>& r) {
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

template<typename K>
std::ostream& operator<<(std::ostream& o, const absl::btree_set<K>& s) {
    o << "{";
    bool first = true;
    for (const auto& k : s) {
        if (!first) {
            o << ", ";
        }
        o << k;
        first = false;
    }
    o << "}";
    return o;
}

} // namespace absl

template<>
struct fmt::formatter<absl::Time> {
    constexpr format_parse_context::iterator parse(format_parse_context& ctx) {
        return ctx.begin();
    }

    format_context::iterator
    format(const absl::Time& t, fmt::format_context& ctx);
};

template<>
struct fmt::formatter<absl::Duration> {
    constexpr format_parse_context::iterator parse(format_parse_context& ctx) {
        return ctx.begin();
    }

    format_context::iterator
    format(const absl::Duration& d, fmt::format_context& ctx);
};

template<>
struct fmt::formatter<std::monostate> {
    constexpr format_parse_context::iterator parse(format_parse_context& ctx) {
        return ctx.begin();
    }
    format_context::iterator format(const std::monostate&, format_context& ctx);
};
