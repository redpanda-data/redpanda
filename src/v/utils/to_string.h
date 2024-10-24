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

#include "base/seastarx.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/print.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>
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

template<typename Rep, typename Period>
inline std::ostream&
operator<<(std::ostream& o, const std::chrono::duration<Rep, Period>& d) {
    fmt::print(
      o,
      "{}",
      std::chrono::duration_cast<std::chrono::milliseconds>(d).count());
    return o;
}

} // namespace std

template<typename T, size_t chunk_size>
struct fmt::formatter<ss::chunked_fifo<T, chunk_size>> {
    using type = ss::chunked_fifo<T, chunk_size>;

    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    typename FormatContext::iterator
    format(const type& fifo, FormatContext& ctx) const {
        fmt::format_to(ctx.out(), "[");
        if (!fifo.empty()) {
            auto it = fifo.begin();
            fmt::format_to(ctx.out(), "{}", *(it++));

            for (; it != fifo.end(); ++it) {
                fmt::format_to(ctx.out(), ", {}", *it);
            }
            return ctx.out();
        }

        fmt::format_to(ctx.out(), "]");
        return ctx.out();
    }
};

namespace absl {
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
} // namespace absl
