// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <concepts>
#include <cstddef>
#include <type_traits>

class iobuf;
class iobuf_parser;

namespace serde {

template<typename Output>
concept SerdeWriteOutput = requires(
  Output& out, const char* src, std::size_t size) {
    out.append(src, size);
    out.reserve(size);
    { out.size_bytes() } -> std::convertible_to<std::size_t>;
};

// How to use:
//
// template<typename T>
// void tag_invoke(tag_t<write_tag>, SerdeWriteOutput auto& out, my_type<T> t) {
//     // write `t` to `out`
// }
//
// template<typename T>
// void tag_invoke(
//   tag_t<read_tag>,
//   iobuf_parser& in,
//   my_type<T>& t,
//   std::size_t const bytes_left_limit) {
//     t = ...; // read from `in` to `t`
// }
//
// Writers that change the canonical encoding of an otherwise fixed-width type
// must specialize `disable_fixed_serde_v<T>` to true.

template<auto& CPO>
using tag_t = std::remove_cvref_t<decltype(CPO)>;

inline constexpr struct write_fn {
    template<typename Output, typename T>
    void operator()(Output& b, T&& x) const {
        return tag_invoke(*this, b, std::forward<T>(x));
    }
} write_tag{};

inline constexpr struct read_fn {
    template<typename T>
    void operator()(
      iobuf_parser& in, T& t, const std::size_t bytes_left_limit) const {
        return tag_invoke(*this, in, t, bytes_left_limit);
    }
} read_tag{};

} // namespace serde
