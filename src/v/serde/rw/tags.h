// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <type_traits>

class iobuf;
class iobuf_parser;

namespace serde {

// How to use:
//
// template<typename T>
// void tag_invoke(tag_t<write_tag>, iobuf& out, my_type<T> t) {
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

template<auto& CPO>
using tag_t = std::remove_cvref_t<decltype(CPO)>;

inline constexpr struct write_fn {
    template<typename T>
    void operator()(iobuf& b, T&& x) const {
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
