// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

class iobuf;
class iobuf_parser;

namespace serde {

template<auto& CPO>
using tag_t = std::remove_cvref_t<decltype(CPO)>;

inline constexpr struct write_fn {
    template<typename T>
    void operator()(iobuf& b, T&& x) const {
        return tag_invoke(*this, b, std::forward<T>(x));
    }
} w{};

inline constexpr struct read_fn {
    template<typename T>
    void operator()(
      iobuf_parser& in, T& t, std::size_t const bytes_left_limit) const {
        return tag_invoke(*this, in, t, bytes_left_limit);
    }
} r{};

} // namespace serde
