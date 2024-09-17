// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/vlog.h"
#include "serde/read_header.h"
#include "serde/rw/tags.h"
#include "serde/serde_exception.h"
#include "serde/type_str.h"
#include "ssx/sformat.h"

namespace serde {

template<typename T>
concept DirectReadable = requires(iobuf_parser& in, const header& h) {
    { T::serde_direct_read(in, h) };
};

template<typename T>
void read_nested(iobuf_parser& in, T& t, const std::size_t bytes_left_limit) {
    read_tag(in, t, bytes_left_limit);
}

template<typename T>
T read_nested(iobuf_parser& in, const std::size_t bytes_left_limit) {
    using Type = std::decay_t<T>;
    static_assert(std::is_default_constructible_v<T> || DirectReadable<T>);
    if constexpr (DirectReadable<T>) {
        const auto h = read_header<Type>(in, bytes_left_limit);
        return Type::serde_direct_read(in, h);
    } else {
        auto t = Type();
        read_nested(in, t, bytes_left_limit);
        return t;
    }
}

template<typename T>
std::decay_t<T> read(iobuf_parser& in) {
    auto ret = read_nested<T>(in, 0U);
    if (unlikely(in.bytes_left() != 0)) {
        throw serde_exception{fmt_with_ctx(
          ssx::sformat,
          "serde: not all bytes consumed after read_nested<{}>(), "
          "bytes_left={}",
          type_str<T>(),
          in.bytes_left())};
    }
    return ret;
}

template<typename T>
void write(iobuf& b, T x) {
    write_tag(b, std::forward<T>(x));
}

template<typename T>
iobuf to_iobuf(T&& t) {
    iobuf b;
    write(b, std::forward<T>(t));
    return b;
}

template<typename T>
T from_iobuf(iobuf b) {
    auto in = iobuf_parser{std::move(b)};
    return read<T>(in);
}

} // namespace serde
