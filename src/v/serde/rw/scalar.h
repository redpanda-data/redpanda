// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/vlog.h"
#include "bytes/iobuf_parser.h"
#include "serde/rw/tags.h"
#include "serde/serde_exception.h"
#include "serde/serde_is_enum.h"
#include "serde/type_str.h"
#include "ssx/sformat.h"

#include <bit>
#include <cinttypes>
#include <type_traits>

namespace serde {

template<typename T>
requires(std::is_scalar_v<std::decay_t<T>> && !serde_is_enum_v<std::decay_t<T>>)
void tag_invoke(
  tag_t<read_tag>, iobuf_parser& in, T& t, const std::size_t bytes_left_limit) {
    using Type = std::decay_t<T>;

    if (unlikely(in.bytes_left() - bytes_left_limit < sizeof(Type))) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "reading type {} of size {}: {} bytes left",
          type_str<Type>(),
          sizeof(Type),
          in.bytes_left()));
    }

    if constexpr (sizeof(Type) == 1) {
        t = in.consume_type<Type>();
    } else if constexpr (std::is_same_v<float, Type>) {
        t = std::bit_cast<float>(le32toh(in.consume_type<std::uint32_t>()));
    } else if constexpr (std::is_same_v<double, Type>) {
        t = std::bit_cast<double>(le64toh(in.consume_type<std::uint64_t>()));
    } else {
        t = ss::le_to_cpu(in.consume_type<Type>());
    }
}

template<typename T>
requires(std::is_scalar_v<std::decay_t<T>> && !serde_is_enum_v<std::decay_t<T>>)
void tag_invoke(tag_t<write_tag>, iobuf& out, T t) {
    using Type = std::decay_t<T>;
    if constexpr (sizeof(Type) == 1) {
        out.append(reinterpret_cast<const char*>(&t), sizeof(t));
    } else if constexpr (std::is_same_v<float, Type>) {
        const auto le_t = htole32(std::bit_cast<std::uint32_t>(t));
        static_assert(sizeof(le_t) == sizeof(Type));
        out.append(reinterpret_cast<const char*>(&le_t), sizeof(le_t));
    } else if constexpr (std::is_same_v<double, Type>) {
        const auto le_t = htole64(std::bit_cast<std::uint64_t>(t));
        static_assert(sizeof(le_t) == sizeof(Type));
        out.append(reinterpret_cast<const char*>(&le_t), sizeof(le_t));
    } else {
        const auto le_t = ss::cpu_to_le(t);
        static_assert(sizeof(le_t) == sizeof(Type));
        out.append(reinterpret_cast<const char*>(&le_t), sizeof(le_t));
    }
}

inline void tag_invoke(tag_t<write_tag>, iobuf& out, bool t) {
    write_tag(out, static_cast<int8_t>(t));
}

inline void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  bool& t,
  const std::size_t bytes_left_limit) {
    int8_t byte;
    read_tag(in, byte, bytes_left_limit);
    t = (byte != 0);
}

} // namespace serde
