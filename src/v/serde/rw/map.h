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
#include "serde/rw/reservable.h"
#include "serde/rw/rw.h"
#include "serde/serde_exception.h"
#include "serde/serde_is_enum.h"
#include "serde/serde_size_t.h"
#include "serde/type_str.h"
#include "ssx/sformat.h"

#include <cinttypes>
#include <type_traits>

namespace serde {

template<typename T>
concept Map = requires(T t) {
    t.emplace(
      std::declval<typename T::key_type>(),
      std::declval<typename T::mapped_type>());
    t.begin();
    t.end();
    { t.size() } -> std::convertible_to<serde_size_t>;
};

void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  Map auto& t,
  const std::size_t bytes_left_limit) {
    using Type = std::decay_t<decltype(t)>;
    const auto size = read_nested<serde_size_t>(in, bytes_left_limit);
    if constexpr (Reservable<Type>) {
        reserve_from_wire(t, size, in);
    }
    for (auto i = 0U; i < size; ++i) {
        typename Type::key_type key;
        typename Type::mapped_type value;
        read_nested(in, key, bytes_left_limit);
        read_nested(in, value, bytes_left_limit);
        t.emplace(std::move(key), std::move(value));
    }
}

template<typename M>
requires Map<std::decay_t<M>>
void tag_invoke(tag_t<write_tag>, iobuf& out, M&& t) {
    if (unlikely(t.size() > std::numeric_limits<serde_size_t>::max())) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "serde: {} size {} exceeds serde_size_t",
          type_str<std::decay_t<M>>(),
          t.size()));
    }
    write(out, static_cast<serde_size_t>(t.size()));
    for (auto& v : t) {
        write(out, v.first);
        write(out, std::forward_like<M>(v.second));
    }
}

} // namespace serde
