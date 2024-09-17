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
#include "serde/rw/map.h"
#include "serde/rw/reservable.h"
#include "serde/rw/rw.h"
#include "serde/serde_exception.h"
#include "serde/serde_size_t.h"
#include "serde/type_str.h"
#include "ssx/sformat.h"

#include <cinttypes>
#include <type_traits>

namespace serde {

template<typename T>
concept Set = requires(T t) {
    t.emplace(std::declval<typename T::key_type>());
    t.begin();
    t.end();
    { t.size() } -> std::convertible_to<serde_size_t>;
};

template<typename T>
concept SetNotMap = Set<T> && !Map<T>;

void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  SetNotMap auto& t,
  const std::size_t bytes_left_limit) {
    using Type = std::decay_t<decltype(t)>;

    const auto size = read_nested<serde_size_t>(in, bytes_left_limit);
    if constexpr (Reservable<Type>) {
        t.reserve(size);
    }
    for (auto i = 0U; i < size; ++i) {
        auto elem = read_nested<typename Type::key_type>(in, bytes_left_limit);
        t.emplace(std::move(elem));
    }
}

void tag_invoke(tag_t<write_tag>, iobuf& out, SetNotMap auto t) {
    using Type = std::decay_t<decltype(t)>;
    if (unlikely(t.size() > std::numeric_limits<serde_size_t>::max())) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "serde: {} size {} exceeds serde_size_t",
          type_str<Type>(),
          t.size()));
    }
    write(out, static_cast<serde_size_t>(t.size()));
    for (auto& e : t) {
        write(out, e);
    }
}

} // namespace serde
