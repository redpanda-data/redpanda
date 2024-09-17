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
#include "serde/rw/reservable.h"
#include "serde/rw/rw.h"
#include "serde/serde_exception.h"
#include "serde/serde_size_t.h"
#include "serde/type_str.h"
#include "ssx/sformat.h"

#include <cinttypes>
#include <limits>
#include <type_traits>

namespace serde {

/**
 * This concept applies to everything that behaves like a vector.
 * Requires push_back, begin/end iterators and size.
 * This applies also to:
 * ss::chunked_fifo, fragmented_vector, ss::circular_buffer, etc.
 */
template<typename T>
concept Vector = requires(T t) {
    t.push_back(std::declval<typename T::value_type>());
    t.begin();
    t.end();
    { t.size() } -> std::convertible_to<std::size_t>;
};

void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  Vector auto& t,
  const std::size_t bytes_left_limit) {
    using Type = std::decay_t<decltype(t)>;
    using value_type = typename Type::value_type;

    const auto size = read_nested<serde_size_t>(in, bytes_left_limit);
    if constexpr (Reservable<decltype(t)>) {
        t.reserve(size);
    }
    for (auto i = 0U; i < size; ++i) {
        t.push_back(read_nested<value_type>(in, bytes_left_limit));
    }
    t.shrink_to_fit();
}

void tag_invoke(tag_t<write_tag>, iobuf& out, Vector auto t) {
    if (unlikely(t.size() > std::numeric_limits<serde_size_t>::max())) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "serde: {} size {} exceeds serde_size_t",
          type_str<decltype(t)>(),
          t.size()));
    }
    write(out, static_cast<serde_size_t>(t.size()));
    for (auto& el : t) {
        write(out, std::move(el));
    }
}

} // namespace serde
