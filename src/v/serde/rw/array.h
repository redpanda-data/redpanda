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
#include "serde/rw/rw.h"
#include "serde/serde_exception.h"
#include "serde/serde_size_t.h"
#include "serde/type_str.h"
#include "ssx/sformat.h"

#include <array>
#include <limits>

namespace serde {

template<typename T, std::size_t Size>
void tag_invoke(tag_t<write_tag>, iobuf& out, std::array<T, Size> t) {
    static_assert(t.size() <= std::numeric_limits<serde_size_t>::max());
    write(out, static_cast<serde_size_t>(t.size()));
    for (auto& el : t) {
        write(out, el);
    }
}

template<typename T, std::size_t Size>
void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  std::array<T, Size>& t,
  const std::size_t bytes_left_limit) {
    using Type = std::decay_t<decltype(t)>;
    using value_type = typename Type::value_type;
    const auto size = read_nested<serde_size_t>(in, bytes_left_limit);
    if (unlikely(size != t.size())) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "reading type {}, size mismatch. expected {} go {}",
          type_str<Type>(),
          t.size(),
          size));
    }
    for (auto i = 0U; i < t.size(); ++i) {
        t[i] = read_nested<value_type>(in, bytes_left_limit);
    }
}

} // namespace serde
