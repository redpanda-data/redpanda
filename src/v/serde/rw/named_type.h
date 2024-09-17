// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "serde/rw/rw.h"
#include "utils/named_type.h"

namespace serde {

template<typename T, typename Tag, typename IsConstexpr>
void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  ::detail::base_named_type<T, Tag, IsConstexpr>& t,
  const std::size_t bytes_left_limit) {
    using Type = std::decay_t<decltype(t)>;
    t = Type{read_nested<typename Type::type>(in, bytes_left_limit)};
}

template<typename T, typename Tag, typename IsConstexpr>
void tag_invoke(
  tag_t<write_tag>,
  iobuf& out,
  ::detail::base_named_type<T, Tag, IsConstexpr> t) {
    return write(out, static_cast<T>(t));
}

} // namespace serde
