// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "serde/rw/fixed.h"
#include "serde/rw/rw.h"

#include <cinttypes>

namespace serde {

namespace detail {

template<typename Tag>
requires(
  !has_nonmember_write_nested<ss::bool_class<Tag>>
  && !disable_fixed_serde_v<ss::bool_class<Tag>>)
struct fixed_serde_traits<ss::bool_class<Tag>> {
    static constexpr bool supported = true;
    static constexpr size_t size = sizeof(int8_t);
    static constexpr bool requires_validation = false;
};

} // namespace detail

template<SerdeWriteOutput Output, typename Tag>
void tag_invoke(tag_t<write_tag>, Output& out, ss::bool_class<Tag> t) {
    write(out, static_cast<int8_t>(bool(t)));
}

template<typename Tag>
void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  ss::bool_class<Tag>& t,
  const std::size_t bytes_left_limit) {
    t = ss::bool_class<Tag>{
      read_nested<std::int8_t>(in, bytes_left_limit) != 0};
}

} // namespace serde
