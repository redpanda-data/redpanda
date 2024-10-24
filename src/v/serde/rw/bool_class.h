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

#include <cinttypes>

namespace serde {

template<typename Tag>
void tag_invoke(tag_t<write_tag>, iobuf& out, ss::bool_class<Tag> t) {
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
