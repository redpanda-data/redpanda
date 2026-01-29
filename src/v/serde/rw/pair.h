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

namespace serde {

template<typename T1, typename T2>
void tag_invoke(tag_t<write_tag>, iobuf& out, const std::pair<T1, T2>& p) {
    write(out, p.first);
    write(out, p.second);
}

template<typename T1, typename T2>
void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  std::pair<T1, T2>& p,
  const std::size_t bytes_left_limit) {
    p.first = read_nested<T1>(in, bytes_left_limit);
    p.second = read_nested<T2>(in, bytes_left_limit);
}

} // namespace serde
