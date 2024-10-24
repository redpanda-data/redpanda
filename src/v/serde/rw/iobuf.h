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
#include "serde/rw/scalar.h"
#include "serde/serde_size_t.h"

namespace serde {

inline void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  iobuf& t,
  const std::size_t bytes_left_limit) {
    t = in.share(read_nested<serde_size_t>(in, bytes_left_limit));
}

inline void tag_invoke(tag_t<write_tag>, iobuf& out, iobuf t) {
    write<serde_size_t>(out, t.size_bytes());
    out.append(std::move(t));
}

} // namespace serde
