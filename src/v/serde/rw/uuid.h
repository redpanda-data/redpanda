// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "serde/rw/tags.h"
#include "utils/uuid.h"

namespace serde {

inline void tag_invoke(tag_t<write_tag>, iobuf& out, uuid_t t) {
    out.append(t.uuid().data, uuid_t::length);
}

inline void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  uuid_t& t,
  const std::size_t /* bytes_left_limit */) {
    in.consume_to(uuid_t::length, t.mutable_uuid().begin());
}

} // namespace serde
