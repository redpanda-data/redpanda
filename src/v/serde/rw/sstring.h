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
#include "serde/serde_size_t.h"

#include <seastar/core/sstring.hh>

namespace serde {

template<typename char_type, typename Size, Size max_size, bool NulTerminate>
inline void tag_invoke(
  tag_t<write_tag>,
  iobuf& out,
  ss::basic_sstring<char_type, Size, max_size, NulTerminate> t) {
    write<serde_size_t>(out, t.size());
    out.append(t.data(), t.size());
}

template<typename char_type, typename Size, Size max_size, bool NulTerminate>
inline void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  ss::basic_sstring<char_type, Size, max_size, NulTerminate>& t,
  const std::size_t bytes_left_limit) {
    auto str = ss::uninitialized_string<
      ss::basic_sstring<char_type, Size, max_size, NulTerminate>>(
      read_nested<serde_size_t>(in, bytes_left_limit));
    in.consume_to(str.size(), str.begin());
    t = str;
}

} // namespace serde
