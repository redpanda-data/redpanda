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
#include "utils/tristate.h"

namespace serde {

template<typename T>
void tag_invoke(tag_t<write_tag>, iobuf& out, tristate<T> t) {
    if (t.is_disabled()) {
        write<int8_t>(out, -1);
    } else if (!t.has_optional_value()) {
        write<int8_t>(out, 0);
    } else {
        write<int8_t>(out, 1);
        write(out, std::move(t.value()));
    }
}

template<typename T>
void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  tristate<T>& t,
  const std::size_t bytes_left_limit) {
    using Type = std::decay_t<decltype(t)>;

    int8_t flag = read_nested<int8_t>(in, bytes_left_limit);
    if (flag == -1) {
        // disabled
        t = tristate<T>{};
    } else if (flag == 0) {
        // empty
        t = tristate<T>(std::nullopt);
    } else if (flag == 1) {
        t = tristate<T>(read_nested<T>(in, bytes_left_limit));
    } else {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "reading type {} of size {}: {} bytes left - unexpected tristate "
          "state flag: {}, expected states are -1,0,1",
          type_str<Type>(),
          sizeof(Type),
          in.bytes_left(),
          flag));
    }
}

} // namespace serde
