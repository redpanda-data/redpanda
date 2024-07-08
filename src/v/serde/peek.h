// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "serde/envelope.h"
#include "serde/rw/rw.h"
#include "serde/rw/scalar.h"
#include "serde/serde_exception.h"

namespace serde {

inline version_t peek_version(iobuf_parser& in) {
    if (unlikely(in.bytes_left() < sizeof(serde::version_t))) {
        throw serde_exception{"cannot peek version"};
    }
    auto version_reader = iobuf_parser{in.peek(sizeof(serde::version_t))};
    return serde::read_nested<serde::version_t>(version_reader, 0);
}

inline serde::serde_size_t peek_body_size(iobuf_parser& in) {
    if (unlikely(in.bytes_left() < envelope_header_size)) {
        throw serde_exception{"cannot peek size"};
    }

    // Take bytes 2-6
    auto header_buf = in.peek(envelope_header_size);
    header_buf.trim_front(2);
    auto size_reader = iobuf_parser{std::move(header_buf)};
    return serde::read_nested<serde::serde_size_t>(size_reader, 0);
}

} // namespace serde
