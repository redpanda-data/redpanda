// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/mvlog/entry.h"

#include <fmt/ostream.h>

#include <type_traits>

namespace storage::experimental::mvlog {

std::ostream& operator<<(std::ostream& o, entry_type t) {
    switch (t) {
    case entry_type::record_batch:
        return o << "record_batch";
    }
    fmt::print(o, "unknown ({})", static_cast<int8_t>(t));
    return o;
}

std::ostream& operator<<(std::ostream& o, const entry_header& h) {
    fmt::print(
      o,
      "{{header_crc: {}, body_size: {}, entry_type: {}}}",
      h.header_crc,
      h.body_size,
      h.type);
    return o;
}

} // namespace storage::experimental::mvlog
