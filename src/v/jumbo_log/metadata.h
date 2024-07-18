// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "serde/envelope.h"
#include "utils/named_type.h"
#include "utils/uuid.h"

#include <fmt/ostream.h>

#include <ostream>
#include <stdint.h>
#include <tuple>

namespace jumbo_log {
using jsn = named_type<uint64_t, struct jumbo_segment_sequence_number_tag>;
using write_intent_id_t = named_type<uint64_t, struct write_intent_id_tag>;

/// The version of the object format. This is used to determine how to
/// interpret the object data.
enum class segment_format_version_t : uint8_t {
    /// The initial version of the segment format.
    v0 = 0,
};

inline std::ostream& operator<<(std::ostream& o, segment_format_version_t v) {
    switch (v) {
    case segment_format_version_t::v0:
        o << "v0";
        break;
    }
    return o;
}

/// A segment object is a reference to the binary object in cloud storage
/// corresponding to a jumbo log segment.
struct segment_object {
    static constexpr serde::version_t redpanda_serde_version = 0;
    static constexpr serde::version_t redpanda_serde_compat_version = 0;

    uuid_t id;
    size_t size_bytes;
    segment_format_version_t format_version;

    friend bool operator==(const segment_object& lhs, const segment_object& rhs)
      = default;

    auto serde_fields() { return std::tie(id, size_bytes, format_version); }

    friend std::ostream&
    operator<<(std::ostream& o, const segment_object& obj) {
        fmt::print(
          o,
          "id: {}, size_bytes: {}, version: {}",
          obj.id,
          obj.size_bytes,
          obj.format_version);
        return o;
    }
};

struct write_intent_segment {
    static constexpr serde::version_t redpanda_serde_version = 0;
    static constexpr serde::version_t redpanda_serde_compat_version = 0;

    write_intent_id_t id;
    segment_object object;

    friend std::ostream&
    operator<<(std::ostream& o, const write_intent_segment& s) {
        fmt::print(o, "id: {}, object: {}", s.id, s.object);
        return o;
    }
};

} // namespace jumbo_log
