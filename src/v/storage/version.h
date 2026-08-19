/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/format_to.h"
#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <fmt/format.h>

#include <stdexcept>
#include <string_view>

namespace storage {

/// Version of the segment file format, encoded in the segment filename
/// ({base_offset}-{term}-{version}.log).
///
/// v1: the segment contains batches from exactly one raft term (the term in
///     the filename); the append path rolls segments on term change.
/// v2: the segment may contain batches from multiple raft terms and the
///     {term} in the filename is the term of the base offset only. Terms of
///     later offsets are recovered from raft configuration batch payloads
///     (group_configuration v_8). The byte layout is unchanged from v1
///     otherwise.
enum class record_version_type { v1, v2 };

inline fmt::iterator format_to(record_version_type version, fmt::iterator out) {
    switch (version) {
    case record_version_type::v1:
        return fmt::format_to(out, "v1");
    case record_version_type::v2:
        return fmt::format_to(out, "v2");
    }
    throw std::runtime_error("Wrong record version");
}

inline record_version_type from_string(std::string_view version) {
    if (version == "v1") {
        return record_version_type::v1;
    }
    if (version == "v2") {
        return record_version_type::v2;
    }
    throw std::invalid_argument(
      fmt::format("Wrong record version name: {}", version));
}

inline ss::sstring to_string(record_version_type version) {
    return ss::sstring(fmt::to_string(version));
}

} // namespace storage
