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

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <fmt/format.h>

#include <stdexcept>
#include <string_view>

namespace storage {

enum class record_version_type { v1 };

inline record_version_type from_string(std::string_view version) {
    if (version == "v1") {
        return record_version_type::v1;
    }
    throw std::invalid_argument(
      fmt::format("Wrong record version name: {}", version));
}

inline ss::sstring to_string(record_version_type version) {
    switch (version) {
    case record_version_type::v1:
        return "v1";
    }
    throw std::runtime_error("Wrong record version");
}

inline std::ostream& operator<<(std::ostream& o, record_version_type v) {
    return o << to_string(v);
}

} // namespace storage
