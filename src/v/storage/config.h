/*
 * Copyright 2023 Redpanda Data, Inc.
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
#include "base/units.h"

#include <seastar/core/sstring.hh>

#include <optional>

namespace storage {

inline constexpr const size_t segment_appender_fallocation_alignment = 4_KiB;

/** Validator for fallocation step configuration setting */
inline std::optional<ss::sstring>
validate_fallocation_step(const size_t& value) {
    if (value % segment_appender_fallocation_alignment != 0) {
        return "Fallocation step must be multiple of 4096";
    } else if (value < segment_appender_fallocation_alignment) {
        return "Fallocation step must be at least 4 KiB (4096)";
    } else if (value > 1_GiB) {
        return "Fallocation step can't be larger than 1 GiB (1073741824)";
    } else {
        return std::nullopt;
    }
}

} // namespace storage
