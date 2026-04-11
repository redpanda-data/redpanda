/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "proto/redpanda/core/admin/internal/cloud_topics/v1/metastore.proto.h"
#include "utils/detailed_error.h"

#include <fmt/core.h>

#include <expected>

namespace cloud_topics::l1 {

enum class debug_serde_errc {
    invalid_uuid,
    missing_key,
    missing_value,
    unknown_row_type,
    decode_failure,
};
using debug_serde_error = detailed_error<debug_serde_errc>;

std::string_view to_string_view(debug_serde_errc e);

/// Encode a typed proto key into the raw LSM key string.
std::expected<ss::sstring, debug_serde_error>
debug_encode_key(const proto::admin::metastore::row_key&);

/// Encode a typed proto value into a serialized iobuf.
std::expected<iobuf, debug_serde_error>
debug_encode_value(const proto::admin::metastore::row_value&);

} // namespace cloud_topics::l1

inline auto format_as(cloud_topics::l1::debug_serde_errc e) {
    return cloud_topics::l1::to_string_view(e);
}
