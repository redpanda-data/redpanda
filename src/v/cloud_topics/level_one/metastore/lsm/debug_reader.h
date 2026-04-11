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

#include "cloud_topics/level_one/metastore/lsm/debug_serde.h"
#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/level_one/metastore/lsm/write_batch_row.h"
#include "container/chunked_vector.h"

#include <expected>
#include <optional>

namespace cloud_topics::l1 {

/// Decodes raw LSM key/value pairs into typed protobuf messages.
/// Used by the ReadRows admin endpoint.
class debug_reader {
public:
    using error = debug_serde_error;
    using errc = debug_serde_errc;

    struct decoded_key {
        row_type type;
        proto::admin::metastore::row_key key;
    };

    /// Decode a raw LSM key string into a row_type and typed proto key.
    static std::expected<decoded_key, error>
    decode_key(std::string_view raw_key);

    /// Decode a raw LSM value iobuf into a typed proto value.
    static std::expected<proto::admin::metastore::row_value, error>
    decode_value(row_type type, iobuf value);

    /// Build a ReadRowsResponse from raw rows and an optional next_key.
    static std::expected<proto::admin::metastore::read_rows_response, error>
    build_response(
      const chunked_vector<write_batch_row>& rows,
      std::optional<ss::sstring> next_key);
};

} // namespace cloud_topics::l1
