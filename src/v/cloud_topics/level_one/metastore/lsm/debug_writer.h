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
#include "cloud_topics/level_one/metastore/lsm/write_batch_row.h"
#include "container/chunked_vector.h"

#include <expected>

namespace cloud_topics::l1 {

/// Translates typed protobuf row representations into write_batch_rows
/// for writing to the LSM metastore. Used by the debug/admin endpoint.
class debug_writer {
public:
    using errc = debug_serde_errc;
    using error = debug_serde_error;

    static std::expected<chunked_vector<write_batch_row>, error>
    build_rows(const proto::admin::metastore::write_rows_request& req);
};

} // namespace cloud_topics::l1
