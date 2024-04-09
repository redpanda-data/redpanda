/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/outcome.h"
#include "model/transform.h"
#include "ssx/semaphore.h"
#include "transform/logging/errc.h"

#pragma once

namespace transform::logging {

namespace io {
struct json_batch {
    json_batch(iobuf n, ss::chunked_fifo<iobuf> e, ssx::semaphore_units units)
      : name(std::move(n))
      , events(std::move(e))
      , _units(std::move(units)) {}

    iobuf name;
    ss::chunked_fifo<iobuf> events;

private:
    ssx::semaphore_units _units;
};
using json_batches = ss::chunked_fifo<json_batch>;

} // namespace io

/**
 * Abstract interface providing cluster and transform system access to a single
 * instance the log manager.
 *
 * Responsibilities may include:
 *   - Creating the transform_logs topic
 *   - Batching and publishing collections of log data to the logs topic
 *   - Computing the output partition for a given transform's outgoing logs.
 */
class client {
public:
    client() = default;
    client(const client&) = delete;
    client& operator=(const client&) = delete;
    client(client&&) = delete;
    client& operator=(client&&) = delete;
    virtual ~client() = default;

    virtual ss::future<errc> initialize() = 0;

    /*
     * Collect io::json_batches into model::record_batch(es) and publish
     *
     * Implementations should aim to batch outgoing records maximally and
     * respect cluster-wide batch size limits.
     */
    virtual ss::future<errc> write(model::partition_id, io::json_batches) = 0;

    virtual result<model::partition_id, errc>
    compute_output_partition(model::transform_name_view name) = 0;
};
} // namespace transform::logging
