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

#pragma once

#include "base/outcome.h"
#include "transform/logging/errc.h"
#include "transform/logging/io.h"
#include "transform/rpc/client.h"

namespace transform::logging {

/**
 * transform::logging::client implementation for managing and producing record
 * batches to _redpanda.transform_logs.
 *
 * Encapsulates a transform::rpc::client, which does the heavy lifing of
 * implementing an internal RPC interface to the rest of Redpanda, and a
 * cluster::metadata_cache for computing the output partition for a certain
 * named transform's logs.
 *
 * See transform::logging::client for more general information about the
 * interface.
 */
class rpc_client final : public client {
public:
    explicit rpc_client(
      rpc::client* rpc_client, cluster::metadata_cache* metadata_cache);
    rpc_client() = delete;
    rpc_client(const rpc_client&) = delete;
    rpc_client& operator=(const rpc_client&) = delete;
    rpc_client(rpc_client&&) = delete;
    rpc_client& operator=(rpc_client&&) = delete;
    ~rpc_client() override = default;

    ss::future<errc> initialize() override;
    ss::future<errc> write(model::partition_id, io::json_batches) override;
    result<model::partition_id, errc>
      compute_output_partition(model::transform_name_view) override;

private:
    ss::future<errc> create_topic();
    rpc::client* _rpc_client;
    cluster::metadata_cache* _metadata_cache;
};

} // namespace transform::logging
