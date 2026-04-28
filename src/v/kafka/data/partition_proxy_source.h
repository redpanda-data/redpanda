/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "container/chunked_vector.h"
#include "kafka/data/partition_proxy.h"
#include "model/ktp.h"

#include <optional>

namespace kafka {

/// Abstraction over how a kafka handler obtains partition_proxies for the
/// partitions on the local shard. The production implementation is backed
/// by cluster::partition_manager (see partition_manager_proxy_source);
/// tests can supply a fake that returns pre-constructed proxies without
/// needing a real partition_manager.
class partition_proxy_source {
public:
    partition_proxy_source() = default;
    partition_proxy_source(const partition_proxy_source&) = delete;
    partition_proxy_source& operator=(const partition_proxy_source&) = delete;
    partition_proxy_source(partition_proxy_source&&) = delete;
    partition_proxy_source& operator=(partition_proxy_source&&) = delete;
    virtual ~partition_proxy_source() = default;

    /// Enumerate ktps for all partitions known to this source.
    virtual chunked_vector<model::ktp> all_ktps() const = 0;

    /// Look up a partition_proxy for the given ktp; returns nullopt if the
    /// partition is not present on this source.
    virtual std::optional<partition_proxy> get(const model::ktp&) = 0;
};

} // namespace kafka
