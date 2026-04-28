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

#include "cluster/fwd.h"
#include "kafka/data/partition_proxy_source.h"

namespace kafka {

/// Production partition_proxy_source implementation backed by a
/// cluster::partition_manager. all_ktps() iterates the partition_manager's
/// partition map; get() forwards to make_partition_proxy.
class partition_manager_proxy_source final : public partition_proxy_source {
public:
    explicit partition_manager_proxy_source(
      cluster::partition_manager& pm) noexcept;

    chunked_vector<model::ktp> all_ktps() const override;
    std::optional<partition_proxy> get(const model::ktp&) override;

private:
    cluster::partition_manager& _pm;
};

} // namespace kafka
