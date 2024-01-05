// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cluster/bootstrap_types.h"
#include "cluster/cluster_bootstrap_service.h"
#include "storage/fwd.h"

#include <seastar/core/sharded.hh>

namespace cluster {

// RPC service used to determine cluster info when bootstrapping a cluster
// or to discover the existence of an existing cluster.
class bootstrap_service : public cluster_bootstrap_service {
public:
    bootstrap_service(
      ss::scheduling_group sg,
      ss::smp_service_group ssg,
      ss::sharded<storage::api>& storage)
      : cluster_bootstrap_service(sg, ssg)
      , _storage(storage) {}

    ss::future<cluster_bootstrap_info_reply> cluster_bootstrap_info(
      cluster_bootstrap_info_request, rpc::streaming_context&) override;

private:
    ss::sharded<storage::api>& _storage;
};

} // namespace cluster
