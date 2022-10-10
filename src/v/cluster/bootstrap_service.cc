// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/bootstrap_service.h"

#include "cluster/bootstrap_types.h"

namespace cluster {

ss::future<cluster_bootstrap_info_reply>
bootstrap_service::cluster_bootstrap_info(
  cluster_bootstrap_info_request&&, rpc::streaming_context&) {
    cluster_bootstrap_info_reply r{};
    // TODO: add fields!
    co_return r;
}

} // namespace cluster
