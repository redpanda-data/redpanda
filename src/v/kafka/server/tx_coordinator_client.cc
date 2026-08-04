// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/tx_coordinator_client.h"

#include "cluster/tx_gateway_frontend.h"
#include "config/configuration.h"

namespace kafka {

routed_tx_coordinator_client::routed_tx_coordinator_client(
  ss::sharded<cluster::tx_gateway_frontend>& tx_frontend)
  : _tx_frontend(tx_frontend) {}

ss::future<cluster::try_abort_reply> routed_tx_coordinator_client::try_abort(
  model::partition_id coordinator_partition,
  model::producer_identity pid,
  model::tx_seq tx_seq) {
    return _tx_frontend.local().route_globally(
      cluster::try_abort_request(
        coordinator_partition,
        pid,
        tx_seq,
        config::shard_local_cfg().internal_rpc_request_timeout_ms.value()));
}

} // namespace kafka
