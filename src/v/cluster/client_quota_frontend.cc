// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/client_quota_frontend.h"

#include "cluster/cluster_utils.h"

namespace cluster::client_quota {

ss::future<cluster::errc> frontend::alter_quotas(
  alter_delta_cmd_data data, model::timeout_clock::time_point tout) {
    alter_quotas_delta_cmd cmd(0 /*unused*/, std::move(data));
    return replicate_and_wait(_stm, _as, std::move(cmd), tout)
      .then(map_update_interruption_error_code);
}

} // namespace cluster::client_quota
