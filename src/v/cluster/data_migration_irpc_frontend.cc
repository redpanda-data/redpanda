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
#include "cluster/data_migration_irpc_frontend.h"

#include "cluster/data_migration_backend.h"
#include "features/feature_table.h"

#include <seastar/core/shard_id.hh>

namespace cluster::data_migrations {

irpc_frontend::irpc_frontend(
  ss::sharded<features::feature_table>& features,
  ssx::single_sharded<backend>& backend)
  : _features(features)
  , _backend(backend) {}

ss::future<check_ntp_states_reply>
irpc_frontend::check_ntp_states(check_ntp_states_request&& req) {
    if (!_features.local().is_active(features::feature::data_migrations)) {
        return ssx::now<check_ntp_states_reply>({});
    }

    return _backend.invoke_on_instance(
      &backend::check_ntp_states_locally, std::move(req));
}

} // namespace cluster::data_migrations
