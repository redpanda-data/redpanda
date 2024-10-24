/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "features/fwd.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "storage/api.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>

#include <optional>

namespace security {
class credential_store;
}

namespace cluster {

/**
 * This class applies the cluster initialization message to
 * - storage, persisting the cluster UUID to the kvstore,
 * - credential_store, to initialize the bootstrap user
 * - members_manager, to initialize node UUID map
 * - TODO: apply the initial licence
 */
class bootstrap_backend final {
public:
    bootstrap_backend(
      ss::sharded<security::credential_store>&,
      ss::sharded<storage::api>&,
      ss::sharded<members_manager>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<feature_backend>&,
      ss::sharded<cluster_recovery_table>&);

    ss::future<std::error_code> apply_update(model::record_batch);

    bool is_batch_applicable(const model::record_batch& b) {
        return b.header().type
               == model::record_batch_type::cluster_bootstrap_cmd;
    }

    ss::future<> fill_snapshot(controller_snapshot&) const;
    ss::future<> apply_snapshot(model::offset, const controller_snapshot&);

private:
    ss::future<std::error_code> apply(bootstrap_cluster_cmd, model::offset);
    ss::future<> apply_cluster_uuid(model::cluster_uuid);

    ss::sharded<security::credential_store>& _credentials;
    ss::sharded<storage::api>& _storage;
    ss::sharded<members_manager>& _members_manager;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<feature_backend>& _feature_backend;
    ss::sharded<cluster_recovery_table>& _cluster_recovery_table;
    std::optional<model::cluster_uuid> _cluster_uuid_applied;
};

} // namespace cluster
