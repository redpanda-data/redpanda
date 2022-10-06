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

#include "cluster/fwd.h"
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
 * This class applies the cluster intitalization message to
 * - itself, storing the cluster UUID value, also duplicating it to the kvstore
 * - credintial_store, to initialize the bootstrap user
 * - TODO: apply the initial licence
 */
class bootstrap_backend final {
public:
    bootstrap_backend(ss::sharded<storage::api>&);

    ss::future<std::error_code> apply_update(model::record_batch);

    bool is_batch_applicable(const model::record_batch& b) {
        return b.header().type
               == model::record_batch_type::cluster_bootstrap_cmd;
    }

private:
    ss::sharded<storage::api>& _storage;
};

} // namespace cluster
