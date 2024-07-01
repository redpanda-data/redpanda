// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cluster/client_quota_serde.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "model/timeout_clock.h"

#include <seastar/core/sharded.hh>

namespace cluster::client_quota {

class frontend final {
public:
    frontend(
      ss::sharded<controller_stm>& stm, ss::sharded<ss::abort_source>& as)
      : _stm(stm)
      , _as(as) {}

    /// Applies the given delta to the client quota store
    /// Should be called ONLY on the controller leader node, but may be called
    /// from any shard
    /// Returns errc::success on success or cluster errors on failure
    /// There are no client quota store-specific errors, because the upsert and
    /// remove operations always succeed regardless of the previous state of the
    /// given quota (eg. there is no error for removing non-existent quotas).
    ss::future<cluster::errc>
      alter_quotas(alter_delta_cmd_data, model::timeout_clock::time_point);

private:
    ss::sharded<controller_stm>& _stm;
    ss::sharded<ss::abort_source>& _as;
};

} // namespace cluster::client_quota
