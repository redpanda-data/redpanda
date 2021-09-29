/*
 * Copyright 2021 Vectorized, Inc.
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
#include "coproc/fwd.h"
#include "seastarx.h"
#include "storage/fwd.h"

#include <seastar/core/sharded.hh>

namespace coproc {

/// Struct of references of external layers of redpanda that coproc will
/// leverage
struct sys_refs {
    ss::sharded<storage::api>& storage;
    ss::sharded<materialized_topics_frontend>& mt_frontend;
    ss::sharded<cluster::topics_frontend>& topics_frontend;
    ss::sharded<cluster::metadata_cache>& metadata_cache;
    ss::sharded<cluster::partition_manager>& partition_manager;
};

} // namespace coproc
