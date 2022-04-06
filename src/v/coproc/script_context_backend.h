/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "coproc/script_context_router.h"
#include "coproc/types.h"
#include "utils/mutex.h"

#include <seastar/core/future.hh>

#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>

namespace coproc {
/// The outputs from a 'process_batch' call to a wasm engine
using output_write_inputs = std::vector<process_batch_reply::data>;

/// Arugments to pass to 'write_materialized', trivially copyable
struct output_write_args {
    coproc::script_id id;
    ss::sharded<cluster::metadata_cache>& metadata;
    ss::sharded<cluster::non_replicable_topics_frontend>& frontend;
    ss::sharded<coproc::partition_manager>& pm;
    routes_t& inputs;
    absl::node_hash_set<model::ntp>& denylist;
    absl::node_hash_map<model::ntp, mutex>& locks;
};

/**
 * Process the wasm engines response may produce side effects within storage
 *
 * The output of a wasm engine may be one of three types:
 * 1. Filter - an ack without an associated transform, bump offset & move on.
 * 2. Normal - create/write transform to a set of materialized logs.
 * 3. Error - Re-try or deregister script depending on error + policy.
 *
 * @params inputs - Output from wasm engine
 * @params args - associated coproc state needed
 */
ss::future<> write_materialized(output_write_inputs, output_write_args);
} // namespace coproc
