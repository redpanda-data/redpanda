/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/configuration.h"
#include "coproc/sys_refs.h"
#include "random/simple_time_jitter.h"
#include "rpc/reconnect_transport.h"
#include "ssx/semaphore.h"
#include "utils/mutex.h"

#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>

#include <chrono>

using namespace std::chrono_literals;

namespace coproc {

/// One instance of this struct exists per shard (held by the pacemaker) and a
/// reference will be passed to all script_contexts on that shard. The data
/// members represent resources that will be shared across all script_contexts.
struct shared_script_resources {
    /// So that there is some variability between the sleep values across all
    /// scripts during their fibers call to ss::sleep_abortable
    simple_time_jitter<ss::lowres_clock> jitter{1s};

    /// Max amount of requests allowed to concurrently hold data in memory
    ssx::semaphore read_sem{
      config::shard_local_cfg().coproc_max_ingest_bytes.value(), "coproc/ssr"};

    /// Underlying transport connection to the wasm engine
    rpc::reconnect_transport transport;

    /// A mutex per materialized log is required as concurrency is not
    /// guaranteed across script contexts. Two scripts writing to the same
    /// underlying log must not have portions of the writes be ordered within
    /// the seastar reactor
    /// NOTE: Pointer stability is explicity requested due to the way iterators
    /// to elements within the collection are used
    absl::node_hash_map<model::ntp, mutex> log_mtx;

    /// Set partitions that are in the middle of being deleted
    absl::node_hash_set<model::ntp> in_progress_deletes;

    /// References to other redpanda components
    sys_refs& rs;

    shared_script_resources(rpc::reconnect_transport t, sys_refs& rs)
      : transport(std::move(t))
      , rs(rs) {}
};

} // namespace coproc
