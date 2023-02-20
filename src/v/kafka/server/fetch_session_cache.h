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

#include "kafka/server/fetch_session.h"
#include "kafka/types.h"
#include "units.h"

#include <seastar/core/metrics_registration.hh>
#include <seastar/core/smp.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>

namespace kafka {

struct fetch_request;
/**
 * Fetch session cache is a core local cache holding incremental fetch
 * sessions. Each core local cache instane assigns session ids that are unique
 * for the node (non overlapping ranges of ids are assigned to each core).
 *
 * The cache evicts not used sessions after configurable period of inactivity.
 * Fetch session cache will stop adding new sessions after its max memory usage
 * is reached.
 **/
class fetch_session_cache {
public:
    explicit fetch_session_cache(std::chrono::milliseconds);
    fetch_session_ctx maybe_get_session(const fetch_request& req);
    size_t size() const { return _sessions.size(); }

private:
    using underlying_t
      = absl::flat_hash_map<fetch_session_id, fetch_session_ptr>;

    static constexpr size_t max_mem_usage = 10_MiB;

    // used to split range of possible session ids to limit memory size we use
    // max_mem_used, this is theoretical limit, the actual number of session
    // held in a cache on single core is limitted by the memory usage.
    size_t max_sessions_per_core() {
        static const size_t v
          = std::numeric_limits<fetch_session_id::type>::max() / ss::smp::count;
        return v;
    }

    std::optional<fetch_session_id> new_session_id();
    void gc_sessions();

    size_t mem_usage() const {
        using debug = absl::container_internal::hashtable_debug_internal::
          HashtableDebugAccess<underlying_t>;
        return debug::AllocatedByteSize(_sessions) + _sessions_mem_usage;
    }

    void register_metrics();

    underlying_t _sessions;
    const fetch_session_id _min_session_id;
    const fetch_session_id _max_session_id;
    fetch_session_id _last_session_id;
    ss::timer<> _session_eviction_timer;
    // min time that will elapse since the session was last used before it is
    // going to be evicted
    std::chrono::milliseconds _session_eviction_duration;

    size_t _sessions_mem_usage = 0;

    ss::metrics::metric_groups _metrics;
};

} // namespace kafka
