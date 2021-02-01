/*
 * Copyright 2020 Vectorized, Inc.
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
#include "coproc/ntp_context.h"
#include "coproc/offset_storage_utils.h"
#include "coproc/script_context.h"
#include "coproc/types.h"
#include "rpc/reconnect_transport.h"
#include "storage/api.h"
#include "storage/snapshot.h"

#include <seastar/core/sharded.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>

#include <absl/container/node_hash_map.h>

namespace coproc {
/**
 * Sharded service that manages the registration of scripts to registered
 * topics. Each script is its own 'fiber', containing its own event loop which
 * reads data from its interested topics, sends to the wasm engine, and
 * processes the response (almost always a write to a materialized log)
 */
class pacemaker {
public:
    /**
     * class constructor
     * @param address of coprocessor engine
     * @param reference to the storage layer
     */
    pacemaker(ss::socket_address, ss::sharded<storage::api>&);

    /**
     * Begins the offset tracking fiber
     */
    ss::future<> start();

    /**
     * Gracefully stops and deregisters all coproc scripts
     */
    ss::future<> stop();

    /**
     * Registers a new coproc script into the system
     *
     * @param script_id You must verify that this is unique by querying all
     shards. A shard only contains an entry for a script id if there is at least
     one partition of a topic for that shard.
     * @param topics vector of pairs of topics and their registration policies
     * @returns a vector of error codes per input topic corresponding to the
     * order of the topic vector passed to this method
     */
    std::vector<errc>
      add_source(script_id, std::vector<topic_namespace_policy>);

    /**
     * Removes a script_id from the pacemaker. Shuts down relevent fibers and
     * deregisters ntps that no longer have any registered interested scripts
     *
     * @param script_id The identifer of the script desired to be shutdown
     */
    ss::future<errc> remove_source(script_id);

    /**
     * @returns true if a matching script id exists on 'this' shard
     */
    bool local_script_id_exists(script_id);

    /**
     * @returns true if a matching ntp exists on 'this' shard
     */
    bool ntp_is_registered(const model::ntp&);

private:
    void do_add_source(
      script_id,
      ntp_context_cache&,
      std::vector<errc>& acks,
      const std::vector<topic_namespace_policy>&);

    struct offset_flush_fiber_state {
        ss::gate gate;
        ss::timer<ss::lowres_clock> timer;
        model::timeout_clock::duration duration;
        storage::snapshot_manager snap;

        offset_flush_fiber_state()
          : duration(
            config::shard_local_cfg().coproc_offset_flush_interval_ms())
          , snap(
              offsets_snapshot_path(),
              storage::snapshot_manager::default_snapshot_filename,
              ss::default_priority_class()) {}
    };

private:
    /// Data to be referenced by script_contexts on the current shard
    shared_script_resources _shared_res;

    /// Main datastructure containing all active script_contexts
    absl::node_hash_map<script_id, std::unique_ptr<script_context>> _scripts;

    /// Referencable cache of active ntps
    ntp_context_cache _ntps;

    /// Responsible for timed persistence of offsets to disk
    offset_flush_fiber_state _offs;
};

} // namespace coproc
