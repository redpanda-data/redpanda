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

#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/topics_frontend.h"
#include "config/configuration.h"
#include "coproc/exception.h"
#include "coproc/offset_storage_utils.h"
#include "coproc/script_context.h"
#include "coproc/shared_script_resources.h"
#include "coproc/sys_refs.h"
#include "coproc/types.h"
#include "rpc/reconnect_transport.h"
#include "storage/fwd.h"
#include "storage/snapshot.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>

#include <absl/container/node_hash_map.h>

namespace coproc {

/**
 * Currently only thrown by 'wait_for_script' when attempting to lookup a script
 * with an id that doesn't exist.
 */
class script_not_found final : public exception {
    using exception::exception;
};

/**
 * Thrown when there are promises queued waiting on coprocessors to be created,
 * but shutdown is called before that occurs
 */
class wait_on_script_future_stranded final : public exception {
    using exception::exception;
};

/**
 * Sharded service that manages the registration of scripts to registered
 * topics. Each script is its own 'fiber', containing its own event loop which
 * reads data from its interested topics, sends to the wasm engine, and
 * processes the response (almost always a write to a materialized log)
 */
class pacemaker final : public ss::peering_sharded_service<pacemaker> {
public:
    /**
     * class constructor
     * @param address of coprocessor engine
     * @param reference to the storage layer
     */
    pacemaker(unresolved_address, sys_refs&);

    /**
     * Begins the offset tracking fiber
     */
    ss::future<> start();

    /**
     * Stops and removes running scripts but keeps their state (offsets)
     */
    ss::future<> reset();

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
     * Removes all script_ids from the pacemaker.
     */
    ss::future<absl::btree_map<script_id, errc>> remove_all_sources();

    /**
     * Returns a future which resolves when script id is started
     */
    ss::future<errc> wait_for_script(script_id);

    /**
     * @returns true if a matching script id exists on 'this' shard
     */
    bool local_script_id_exists(script_id);

    /// returns the number of running / registered fibers
    size_t n_registered_scripts() const { return _scripts.size(); }

    /// returns a handle to the reconnect transport
    shared_script_resources& resources() { return _shared_res; }

    /// Calls the function while materialized topic is on the denylist
    ///
    /// Its not enough to add the topic to the denylist, it must also be
    /// checked that all fibers are respecting this, i.e. don't have in-progress
    /// writes to these topics. After the function has been invoked the topic
    /// will be removed from the blacklist.
    template<typename Fn>
    ss::future<>
    with_hold(const model::ntp& source, const model::ntp& materialized, Fn fn) {
        _shared_res.in_progress_deletes.emplace(materialized);
        std::vector<ss::future<>> fs;
        for (auto& [_, script] : _scripts) {
            fs.emplace_back(script->remove_output(source, materialized));
        }
        /// When this future completes, it can safely be assumed that all fibers
        /// are respecting the new addition to the blacklist
        co_await ss::when_all_succeed(fs.begin(), fs.end());
        co_await fn();
        _shared_res.log_mtx.erase(materialized);
        /// Releases the barrier, if any fibers wish to recreate the topic its
        /// safe to do so now
        _shared_res.in_progress_deletes.erase(materialized);
    }

private:
    void do_add_source(
      script_id,
      routes_t&,
      std::vector<errc>& acks,
      const std::vector<topic_namespace_policy>&);

    void fire_updates(script_id, errc);

    void save_routes();

    struct offset_flush_fiber_state {
        ss::timer<ss::lowres_clock> timer;
        model::timeout_clock::duration duration;
        storage::simple_snapshot_manager snap;

        static ss::sstring snapshot_filename() {
            return fmt::format(
              "{}-{}",
              storage::simple_snapshot_manager::default_snapshot_filename,
              ss::this_shard_id());
        }

        offset_flush_fiber_state()
          : duration(
            config::shard_local_cfg().coproc_offset_flush_interval_ms())
          , snap(
              offsets_snapshot_path(),
              snapshot_filename(),
              ss::default_priority_class()) {}
    };

private:
    /// Alerting mechanism for script startup
    absl::node_hash_map<script_id, std::vector<ss::promise<errc>>> _updates;

    /// Cached offsets read from disk on startup
    absl::flat_hash_map<script_id, routes_t> _cached_routes;

    /// Data to be referenced by script_contexts on the current shard
    shared_script_resources _shared_res;

    /// Main datastructure containing all active script_contexts
    absl::node_hash_map<script_id, std::unique_ptr<script_context>> _scripts;

    /// Responsible for timed persistence of offsets to disk
    offset_flush_fiber_state _offs;

    /// All async actions pacemaker starts are within the context of this gate
    ss::gate _gate;
};

} // namespace coproc
