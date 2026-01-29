/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster_link/replication/partition_replicator.h"
#include "cluster_link/replication/replication_probe.h"
#include "cluster_link/replication/types.h"
#include "container/chunked_hash_map.h"
#include "ssx/work_queue.h"

namespace cluster_link::replication {

/**
 * Link replication manager is responsible for managing the lifecycle of
 * partition replicators for a given cluster link. One instance per cluster link
 * and shard.
 */
class link_replication_manager {
public:
    explicit link_replication_manager(
      ss::scheduling_group,
      std::unique_ptr<link_configuration_provider> config_provider,
      std::unique_ptr<data_source_factory> source_factory,
      std::unique_ptr<data_sink_factory> sink_factory,
      std::optional<replication_probe::configuration> cfg_probe = std::nullopt);

    ss::future<> start(link_data_probe_ptr data_probe = nullptr);

    ss::future<> stop();

    void start_replicator(::model::ntp, ::model::term_id);
    // term is optional because a replica being unmanaged out of the shard
    // can no longer has a term that we can access.
    void stop_replicator(::model::ntp, std::optional<::model::term_id>);

    // Stop all replicators for a given topic, or all replicators if no topic
    // is specified.
    void stop_replicators(std::optional<::model::topic> topic = std::nullopt);

    chunked_hash_map<::model::ntp, partition_offsets_report>
    get_partition_offsets_report() const;

    std::optional<partition_offsets_report>
    get_partition_offsets_report(const ::model::ntp&) const;

    void set_data_probe(link_data_probe_ptr);
    void unset_data_probe();

private:
    ss::future<> do_start_replicator(::model::ntp, ::model::term_id);
    ss::future<>
      do_stop_replicator(::model::ntp, std::optional<::model::term_id>);
    bool can_reconcile_now();
    ss::future<> reconcile();
    ss::future<> reconcile_ntp_once(::model::ntp ntp, ssx::semaphore_units);

    // Allowed operations on a replicator
    enum class op_type : uint8_t { start, stop };
    friend std::ostream& operator<<(std::ostream& os, op_type);
    struct ntp_target_state {
        op_type op;
        std::optional<::model::term_id> term;
        fmt::iterator format_to(fmt::iterator) const;
        // merges new_desired into this. Returns true if successful, false if
        // new_desired is stale and rejected.
        bool merge(const ::model::ntp&, ntp_target_state new_desired);
        bool operator==(const ntp_target_state& other) const = default;
    };

    struct ntp_reconciliation_state {
        //  Only set when a reconciliation is in progress and is set
        // to the state being reconciled to.
        std::optional<ntp_target_state> in_progress;
        // Desired state to reconcile to. This is updated when new requests
        // appear. If there is a desired state already, we attempt to merge the
        // new desired state into the existing one if possible.
        std::optional<ntp_target_state> desired;

        // Sets the desired state. Returns true if successful, false if the
        // new desired state is stale and rejected.
        bool
        set_desired(const ::model::ntp& ntp, ntp_target_state new_desired) {
            if (desired) {
                return desired->merge(ntp, new_desired);
            }
            desired = new_desired;
            return true;
        }

        // Returns true if reconciliation is needed.
        bool needs_reconciliation() const { return desired && !in_progress; }
        bool reconciliation_complete() const {
            return !desired && !in_progress;
        }
    };

    // List of pending reconciliations. Each ntp maintains a desired target
    // state to reconcile to, and an in-progress state if a reconciliation is
    // ongoing. Multiple desired state updates can be merged while a
    // reconciliation is in progress.
    chunked_hash_map<::model::ntp, ntp_reconciliation_state> _pending;
    ss::condition_variable _pending_cv;
    ssx::semaphore _max_reconciliations{32, "link-replicator-mgr"};

    ss::scheduling_group _sg;
    std::unique_ptr<link_configuration_provider> _config_provider;
    std::unique_ptr<data_source_factory> _source_factory;
    std::unique_ptr<data_sink_factory> _sink_factory;
    chunked_hash_map<::model::ntp, std::unique_ptr<partition_replicator>>
      _replicators;
    std::optional<replication_probe::configuration> _cfg_probe;
    link_data_probe_ptr _link_data_probe;
    ss::gate _gate;
    ss::abort_source _as;
};

} // namespace cluster_link::replication
