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
    static constexpr auto start_offset_synch_interval = std::chrono::seconds{
      30};
    ss::future<> do_start_replicator(::model::ntp, ::model::term_id);
    ss::future<>
      do_stop_replicator(::model::ntp, std::optional<::model::term_id>);
    bool has_pending_actions();
    ss::future<> reconcile();

    void run_start_actions();
    void run_stop_actions();

private:
    ss::scheduling_group _sg;
    std::unique_ptr<link_configuration_provider> _config_provider;
    std::unique_ptr<data_source_factory> _source_factory;
    std::unique_ptr<data_sink_factory> _sink_factory;
    ss::future<> maybe_sync_start_offsets();
    ssx::work_queue _queue;
    chunked_hash_map<::model::ntp, ::model::term_id> _pending_starts;
    chunked_hash_map<::model::ntp, std::optional<::model::term_id>>
      _pending_stops;
    ss::condition_variable _pending_changes_cv;
    chunked_hash_map<::model::ntp, std::unique_ptr<partition_replicator>>
      _replicators;
    std::optional<replication_probe::configuration> _cfg_probe;
    link_data_probe_ptr _link_data_probe;
    ss::gate _gate;
    ss::abort_source _as;
};

} // namespace cluster_link::replication
