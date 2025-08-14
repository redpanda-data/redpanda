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
      std::unique_ptr<data_source_factory> source_factory,
      std::unique_ptr<data_sink_factory> sink_factory);

    ss::future<> start();

    ss::future<> stop();

    void start_replicator(model::ntp, model::term_id);
    // term is optional because a replica being unmanaged out of the shard
    // can no longer has a term that we can access.
    void stop_replicator(model::ntp, std::optional<model::term_id>);

private:
    ss::future<> do_start_replicator(model::ntp, model::term_id);
    ss::future<> do_stop_replicator(model::ntp, std::optional<model::term_id>);
    ss::scheduling_group _sg;
    std::unique_ptr<data_source_factory> _source_factory;
    std::unique_ptr<data_sink_factory> _sink_factory;
    ssx::work_queue _queue;
    chunked_hash_map<model::ntp, std::unique_ptr<partition_replicator>>
      _replicators;
    ss::gate _gate;
};

} // namespace cluster_link::replication
