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

#include "cloud_topics/housekeeper/housekeeper.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cluster/partition.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "ssx/work_queue.h"

namespace cloud_topics {

// A manager of many different housekeepers that are running at once.
//
// Also known as the butler :upside_down_smile:
class housekeeper_manager {
public:
    explicit housekeeper_manager(l1::metastore* metastore);
    housekeeper_manager(const housekeeper_manager&) = delete;
    housekeeper_manager(housekeeper_manager&&) = delete;
    housekeeper_manager& operator=(const housekeeper_manager&) = delete;
    housekeeper_manager& operator=(housekeeper_manager&&) = delete;
    ~housekeeper_manager() = default;

    // Start a housekeeper for this partition.
    void start_housekeeper(
      model::topic_id_partition, ss::lw_shared_ptr<cluster::partition>);

    // Stop a housekeeper for this partition.
    void stop_housekeeper(model::topic_id_partition);

    // Start the housekeeper manager.
    ss::future<> start();

    // Stop the manager as well as all it's housekeepers.
    ss::future<> stop();

    // The state for a housekeeper manager.
    struct state {
        ss::lw_shared_ptr<cluster::partition> partition;
        std::unique_ptr<housekeeper> housekeeper;
    };

private:
    l1::metastore* _l1_metastore;
    chunked_hash_map<model::topic_id_partition, state> _state;
    std::unique_ptr<housekeeper::l0_metadata_storage> _l0_metastore;
    std::unique_ptr<housekeeper::retention_configuration>
      _retention_configuration;
    ssx::work_queue _queue;
};

} // namespace cloud_topics
