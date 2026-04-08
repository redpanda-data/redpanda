/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "ssx/work_queue.h"

#include <seastar/core/sharded.hh>

#include <memory>

namespace cloud_io {
class remote;
} // namespace cloud_io

namespace cluster {
class partition;
} // namespace cluster

namespace cloud_topics {

// Manages topic manifest uploads for cloud topics partitions.
// Starts an upload loop when a partition becomes leader, stops when leadership
// is lost, and re-uploads when properties change.
class topic_manifest_upload_manager {
public:
    topic_manifest_upload_manager(
      ss::sharded<cloud_io::remote>& remote,
      cloud_storage_clients::bucket_name bucket);
    ~topic_manifest_upload_manager();

    topic_manifest_upload_manager(const topic_manifest_upload_manager&)
      = delete;
    topic_manifest_upload_manager&
    operator=(const topic_manifest_upload_manager&) = delete;
    topic_manifest_upload_manager(topic_manifest_upload_manager&&) = delete;
    topic_manifest_upload_manager&
    operator=(topic_manifest_upload_manager&&) = delete;

    // Called on leadership change or properties change.
    // If partition is non-null, starts an upload loop or signals re-upload if
    // one is already running. Otherwise, stops any existing loop.
    void on_leadership_or_properties_change(
      model::topic_id_partition tidp,
      ss::optimized_optional<ss::lw_shared_ptr<cluster::partition>> partition);
    ss::future<> reset_or_signal_loop(
      model::topic_id_partition tidp,
      ss::optimized_optional<ss::lw_shared_ptr<cluster::partition>> partition);

    ss::future<> start();
    ss::future<> stop();

private:
    class loop;

    ss::sharded<cloud_io::remote>& _remote;
    cloud_storage_clients::bucket_name _bucket;
    chunked_hash_map<model::topic_id_partition, std::unique_ptr<loop>> _loops;
    ssx::work_queue _queue;
};

} // namespace cloud_topics
