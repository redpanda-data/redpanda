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

#include "cloud_topics/read_replica/metadata_provider.h"
#include "cloud_topics/read_replica/partition_metadata.h"
#include "cluster/utils/partition_change_notifier.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "ssx/work_queue.h"

#include <seastar/core/sharded.hh>

namespace cloud_io {
class remote;
} // namespace cloud_io

namespace cloud_topics::l1 {
class manifest_io;
} // namespace cloud_topics::l1

namespace cluster {
class partition;
class partition_manager;
class topic_table;
} // namespace cluster

namespace cloud_topics::read_replica {

class state_refresh_loop;
class snapshot_manager;

// Manages partition metadata, listening for manage and unmanage commands for
// partitions of read replica cloud topics.
class metadata_manager : public metadata_provider {
public:
    metadata_manager(
      std::unique_ptr<cluster::partition_change_notifier> notifier,
      ss::sharded<cluster::partition_manager>& partition_mgr,
      snapshot_manager* snapshot_mgr,
      ss::sharded<cloud_io::remote>& remote,
      ss::sharded<cluster::topic_table>& topics);

    metadata_manager(const metadata_manager&) = delete;
    metadata_manager& operator=(const metadata_manager&) = delete;
    metadata_manager(metadata_manager&&) = delete;
    metadata_manager& operator=(metadata_manager&&) = delete;

    // Needs to be defined in .cc file where state_refresh_loop is complete
    ~metadata_manager();

    // Start the metadata manager and begin receiving notifications.
    // Should be called after all dependencies are ready.
    void start();

    // Stop the metadata manager and unregister notifications.
    ss::future<> stop() override;

    // Implement metadata_provider interface
    std::optional<partition_metadata>
    get_metadata(const model::ntp&) const override;

private:
    void on_partition_notification(
      cluster::partition_change_notifier::notification_type type,
      const model::ntp& ntp,
      std::optional<cluster::partition_change_notifier::partition_state> state);

    void enqueue_refresh_loop(
      ss::lw_shared_ptr<cluster::partition> partition,
      model::term_id term,
      model::topic_id topic_id,
      cloud_storage::remote_label);
    void enqueue_refresh_loop_stop(const model::ntp&);

    // Reset refresh loop for a partition (start/stop based on leadership)
    void start_refresh_loop(
      ss::lw_shared_ptr<cluster::partition> partition,
      model::term_id term,
      model::topic_id topic_id,
      cloud_storage::remote_label);
    ss::future<> stop_refresh_loop(model::ntp ntp);

    void on_leadership_change(
      const model::ntp&,
      const model::topic_id& tidp,
      const cloud_storage::remote_label&,
      std::optional<model::term_id>);
    void on_partition_change(
      const model::ntp&, const model::topic_id& tidp, bool has_partition);

    // This gets notified when there is a read replica cloud topic partition on
    // this shard.
    std::unique_ptr<cluster::partition_change_notifier> notifier_;
    std::optional<cluster::notification_id_type> notification_;

    ss::sharded<cluster::partition_manager>& partition_mgr_;
    ss::sharded<cluster::topic_table>& topic_table_;

    // Tracks active state refresh loops (leader-only)
    chunked_hash_map<model::ntp, std::unique_ptr<state_refresh_loop>>
      refresh_loops_;

    // Work queue to serialize refresh loop operations
    ssx::work_queue queue_;

    // Dependencies for creating refresh loops
    snapshot_manager* snapshot_mgr_;
    ss::sharded<cloud_io::remote>& remote_;

    chunked_hash_map<model::ntp, partition_metadata> metadata_;
};

} // namespace cloud_topics::read_replica
