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

#include "absl/container/node_hash_map.h"
#include "base/seastarx.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/reconciler/reconciliation_consumer.h"
#include "cluster/notification.h"
#include "cluster/partition.h"
#include "cluster/partition_manager.h"
#include "container/chunked_vector.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <memory>
#include <optional>

namespace cloud_topics {
class data_plane_api;
class frontend;
} // namespace cloud_topics

namespace cloud_topics::reconciler {

/*
 * The reconciler runs on every shard. It queries the leader of cloud topic
 * partitions for new kafka batches. These batches are packaged up into L1
 * objects and uploaded into the cloud. Finally, overlay batches are committed
 * into each partition represented within an uploaded L1 object.
 */
class reconciler {
public:
    reconciler(
      cluster::partition_manager*,
      data_plane_api*,
      l1::io*,
      cluster::metadata_cache*);

    reconciler(const reconciler&) = delete;
    reconciler& operator=(const reconciler&) = delete;
    reconciler(reconciler&&) noexcept = delete;
    reconciler& operator=(reconciler&&) noexcept = delete;
    ~reconciler() = default;

    ss::future<> start();
    ss::future<> stop();

private:
    /*
     * An attached partition is a partition that the reconciler is tracking and
     * periodically processing. Partitions are attached/detached via upcalls
     * from the cluster module. The reconciler operates on the leaders of
     * partitions with affinity to the local shard.
     */
    struct attached_partition_info {
        explicit attached_partition_info(
          ss::lw_shared_ptr<cluster::partition> p)
          : partition(std::move(p)) {}

        ss::lw_shared_ptr<cluster::partition> partition;

        /*
         * Last reconciled offset. this forms the starting offset when querying
         * the partition for new data. In later versions of the system this will
         * be stored in and queried from the partition itself.
         * TODO: Rename this, and set it using the L0 LRO and the L1 metastore.
         */
        kafka::offset lro;
    };

    using attached_partition = ss::lw_shared_ptr<attached_partition_info>;

    absl::node_hash_map<model::ntp, attached_partition> _partitions;

    void attach_partition(ss::lw_shared_ptr<cluster::partition>);
    void detach_partition(const model::ntp&);

    cluster::notification_id_type _manage_notify_handle;
    cluster::notification_id_type _unmanage_notify_handle;

private:
    static constexpr size_t max_object_size = 64_MiB;

    /*
     * Metadata about a partition in an L1 object, used for committing.
     * TODO: Update to commit using the L1 metastore.
     */
    struct partition_commit_info {
        attached_partition partition;
        partition_metadata metadata;
    };

    /*
     * An L1 object built using object_builder with associated partition
     * metadata.
     */
    struct built_object {
        l1::object_builder::object_info object_info;
        chunked_vector<partition_commit_info> partitions;
    };

    // Top-level background worker that drives reconciliation.
    ss::future<> reconciliation_loop();
    ssx::semaphore _control_sem{0, "reconciler::semaphore"};

    /*
     * One round of reconciliation in which data from one or more partitions may
     * be reconciled into an L1 object. Operates on the set of currently
     * attached partitions.
     */
    ss::future<> reconcile();

    /*
     * Reconciliation is a three step process. First, an L1 object is built,
     * then it is uploaded to cloud storage, and finally it is committed.
     * TODO: This process occurs for each domain, once using the metastore.
     */
    ss::future<std::optional<built_object>>
    build_object(l1::object_builder*, l1::staging_file*);
    ss::future<>
    commit_object(const l1::object_id&, const partition_commit_info&);

    /*
     * Build a partition reader that returns batches to be reconciled. Reading
     * will start from the last reconcilied offset. If there is no data that
     * needs to be reconciled then an empty reader is returned.
     */
    ss::future<model::record_batch_reader>
    make_reader(frontend*, kafka::offset start_offset, size_t);

    /*
     * Convert an ntp to a topic_id_partition using the metadata cache.
     * Returns nullopt if the topic doesn't exist or doesn't have a topic_id.
     */
    std::optional<model::topic_id_partition>
    ntp_to_topic_id_partition(const model::ntp& ntp) const;

private:
    cluster::partition_manager* _partition_manager;
    data_plane_api* _data_plane;
    l1::io* _l1_io;
    cluster::metadata_cache* _metadata_cache;
    ss::gate _gate;
    ss::abort_source _as;
};

} // namespace cloud_topics::reconciler
