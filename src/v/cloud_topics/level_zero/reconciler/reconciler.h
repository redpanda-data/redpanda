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
#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/level_zero/reconciler/range_batch_consumer.h"
#include "cluster/notification.h"
#include "cluster/partition.h"
#include "cluster/partition_manager.h"
#include "container/chunked_vector.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <optional>

namespace experimental::cloud_topics {
class data_plane_api;
}

namespace experimental::cloud_topics::reconciler {

/*
 * The reconciler runs on every shard. It queries the leader of cloud topic
 * partitions for new kafka batches. These batches are packaged up into L1
 * objects and uploaded into the cloud. Finally, overlay batches are committed
 * into each partition represented within an uploaded L1 object.
 */
class reconciler {
public:
    reconciler(
      ss::sharded<cluster::partition_manager>*,
      ss::sharded<cloud_io::remote>*,
      data_plane_api*,
      std::optional<cloud_storage_clients::bucket_name> = std::nullopt);

    reconciler(const reconciler&) = delete;
    reconciler& operator=(const reconciler&) = delete;
    reconciler(reconciler&&) noexcept = delete;
    reconciler& operator=(reconciler&&) noexcept = delete;
    ~reconciler() = default;

    ss::future<> start();
    ss::future<> stop();

private:
    /*
     * an attached partition is a partition that the reconciler is tracking and
     * periodically processing. partitions are attached/detatched via upcalls
     * from the cluster module. the reconciler operates on the leaders of
     * partitions with affinity to the local shard.
     */
    struct attached_partition_info {
        explicit attached_partition_info(
          ss::lw_shared_ptr<cluster::partition> p)
          : partition(std::move(p)) {}

        ss::lw_shared_ptr<cluster::partition> partition;

        /*
         * last reconciled offset. this forms the starting offset when querying
         * the partition for new data. In later versions of the system this will
         * be stored in and queried from the partition itself.
         */
        kafka::offset lro;
    };

    using attached_partition = ss::lw_shared_ptr<attached_partition_info>;

    // currently attached partitions
    absl::node_hash_map<model::ntp, attached_partition> _partitions;

    void attach_partition(ss::lw_shared_ptr<cluster::partition>);
    void detach_partition(const model::ntp&);

    cluster::notification_id_type _manage_notify_handle;
    cluster::notification_id_type _unmanage_notify_handle;

private:
    static constexpr size_t max_object_size = 4_MiB;

    /*
     * metadata about a materialized range of batches stored in an L1 object.
     * after an object is created and uploaded, this metadata is used to drive
     * the creation and replication of overlay batches to each partition.
     *
     * partition - the source partition
     * physical extent - position within the object
     * range info - additional metadata (e.g. kafka offset extent)
     */
    struct object_range_info {
        attached_partition partition;
        uint64_t physical_offset_start;
        uint64_t physical_offset_end;
        range_info info;
    };

    /*
     * a staged / materialized L1 object.
     *
     * data - the payload
     * ranges - metadata about each range in the payload
     */
    struct object {
        iobuf data;
        chunked_vector<object_range_info> ranges;

        // add a range from the given partition
        void add(range, const attached_partition&);
    };

    // top-level background worker that drives reconciliation
    ss::future<> reconciliation_loop();
    ssx::semaphore _control_sem{0, "reconciler::semaphore"};

    /*
     * one round of reconciliation in which data from one or more partitions may
     * be reconciled into an L1 object. operates on the set of currently
     * attached partitions.
     */
    ss::future<> reconcile();

    /*
     * reconciliation is a three step process. first an L1 object is built, then
     * it is uploaded to cloud storage, and finally its committed.
     */
    ss::future<std::optional<object>> build_object();
    ss::future<cloud_io::upload_result>
      upload_object(cloud_storage_clients::object_key, iobuf);
    ss::future<> commit_object(
      const cloud_storage_clients::object_key&, const object_range_info&);

    /*
     * build a partition reader that returns batches to be reconciled. reading
     * will start from the last reconcilied offset. if there is no data that
     * needs to be reconciled then an empty reader is returned.
     */
    ss::future<model::record_batch_reader>
    make_reader(const attached_partition&, size_t);

private:
    ss::sharded<cluster::partition_manager>* _partition_manager;
    ss::sharded<cloud_io::remote>* _cloud_io;
    data_plane_api* _data_plane;
    cloud_storage_clients::bucket_name _bucket;
    ss::gate _gate;
    ss::abort_source _as;
};

} // namespace experimental::cloud_topics::reconciler
