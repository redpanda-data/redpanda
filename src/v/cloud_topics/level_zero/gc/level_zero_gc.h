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

#include "cloud_io/io_result.h"
#include "cloud_storage_clients/client.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/level_zero/gc/level_zero_gc_probe.h"
#include "cloud_topics/types.h"
#include "container/chunked_hash_map.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <expected>

using namespace std::chrono_literals;

namespace cloud_io {
class remote;
}

namespace cluster {
class controller_stm;
class health_monitor_frontend;
class topic_table;
class members_table;
} // namespace cluster

namespace cloud_topics {

/*
 * Garbage collection for level-zero data objects.
 *
 * Every L0 data object is associated with a global epoch that is assigned at
 * creation time and is stored in the object name as a prefix [0]:
 *
 *    .../level_zero/data/00000-<uuid>
 *    .../level_zero/data/00999-<uuid>
 *    .../level_zero/data/00999-<uuid>
 *    .../level_zero/data/01005-<uuid>
 *
 * The process of L0 garbage collection is a two-step process:
 *
 *    1. Determine the maximum epoch, M, that is safe to collect.
 *    2. Delete objects associated with epochs less than or equal to M.
 *
 * In addition to the epoch requirement (1), additional constraints on
 * collection may be applied. For example, rather than immediately removing L0
 * data objects as they become eligible for collection, delaying removal (e.g.
 * by 1 day) can support recovery in cases involving certain types of accidental
 * deletion.
 *
 * The `level_zero_gc` class implements L0 garbage collection as described
 * above. It uses two service provider interfaces defined in the class. The
 * `epoch_source` interface provides access to the safe-to-delete epoch, and
 * the `object_storage` interface provides access to listing and deleting
 * objects from the configured storage service.
 *
 * Incremental collection
 * ======================
 *
 * Assume that a system has a current safe-to-delete epoch (M) and all objects
 * with epochs less than M have been removed from storage. We say in this case
 * that M has been cleaned. How does the system behave when the safe-to-delete
 * epoch advances to M+N?
 *
 * If the garbage collection process knows that M has been cleaned, then it
 * need only query for objects associated with epochs [M+1, ..., M+N). However,
 * if knowledge of M having been cleaned is not available, then incremental
 * progress cannot be achieved and the collection process will need to consider
 * all epochs [0, M+N). Therefore, incremental collection requires persisting
 * state about the progress made by the collection process.
 *
 * The normal approach to persisting this type of information is as metadata
 * inside an internal Redpanda topic. For example persist a message recording
 * that a particular epoch has been cleaned, and later read these messages
 * before starting the collection.
 *
 * Rather than introducing a new internal topic and additional metadata
 * management, we can exploit a property common to object storage systems [1]
 * that allows us to avoid this explicit management by offloading it to the
 * storage system itself: lexicographically ordered, global object listing API.
 *
 * This ordering property is used by cloud topics to implement a stateless
 * garbage collection process. First, cloud topics arranges for a common prefix
 * of L0 data objects to be named such that their lexicographic ordering
 * corresponds to epoch ordering. This ensures that object listing will return
 * L0 data objects from smaller epochs first. As the collection process removes
 * objects from smaller epochs, these objects will no longer appear in
 * subsequent listings, achieving the same incremental collection optimization.
 *
 * Scalability
 * ===========
 *
 * Level zero GC currently runs as a singleton process in the cluster. This does
 * introduce a scalability limitation as it pertains to GC being able to keep
 * pace with data ingress rates.
 *
 * A node with any non-zero ingress rate will upload at least four L0
 * objects per second. Thus a five node cluster will upload a minimum of about
 * 20 objects per second. In contrast, a cluster with an ingress rate of 4 GB/s
 * using 4 MB L0 data object will upload around 1000 objects per second. AWS S3
 * allows batch deletes of 1000 objects per request. So as we approach
 * supporting 4 GB/s in a cluster L0 GC will need to be able perform around one
 * maximum batch delete request per second.
 *
 * It is expect that we will need to improve scalability at some point. For
 * certain types of bottlenecks, the easiest improvement is to utilize
 * additional cores on a single node, and scaling out to multiple nodes is also
 * possible.
 *
 * Footnotes
 * =========
 *
 * [0]: TODO(noah) insert reference for epoch assignment.
 *
 * [1]: Most object storage systems state explicitly that object listings are
 * sorted in lexicographic order. However, some lesser used systems either (1)
 * explicitly state that this is not the case or (2) have configuration options
 * that allow lexicographic ordering to be disabled. As described above,
 * cloud topics currently assumes that object listings are in lexicographic
 * ordering.
 *
 * When used with a system that produces non-lexicographic orderings, the
 * stateless process will operate correctly, but may be highly inefficient. The
 * GC process will log a warning if it appears such a system is being used.
 * Transitioning to a more general solution that is efficient even for systems
 * that do not support lexicographic order is straight forward by processing
 * epochs starting at 0.
 */
struct level_zero_gc_config {
    config::binding<std::chrono::milliseconds> deletion_grace_period;
    config::binding<std::chrono::milliseconds> throttle_progress;
    config::binding<std::chrono::milliseconds> throttle_no_progress;
};

class level_zero_gc {
public:
    /*
     * Object storage interface used by L0 GC.
     */
    class object_storage {
    public:
        object_storage() = default;
        object_storage(const object_storage&) = delete;
        object_storage(object_storage&&) = delete;
        object_storage& operator=(const object_storage&) = delete;
        object_storage& operator=(object_storage&&) = delete;
        virtual ~object_storage() = default;

        /*
         * Implementations are expected to limit the listing to only L0 data
         * objects, and provide the listing in _globally_ lexicographic order.
         */
        virtual seastar::future<std::expected<
          cloud_storage_clients::client::list_bucket_result,
          cloud_storage_clients::error_outcome>>
        list_objects(
          seastar::abort_source*,
          std::optional<cloud_storage_clients::object_key> prefix
          = std::nullopt,
          std::optional<ss::sstring> continuation_token = std::nullopt)
          = 0;

        virtual seastar::future<std::expected<void, cloud_io::upload_result>>
        delete_objects(
          seastar::abort_source*,
          chunked_vector<cloud_storage_clients::client::list_bucket_item>)
          = 0;
    };

    /*
     * Interface for computing the maximum epoch eligible for GC.
     */
    class epoch_source {
    public:
        struct partitions_snapshot {
            using partition_map = chunked_hash_map<
              model::topic_namespace,
              chunked_vector<model::partition_id>,
              model::topic_namespace_hash,
              model::topic_namespace_eq>;

            partition_map partitions;
            cluster_epoch snap_revision;
        };

        using partitions_max_gc_epoch = chunked_hash_map<
          model::topic_namespace,
          chunked_hash_map<model::partition_id, cluster_epoch>,
          model::topic_namespace_hash,
          model::topic_namespace_eq>;

        epoch_source() = default;
        epoch_source(const epoch_source&) = default;
        epoch_source(epoch_source&&) = delete;
        epoch_source& operator=(const epoch_source&) = default;
        epoch_source& operator=(epoch_source&&) = delete;
        virtual ~epoch_source() = default;

        /*
         * L0 objects with epochs <= the return value may be deleted. An
         * expected return value of std::nullopt is not an error, but rather
         * indicates that no GC eligible epoch could yet be determined.
         */
        virtual seastar::future<
          std::expected<std::optional<cluster_epoch>, std::string>>
        max_gc_eligible_epoch(seastar::abort_source*);

        /*
         * Snapshot of existing cloud topic partition identifiers along with the
         * maximum possible GC eligible epoch for the set of partitions.
         */
        virtual seastar::future<std::expected<partitions_snapshot, std::string>>
        get_partitions(seastar::abort_source*) = 0;

        /*
         * Reported max GC eligible epochs for cloud topic partitions.
         */
        virtual seastar::future<
          std::expected<partitions_max_gc_epoch, std::string>>
        get_partitions_max_gc_epoch(seastar::abort_source*) = 0;
    };

    /**
     * Interface for determining the total number of shards in the cluster
     * and the current shard's position in logical, ordered list of shard IDs
     * starting at 0 (node 0, shard 0) and ending at total_shards - 1.
     */
    struct node_info {
        node_info() = default;
        node_info(const node_info&) = default;
        node_info(node_info&&) = delete;
        node_info& operator=(const node_info&) = default;
        node_info& operator=(node_info&&) = delete;
        virtual ~node_info() = default;

        virtual size_t shard_index() const = 0;
        virtual size_t total_shards() const = 0;
    };

public:
    /*
     * Construct with the given storage and epoch providers. This interface is
     * intended to be used by tests which swap in mock implementations.
     */
    level_zero_gc(
      level_zero_gc_config,
      std::unique_ptr<object_storage>,
      std::unique_ptr<epoch_source>,
      std::unique_ptr<node_info>);

    /*
     * Construct with default implementations of storage and epoch providers.
     */
    level_zero_gc(
      model::node_id,
      cloud_io::remote*,
      cloud_storage_clients::bucket_name,
      seastar::sharded<cluster::health_monitor_frontend>*,
      seastar::sharded<cluster::controller_stm>*,
      seastar::sharded<cluster::topic_table>*,
      seastar::sharded<cluster::members_table>*);

    ~level_zero_gc();

    /*
     * Request that GC be started or paused. These can be called multiple times
     * and in any order. The last invocation will eventually take effect.
     */
    void start();
    void pause();

    /*
     * Request and wait for GC to be completely stopped. After calling shutdown,
     * calling start() or pause() will have no effect.
     */
    seastar::future<> stop();

private:
    level_zero_gc_config config_;
    std::unique_ptr<epoch_source> epoch_source_;

    bool should_run_;
    bool should_shutdown_;
    seastar::abort_source asrc_;
    seastar::condition_variable worker_cv_;
    seastar::future<> worker_;

    seastar::future<> worker();
    enum class collection_error : int8_t;
    seastar::future<std::expected<size_t, collection_error>> try_to_collect();
    seastar::future<std::expected<size_t, collection_error>>
    do_try_to_collect(std::optional<cluster_epoch>&);

    level_zero_gc_probe probe_;

    class list_delete_worker;
    std::unique_ptr<list_delete_worker> delete_worker_{};
};

/**
 * @brief Compute a subrange of [0,999] for some shard.
 *
 * Aims to partition the object prefix space ([0-999]) perfectly (i.e. with no
 * missing prefixes or overlap between shards). As a result, might _not_ assign
 * a sub-range to some shard, e.g. if the shard index exceeds the total number
 * of prefixes.
 *
 * For example:
 *   - 2 nodes, 5 shards per node (10 total shards)
 *     - compute_prefix_range(0,10) -> {.min=0,.max=99} (node 0,shard 0)
 *     - compute_prefix_range(8,10) -> {.min=800,.max=899} (node 1,shard 3)
 *  - 3 nodes, 3 shards per node (9 total shards)
 *     - compute_prefix_range(8,9)  -> {.min=888,.max=999} (node 2,shard 2)
 * @param shard_idx - Shard index as computed by an implementation of node_info
 * @param total_shards - Total number of shards in the cluster
 */
struct prefix_range_inclusive;
std::optional<prefix_range_inclusive>
compute_prefix_range(size_t shard_idx, size_t total_shards);

} // namespace cloud_topics
