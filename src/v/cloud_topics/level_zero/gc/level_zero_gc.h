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

#include "cloud_topics/level_zero/gc/level_zero_gc_probe.h"
#include "cloud_topics/level_zero/gc/level_zero_gc_types.h"
#include "random/generators.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/noncopyable_function.hh>

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

/*
 * State Machine
 * =============
 *
 *  Construction
 *       |
 *       v
 *  +--------+
 *  | paused |<-----------------------------+
 *  +---+----+                              |
 *      | start()                           |
 *      v                                   |
 *  +----------------+                      |
 *  | worker loop top|<-----------+         |
 *  +-------+--------+            |         |
 *          |                     |         |
 *          | check safety        |         |
 *          |                     |         |
 *     ok +-+-+ !ok               |         |
 *    +---+   +----+              |         |
 *    v            v              |         |
 *  +---------+ +---------------+ |         |
 *  | running | |safety_blocked | |         |
 *  |         | |               | |         |
 *  | backoff | | increment     | |         |
 *  |  then   | |  probe,       | |         |
 *  | collect | |  sleep        | |         |
 *  +----+----+ +-------+-------+ |         |
 *       |              |         |         |
 *       +------+-------+         |         |
 *              |                 |         |
 *              +-----------------+         |
 *                  loop back               |
 *                                          |
 *  pause() from running or safety_blocked  |
 *  ----------------------------------------+
 *
 *  reset() from any non-stopped state:
 *
 *  +----------+
 *  |resetting | drains delete worker, then
 *  +----+-----+ restores prior run/pause state
 *       |
 *       v
 *  was_running?
 *    yes -> running
 *    no  -> paused
 *
 *  stop() from any state:
 *
 *  +---------+  worker done  +---------+
 *  |stopping |-------------->| stopped |
 *  +---------+               +---------+
 *                             (terminal)
 *
 *  The running/safety_blocked distinction is not an
 *  explicit transition. Both are phases of the worker
 *  loop -- each iteration checks can_proceed() and
 *  takes the corresponding branch. The state returned
 *  by get_state() reflects whichever branch would be
 *  taken at query time.
 *
 *  get_state() priority:
 *    should_shutdown_ > resetting_ > !should_run_ >
 *    safety_monitor_.can_proceed()
 *
 *  start()/pause() block while resetting_ is true.
 */
template<class Clock = ss::lowres_clock>
class level_zero_gc_t {
public:
    using jitter_fn = ss::noncopyable_function<typename Clock::duration(
      typename Clock::duration)>;

    // Add positive jitter to despread wake times across
    // shards. +[0, 10%] of the computed backoff.
    static constexpr auto default_jitter =
      [](Clock::duration base) -> Clock::duration {
        using namespace std::chrono_literals;
        // 2m is sort of arbitrary here with the aim of roughly staggering GC
        // wakeups. "longer than a collection round but much shorter than a
        // grace period"
        constexpr typename Clock::duration max_jitter{120s};
        auto ub = std::min(base / 10, max_jitter);
        return std::chrono::milliseconds{random_generators::get_int(ub / 1ms)};
    };

    /*
     * Construct with the given storage and epoch providers. This interface is
     * intended to be used by tests which swap in mock implementations.
     */
    level_zero_gc_t(
      level_zero_gc_config,
      std::unique_ptr<l0::gc::object_storage>,
      std::unique_ptr<l0::gc::epoch_source>,
      std::unique_ptr<l0::gc::node_info>,
      std::unique_ptr<l0::gc::safety_monitor>,
      jitter_fn = default_jitter);

    /*
     * Construct with default implementations of storage and epoch providers.
     */
    level_zero_gc_t(
      model::node_id,
      cloud_io::remote*,
      cloud_storage_clients::bucket_name,
      seastar::sharded<cluster::health_monitor_frontend>*,
      seastar::sharded<cluster::controller_stm>*,
      seastar::sharded<cluster::topic_table>*,
      seastar::sharded<cluster::members_table>*);

    ~level_zero_gc_t();

    /*
     * Request that GC be started or paused. These can be called multiple times
     * and in any order. The last invocation will eventually take effect.
     *
     * If a reset is in progress, these will block until it completes or
     * control_timeout expires.
     */
    seastar::future<> start();
    seastar::future<> pause();

    /// Reset internal GC state without a full stop/start cycle.
    /// Drains in-flight operations, clears pagination and prefix
    /// iteration state, and prepares for a fresh collection sweep.
    /// GC resumes automatically if it was running before the reset.
    seastar::future<> reset();

    /*
     * Request and wait for GC to be completely stopped. After calling shutdown,
     * calling start() or pause() will have no effect.
     */
    seastar::future<> stop();

    /**
     * @brief Compute the runtime state of this GC instance.
     */
    l0::gc::state get_state() const;

private:
    seastar::condition_variable worker_cv_;
    seastar::condition_variable reset_cv_;

    level_zero_gc_config config_;
    std::unique_ptr<l0::gc::epoch_source> epoch_source_;
    std::unique_ptr<l0::gc::safety_monitor> safety_monitor_;
    jitter_fn jitter_fn_;

    bool should_run_;
    bool should_shutdown_;
    bool resetting_{false};
    seastar::abort_source asrc_;
    seastar::abort_source backoff_asrc_;
    seastar::future<> worker_;

    seastar::future<> worker();

    seastar::future<
      std::expected<l0::gc::collection_outcome, l0::gc::collection_error>>
    try_to_collect();

    /// Returns per-page outcome, or nullopt when all prefixes are exhausted.
    seastar::future<std::expected<
      std::optional<l0::gc::collection_outcome>,
      l0::gc::collection_error>>
    do_try_to_collect(std::optional<cluster_epoch>&);

    level_zero_gc_probe probe_;

    /// Set by start() and reset() to force the worker to skip its next
    /// backoff sleep so the first round after a state change runs
    /// immediately.
    bool skip_backoff_{false};

    class list_delete_worker;
    std::unique_ptr<list_delete_worker> delete_worker_{};
};

using level_zero_gc = level_zero_gc_t<>;

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
