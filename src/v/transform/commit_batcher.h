/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/outcome.h"
#include "cluster/errc.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "model/transform.h"

#include <seastar/core/abort_source.hh>

#include <chrono>

namespace transform {

/**
 * An interface for a offset tracking KV store writer.
 *
 * It supports determining the owner of a given key, and then batch commiting
 * values for a partition within the KV store.
 */
class offset_committer {
public:
    offset_committer() = default;
    offset_committer(const offset_committer&) = default;
    offset_committer(offset_committer&&) = default;
    offset_committer& operator=(const offset_committer&) = default;
    offset_committer& operator=(offset_committer&&) = default;
    virtual ~offset_committer() = default;

    /**
     * Lookup which partition owns a given key.
     */
    virtual ss::future<result<model::partition_id, cluster::errc>>
      find_coordinator(model::transform_offsets_key) = 0;

    /**
     * Commit a batch of data for a partition.
     *
     * The partition **must** be owner of every key in the batch.
     */
    virtual ss::future<cluster::errc> batch_commit(
      model::partition_id coordinator,
      absl::
        btree_map<model::transform_offsets_key, model::transform_offsets_value>)
      = 0;
};

/**
 * Batches commits of current processor offsets.
 *
 * This is required to make offset commits a function of the number of cores in
 * the system instead of a function of the number of processors. This is highly
 * desirable and allows for a much higher amount of processors in the system
 * without needing a more advanced mechanism for sharding the transform offset
 * store or a dynamic backpressure/throttling mechanism.
 *
 * We periodically commit all offsets as a single batch at a fixed interval.
 * There is a seperate loop that maps key to the corresponding shard so that the
 * RPC traffic for looking up the coordinator is also a scaling function based
 * on cores. There is no GC mechanism for removing a coordinator -> key mapping
 * and instead it's expected that processors explicitly unregister their mapping
 * when shutdown.
 */
template<typename ClockType = ss::lowres_clock>
class commit_batcher {
    static_assert(
      std::is_same_v<ClockType, ss::lowres_clock>
        || std::is_same_v<ClockType, ss::manual_clock>,
      "Only lowres or manual clocks are supported");

public:
    explicit commit_batcher(
      config::binding<std::chrono::milliseconds> commit_interval,
      std::unique_ptr<offset_committer>);

    ss::future<> start();
    ss::future<> stop();

    /**
     * When moving nodes, we need to give the previous node a chance to commit
     * the pending interval otherwise leadership changes will likely cause
     * duplicate processing.
     *
     * While the duplicates are inevitable in our current system, reduce the
     * likelyhood of these by waiting for the previous node to flush it's commit
     * offsets. We do this by assuming every node has the same commit interval
     * and waiting for one commit interval to pass. This is a best effort to
     * allow for flushing at the cost of latency. However, we mostly try to keep
     * leadership stable, so this should not make a large difference in
     * practice.
     */
    ss::future<>
    wait_for_previous_flushes(model::transform_offsets_key, ss::abort_source*);

    /**
     * Preload the coordinator information for a given key.
     *
     * This is not a requirement as partitions can change and internally the
     * batcher will handle reloading the partition for a key.
     */
    void preload(model::transform_offsets_key);

    /**
     * Unload the coordinator information as it's not going to be needed anymore
     * (no more calls to `commit_offset` will be called on this core).
     *
     * This is a used instead of a cache eviction algorithm so it's important
     * that users of this class call this to unload the key and reduce memory
     * usage.
     */
    void unload(model::transform_offsets_key);

    /**
     * Enqueue a commit to be batched with other commits.
     */
    ss::future<> commit_offset(
      model::transform_offsets_key, model::transform_offsets_value);

    /**
     * Flush all of the pending commits to the underlying offset_committer.
     */
    ss::future<> flush();

private:
    ss::future<> find_coordinator_loop();
    /**
     * Return true if all requests to find coordinators failed.
     *
     * This can be used by the coordinator loop to backoff if requests are
     * failing instead of retrying in a loop.
     */
    ss::future<bool> assign_coordinators();

    using kv_map = absl::
      btree_map<model::transform_offsets_key, model::transform_offsets_value>;

    /**
     * Flush all of the pending commits to the underlying offset_committer.
     */
    ss::future<> do_flush(model::partition_id, kv_map);

    kv_map _unbatched;
    absl::btree_map<model::partition_id, kv_map> _batched;
    absl::btree_map<model::transform_offsets_key, model::partition_id>
      _coordinator_cache;

    ss::condition_variable _unbatched_cond_var;
    std::unique_ptr<offset_committer> _offset_committer;
    config::binding<std::chrono::milliseconds> _commit_interval;
    ss::timer<ClockType> _timer;
    ss::abort_source _as;
    ss::gate _gate;
};

} // namespace transform
