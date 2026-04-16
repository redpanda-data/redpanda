/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "raft/consensus.h"
#include "raft/group_manager.h"
#include "rpc/connection_cache.h"
#include "ssx/abort_source.h"
#include "ssx/checkpoint_mutex.h"

#include <seastar/core/distributed.hh>
#include <seastar/core/gate.hh>

#include <expected>
#include <limits>

namespace cluster {

class controller_stm;

// A sharded service on every core that is responsible for returning the
// cluster's epoch.
//
// The cluster epoch is a monotonically increasing value that is currently used
// in the Cloud Topics's L0 implementation.
//
// If the cluster_epoch_service is hosting the controller leader on the same
// shard, it becomes responsible for driving the epoch forward, and does this by
// periodically committing a checkpoint batch to raft0. This checkpointed
// batch's offset becomes the new epoch for the cluster until another epoch is
// committed to the log. It's important we derive the epoch from the log so we
// can correlate it with other cluster level operations (adding partitions).
//
// The update mechanism for epochs is as follows:
//
// 1. Shard0 on each broker either fetches the epoch from itself (because it is
//    controller leader) or it makes an RPC to go and fetch from the leader its
//    current epoch.
// 2. Non shard0 will go fetch the epoch from shard0. If shard0 needs an update
//    it may wait for that update to complete before returning to update the
//    local shard's cached epoch.
template<typename Clock = ss::lowres_clock>
class cluster_epoch_service
  : public ss::peering_sharded_service<cluster_epoch_service<Clock>> {
    class raft0_state;

public:
    cluster_epoch_service(
      model::node_id, ss::sharded<rpc::connection_cache>*) noexcept;
    // **For testing** support injecting a custom "remote fetch epoch" function.
    explicit cluster_epoch_service(
      ss::noncopyable_function<
        ss::future<std::expected<int64_t, std::error_code>>(ss::abort_source*)>
        fn) noexcept;
    cluster_epoch_service(const cluster_epoch_service&) = delete;
    cluster_epoch_service(cluster_epoch_service&&) = delete;
    cluster_epoch_service& operator=(const cluster_epoch_service&) = delete;
    cluster_epoch_service& operator=(cluster_epoch_service&&) = delete;
    ~cluster_epoch_service() noexcept;

    ss::future<> start();
    ss::future<> stop();

    // Invalidate any caching that may (or may not) be going on of the current
    // epoch.
    //
    // This should only be used if another part of the system actually observed
    // a higher epoch, otherwise this will needlessly invalidate the cache.
    //
    // To ensure we are not continually invaliding epochs, you must pass the
    // epoch that caused the monotonicity violation (ie. that you got back from
    // `get_cached_epoch`) in order to actually invalidate the cache.
    ss::future<>
    invalidate_epoch_cache(int64_t epoch_causing_monotonicity_violation);

    // Returns the current epoch (with caching) for the cluster.
    //
    // May be called on any shard.
    //
    // The future may end up returning an error code if cached epoch is too old
    // and we have not been able to reach the controller leader to update it.
    ss::future<std::expected<int64_t, std::error_code>>
    get_cached_epoch(seastar::abort_source*);

    // Returns the current epoch for the cluster.
    //
    // Returns an std::nullopt if not currently the leader of raft0.
    //
    // REQUIRES: Must only be called on shard0.
    ss::future<std::optional<int64_t>> get_current_epoch();

    // Set the controller stm instance used to generate
    // the cluster epoch from.
    //
    // Also sets the raft_group manager, which is used to subscribe to
    // leadership changes on raft0.
    //
    // Must only be set on shard0
    void set_raft0(
      ss::lw_shared_ptr<raft::consensus> raft0,
      ss::sharded<controller_stm>& controller_stm,
      ss::sharded<raft::group_manager>& raft_manager) noexcept;

private:
    // The cached epoch should be updated, but don't block on it.
    bool cache_entry_expired() const noexcept;
    // The cached epoch needs updating, and block we need to block on it.
    bool cache_entry_needs_updated() const noexcept;
    // Fetch the epoch from shard0, and also return the time it was fetched
    // from that shard. Note that we're using ss::low_res clock from another
    // shard, which might be different from the current shard's low_res::clock
    // but it should be "good enough" in practice, since they are all based on
    // the system clock.
    ss::future<std::expected<
      std::tuple<int64_t, typename Clock::time_point>,
      std::error_code>>
    shard0_get_epoch(ssx::sharded_abort_source*);
    // Update the epoch
    ss::future<std::error_code> do_update_epoch(ss::abort_source*);
    // Fetch the epoch from the leader node
    ss::future<std::expected<int64_t, std::error_code>>
    fetch_leader_epoch(ss::abort_source*);

    // The currently cached epoch
    int64_t _cached_epoch{std::numeric_limits<int64_t>::min()};
    // The last time the epoch was cached
    Clock::time_point _cached_epoch_time{Clock::time_point::min()};
    // The last time the epoch actually ratcheted forward
    Clock::time_point _epoch_updated_time{Clock::time_point::min()};

    // Mutex guarding cross shard calls and RPCs to update the cache.
    ssx::checkpoint_mutex _mu{"cluster_epoch_generator"};

    ss::gate _gate;
    ss::abort_source _abort_source;

    ss::noncopyable_function<
      ss::future<std::expected<int64_t, std::error_code>>(ss::abort_source*)>
      _do_fetch_leader_epoch_fn;

    std::unique_ptr<raft0_state> _shard0_state;
};

} // namespace cluster
