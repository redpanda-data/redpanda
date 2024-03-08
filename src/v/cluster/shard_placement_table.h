/*
 * Copyright 2024 Redpanda Data, Inc.
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
#include "cluster/types.h"
#include "utils/mutex.h"

#include <seastar/core/sharded.hh>

#include <absl/container/node_hash_map.h>

namespace cluster {

/// Node-local data structure tracking ntp -> shard mapping for partition
/// replicas hosted by this node. Both target state and current shard-local
/// state for each ntp are tracked. Target state is supposed to be modified by
/// shard_balancer and current shard-local state is supposed to be modified by
/// controller_backend as it creates/deletes/moves partitions.
///
/// Currently shard-local and target states are in-memory and target states
/// duplicate shard assignments that are stored in topic_table, but in the
/// future they will be persisted in the kvstore and target states will be set
/// independently.
///
/// Note that in contrast to `cluster::shard_table` (that helps other parts of
/// the system to find the shard where the `cluster::partition` object for some
/// ntp resides) this is a more granular table that is internal to the partition
/// reconciliation process.
class shard_placement_table
  : public ss::peering_sharded_service<shard_placement_table> {
public:
    // assignment modification methods must be called on this shard
    static constexpr ss::shard_id assignment_shard_id = 0;

    enum class hosted_status {
        /// Cross-shard transfer is in progress, we are the destination
        receiving,
        /// Normal state, we can start the partition instance.
        hosted,
        /// We have transferred our state to somebody else, now our copy must be
        /// deleted.
        obsolete,
    };

    /// Current state of shard-local partition kvstore data on this shard.
    struct shard_local_state {
        model::revision_id log_revision;
        hosted_status status;

        static shard_local_state initial(model::revision_id log_revision) {
            return shard_local_state{
              .log_revision = log_revision,
              .status = hosted_status::hosted,
            };
        }

        static shard_local_state receiving(model::revision_id log_revision) {
            return shard_local_state{
              .log_revision = log_revision,
              .status = hosted_status::receiving,
            };
        }

        friend std::ostream&
        operator<<(std::ostream&, const shard_local_state&);
    };

    /// A struct holding both current shard-local and target states for an ntp.
    struct placement_state {
        /// Current shard-local state for this ntp. Will be non-null if
        /// some kvstore state for this ntp exists on this shard.
        std::optional<shard_local_state> local;
        /// Current target shard for this ntp. Will be non-null on the target
        /// shard itself, as well as shards with non-null local state.
        std::optional<shard_placement_target> target;
        /// Revision of the target shard.
        model::shard_revision_id shard_revision;

        // invariant: if local is nullopt, then expected_on_this_shard() is true
        // (there is no point in keeping an item that has no local state as well
        // as is not expected on this shard)
        bool expected_on_this_shard() const {
            return !_next && target && target->shard == ss::this_shard_id();
        }

        friend std::ostream& operator<<(std::ostream&, const placement_state&);

    private:
        friend class shard_placement_table;

        placement_state() = default;
        placement_state(
          const shard_placement_target& target, model::shard_revision_id rev)
          : target(target)
          , shard_revision(rev) {}

        /// If this shard is the initial shard for this partition on this node,
        /// this field will contain the corresponding shard revision.
        model::shard_revision_id _is_initial_at_revision;
        /// If x-shard transfer is in progress, will hold the destination. Note
        /// that it is initialized from target but in contrast to target, it
        /// can't change mid-transfer.
        std::optional<ss::shard_id> _next;
    };

    // must be called on each shard
    ss::future<> initialize(const topic_table&, model::node_id self);

    using shard_callback_t
      = std::function<void(const model::ntp&, model::shard_revision_id)>;

    /// Must be called only on assignment_shard_id. Shard callback will be
    /// called on shards that have been modified by this invocation.
    ss::future<> set_target(
      const model::ntp&,
      std::optional<shard_placement_target>,
      model::shard_revision_id,
      shard_callback_t);

    // getters

    std::optional<placement_state> state_on_this_shard(const model::ntp&) const;

    // partition lifecycle methods

    ss::future<std::error_code>
    prepare_create(const model::ntp&, model::revision_id expected_log_rev);

    // return value is a tri-state:
    // * if it returns a shard_id value, a transfer to that shard must be
    // performed
    // * if it returns errc::success, transfer has already been performed
    // * else, we must wait before we begin the transfer.
    ss::future<result<ss::shard_id>>
    prepare_transfer(const model::ntp&, model::revision_id expected_log_rev);

    ss::future<> finish_transfer_on_destination(
      const model::ntp&, model::revision_id expected_log_rev);

    ss::future<> finish_transfer_on_source(
      const model::ntp&, model::revision_id expected_log_rev);

    ss::future<std::error_code>
    finish_delete(const model::ntp&, model::revision_id expected_log_rev);

private:
    void set_target_on_this_shard(
      const model::ntp&,
      std::optional<shard_placement_target>,
      model::shard_revision_id,
      bool is_initial,
      shard_callback_t);

    ss::future<> do_delete(const model::ntp&, placement_state&);

private:
    // per-shard state
    //
    // node_hash_map for pointer stability
    absl::node_hash_map<model::ntp, placement_state> _states;

    // only on shard 0, _ntp2target will hold targets for all ntps on this node.
    mutex _mtx{"shard_placement_table"};
    absl::node_hash_map<model::ntp, shard_placement_target> _ntp2target;
};

std::ostream& operator<<(std::ostream&, shard_placement_table::hosted_status);

} // namespace cluster
