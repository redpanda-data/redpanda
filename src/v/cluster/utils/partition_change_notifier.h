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
#include "cluster/notification.h"
#include "cluster/topic_configuration.h"
#include "model/fundamental.h"

#include <seastar/util/noncopyable_function.hh>

namespace cluster {

// Unified interface for partition level notifications.
// Supports notifications for partitions with replicas on the current shard.
class partition_change_notifier {
public:
    using notify_current_state
      = ss::bool_class<struct notify_current_state_tag>;
    enum class notification_type : int8_t {
        // Notification for leadership change of a partition.
        // Notified on assuming leadership or losing leadership.
        // For leadership changes for all partitions, not just those on the
        // current shard,  use cluster::partition_leaders_table.
        leadership_change,
        // A new replica is now being managed by the current shard.
        // New replicas assignments typically come from topic creations and
        // partition movements.
        partition_replica_assigned,
        // An existing replica is no longer managed by the current shard.
        // There will be a separate notification for leadership drop if the
        // replica was a leader.
        partition_replica_unassigned,
        // Properties of the replica's partition have changed.
        partition_properties_change
    };
    struct partition_state {
        partition_state(
          model::term_id term,
          bool is_leader,
          std::optional<cluster::topic_configuration> topic_cfg)
          : term(term)
          , is_leader(is_leader)
          , topic_cfg(std::move(topic_cfg)) {}
        model::term_id term;
        bool is_leader;
        std::optional<cluster::topic_configuration> topic_cfg;
    };
    // partition_state is only set if the partition is still active
    // on the shard. If unset the processor can assume that the partition
    // replica is no longer active on the shard.
    using notification_cb_t = ss::noncopyable_function<void(
      notification_type, const model::ntp&, std::optional<partition_state>)>;

    partition_change_notifier() = default;
    partition_change_notifier(const partition_change_notifier&) = delete;
    partition_change_notifier(partition_change_notifier&&) = delete;
    partition_change_notifier&
    operator=(const partition_change_notifier&) = delete;
    partition_change_notifier& operator=(partition_change_notifier&&) = delete;

    virtual ~partition_change_notifier() = default;

    virtual notification_id_type register_partition_notifications(
      notification_cb_t, notify_current_state = notify_current_state::no) = 0;

    virtual void unregister_partition_notifications(notification_id_type) = 0;
};
} // namespace cluster
