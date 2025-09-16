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

#include "cloud_topics/level_one/compaction/log_collector.h"
#include "cluster/notification.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/topic_table.h"

namespace cloud_topics::l1 {

class compaction_scheduler;

class partition_leader_log_collector : public log_collector {
public:
    partition_leader_log_collector(
      compaction_scheduler*,
      model::node_id,
      ss::sharded<cluster::partition_leaders_table>*,
      ss::sharded<cluster::topic_table>*);

    // Sets up `*_notify_handles` using pointers to `cluster` utilities.
    ss::future<> start() final;

    // Tears down `*_notify_handles` using pointers to `cluster` utilities.
    ss::future<> stop() final;

private:
    // Registers/unregisters `ntp`s with the `compaction_scheduler` using
    // `ntp_delta` notifications from the `topic_table`. The following cases are
    // handled:
    // 1. A cloud-topic enabled `ntp` becomes `compact`-enabled (register)
    // 2. A cloud-topic enabled `ntp` is no longer `compact`-enabled
    // (unregister)
    // 3. A currently managed `ntp` is removed (unregister)
    //
    // Register operations can be performed synchronously while unregister
    // operations are performed in a backgrounded fiber (see
    // `compaction_scheduler::unmanage_partition()`).
    void on_ntp_change(cluster::topic_table::ntp_delta);

    // Registers/unregisters `ntp`s with the `compaction_scheduler` using
    // leadership notifications from the `partition_leaders_table`. The
    // following cases are handled:
    // 1. A cloud-topic, `compact`-enabled `ntp` becomes the leader on a shard
    // on this node (register)
    // 2. A cloud-topic, `compact`-enabled `ntp` steps down from being the
    // leader on a shard on this node (unregister)
    //
    // Register operations can be performed synchronously while unregister
    // operations are performed in a backgrounded fiber (see
    // `compaction_scheduler::unmanage_partition()`).
    void on_leadership_change(model::ntp, model::node_id);

    // The `node_id` of the current broker.
    model::node_id _self;

    ss::gate _gate;

    // A notification handle that tracks `ntp_delta` notifications from the
    // `topic_table` (notably property updates & removals).
    cluster::notification_id_type _ntp_notify_handle;

    // A notification handle that tracks leadership notifications from the
    // `partition_leaders_table`.
    cluster::notification_id_type _leader_notify_handle;

    ss::sharded<cluster::partition_leaders_table>* _leaders;
    ss::sharded<cluster::topic_table>* _topic_table;
};

} // namespace cloud_topics::l1
