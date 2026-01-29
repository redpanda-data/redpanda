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

#include "base/seastarx.h"
#include "cluster/metadata_cache.h"
#include "cluster/notification.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/topic_table.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace cloud_topics::l1 {

struct compaction_cluster_state {
    model::node_id self;
    ss::sharded<cluster::partition_leaders_table>* leaders_table;
    ss::sharded<cluster::topic_table>* topic_table;
    ss::sharded<cluster::metadata_cache>* metadata_cache;
    ss::sharded<cluster::shard_table>* shard_table;
    ss::sharded<cluster::partition_manager>* partition_manager;
};

// Responsible for pushing CTPs/logs that require compaction to the
// `compaction_scheduler` for managing, as well as their removal. Provides a
// very limited interface to the user- implementors require only adding
// `start_collecting_logs()` and `stop_collecting_logs()` functions in their
// concrete classes.
class log_collector {
public:
    virtual ~log_collector() noexcept = default;

    // Starts the `log_collector` by calling `manage_logs()`, allowing it to
    // push CTPs that require compaction to the `compaction_scheduler`, or
    // remove CTPs that no longer require compaction from the
    // `compaction_scheduler`.
    ss::future<> start();

    // Stops the `log_collector` by calling `unmanage_logs()`. The
    // `compaction_scheduler` will no longer receive updates on which CTPs
    // require managing. This should only be invoked during application
    // shutdown.
    ss::future<> stop();

protected:
    // Called during start-up. Initiates log collection, allowing for
    // registration/deregistration of logs with the `compaction_scheduler`. For
    // example, setting up a callback with a cluster object that pushes newly
    // managed CTPs to the `compaction_scheduler`.
    virtual ss::future<> start_collecting_logs() = 0;

    // Called during tear-down. Stops the `log_collector` from making further
    // registrations/deregistrations of logs with the `compaction_scheduler`
    // (stopping call-backs, destructing objects or closing background loops,
    // etc.)
    virtual ss::future<> stop_collecting_logs() = 0;
};

// An implementation of the `log_collector` that pushes CTPs which have leaders
// on this broker (irrespective of shard) to the `compaction_scheduler`.
class partition_leader_log_collector : public log_collector {
public:
    // Typedefs for necessary functions to call into the `compaction_scheduler`.
    using manage_cb_t = ss::noncopyable_function<void(
      const model::ntp&, const model::topic_id_partition&, std::string_view)>;
    using unmanage_cb_t
      = ss::noncopyable_function<void(model::ntp, std::string_view)>;
    using is_managed_cb_t = ss::noncopyable_function<bool(const model::ntp&)>;

    partition_leader_log_collector(
      manage_cb_t,
      unmanage_cb_t,
      is_managed_cb_t,
      model::node_id,
      ss::sharded<cluster::partition_leaders_table>*,
      ss::sharded<cluster::topic_table>*);

protected:
    // Sets up `*_notify_handles` using pointers to `cluster` utilities.
    ss::future<> start_collecting_logs() final;

    // Tears down `*_notify_handles` using pointers to `cluster` utilities.
    ss::future<> stop_collecting_logs() final;

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
    void on_ntp_change(const cluster::topic_table::ntp_delta&);

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
    void on_leadership_change(const model::ntp&, model::node_id);

    // Callback to register a partition with the `compaction_scheduler`.
    manage_cb_t _manage_cb;

    // Callback to deregister a partition with the `compaction_scheduler`.
    unmanage_cb_t _unmanage_cb;

    // Callback to query whether a partition is registered with the
    // `compaction_scheduler`.
    is_managed_cb_t _is_managed_cb;

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

std::unique_ptr<log_collector> make_default_log_collector(
  partition_leader_log_collector::manage_cb_t,
  partition_leader_log_collector::unmanage_cb_t,
  partition_leader_log_collector::is_managed_cb_t,
  compaction_cluster_state);

} // namespace cloud_topics::l1
