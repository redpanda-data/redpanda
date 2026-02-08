/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "ssx/actor.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>

namespace cluster {
class members_table;
class health_monitor_frontend;
class controller_stm;
class topic_table;
} // namespace cluster

namespace rpc {
class connection_cache;
} // namespace rpc

namespace cloud_topics::l0::gc {

class epoch_barrier_coordinator;

struct barrier_needs_loop_tag;

/// Leader-gated manager that drives the cluster-wide epoch barrier protocol.
///
/// Runs on the leader of L1 metastore partition 0. When leadership is
/// acquired, a background loop is started that periodically:
///   1. Collects a candidate epoch C from the existing epoch source.
///   2. Fans out Invalidate(C) RPCs to every node so they stop using
///      epochs <= C for new writes.
///   3. Polls all nodes until every node reports no in-flight uploads
///      at epochs <= C.
///   4. Publishes C as the new safe-to-GC epoch.
class epoch_barrier_manager
  : public ssx::actor<
      ss::bool_class<barrier_needs_loop_tag>,
      1,
      ssx::overflow_policy::drop_oldest> {
public:
    using needs_loop = ss::bool_class<barrier_needs_loop_tag>;

    epoch_barrier_manager(
      model::node_id self,
      ss::sharded<cluster::members_table>*,
      ss::sharded<::rpc::connection_cache>*,
      ss::sharded<epoch_barrier_coordinator>*,
      ss::sharded<cluster::health_monitor_frontend>*,
      ss::sharded<cluster::controller_stm>*,
      ss::sharded<cluster::topic_table>*);

    ~epoch_barrier_manager() override;

    void enqueue_loop_reset(needs_loop needs);

    ss::future<> stop() override;

    /// Returns the latest safe-to-GC epoch published by the barrier, or
    /// nullopt if no barrier round has completed yet.
    std::optional<cluster_epoch> safe_epoch() const noexcept {
        return _safe_epoch;
    }

protected:
    ss::future<> process(needs_loop needs) override;
    void on_error(std::exception_ptr ex) noexcept override;

private:
    class barrier_loop;

    ss::future<> reset_loop(needs_loop needs);

    model::node_id _self;
    ss::sharded<cluster::members_table>* _members;
    ss::sharded<::rpc::connection_cache>* _connections;
    ss::sharded<epoch_barrier_coordinator>* _coordinator;
    ss::sharded<cluster::health_monitor_frontend>* _health_monitor;
    ss::sharded<cluster::controller_stm>* _controller_stm;
    ss::sharded<cluster::topic_table>* _topic_table;
    std::unique_ptr<barrier_loop> _loop;
    std::optional<cluster_epoch> _safe_epoch;
};

} // namespace cloud_topics::l0::gc
