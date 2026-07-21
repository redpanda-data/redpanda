/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "cluster/fwd.h"
#include "cluster/notification.h"
#include "config/property.h"
#include "model/metadata.h"
#include "random/simple_time_jitter.h"
#include "ssx/single_fiber_executor.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/noncopyable_function.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace cluster {

class health_manager {
    using clock_type = ss::lowres_clock;
    static constexpr std::chrono::seconds set_replicas_timeout = 15s;

public:
    static constexpr ss::shard_id shard = 0;

    health_manager(
      model::node_id,
      size_t,
      std::chrono::milliseconds,
      config::binding<size_t> max_concurrent_moves,
      ss::sharded<topic_table>&,
      ss::sharded<topics_frontend>&,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<members_table>&,
      ss::sharded<ss::abort_source>&);

    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<bool>
    ensure_topic_replication(model::topic_namespace_view, ss::abort_source&);
    // Submits a fresh reconcile run, interrupting any run in flight.
    void submit_reconcile();
    ss::future<> reconcile_loop(ss::abort_source&);
    ss::future<> do_reconcile(ss::abort_source&);
    // Returns true if interrupted via the abort source.
    ss::future<bool>
    sleep_or_aborted(std::chrono::milliseconds, ss::abort_source&);

    model::node_id _self;
    size_t _target_replication_factor;
    simple_time_jitter<clock_type, std::chrono::milliseconds> _tick_jitter;
    config::binding<size_t> _max_concurrent_moves;
    ss::sharded<topic_table>& _topics;
    ss::sharded<topics_frontend>& _topics_frontend;
    ss::sharded<partition_leaders_table>& _leaders;
    ss::sharded<members_table>& _members;
    ss::sharded<ss::abort_source>& _as;
    notification_id_type _leadership_notification_id{};
    notification_id_type _members_notification_id{};
    ssx::single_fiber_executor<
      ss::noncopyable_function<ss::future<>(ss::abort_source&)>>
      _reconciliation_executor;
    // Declared last: unsubscribed before the executor its callback references.
    ss::optimized_optional<ss::abort_source::subscription> _as_sub;
};

} // namespace cluster
