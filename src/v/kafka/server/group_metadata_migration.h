
/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/fwd.h"
#include "cluster/types.h"
#include "kafka/server/fwd.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <chrono>

#pragma once

namespace kafka {

struct group_metadata_migration {
public:
    group_metadata_migration(
      cluster::controller&, ss::sharded<kafka::group_router>&);

    // starts the migration process
    ss::future<> start(ss::abort_source&);

    // awaits for the migration to finish
    ss::future<> await();

private:
    static constexpr auto default_timeout = std::chrono::seconds(5);

    static const cluster::feature_barrier_tag preparing_barrier_tag;
    static const cluster::feature_barrier_tag active_barrier_tag;

    model::timeout_clock::time_point default_deadline() {
        return model::timeout_clock::now() + default_timeout;
    }

    ss::future<> do_apply();
    ss::future<> migrate_metadata();
    ss::future<> activate_feature(ss::abort_source&);
    ss::future<> do_activate_feature(ss::abort_source&);

    void dispatch_ntp_migration(model::ntp);

    cluster::feature_table& feature_table();
    cluster::feature_manager& feature_manager();
    ss::abort_source& abort_source();

    cluster::controller& _controller;
    ss::sharded<kafka::group_router>& _group_router;
    ss::gate _partitions_gate;
    ss::gate _background_gate;

    // We need to subscribe on stop_signal as to stop
    // loop inside activate_feature. Because stop_signal
    // does not wait background fiber and will dbe deleted
    // before fiber will be stopped.
    ss::optimized_optional<ss::abort_source::subscription> _sub;
    ss::abort_source _as;
};

} // namespace kafka
