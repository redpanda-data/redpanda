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
#include "cluster/fwd.h"
#include "config/property.h"
#include "features/fwd.h"
#include "utils/mutex.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

/**
 * Service responsible for managing transactional coordinator topic.
 *
 * The tx_topic_manager, creates the 'kafka_internal/tx' topic and handles its
 * property updates.
 *
 * The tx_topic_manager is created on shard 0 only.
 */
namespace cluster {

class tx_topic_manager {
public:
    static constexpr ss::shard_id shard = 0;

    tx_topic_manager(
      controller& controller,
      ss::sharded<features::feature_table>& features,
      config::binding<int32_t> partition_count,
      config::binding<uint64_t> segment_size,
      config::binding<std::chrono::milliseconds> retention_duration);

    ss::future<> start();

    ss::future<> stop();

    ss::future<std::error_code> create_and_wait_for_coordinator_topic();

private:
    ss::future<std::error_code> try_create_coordinator_topic();

    void reconcile_topic_properties();

    ss::future<> do_reconcile_topic_properties();

    controller& _controller;
    ss::sharded<features::feature_table>& _features;
    config::binding<int32_t> _partition_count;
    config::binding<uint64_t> _segment_size;
    config::binding<std::chrono::milliseconds> _retention_duration;
    ss::gate _gate;
    mutex _reconciliation_mutex{"tx_topic_manager::reconciliation_mutex"};
};
} // namespace cluster
