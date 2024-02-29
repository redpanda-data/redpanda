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

#include "cluster/tx_topic_manager.h"

#include "base/vassert.h"
#include "cluster/controller.h"
#include "cluster/controller_api.h"
#include "cluster/topics_frontend.h"
#include "features/feature_table.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"
#include "tristate.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>

#include <system_error>

namespace cluster {

namespace {
static constexpr auto topic_operation_timeout = 20s;

} // namespace

tx_topic_manager::tx_topic_manager(
  controller& controller,
  ss::sharded<features::feature_table>& features,
  config::binding<int32_t> partition_count,
  config::binding<uint64_t> segment_size,
  config::binding<std::chrono::milliseconds> retention_duration)
  : _controller(controller)
  , _features(features)
  , _partition_count(std::move(partition_count))
  , _segment_size(std::move(segment_size))
  , _retention_duration(std::move(retention_duration)) {}

ss::future<> tx_topic_manager::start() { co_return; }

ss::future<> tx_topic_manager::stop() { return _gate.close(); }

ss::future<std::error_code> tx_topic_manager::try_create_coordinator_topic() {
    const int32_t partition_count
      = _features.local().is_active(features::feature::transaction_partitioning)
          ? _partition_count()
          : 1;

    cluster::topic_configuration topic_cfg{
      model::kafka_internal_namespace,
      model::tx_manager_topic,
      partition_count,
      _controller.internal_topic_replication()};

    topic_cfg.properties.segment_size = _segment_size();
    topic_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>(_retention_duration());

    topic_cfg.properties.cleanup_policy_bitflags
      = config::shard_local_cfg().transaction_coordinator_cleanup_policy();

    vlog(
      txlog.info,
      "Creating transaction manager topic {} with {} partitions",
      model::tx_manager_nt,
      partition_count);

    return _controller.get_topics_frontend()
      .local()
      .autocreate_topics(
        {std::move(topic_cfg)},
        config::shard_local_cfg().create_topic_timeout_ms() * partition_count)
      .then([](std::vector<cluster::topic_result> results) {
          vassert(
            results.size() == 1,
            "Expected one result related with tx manager topic creation, "
            "received answer with {} results",
            results.size());
          const auto& result = results[0];
          return make_error_code(result.ec);
      })
      .handle_exception([](std::exception_ptr e) {
          vlog(
            txlog.warn,
            "Error creating tx manager topic {} - {}",
            model::tx_manager_nt,
            e);

          return make_error_code(errc::topic_operation_error);
      });
}

ss::future<std::error_code>
tx_topic_manager::create_and_wait_for_coordinator_topic() {
    auto ec = co_await try_create_coordinator_topic();
    if (!(ec == errc::success || ec == errc::topic_already_exists)) {
        vlog(
          txlog.warn,
          "Error creating tx manager topic {} - {}",
          model::tx_manager_nt,
          ec.message());
        co_return errc::topic_not_exists;
    }

    try {
        auto ec = co_await _controller.get_api().local().wait_for_topic(
          model::tx_manager_nt,
          topic_operation_timeout + model::timeout_clock::now());

        if (ec) {
            vlog(
              txlog.warn,
              "Error waiting for transaction manager topic {} to be created "
              "- "
              "{}",
              model::tx_manager_nt,
              ec);
            // topic is creating, reply with not_coordinator error for
            // the client to retry
            co_return tx_errc::partition_not_exists;
        }
    } catch (const ss::timed_out_error& e) {
        vlog(
          txlog.warn,
          "Timeout waiting for transaction manager topic {} to be created - "
          "{}",
          model::tx_manager_nt,
          e);
        co_return errc::timeout;
    }

    co_return errc::success;
}

} // namespace cluster
