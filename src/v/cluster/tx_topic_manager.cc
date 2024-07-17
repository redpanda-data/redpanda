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
#include "cluster/logger.h"
#include "cluster/topics_frontend.h"
#include "features/feature_table.h"
#include "model/namespace.h"
#include "ssx/future-util.h"
#include "utils/tristate.h"

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

ss::future<> tx_topic_manager::start() {
    co_await do_reconcile_topic_properties();

    _segment_size.watch([this] { reconcile_topic_properties(); });
    _retention_duration.watch([this] { reconcile_topic_properties(); });
}

void tx_topic_manager::reconcile_topic_properties() {
    ssx::spawn_with_gate(
      _gate, [this] { return do_reconcile_topic_properties(); });
}

ss::future<> tx_topic_manager::do_reconcile_topic_properties() {
    /**
     * We hold mutex to make sure only one instance of reconciliation loop is
     * active. Properties update evens are asynchronous hence there would be a
     * possibility to have more than one reconciliation process active.
     */
    auto u = co_await _reconciliation_mutex.get_units();

    vlog(txlog.trace, "Reconciling tx manager topic properties");
    auto tp_md = _controller.get_topics_state().local().get_topic_metadata_ref(
      model::tx_manager_nt);
    // nothing to do, topic does not exists
    if (!tp_md) {
        vlog(
          txlog.trace,
          "Transactional manager topic does not exist. Skipping "
          "reconciliation");
        co_return;
    }

    const auto& topic_properties
      = tp_md.value().get().get_configuration().properties;
    const bool needs_update
      = topic_properties.retention_duration
          != tristate<std::chrono::milliseconds>(_retention_duration())
        || topic_properties.segment_size != _segment_size();

    if (!needs_update) {
        vlog(
          txlog.trace,
          "Transactional manager topic does not need properties update");
        co_return;
    }

    topic_properties_update topic_update(model::tx_manager_nt);
    topic_update.properties.retention_duration.value
      = tristate<std::chrono::milliseconds>(_retention_duration());
    topic_update.properties.retention_duration.op
      = incremental_update_operation::set;
    topic_update.properties.segment_size.value = _segment_size();
    topic_update.properties.segment_size.op = incremental_update_operation::set;
    try {
        vlog(
          txlog.info,
          "Updating properties of transactional manager topic with: {}",
          topic_update);

        auto results = co_await _controller.get_topics_frontend()
                         .local()
                         .update_topic_properties(
                           {std::move(topic_update)},
                           topic_operation_timeout
                             + model::timeout_clock::now());
        vassert(
          results.size() == 1,
          "Transaction topic manager update properties requests contains only "
          "one topic therefore one result is expected, actual results: {}",
          results.size());

        const auto& result = results[0];
        if (result.ec == errc::success) {
            co_return;
        }
        vlog(
          txlog.warn,
          "Unable to update transaction manager topic properties - {}",
          make_error_code(result.ec).message());
    } catch (...) {
        vlog(
          txlog.warn,
          "Unable to update transaction manager topic properties - {}",
          std::current_exception());
    }
    /**
     * In case of an error, retry after a while
     */
    if (!_gate.is_closed()) {
        co_await ss::sleep_abortable(
          10s, _controller.get_abort_source().local());

        reconcile_topic_properties();
    }
}

ss::future<> tx_topic_manager::stop() { return _gate.close(); }

ss::future<std::error_code> tx_topic_manager::try_create_coordinator_topic() {
    const int32_t partition_count = _partition_count();
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
            co_return tx::errc::partition_not_exists;
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
