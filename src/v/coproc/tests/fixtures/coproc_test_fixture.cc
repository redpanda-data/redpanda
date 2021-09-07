/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/tests/fixtures/coproc_test_fixture.h"

#include "config/configuration.h"
#include "coproc/logger.h"
#include "coproc/tests/fixtures/fixture_utils.h"
#include "kafka/server/materialized_partition.h"
#include "kafka/server/replicated_partition.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "test_utils/async.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

#include <chrono>

coproc_test_fixture::coproc_test_fixture() {
    ss::smp::invoke_on_all([]() {
        auto& config = config::shard_local_cfg();
        config.get("coproc_offset_flush_interval_ms").set_value(500ms);
    }).get0();
    _root_fixture = std::make_unique<redpanda_thread_fixture>();
}

ss::future<>
coproc_test_fixture::enable_coprocessors(std::vector<deploy> copros) {
    std::vector<coproc::wasm::event> events;
    events.reserve(copros.size());
    std::transform(
      copros.begin(), copros.end(), std::back_inserter(events), [](deploy& e) {
          return coproc::wasm::event(e.id, std::move(e.data));
      });
    return _publisher
      .publish_events(
        coproc::wasm::make_event_record_batch_reader({std::move(events)}))
      .discard_result();
}

ss::future<>
coproc_test_fixture::disable_coprocessors(std::vector<uint64_t> ids) {
    std::vector<coproc::wasm::event> events;
    events.reserve(ids.size());
    std::transform(
      ids.begin(), ids.end(), std::back_inserter(events), [](uint64_t id) {
          return coproc::wasm::event(id);
      });
    return _publisher
      .publish_events(
        coproc::wasm::make_event_record_batch_reader({std::move(events)}))
      .discard_result();
}

ss::future<> coproc_test_fixture::setup(log_layout_map llm) {
    co_await _root_fixture->wait_for_controller_leadership();
    co_await _publisher.start();
    for (auto& p : llm) {
        co_await _root_fixture->add_topic(
          model::topic_namespace(model::kafka_namespace, p.first), p.second);
    }
}

ss::future<> coproc_test_fixture::restart() {
    auto data_dir = _root_fixture->data_dir;
    _root_fixture->remove_on_shutdown = false;
    _root_fixture = nullptr;
    _root_fixture = std::make_unique<redpanda_thread_fixture>(
      std::move(data_dir));
    co_await _root_fixture->wait_for_controller_leadership();
}

/// TODO: Code duplication remove after a rebase of dev
static std::optional<kafka::partition_proxy> make_partition_proxy(
  const model::materialized_ntp& ntp,
  ss::lw_shared_ptr<cluster::partition> partition,
  cluster::partition_manager& pm) {
    if (!ntp.is_materialized()) {
        return kafka::make_partition_proxy<kafka::replicated_partition>(
          partition);
    }
    if (auto log = pm.log(ntp.input_ntp()); log) {
        return kafka::make_partition_proxy<kafka::materialized_partition>(*log);
    }
    return std::nullopt;
}

ss::future<std::optional<model::record_batch_reader::data_t>>
coproc_test_fixture::drain(
  const model::ntp& ntp,
  std::size_t limit,
  model::offset offset,
  model::timeout_clock::time_point timeout) {
    const auto m_ntp = model::materialized_ntp(std::move(ntp));
    auto shard_id = _root_fixture->app.shard_table.local().shard_for(
      m_ntp.source_ntp());
    if (!shard_id) {
        vlog(
          coproc::coproclog.error,
          "No ntp exists, cannot drain from ntp: {}",
          m_ntp.input_ntp());
        return ss::make_ready_future<
          std::optional<model::record_batch_reader::data_t>>(std::nullopt);
    }
    vlog(
      coproc::coproclog.info,
      "searching for ntp {} on shard id {} ...with value for limit: {}",
      m_ntp.input_ntp(),
      *shard_id,
      limit);
    return _root_fixture->app.partition_manager.invoke_on(
      *shard_id,
      [m_ntp, limit, offset, timeout](cluster::partition_manager& pm) {
          return tests::cooperative_spin_wait_with_timeout(
                   60s,
                   [&pm, m_ntp] {
                       auto partition = pm.get(m_ntp.source_ntp());
                       return partition && partition->is_leader()
                              && (!m_ntp.is_materialized() || pm.log(m_ntp.input_ntp()));
                   })
            .then([&pm, m_ntp, limit, offset, timeout] {
                auto partition = pm.get(m_ntp.source_ntp());
                auto partition_proxy = make_partition_proxy(
                  m_ntp, partition, pm);
                return do_drain(
                         offset,
                         limit,
                         timeout,
                         [pp = std::move(partition_proxy)](
                           model::offset next_offset) mutable {
                             return pp->make_reader(log_rdr_cfg(next_offset));
                         })
                  .then([](auto data) {
                      return std::optional<model::record_batch_reader::data_t>(
                        std::move(data));
                  });
            });
      });
}

ss::future<model::offset> coproc_test_fixture::push(
  const model::ntp& ntp, model::record_batch_reader rbr) {
    auto shard_id = _root_fixture->app.shard_table.local().shard_for(ntp);
    if (!shard_id) {
        vlog(
          coproc::coproclog.error,
          "No ntp exists, cannot push data to log: {}",
          ntp);
        return ss::make_ready_future<model::offset>(model::offset(-1));
    }
    vlog(
      coproc::coproclog.info,
      "Pushing record_batch_reader to ntp: {} on shard_id: {}",
      ntp,
      *shard_id);
    return _root_fixture->app.partition_manager.invoke_on(
      *shard_id,
      [ntp, rbr = std::move(rbr)](cluster::partition_manager& pm) mutable {
          auto partition = pm.get(ntp);
          return tests::cooperative_spin_wait_with_timeout(
                   60s,
                   [partition] { return partition && partition->is_leader(); })
            .then([rbr = std::move(rbr), partition]() mutable {
                return partition
                  ->replicate(
                    std::move(rbr),
                    raft::replicate_options(
                      raft::consistency_level::quorum_ack))
                  .then([](auto r) { return r.value().last_offset; });
            });
      });
}
