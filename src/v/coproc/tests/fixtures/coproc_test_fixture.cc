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
#include "kafka/server/partition_proxy.h"
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

static ss::future<std::optional<ss::shard_id>>
wait_for_partition(const model::ntp& ntp, cluster::shard_table& shard_table) {
    return ss::do_with(
      std::optional<ss::shard_id>(),
      [ntp, &shard_table](std::optional<ss::shard_id>& shard) {
          return tests::cooperative_spin_wait_with_timeout(
                   60s,
                   [ntp, &shard, &shard_table] {
                       shard = shard_table.shard_for(ntp);
                       return shard.has_value();
                   })
            .then([&shard] { return shard; });
      });
}

ss::future<std::optional<model::record_batch_reader::data_t>>
coproc_test_fixture::drain(
  model::ntp ntp,
  std::size_t limit,
  model::offset offset,
  model::timeout_clock::time_point timeout) {
    auto shard_id = co_await wait_for_partition(
      ntp, _root_fixture->app.shard_table.local());
    if (!shard_id) {
        vlog(
          coproc::coproclog.error,
          "No ntp exists, cannot drain from ntp: {}",
          ntp);
        co_return std::nullopt;
    }
    vlog(
      coproc::coproclog.info,
      "searching for ntp {} on shard id {} ...with value for limit: {}",
      ntp,
      *shard_id,
      limit);
    using ret_t = std::optional<model::record_batch_reader::data_t>;
    auto& cache = _root_fixture->app.metadata_cache;
    co_return co_await _root_fixture->app.partition_manager.invoke_on(
      *shard_id,
      [this, ntp, limit, offset, timeout, &cache](
        cluster::partition_manager& pm) -> ss::future<ret_t> {
          auto partition_proxy = kafka::make_partition_proxy(
            ntp, cache.local(), pm);
          auto r = co_await do_drain(
            offset,
            limit,
            timeout,
            [partition_proxy = std::move(partition_proxy)](
              model::offset next_offset) mutable {
                return partition_proxy->make_reader(log_rdr_cfg(next_offset));
            });
          co_return ret_t(std::move(r));
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
