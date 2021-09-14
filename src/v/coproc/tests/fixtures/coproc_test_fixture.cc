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
#include "coproc/tests/utils/kafka_publish_consumer.h"
#include "kafka/client/client.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "test_utils/async.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

#include <chrono>

namespace {

std::unique_ptr<kafka::client::client> make_client() {
    kafka::client::configuration cfg;
    cfg.brokers.set_value(std::vector<unresolved_address>{
      config::shard_local_cfg().kafka_api()[0].address});
    cfg.retries.set_value(size_t(1));
    return std::make_unique<kafka::client::client>(to_yaml(cfg));
}

} // namespace

coproc_test_fixture::coproc_test_fixture() {
    ss::smp::invoke_on_all([]() {
        auto& config = config::shard_local_cfg();
        config.get("coproc_offset_flush_interval_ms").set_value(500ms);
    }).get0();
    _root_fixture = std::make_unique<redpanda_thread_fixture>();
}

coproc_test_fixture::~coproc_test_fixture() {
    if (_client) {
        _client->stop().get();
    }
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
    _client = make_client();
    co_await _client->connect();
    co_await _client->update_metadata();
}

ss::future<> coproc_test_fixture::restart() {
    auto data_dir = _root_fixture->data_dir;
    _root_fixture->remove_on_shutdown = false;
    _root_fixture = nullptr;
    _root_fixture = std::make_unique<redpanda_thread_fixture>(
      std::move(data_dir));
    co_await _root_fixture->wait_for_controller_leadership();
}

ss::future<ss::stop_iteration> coproc_test_fixture::fetch_partition(
  model::record_batch_reader::data_t& events,
  model::offset& o,
  model::topic_partition tp) {
    auto response = co_await _client->fetch_partition(tp, o, 64_KiB, 1000ms);
    if (
      response.data.error_code
      == kafka::error_code::unknown_topic_or_partition) {
        co_await ss::sleep(50ms);
        co_await _client->update_metadata();
        co_return ss::stop_iteration::no;
    }
    if (response.data.error_code != kafka::error_code::none) {
        vlog(
          coproc::coproclog.info,
          "error: {}, {}",
          tp,
          response.data.error_code);
        co_return ss::stop_iteration::yes;
    }
    vassert(response.data.topics.size() == 1, "Unexpected partition size");
    auto& p = response.data.topics[0];
    vassert(p.partitions.size() == 1, "Unexpected responses size");
    auto& pr = p.partitions[0];
    if (pr.error_code == kafka::error_code::none) {
        auto rbr = make_record_batch_reader<kafka::batch_reader>(
          std::move(*pr.records));
        auto batches = co_await consume_reader_to_memory(
          std::move(rbr), model::no_timeout);
        for (auto& batch : batches) {
            events.push_back(std::move(batch));
            o = events.back().last_offset() + model::offset(1);
        }
    }
    co_return ss::stop_iteration::no;
}

ss::future<std::optional<model::record_batch_reader::data_t>>
coproc_test_fixture::drain(
  model::ntp ntp,
  std::size_t limit,
  model::offset offset,
  model::timeout_clock::time_point timeout) {
    vlog(coproc::coproclog.info, "Making request to fetch from ntp: {}", ntp);
    model::topic_partition tp{ntp.tp.topic, ntp.tp.partition};
    model::record_batch_reader::data_t events;
    ss::stop_iteration stop{};
    auto start_time = model::timeout_clock::now();
    while ((stop != ss::stop_iteration::yes) && (events.size() < limit)
           && (start_time < timeout)) {
        stop = co_await fetch_partition(events, offset, tp);
        start_time = model::timeout_clock::now();
    }
    co_return std::optional<model::record_batch_reader::data_t>(
      std::move(events));
}

ss::future<model::offset>
coproc_test_fixture::push(model::ntp ntp, model::record_batch_reader rbr) {
    vlog(coproc::coproclog.info, "About to produce to ntp: {}", ntp);
    auto result = co_await std::move(rbr).for_each_ref(
      kafka_publish_consumer(
        *_client, model::topic_partition{ntp.tp.topic, ntp.tp.partition}),
      model::no_timeout);
    for (const auto& r : result.responses) {
        vassert(
          r.error_code != kafka::error_code::unknown_topic_or_partition,
          "Input logs should already exist, use setup() before starting test");
    }
    co_return result.last_offset;
}
