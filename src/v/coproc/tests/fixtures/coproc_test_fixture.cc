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

#include "coproc/tests/fixtures/coproc_test_fixture.h"

#include "config/configuration.h"
#include "coproc/api.h"
#include "coproc/event_handler.h"
#include "coproc/logger.h"
#include "coproc/pacemaker.h"
#include "coproc/tests/utils/event_publisher_utils.h"
#include "coproc/tests/utils/kafka_publish_consumer.h"
#include "kafka/client/client.h"
#include "kafka/client/client_fetch_batch_reader.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "test_utils/async.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

#include <chrono>
#include <variant>

namespace {

std::unique_ptr<kafka::client::client> make_client() {
    kafka::client::configuration cfg;
    cfg.brokers.set_value(std::vector<net::unresolved_address>{
      config::node().kafka_api()[0].address});
    cfg.retries.set_value(size_t(5));
    cfg.retry_base_backoff.set_value(10ms);
    cfg.produce_batch_delay.set_value(0ms);
    return std::make_unique<kafka::client::client>(
      to_yaml(cfg, config::redact_secrets::no));
}

} // namespace

coproc_api_fixture::coproc_api_fixture() {
    ss::smp::invoke_on_all([]() {
        auto& config = config::shard_local_cfg();
        config.get("coproc_offset_flush_interval_ms").set_value(500ms);
        config.get("enable_coproc").set_value(true);
    }).get0();
}

coproc_api_fixture::~coproc_api_fixture() {
    if (_client) {
        _client->stop().get();
    }
}

ss::future<> coproc_api_fixture::start() {
    try {
        _client = make_client();
        co_await _client->connect();
    } catch (const kafka::client::broker_error& ex) {
        throw std::runtime_error(
          ssx::sformat("Coproc fixture failed to connect to broker: {}", ex));
    }
    co_await coproc::wasm::create_coproc_internal_topic(*_client);
}

ss::future<>
coproc_api_fixture::push_wasm_events(std::vector<coproc::wasm::event> events) {
    auto result = co_await coproc::wasm::publish_events(
      *_client,
      coproc::wasm::make_event_record_batch_reader({std::move(events)}));
    vassert(result.size() == 1, "Multiple responses not expected");
    vassert(
      result[0].error_code == kafka::error_code::none,
      "Error when pushing coproc event: {}",
      result[0].error_code);
}

ss::future<>
coproc_api_fixture::enable_coprocessors(std::vector<deploy> copros) {
    std::vector<coproc::wasm::event> events;
    std::vector<ss::future<>> wait;
    events.reserve(copros.size());
    for (auto& e : copros) {
        events.emplace_back(e.id, std::move(e.data));
        _active_ids.emplace(coproc::script_id(e.id));
    }
    co_await push_wasm_events(std::move(events));
    co_await ss::when_all_succeed(wait.begin(), wait.end());
}

ss::future<>
coproc_api_fixture::disable_coprocessors(std::vector<uint64_t> ids) {
    std::vector<coproc::wasm::event> events;
    events.reserve(ids.size());
    for (auto id : ids) {
        events.emplace_back(coproc::wasm::event(id));
        _active_ids.erase(coproc::script_id(id));
    }
    co_await push_wasm_events(std::move(events));
}

ss::future<> coproc_test_fixture::setup(log_layout_map llm) {
    co_await _root_fixture->wait_for_controller_leadership();
    co_await coproc_api_fixture::start();
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

static ss::future<model::offset>
list_topic_offset(kafka::client::client& c, model::topic_partition tp) {
    vlog(coproc::coproclog.info, "Making request to fetch offset: {}", tp);
    auto r = co_await c.list_offsets(tp);
    vassert(r.data.topics.size() == 1, "Expected one response");
    auto& topics = r.data.topics;
    vassert(topics[0].name == tp.topic, "Unexpected response");
    vassert(r.data.topics[0].partitions.size() == 1, "Expected one partition");
    auto& partitions = r.data.topics[0].partitions;
    vassert(
      partitions[0].partition_index == tp.partition, "Unexpected partition id");
    co_return partitions[0].offset;
}

class no_new_data_error final : public std::exception {};

ss::future<model::record_batch_reader::data_t> coproc_api_fixture::do_consume(
  model::topic_partition tp,
  model::offset start_offset,
  std::size_t records_read,
  model::timeout_clock::time_point timeout) {
    if (model::timeout_clock::now() > timeout) {
        throw ss::timed_out_error();
    }
    auto last_offset = co_await list_topic_offset(get_client(), tp);
    if (last_offset <= start_offset) {
        throw no_new_data_error();
    }
    vlog(
      coproc::coproclog.info,
      "Making request to fetch from tp: {}, start_offset: {}, "
      "last_offset: {}, records_read: {}",
      tp,
      start_offset,
      last_offset,
      records_read);
    auto rbr = kafka::client::make_client_fetch_batch_reader(
      *_client, tp, start_offset, last_offset);
    co_return co_await consume_reader_to_memory(std::move(rbr), timeout);
}

ss::future<model::record_batch_reader::data_t> coproc_api_fixture::consume(
  model::ntp ntp,
  std::size_t limit,
  model::offset start_offset,
  model::timeout_clock::time_point timeout) {
    model::topic_partition tp{ntp.tp.topic, ntp.tp.partition};
    model::record_batch_reader::data_t batches;
    std::size_t records_read = 0;
    while (records_read < limit) {
        bool need_sleep = false;
        try {
            auto nbatches = co_await do_consume(
              tp, start_offset, records_read, timeout);
            start_offset = ++nbatches.back().last_offset();
            for (auto& rb : nbatches) {
                records_read += rb.record_count();
                batches.push_back(std::move(rb));
            }
        } catch (kafka::exception_base& ex) {
            vlog(
              coproc::coproclog.error,
              "Kafka client error: {} - code: {}",
              ex,
              ex.error);
        } catch (const no_new_data_error&) {
            need_sleep = true;
        }
        if (need_sleep) {
            vlog(coproc::coproclog.info, "{}, sleeping 100ms", tp);
            co_await ss::sleep(100ms);
        }
    }
    co_return batches;
}

ss::future<>
coproc_api_fixture::produce(model::ntp ntp, model::record_batch_reader rbr) {
    vlog(coproc::coproclog.info, "About to produce to ntp: {}", ntp);
    auto result = co_await std::move(rbr).for_each_ref(
      kafka_publish_consumer(
        *_client, model::topic_partition{ntp.tp.topic, ntp.tp.partition}),
      model::no_timeout);
    for (const auto& r : result.responses) {
        vassert(
          r.error_code == kafka::error_code::none,
          "Produce did not occur: {}",
          r.error_code);
    }
}

ss::future<>
coproc_test_fixture::enable_coprocessors(std::vector<deploy> copros) {
    std::vector<coproc::script_id> ids;
    std::transform(
      copros.cbegin(),
      copros.cend(),
      std::back_inserter(ids),
      [](const deploy& d) { return coproc::script_id(d.id); });
    co_await coproc_api_fixture::enable_coprocessors(std::move(copros));
    co_await ss::parallel_for_each(ids, [this](coproc::script_id id) {
        auto& p = _root_fixture->app.coprocessing->get_pacemaker();
        return coproc::wasm::wait_for_copro(p, id);
    });
}
