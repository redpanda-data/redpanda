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
#include "coproc/api.h"
#include "coproc/logger.h"
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
    cfg.brokers.set_value(
      std::vector<unresolved_address>{config::node().kafka_api()[0].address});
    cfg.retries.set_value(size_t(1));
    cfg.produce_batch_delay.set_value(0ms);
    return std::make_unique<kafka::client::client>(to_yaml(cfg));
}

} // namespace

coproc_test_fixture::coproc_test_fixture() {
    ss::smp::invoke_on_all([]() {
        auto& config = config::shard_local_cfg();
        config.get("coproc_offset_flush_interval_ms").set_value(500ms);
        config.get("enable_coproc").set_value(true);
    }).get0();
    _root_fixture = std::make_unique<redpanda_thread_fixture>();
}

coproc_test_fixture::~coproc_test_fixture() {
    if (_client) {
        _client->stop().get();
    }
}

ss::future<>
coproc_test_fixture::push_wasm_events(std::vector<coproc::wasm::event> events) {
    auto result = co_await coproc::wasm::publish_events(
      *_client,
      coproc::wasm::make_event_record_batch_reader({std::move(events)}));
    vassert(result.size() == 1, "Multiple responses not expected");
    vassert(
      result[0].error_code == kafka::error_code::none,
      "Error when pushing coproc event");
}

ss::future<>
coproc_test_fixture::enable_coprocessors(std::vector<deploy> copros) {
    std::vector<coproc::wasm::event> events;
    std::vector<ss::future<>> wait;
    events.reserve(copros.size());
    for (auto& e : copros) {
        events.emplace_back(e.id, std::move(e.data));
        _active_ids.emplace(coproc::script_id(e.id));
        wait.push_back(wait_for_copro(coproc::script_id(e.id)));
    }
    co_await push_wasm_events(std::move(events));
    co_await ss::when_all_succeed(wait.begin(), wait.end());
}

ss::future<>
coproc_test_fixture::disable_coprocessors(std::vector<uint64_t> ids) {
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
    _client = make_client();
    co_await _client->connect();
    co_await coproc::wasm::create_coproc_internal_topic(*_client);
    for (auto& p : llm) {
        co_await _root_fixture->add_topic(
          model::topic_namespace(model::kafka_namespace, p.first), p.second);
    }
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

template<typename T>
static ss::future<T> do_kafka_request(
  kafka::client::client* c,
  model::timeout_clock::time_point timeout,
  ss::noncopyable_function<ss::future<T>(kafka::client::client*)> fn) {
    while (model::timeout_clock::now() < timeout) {
        auto k_err = kafka::error_code::none;
        try {
            co_return co_await fn(c);
        } catch (const kafka::exception_base& ex) {
            /// Due to the fact that you can't co_await on a future within a
            /// catch block, save the error code for use outside of the block
            k_err = ex.error;
        }
        if (k_err == kafka::error_code::unknown_topic_or_partition) {
            co_await c->update_metadata();
        } else {
            vlog(coproc::coproclog.error, "Kafka client error: {}", k_err);
        }
        co_await ss::sleep(100ms);
    }
    throw ss::timed_out_error();
}

static ss::future<model::offset>
list_topic_offset(kafka::client::client* c, model::topic_partition tp) {
    vlog(coproc::coproclog.info, "Making request to fetch offset: {}", tp);
    auto r = co_await c->list_offsets(tp);
    vassert(r.data.topics.size() == 1, "Expected one response");
    auto& topics = r.data.topics;
    vassert(topics[0].name == tp.topic, "Unexpected response");
    vassert(r.data.topics[0].partitions.size() == 1, "Expected one partition");
    auto& partitions = r.data.topics[0].partitions;
    vassert(
      partitions[0].partition_index == tp.partition, "Unexpected partition id");
    co_return partitions[0].offset;
}

ss::future<model::record_batch_reader::data_t> coproc_test_fixture::consume(
  model::ntp ntp,
  model::offset start_offset,
  model::offset last_offset,
  model::timeout_clock::time_point timeout) {
    /// Waits for all coprocessors to enter an idle state, if they are currently
    /// ingesting/pushing data and the consume occurs, then its highly likley
    /// that less data then expected will be read
    co_await wait_until_all_idle();
    model::topic_partition tp{ntp.tp.topic, ntp.tp.partition};
    if (last_offset == model::model_limits<model::offset>::max()) {
        last_offset = co_await do_kafka_request<model::offset>(
          _client.get(), timeout, [tp](kafka::client::client* c) {
              return list_topic_offset(c, tp);
          });
        vlog(
          coproc::coproclog.info,
          "last_offset {} obtained from ntp: {}",
          last_offset,
          ntp);
    }
    vlog(coproc::coproclog.info, "Making request to fetch from ntp: {}", ntp);
    co_return co_await do_kafka_request<model::record_batch_reader::data_t>(
      _client.get(),
      timeout,
      [tp, start_offset, last_offset, timeout](kafka::client::client* c) {
          auto rbr = kafka::client::make_client_fetch_batch_reader(
            *c, tp, start_offset, last_offset);
          return consume_reader_to_memory(std::move(rbr), timeout);
      });
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

ss::future<> coproc_test_fixture::wait_until_all_idle() {
    std::vector<ss::future<>> fs;
    for (const coproc::script_id id : _active_ids) {
        fs.push_back(wait_until_idle(id).then([id] {
            vlog(coproc::coproclog.info, "Script {} entered idle state", id);
        }));
    }
    co_return co_await ss::when_all_succeed(fs.begin(), fs.end());
}

ss::future<> coproc_test_fixture::wait_until_idle(coproc::script_id id) {
    vlog(coproc::coproclog.info, "Waiting for script {} to be idle", id);
    auto& p = _root_fixture->app.coprocessing->get_pacemaker();
    return p.invoke_on_all([id](coproc::pacemaker& p) {
        return p.wait_idle_state(id).handle_exception_type(
          [](const coproc::exception&) { return ss::now(); });
    });
}

ss::future<> coproc_test_fixture::wait_for_copro(coproc::script_id id) {
    vlog(coproc::coproclog.info, "Waiting for script {}", id);
    auto& p = _root_fixture->app.coprocessing->get_pacemaker();
    auto r = co_await p.map_reduce0(
      [id](coproc::pacemaker& p) { return p.wait_for_script(id); },
      std::vector<coproc::errc>(),
      reduce::push_back());
    bool failed = std::all_of(r.begin(), r.end(), [](coproc::errc e) {
        return e == coproc::errc::topic_does_not_exist;
    });
    if (failed) {
        throw coproc::exception(
          fmt_with_ctx(ssx::sformat, "Failed to deploy script: {}", id));
    }
    vlog(coproc::coproclog.info, "Script {} successfully deployed!");
}
