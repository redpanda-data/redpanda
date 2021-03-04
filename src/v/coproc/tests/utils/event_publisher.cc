/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/tests/utils/event_publisher.h"

#include "coproc/logger.h"
#include "coproc/reference_window_consumer.hpp"
#include "coproc/tests/utils/wasm_event_generator.h"
#include "coproc/types.h"
#include "coproc/wasm_event.h"
#include "kafka/protocol/create_topics.h"
#include "kafka/protocol/errors.h"
#include "model/namespace.h"
#include "model/record.h"
#include "storage/parser_utils.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>

namespace {

kafka::client::client make_client() {
    kafka::client::configuration cfg;
    cfg.brokers.set_value(std::vector<unresolved_address>{
      config::shard_local_cfg().kafka_api()[0].address});
    cfg.retries.set_value(size_t(1));
    return kafka::client::client{cfg};
}

} // namespace

namespace coproc::wasm {

class batch_verifier {
public:
    ss::future<ss::stop_iteration> operator()(const model::record_batch& rb) {
        auto f = verify_records(rb);
        if (rb.compressed()) {
            f = storage::internal::decompress_batch(rb).then(
              [this](model::record_batch rb) { return verify_records(rb); });
        }
        return f.then([this](bool valid) {
            _all_valid &= valid;
            return _all_valid ? ss::stop_iteration::no
                              : ss::stop_iteration::yes;
        });
    }

    bool end_of_stream() const {
        if (!_all_valid) {
            vlog(
              coproc::coproclog.error,
              "batch_verifier enountered an invalid batch...");
        }
        return _all_valid;
    }

private:
    ss::future<bool> verify_records(const model::record_batch& rb) const {
        bool result = true;
        co_await model::for_each_record(rb, [&result](const model::record& r) {
            result &= (validate_event(r) == errc::none);
        });
        co_return result;
    }

private:
    /// If at least one event isn't valid the validator will stop early and the
    /// value of this var will be false
    bool _all_valid{true};
};

class publisher {
public:
    explicit publisher(kafka::client::client& client, model::topic_partition tp)
      : _client(client)
      , _publish_tp(std::move(tp)) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch& rb) {
        _results.push_back(
          co_await _client.produce_record_batch(_publish_tp, std::move(rb)));
        co_return ss::stop_iteration::no;
    }

    std::vector<kafka::produce_response::partition> end_of_stream() {
        return std::move(_results);
    }

private:
    std::vector<kafka::produce_response::partition> _results;
    kafka::client::client& _client;
    model::topic_partition _publish_tp;
};

event_publisher::event_publisher()
  : _client{make_client()} {}

ss::future<> event_publisher::start() {
    /// Create the internal topic, THEN update the clients internal metadata so
    /// it can have the correct list of topics per broker
    return _client.connect()
      .then([this] { return create_coproc_internal_topic(); })
      .then([this] { return _client.update_metadata(); });
}

ss::future<> event_publisher::create_coproc_internal_topic() {
    return _client
      .dispatch([]() {
          return kafka::create_topics_request{.data{
            .topics = {kafka::creatable_topic{
              .name = model::coprocessor_internal_topic,
              .num_partitions = 1,
              .replication_factor = 1}},
            .timeout_ms = std::chrono::seconds(2),
            .validate_only = false}};
      })
      .then([](kafka::create_topics_response response) {
          /// Asserting here is better then letting a test timeout, it would be
          /// more difficult to debug the failure in the latter case
          vassert(!response.data.topics.empty(), "Response shouldn't be empty");
          vassert(
            response.data.topics[0].name == model::coprocessor_internal_topic,
            "Expected topic wasn't created");
          vassert(
            response.data.topics[0].error_code == kafka::error_code::none,
            "Error when attempting to create topic");
      });
}

ss::future<wasm::publish_result>
event_publisher::publish_events(model::record_batch_reader reader) {
    return std::move(reader).for_each_ref(
      coproc::reference_window_consumer(
        coproc::wasm::batch_verifier(),
        coproc::wasm::publisher(_client, model::coprocessor_internal_tp)),
      model::no_timeout);
}

ss::future<publish_result>
event_publisher::enable_coprocessors(std::vector<deploy> copros) {
    std::vector<coproc::wasm::event> events;
    events.reserve(copros.size());
    std::transform(
      copros.begin(), copros.end(), std::back_inserter(events), [](deploy& e) {
          return coproc::wasm::event(e.id, std::move(e.data));
      });
    return publish_events(
      coproc::wasm::make_event_record_batch_reader({std::move(events)}));
}

ss::future<publish_result>
event_publisher::disable_coprocessors(std::vector<coproc::script_id> ids) {
    std::vector<coproc::wasm::event> events;
    events.reserve(ids.size());
    std::transform(
      ids.begin(),
      ids.end(),
      std::back_inserter(events),
      [](coproc::script_id id) { return coproc::wasm::event(id); });
    return publish_events(
      coproc::wasm::make_event_record_batch_reader({std::move(events)}));
}

} // namespace coproc::wasm
