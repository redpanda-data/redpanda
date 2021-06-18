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
#include "kafka/client/publish_batch_consumer.h"
#include "kafka/protocol/create_topics.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/produce.h"
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
    return kafka::client::client{to_yaml(cfg)};
}

} // namespace

namespace coproc::wasm {

class batch_verifier {
public:
    ss::future<ss::stop_iteration> operator()(const model::record_batch& rb) {
        vassert(!rb.compressed(), "Records should not have been compressed");
        co_await model::for_each_record(rb, [this](const model::record& r) {
            _all_valid &= (validate_event(r) == errc::none);
        });
        co_return _all_valid ? ss::stop_iteration::no : ss::stop_iteration::yes;
    }

    bool end_of_stream() const { return _all_valid; }

private:
    /// If at least one event isn't valid the validator will stop early and the
    /// value of this var will be false
    bool _all_valid{true};
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

ss::future<std::vector<kafka::produce_response::partition>>
event_publisher::publish_events(model::record_batch_reader reader) {
    /// TODO: our kafka client doesn't support producing compressed batches,
    /// however to emmulate the real situation best we should eventually
    /// have our unit tests do this once support for this lands.
    return std::move(reader)
      .for_each_ref(
        storage::internal::decompress_batch_consumer(), model::no_timeout)
      .then([this](model::record_batch_reader rbr) {
          return std::move(rbr)
            .for_each_ref(
              coproc::reference_window_consumer(
                coproc::wasm::batch_verifier(),
                kafka::client::publish_batch_consumer(
                  _client, model::coprocessor_internal_tp)),
              model::no_timeout)
            .then([](auto tuple) {
                vassert(std::get<0>(tuple), "crc checks failed");
                return std::move(std::get<1>(tuple));
            });
      });
}

} // namespace coproc::wasm
