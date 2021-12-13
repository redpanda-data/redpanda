/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/tests/utils/event_publisher_utils.h"

#include "config/node_config.h"
#include "coproc/logger.h"
#include "coproc/pacemaker.h"
#include "coproc/reference_window_consumer.hpp"
#include "coproc/tests/utils/kafka_publish_consumer.h"
#include "coproc/wasm_event.h"
#include "kafka/protocol/create_topics.h"
#include "storage/parser_utils.h"

#include <seastar/core/coroutine.hh>

namespace coproc::wasm {

class batch_verifier {
public:
    ss::future<ss::stop_iteration> operator()(const model::record_batch& rb) {
        vassert(!rb.compressed(), "Records should not have been compressed");
        co_await model::for_each_record(rb, [this](const model::record& r) {
            auto validate_res = coproc::wasm::validate_event(r);
            _all_valid &= validate_res.has_value();
        });
        co_return _all_valid ? ss::stop_iteration::no : ss::stop_iteration::yes;
    }

    bool end_of_stream() const { return _all_valid; }

private:
    /// If at least one event isn't valid the validator will stop early and the
    /// value of this var will be false
    bool _all_valid{true};
};

ss::future<> create_coproc_internal_topic(kafka::client::client& client) {
    return client
      .create_topic(kafka::creatable_topic{
        .name = model::coprocessor_internal_topic,
        .num_partitions = 1,
        .replication_factor = 1})
      .then([](kafka::create_topics_response response) {
          /// Asserting here is better then letting a test timeout, it would be
          /// more difficult to debug the failure in the latter case
          vassert(!response.data.topics.empty(), "Response shouldn't be empty");
          vassert(
            response.data.topics[0].name == model::coprocessor_internal_topic,
            "Expected topic wasn't created");
          auto ec = response.data.topics[0].error_code;
          vassert(
            ec == kafka::error_code::none,
            "Error when attempting to create topic: {}",
            ec);
      });
}

ss::future<std::vector<kafka::produce_response::partition>> publish_events(
  kafka::client::client& client, model::record_batch_reader reader) {
    /// TODO: our kafka client doesn't support producing compressed batches,
    /// however to emmulate the real situation best we should eventually
    /// have our unit tests do this once support for this lands.
    return std::move(reader)
      .for_each_ref(
        storage::internal::decompress_batch_consumer(), model::no_timeout)
      .then([&client](model::record_batch_reader rbr) {
          return std::move(rbr)
            .for_each_ref(
              coproc::reference_window_consumer(
                coproc::wasm::batch_verifier(),
                kafka_publish_consumer(client, model::coprocessor_internal_tp)),
              model::no_timeout)
            .then([](auto tuple) {
                vassert(std::get<0>(tuple), "crc checks failed");
                return std::move(std::get<1>(tuple)).responses;
            });
      });
}

ss::future<>
wait_for_copro(ss::sharded<coproc::pacemaker>& p, coproc::script_id id) {
    vlog(coproc::coproclog.info, "Waiting for script {}", id);
    auto r = co_await p.map_reduce0(
      [id](coproc::pacemaker& p) { return p.wait_for_script(id); },
      std::vector<coproc::errc>(),
      reduce::push_back());
    bool failed = std::all_of(r.begin(), r.end(), [](coproc::errc e) {
        return e == coproc::errc::topic_does_not_exist;
    });
    if (failed) {
        throw std::runtime_error(
          fmt_with_ctx(ssx::sformat, "Failed to deploy script: {}", id));
    }
    vlog(coproc::coproclog.info, "Script {} successfully deployed!", id);
}

} // namespace coproc::wasm
