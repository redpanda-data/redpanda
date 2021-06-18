/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "kafka/client/client.h"
#include "kafka/protocol/produce.h"

#include <seastar/core/coroutine.hh>

namespace kafka::client {

class publish_batch_consumer {
public:
    publish_batch_consumer(client& client, model::topic_partition tp)
      : _client(client)
      , _publish_tp(std::move(tp)) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch& rb) {
        /// TODO: Better error handling
        _results.push_back(
          co_await _client.produce_record_batch(_publish_tp, std::move(rb)));
        co_return ss::stop_iteration::no;
    }

    std::vector<kafka::produce_response::partition> end_of_stream() {
        return std::move(_results);
    }

private:
    std::vector<kafka::produce_response::partition> _results;
    client& _client;
    model::topic_partition _publish_tp;
};

} // namespace kafka::client
