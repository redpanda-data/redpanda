/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "container/chunked_hash_map.h"
#include "kafka/client/assignment_plans.h"
#include "kafka/client/broker.h"
#include "kafka/client/brokers.h"
#include "kafka/client/configuration.h"
#include "kafka/client/fetch_session.h"
#include "kafka/client/topic_cache.h"
#include "kafka/protocol/fetch.h"
#include "model/fundamental.h"

#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/node_hash_map.h>
#include <absl/hash/hash.h>

#include <optional>

namespace kafka::client {

// consumer manages the lifetime of a consumer with static partition assignments
class manual_consumer final
  : public ss::enable_lw_shared_from_this<manual_consumer> {
    using broker_reqs_t = absl::node_hash_map<shared_broker_t, fetch_request>;

public:
    struct cursor {
        model::offset offset;
    };
    using cursors = chunked_hash_map<
      model::topic,
      chunked_hash_map<model::partition_id, cursor>>;

    manual_consumer(
      const configuration& config,
      topic_cache& topic_cache,
      brokers& brokers,
      cursors cursors,
      ss::noncopyable_function<ss::future<>(std::exception_ptr)> mitigater);

    void upsert_partition(model::topic_partition, model::offset);
    void remove_partition(model::topic_partition);

    ss::future<fetch_response> fetch();

private:
    ss::future<> stop();

    ss::future<> await_mitigate();
    void start_mitigate(std::exception_ptr eptr);

    ss::future<fetch_response> do_fetch();
    ss::future<fetch_response> dispatch_fetch(shared_broker_t, fetch_request);

    // The base template for handling response errors
    template<typename request_factory, typename response_t>
    ss::future<response_t>
    maybe_process_response_errors(request_factory req, response_t res);

    // Some template specializations for handling response errors of specific
    // response types
    template<typename request_factory>
    ss::future<metadata_response>
    maybe_process_response_errors(request_factory req, metadata_response res);

    const configuration& _config;
    topic_cache& _topic_cache;
    brokers& _brokers;
    ss::abort_source _as;
    ss::gate _gate{};

    cursors _cursors{};
    absl::node_hash_map<shared_broker_t, fetch_session> _fetch_sessions;
    ss::noncopyable_function<ss::future<>(std::exception_ptr)>
      _external_mitigate;
    std::optional<ss::shared_promise<>> _mitigation_promise{std::nullopt};
};

using shared_manual_consumer_t = ss::lw_shared_ptr<manual_consumer>;

} // namespace kafka::client
