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
#include "base/seastarx.h"
#include "kafka/client/direct_consumer/direct_consumer.h"
#include "src/v/kafka/client/direct_consumer/verifier/proto/verifier.proto.h"

#include <seastar/http/api_docs.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>
#include <seastar/util/log.hh>
namespace kafka::client {
inline ss::logger v_logger("dc-verifier");

struct partition_stats {
    kafka::offset last_fetched_offset;
    size_t fetched_records = 0;
    size_t fetched_bytes = 0;
    model::timestamp last_fetched_timestamp;
    kafka::error_code last_error_code = kafka::error_code::none;
};

/**
 * Consumer process responsible for managing a single direct consumer instance.
 * It encapsulates the cluster and consumer, providing methods to start and stop
 * them.
 * The runner is executing a single consumer fetch loop and maintains consumer
 * stats.
 */
struct consumer_runner {
    consumer_runner(connection_configuration, direct_consumer::configuration);

    ss::future<> start();
    ss::future<> stop();

    ss::future<>
    assign_partitions(chunked_vector<topic_assignment> assignments) {
        return _consumer->assign_partitions(std::move(assignments));
    }

    ss::future<> unassign_partitions(
      chunked_vector<model::topic_partition> topic_partitions) {
        return _consumer->unassign_partitions(std::move(topic_partitions));
    }

    void update_configuration(direct_consumer::configuration cfg) {
        _consumer->update_configuration(std::move(cfg));
    }

    // Getters for stats access
    size_t total_bytes() const { return _total_bytes; }
    size_t total_records() const { return _total_records; }
    kafka::error_code last_error_code() const { return _last_error_code; }
    const topic_partition_map<partition_stats>& stats() const { return _stats; }
    size_t non_monotonic_fetches() const { return _non_monotonic_fetches; }

private:
    ss::future<> do_fetch();
    void report_stats();
    kafka::client_id _id;
    std::unique_ptr<cluster> _cluster;
    std::unique_ptr<direct_consumer> _consumer;
    topic_partition_map<partition_stats> _stats;
    ss::gate _gate;
    ss::abort_source _abort_source;
    ss::timer<> _reporter;
    size_t _total_bytes = 0;
    size_t _total_records = 0;

    size_t _last_reported_bytes = 0;
    size_t _last_reported_records = 0;
    size_t _non_monotonic_fetches = 0;
    kafka::error_code _last_error_code = kafka::error_code::none;
    std::chrono::steady_clock::time_point _last_report_time
      = std::chrono::steady_clock::now();
};

/**
 * The service is the heart of the verifier application. It manages the
 * lifecycle of direct consumers and handles requests to create and manage them.
 */
class verifier_service_impl : public kafka::client::verifier::verifier_service {
public:
    verifier_service_impl() = default;

    ss::future<verifier::api_response>
      create_consumer(verifier::create_direct_consumer_request) final;

    ss::future<verifier::api_response>
      assign_partitions(verifier::assign_partitions_request) final;

    ss::future<verifier::api_response>
      unassign_partitions(verifier::unassign_partitions_request) final;

    ss::future<verifier::consumer_state_response>
      get_consumer_state(verifier::get_consumer_state_request) final;

    seastar::future<verifier::api_response>
      status(verifier::status_request) final;

    ss::future<> stop();

private:
    chunked_hash_map<kafka::client_id, consumer_runner> _consumers;
};

/**
 * HTTP server listener configuration
 */
struct listener_configuration {
    ss::sstring host;
    int16_t port;
};

using request_handler_fn
  = ss::noncopyable_function<ss::future<std::unique_ptr<ss::http::reply>>(
    std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply>)>;
/**
 * Class responsible for setting up the HTTP server and dispatching requests
 * to the verifier service.
 */
class verifier_server {
public:
    explicit verifier_server(listener_configuration, verifier_service_impl*);

    ss::future<> start();
    ss::future<> stop();

private:
    using handler_fun
      = std::function<ss::future<std::unique_ptr<ss::http::reply>>(
        std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply>)>;
    struct handler_impl : public ss::httpd::handler_base {
        using handler_base::handler_base;

        explicit handler_impl(verifier_server*, handler_fun);

        ss::future<std::unique_ptr<ss::http::reply>> handle(
          const ss::sstring& path,
          std::unique_ptr<ss::http::request> req,
          std::unique_ptr<ss::http::reply> reply) final;

        verifier_server* server;
        handler_fun handler;
    };

    void setup_routes();

    ss::shard_id client_shard_id() const;
    ss::gate _gate;
    listener_configuration _config;
    ss::httpd::http_server _httpd;
    verifier_service_impl* _service;
};

class verifier_application {
public:
    ss::future<int> run(listener_configuration conf);

private:
    // Currently for simplicity, we run a verfier service on a single shard.
    std::unique_ptr<verifier_service_impl> _service;
    ss::sharded<verifier_server> _server;
};
} // namespace kafka::client
