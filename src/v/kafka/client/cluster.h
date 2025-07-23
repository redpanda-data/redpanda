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
#include "kafka/client/brokers.h"
#include "kafka/client/topic_cache.h"
#include "utils/prefix_logger.h"
namespace kafka::client {
/**
 * Class representing a cluster metadata. This class is responsible for
 * managing broker connections and handling metadata updates. It also keeps a
 * cache of the cluster topics state.
 */
class cluster {
public:
    explicit cluster(connection_configuration config);

    ss::future<> start();
    ss::future<> stop();

    /**
     * Provides an access to the cluster topic cache.
     *   TODO: make it const& to avoid accidental
     *   modifications of the internal state.
     */
    topic_cache& get_topics() { return _topic_cache; }
    /**
     *   Provides an access to the  cluster brokers connections.
     *
     *   TODO: make it const& to avoid accidental
     *   modifications of the internal state.
     */
    brokers& get_brokers() { return _brokers; }
    /**
     * Dispatches a request to a specific broker identified by `broker_id`.
     * If the broker is not found, it throws a `broker_error`.
     *
     * NOTE: this method signature is about to change when we are going to
     * implement version negotiation, it will include the api_version.
     */
    template<typename Req, typename Ret = typename Req::api_type::response_type>
    requires(KafkaApi<typename Req::api_type>)
    ss::future<Ret> dispatch_to(model::node_id broker_id, Req request) {
        auto broker = _brokers.find(broker_id);
        if (!broker) {
            throw broker_error(
              broker_id, error_code::broker_not_available, "Broker not found");
        }
        return broker->dispatch(std::move(request));
    }
    /**
     * Dispatches a request to a randomly selected broker from the connected
     * brokers. If no brokers are connected, it will first request metadata
     * update and at the same time it will reconnect to the seed brokers if
     * required.
     *
     * NOTE: this method signature is about to change when we are going to
     * implement version negotiation, it will include the api_version.
     */

    template<typename Req, typename Ret = typename Req::api_type::response_type>
    requires(KafkaApi<typename Req::api_type>)
    ss::future<Ret> dispatch_to_any(Req request) {
        if (_brokers.empty()) {
            co_await request_metadata_update();
        }
        co_return co_await _brokers.any()->dispatch(std::move(request));
    }
    /**
     * Requests metadata update from the remote cluster. If any other request is
     * pending it will wait for it to finish and skip requesting the update once
     * again.
     *
     * This method propages the exception to the caller, that may change
     * in the future but now it is needed because of the way how PandaProxy and
     * SchemaRegistry works with ephemeral credentials.
     */
    ss::future<> request_metadata_update();

    std::optional<model::node_id> get_controller_id() const {
        return _controller_id;
    }

    bool is_connected() const { return !_brokers.empty(); }

    /**
     * Sets SASL configuration to use by the cluster broker connections.
     */
    void set_sasl_configuration(std::optional<sasl_configuration> creds);

    const std::optional<sasl_configuration>& get_sasl_configuration() const {
        return _config.sasl_cfg;
    }

private:
    ss::future<> update_metadata();
    ss::future<> dispatch_metadata_request();
    ss::future<> initialize_metadata_with_seed();
    void update_timer_callback();
    ss::future<> apply_metadata(metadata_response reply);

    connection_configuration _config;
    prefix_logger _logger;
    topic_cache _topic_cache;
    brokers _brokers;
    size_t _next_seed;

    std::optional<model::node_id> _controller_id;
    std::optional<ss::sstring> _cluster_id;

    ss::timer<> _metadata_update_timer;
    mutex _update_lock{"kc/metadata_update_lock"};
    ss::lowres_clock::time_point _last_update_time
      = ss::lowres_clock::time_point::min();

    ss::gate _gate;
    ss::abort_source _as;
};
} // namespace kafka::client
