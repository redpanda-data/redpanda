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
#include "kafka/client/configuration.h"
#include "kafka/client/topic_cache.h"
#include "kafka/client/types.h"
#include "ssx/work_queue.h"
#include "utils/notification_list.h"
#include "utils/prefix_logger.h"
namespace kafka::client {
/**
 * Class representing a cluster metadata. This class is responsible for
 * managing broker connections and handling metadata updates. It also keeps a
 * cache of the cluster topics state.
 */
class cluster {
public:
    using callback_id = named_type<int16_t, struct callback_id_tag>;
    using metadata_callback
      = ss::noncopyable_function<void(const metadata_update&)>;

    explicit cluster(connection_configuration config);

    cluster(
      connection_configuration config,
      std::unique_ptr<broker_factory> broker_factory);

    cluster(cluster&&) = delete;
    cluster(const cluster&) = delete;
    cluster& operator=(cluster&&) = delete;
    cluster& operator=(const cluster&) = delete;
    ~cluster() noexcept = default;

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
    ss::future<Ret>
    dispatch_to(model::node_id broker_id, Req request, api_version version) {
        auto broker = _brokers.find(broker_id);
        if (!broker) {
            throw broker_error(
              broker_id, error_code::broker_not_available, "Broker not found");
        }
        return broker->dispatch(std::move(request), version)
          .then([](response_t response) {
              return std::get<Ret>(std::move(response));
          });
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
    ss::future<Ret> dispatch_to_any(Req request, api_version version) {
        if (_brokers.empty()) {
            co_await request_metadata_update();
        }
        co_return co_await _brokers.any()
          ->dispatch(std::move(request), version)
          .then([](response_t response) {
              return std::get<Ret>(std::move(response));
          });
    }
    /**
     * Requests metadata update from the remote cluster. If any other request is
     * pending it will wait for it to finish and skip requesting the update once
     * again.
     *
     * This method propages the exception to the caller, that may change
     * in the future but now it is needed because of the way how PandaProxy and
     * SchemaRegistry works with ephemeral credentials.
     *
     * \param topics_request_list optional list of topics to request metadata
     * on.  If `std::nullopt`, then request update on all topics, or only the
     * topics in the list.  Any empty list means no topics to request update on
     */
    ss::future<> request_metadata_update(
      std::optional<chunked_vector<model::topic>> topics_request_list
      = std::nullopt);

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
    /**
     * Callbacks for receiving metadata updates.
     * The callback will be called with the metadata response data.
     *
     * NOTE: the callback is called every time the metadata is updated, even it
     * it didn't change
     */
    callback_id register_metadata_cb(metadata_callback cb) {
        return _notifications.register_cb(std::move(cb));
    }

    void unregister_metadata_cb(callback_id id) {
        _notifications.unregister_cb(id);
    }
    /**
     * Returns the range of versions that is supported by all the brokers in the
     * cluster. It connects to the brokers if necessary.
     */
    ss::future<std::optional<api_version_range>> supported_api_versions(
      api_key key,
      std::optional<std::reference_wrapper<ss::abort_source>> = std::nullopt);

    /**
     * Returns the range of versions that is supported the requested broker.
     * Connection to the broker is established if necessary.
     */
    ss::future<std::optional<api_version_range>> supported_api_versions(
      model::node_id id,
      api_key key,
      std::optional<std::reference_wrapper<ss::abort_source>> = std::nullopt);

    /**
     * Returns logger prefixed with the client id.
     */
    prefix_logger& logger() { return _logger; }

    auto get_broker_ids() const { return _brokers.get_broker_ids(); }

    cluster_authorized_operations get_cluster_authorized_operations() const {
        return _cluster_authorized_operations;
    }

    void update_configuration(connection_configuration);
    const connection_configuration& configuration() const { return _config; }

private:
    ss::future<> update_metadata(
      std::optional<chunked_vector<model::topic>> topics_request_list
      = std::nullopt);
    ss::future<> dispatch_and_apply_metadata_updates(
      std::optional<chunked_vector<model::topic>> topics_request_list
      = std::nullopt);
    ss::future<metadata_response> dispatch_metadata_request(
      shared_broker_t broker,
      std::optional<chunked_vector<model::topic>> topics_request_list
      = std::nullopt,
      std::optional<api_version> requested_version = std::nullopt);
    ss::future<describe_cluster_response>
    dispatch_describe_cluster_request(shared_broker_t broker);
    ss::future<> initialize_metadata_with_seed();
    ss::future<> initialize_with_describe_cluster(shared_broker_t);
    ss::future<> initialize_with_metadata(shared_broker_t);
    void update_timer_callback();
    ss::future<> apply_metadata(metadata_update reply);

    ss::future<> do_update_configuration(connection_configuration config);

    // This multiplier is applied to the max_metadata_age to define the
    // topic_timeout for the topic_cache
    static constexpr int metadata_age_multiplier = 3;

    connection_configuration _config;
    prefix_logger _logger;
    topic_cache _topic_cache;
    brokers _brokers;
    size_t _next_seed;

    std::optional<model::node_id> _controller_id;
    std::optional<ss::sstring> _cluster_id;
    cluster_authorized_operations _cluster_authorized_operations{
      cluster_authorized_operations_not_set};

    ss::timer<> _metadata_update_timer;
    ssx::mutex _update_lock{"kc/metadata_update_lock"};
    ss::lowres_clock::time_point _last_update_time
      = ss::lowres_clock::time_point::min();
    notification_list<metadata_callback, callback_id> _notifications;
    ssx::work_queue _update_config_queue;
    ss::gate _gate;
    ss::abort_source _as;
};
} // namespace kafka::client
