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

#pragma once

#include "kafka/client/api_types.h"
#include "kafka/client/configuration.h"
#include "kafka/client/exceptions.h"
#include "kafka/client/logger.h"
#include "kafka/client/transport.h"
#include "net/connection.h"
#include "security/scram_algorithm.h"
#include "ssx/mutex.h"
#include "utils/prefix_logger.h"

#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>

namespace kafka::client {
struct api_version_range {
    api_version min{0};
    api_version max{0};

    bool is_supported(api_version v) const { return v >= min && v <= max; }

    friend std::ostream&
    operator<<(std::ostream& os, const api_version_range& r);

    friend bool operator==(
      const api_version_range& lhs, const api_version_range& rhs) = default;
};
/**
 * Broker interface that defines the methods required for a Kafka broker
 * implementation. This interface is used to abstract the communication with
 * a Kafka broker, allowing for different implementations (e.g., remote brokers,
 * mock brokers for testing, etc.) to be used interchangeably.
 */
class broker {
public:
    virtual ~broker() = default;
    virtual ss::future<response_t> dispatch(
      request_t,
      api_version version,
      std::optional<std::reference_wrapper<ss::abort_source>> as = std::nullopt)
      = 0;

    virtual model::node_id id() const = 0;

    virtual ss::future<> stop() = 0;

    /**
     * Returns the supported versions for the given API key. May connect to the
     * broker if necessary.
     */
    virtual ss::future<std::optional<api_version_range>> get_supported_versions(
      api_key key,
      std::optional<std::reference_wrapper<ss::abort_source>>
      = std::nullopt) = 0;

    virtual const net::unresolved_address& get_address() const = 0;
};
/**
 * Default implementation of a broker.
 */
class remote_broker
  : public broker
  , public ss::enable_shared_from_this<remote_broker> {
public:
    remote_broker(
      model::node_id node_id,
      const connection_configuration& config,
      std::unique_ptr<transport> transport);

    ss::future<response_t> dispatch(
      request_t r,
      api_version version,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt) final;

    model::node_id id() const final { return _node_id; }

    ss::future<> stop() final {
        _reconnect_as.request_abort();
        _reconnect_mutex.broken();
        co_await _gate.close();
        co_await _transport->stop();
    }

    const net::unresolved_address& get_address() const final {
        return _transport->server_address();
    }

    ss::future<std::optional<api_version_range>> get_supported_versions(
      api_key key,
      std::optional<std::reference_wrapper<ss::abort_source>>
      = std::nullopt) final;

private:
    enum class auth_state : int8_t {
        none,
        in_progress,
        authenticated,
    };

    template<typename ReqT>
    void log_request(const ReqT& request, api_version version) {
        using api_t = typename ReqT::api_type;
        vlog(
          kcwire.trace,
          "{} - node_id: {} @ {}:{} Sending request {{ api_type: {}, version: "
          "{}, request: {} }}",
          _config->get_client_id(),
          _node_id,
          _transport->server_address().host(),
          _transport->server_address().port(),
          api_t::name,
          version,
          request);
    }

    template<typename RespT>
    void log_response(const RespT& resp) {
        using api_t = typename RespT::api_type;
        vlog(
          kcwire.trace,
          "{} - node_id: {} @ {}:{} Received response {{ api_type: {}, "
          "response: {} }}",
          _config->get_client_id(),
          _node_id,
          _transport->server_address().host(),
          _transport->server_address().port(),
          api_t::name,
          resp);
    }

    template<
      typename ReqT,
      typename RespT = typename ReqT::api_type::response_type>
    requires(KafkaApi<typename ReqT::api_type>)
    ss::future<RespT> do_dispatch(ReqT r, api_version version) {
        log_request(r, version);
        auto response = co_await _transport->dispatch(std::move(r), version);
        log_response(response);
        co_return response;
    }
    /**
     * Connects to the broker and handles authentication if needed.
     */
    ss::future<> maybe_initialize_connection(
      std::optional<std::reference_wrapper<ss::abort_source>> as);
    ss::future<>
    maybe_reconnect(std::optional<std::reference_wrapper<ss::abort_source>> as);

    ss::future<> connect(model::timeout_clock::time_point);

    ss::future<> maybe_authenticate();
    ss::future<> initialize_versions();

    ss::future<> connect_with_retries(
      std::optional<std::reference_wrapper<ss::abort_source>>);

    bool needs_authentication() const {
        return _config->sasl_cfg.has_value()
               && _authentication_state == auth_state::none;
    }

    api_version get_sasl_authenticate_request_version() const;
    api_version get_sasl_handshake_request_version() const;

    ss::future<> do_authenticate();
    /*
     * SASL handshake negotiates mechanism. In this case that process is simple:
     * if the server doesn't support the requested mechanism there is no
     * fallback.
     */
    ss::future<> do_sasl_handshake(ss::sstring mechanism);
    template<typename ScramAlgo>
    ss::future<>
    do_authenticate_scram(ss::sstring username, ss::sstring password);
    ss::future<>
    do_authenticate_scram256(ss::sstring username, ss::sstring password);
    ss::future<>
    do_authenticate_scram512(ss::sstring username, ss::sstring password);
    ss::future<> do_authenticate_oauthbearer(ss::sstring token);
    ss::future<>
    do_authenticate_plain(ss::sstring username, ss::sstring password);
    ss::future<security::server_first_message>
    send_scram_client_first(const security::client_first_message& client_first);
    ss::future<security::server_final_message>
    send_scram_client_final(const security::client_final_message& client_final);

    model::node_id _node_id;
    std::unique_ptr<transport> _transport;
    const connection_configuration* _config;
    ssx::mutex _reconnect_mutex{"broker::reconnect_mutex"};
    ss::gate _gate;
    prefix_logger _logger;
    auth_state _authentication_state = auth_state::none;
    // We store the versions in flat has map as the number of supported
    // versions is expected to be small.
    absl::flat_hash_map<kafka::api_key, api_version_range> _supported_versions;
    ss::abort_source _reconnect_as;
};

using shared_broker_t = ss::shared_ptr<broker>;

/**
 * Simple class used to create broker objects. Created broker objects use
 * configuration provided when creating the factory.
 */
struct broker_factory {
    virtual ss::future<shared_broker_t>
    create_broker(model::node_id, net::unresolved_address addr) = 0;
    virtual ~broker_factory() = default;
};

/**
 * Simple class used to create broker objects. Created broker objects use
 * configuration provided when creating the factory.
 */
struct remote_broker_factory : public broker_factory {
    remote_broker_factory(
      const connection_configuration& config, prefix_logger& logger);

    ss::future<shared_broker_t>
    create_broker(model::node_id, net::unresolved_address addr) final;

private:
    const connection_configuration& _config;
    prefix_logger* _logger;
    ss::sstring _client_id;
};

} // namespace kafka::client
