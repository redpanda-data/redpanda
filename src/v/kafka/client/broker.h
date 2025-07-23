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

#include "kafka/client/configuration.h"
#include "kafka/client/exceptions.h"
#include "kafka/client/logger.h"
#include "kafka/client/transport.h"
#include "net/connection.h"
#include "security/scram_algorithm.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"

#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>

namespace kafka::client {

class broker : public ss::enable_lw_shared_from_this<broker> {
public:
    broker(
      model::node_id node_id,
      const connection_configuration& config,
      std::unique_ptr<transport> transport);

    template<
      typename ReqT,
      typename Ret = typename ReqT::api_type::response_type>
    requires(KafkaApi<typename ReqT::api_type>)
    ss::future<Ret> dispatch(
      ReqT r,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt) {
        auto holder = _gate.hold();
        try {
            co_await maybe_initialize_connection(as);
            co_return co_await do_dispatch(std::move(r));
        } catch (const kafka_request_disconnected_exception&) {
            vlog(
              _logger.warn,
              "request dispatch error - {}",
              std::current_exception());
            throw broker_error(_node_id, error_code::broker_not_available);
        } catch (const std::system_error& e) {
            if (net::is_reconnect_error(e)) {
                throw broker_error(_node_id, error_code::broker_not_available);
            }
            throw;
        }
    }

    model::node_id id() const { return _node_id; }

    ss::future<> stop() {
        _reconnect_as.request_abort();
        _reconnect_mutex.broken();
        co_await _gate.close();
        co_await _transport->stop();
    }

    const net::unresolved_address& get_address() const {
        return _transport->server_address();
    }

    template<typename ReqT>
    requires(KafkaApi<typename ReqT::api_type>)
    api_version api_version_for() const {
        return api_version_for(ReqT::api_type::key);
    }

    api_version api_version_for(api_key key) const;

private:
    enum class auth_state : int8_t {
        none,
        in_progress,
        authenticated,
    };

    template<typename ReqT>
    void log_request(const ReqT& request) {
        using api_t = typename ReqT::api_type;
        vlog(
          kcwire.trace,
          "{} - node_id: {} @ {}:{} Sending request {{ api_type: {}, request: "
          "{} }}",
          _config->get_client_id(),
          _node_id,
          _transport->server_address().host(),
          _transport->server_address().port(),
          api_t::name,
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
    ss::future<RespT> do_dispatch(ReqT r) {
        log_request(r);
        auto response = co_await _transport->dispatch(
          std::move(r), api_version_for<ReqT>());
        log_response(r);
        co_return response;
    }
    /**
     * Connects to the broker and handles authentication if needed.
     */
    ss::future<> maybe_initialize_connection(
      std::optional<std::reference_wrapper<ss::abort_source>> as);

    ss::future<> connect(model::timeout_clock::time_point);

    ss::future<> maybe_authenticate();

    ss::future<> connect_with_retries(
      std::optional<std::reference_wrapper<ss::abort_source>>);

    bool needs_authentication() const {
        return _config->sasl_cfg.has_value()
               && _authentication_state == auth_state::none;
    }

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
    ss::future<security::server_first_message>
    send_scram_client_first(const security::client_first_message& client_first);
    ss::future<security::server_final_message>
    send_scram_client_final(const security::client_final_message& client_final);

    model::node_id _node_id;
    std::unique_ptr<transport> _transport;
    const connection_configuration* _config;
    mutex _reconnect_mutex{"broker::reconnect_mutex"};
    ss::gate _gate;
    prefix_logger _logger;
    auth_state _authentication_state = auth_state::none;
    ss::abort_source _reconnect_as;
};

using shared_broker_t = ss::lw_shared_ptr<broker>;

/**
 * Simple class used to create broker objects. Created broker objects use
 * configuration provided when creating the factory.
 */
struct broker_factory {
    broker_factory(
      const connection_configuration& config, prefix_logger& logger);

    ss::future<shared_broker_t>
    create_broker(model::node_id, net::unresolved_address addr);

private:
    const connection_configuration& _config;
    prefix_logger* _logger;
    ss::sstring _client_id;
};

struct broker_hash {
    using is_transparent = void;
    size_t operator()(const shared_broker_t& b) const {
        return absl::Hash<model::node_id>{}(b->id());
    }
    size_t operator()(model::node_id n_id) const {
        return absl::Hash<model::node_id>{}(n_id);
    }
};

struct broker_eq {
    using is_transparent = void;
    bool
    operator()(const shared_broker_t& lhs, const shared_broker_t& rhs) const {
        return lhs->id() == rhs->id();
    }
    bool operator()(model::node_id node_id, const shared_broker_t& b) const {
        return node_id == b->id();
    }
    bool operator()(const shared_broker_t& b, model::node_id node_id) const {
        return b->id() == node_id;
    }
};

} // namespace kafka::client
