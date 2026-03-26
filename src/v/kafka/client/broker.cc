// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/broker.h"

#include "base/seastarx.h"
#include "kafka/protocol/sasl_authenticate.h"
#include "kafka/protocol/sasl_handshake.h"
#include "net/connection.h"
#include "random/generators.h"
#include "security/oidc_authenticator.h"
#include "security/plain_authenticator.h"
#include "security/scram_authenticator.h"
#include "thirdparty/c-ares/ares.h"
#include "utils/backoff_policy.h"
#include "utils/unresolved_address.h"
#include "version/version.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/net/dns.hh>
using namespace std::chrono_literals;
namespace {
bool is_dns_failure_error(const std::system_error& e) {
    if (e.code().category() == ss::net::dns::error_category()) {
        switch (e.code().value()) {
        case ARES_ENOTFOUND:
        case ARES_ENODATA:
        case ARES_ETIMEOUT:
        case ARES_ECONNREFUSED:
            return true;
        default:
            return false;
        }
    }

    return false;
}
} // namespace

namespace kafka::client {

remote_broker::remote_broker(
  model::node_id node_id,
  const connection_configuration& config,
  std::unique_ptr<transport> transport)
  : _node_id(node_id)
  , _transport(std::move(transport))
  , _config(&config)
  , _logger(
      kclog,
      fmt::format(
        "{} - node_id: {} @ {}:{}",
        _config->get_client_id(),
        _node_id,
        _transport->server_address().host(),
        _transport->server_address().port())) {}

ss::future<> remote_broker::connect(model::timeout_clock::time_point deadline) {
    try {
        vlog(_logger.debug, "Connecting");
        co_await _transport->connect(deadline);
    } catch (const std::system_error& ex) {
        vlog(_logger.warn, "Connection error - {}", ex);
        if (net::is_reconnect_error(ex) || is_dns_failure_error(ex)) {
            throw broker_error(_node_id, error_code::broker_not_available);
        }
        throw ex;
    }
}

ss::future<> remote_broker::maybe_initialize_connection(
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    if (_transport->is_valid() && !needs_authentication()) {
        co_return;
    }
    co_await maybe_reconnect(as);
    co_await maybe_authenticate();
}

ss::future<> remote_broker::maybe_reconnect(
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    /**
     * We protect the connection initialization with a mutex to ensure
     * that only one connection attempt is made at a time.
     *
     * TODO: consider moving authentication into the broker, to avoid going
     * through the external broker api and tracking the authentication state.
     */
    auto u = as.has_value() ? co_await _reconnect_mutex.get_units(as->get())
                            : co_await _reconnect_mutex.get_units();

    co_await connect_with_retries(as);
}

ss::future<> remote_broker::connect_with_retries(
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    if (_transport->is_valid()) {
        co_return;
    }
    auto deadline = model::timeout_clock::now() + _config->connection_timeout;

    // Every time broker is reconnected its authentication state is reset
    // to `none` so that it can be re-authenticated if needed.
    _authentication_state = auth_state::none;
    // todo: add configuration for backoff policy
    auto backoff_policy = ::make_exponential_backoff_policy<ss::lowres_clock>(
      _config->connection_timeout / 20, _config->connection_timeout);
    /**
     * Use a fraction of the connection timeout to make sure that the connection
     * attempt can be retried before the deadline.
     */
    const auto retry_interval = _config->connection_timeout / 5;
    while (!_gate.is_closed()) {
        if (as) {
            as->get().check();
        }
        _reconnect_as.check();
        if (model::timeout_clock::now() >= deadline) {
            vlog(
              _logger.warn,
              "Connection attempted timed out after {} seconds",
              _config->connection_timeout / 1s);
            // todo: change error handing
            throw broker_error(_node_id, error_code::broker_not_available);
        }
        try {
            co_await connect(model::timeout_clock::now() + retry_interval);
            co_await initialize_versions();
            vlog(_logger.debug, "Broker connection established");
            co_return;
        } catch (...) {
            vlog(
              _logger.warn,
              "Connection error, next retry in {}ms - {}",
              backoff_policy.current_backoff_duration() / 1ms,
              std::current_exception());
        }

        co_await ss::sleep_abortable(
          backoff_policy.current_backoff_duration(),
          as.has_value() ? as->get() : _reconnect_as);
        backoff_policy.next_backoff();
    }
}

ss::future<> remote_broker::maybe_authenticate() {
    if (!needs_authentication()) {
        co_return;
    }
    _authentication_state = auth_state::in_progress;
    try {
        vlog(_logger.debug, "Authenticating broker");
        co_await do_authenticate();
        _authentication_state = auth_state::authenticated;
    } catch (...) {
        vlog(
          _logger.warn, "Authentication error - {}", std::current_exception());
        throw;
    }
}
namespace {
api_versions_request make_api_versions_request(api_version version) {
    api_versions_request req;
    if (version >= api_version(3)) {
        /**
         * In order to work with Kafka brokers this must match the following
         * regex:
         *
         * [a-zA-Z0-9](?:[a-zA-Z0-9\\-.]*[a-zA-Z0-9])?
         */
        req.data.client_software_name = "redpanda-client";
        req.data.client_software_version = ss::sstring(redpanda_git_version());
    }
    return req;
}

api_version find_api_versions_request_version(
  const chunked_vector<api_versions_response_key>& api_keys) {
    for (auto& api : api_keys) {
        if (api.api_key == api_versions_api::key) {
            return api_version(api.max_version);
        }
    }
    // if not found, return the minimum valid version
    return api_versions_api::min_valid;
}

} // namespace
ss::future<> remote_broker::initialize_versions() {
    vlog(_logger.trace, "Requesting API versions");
    api_versions_request request;
    request.data.client_software_name = "redpanda-client";
    request.data.client_software_version = ss::sstring(redpanda_version());
    auto response_buffer = co_await _transport->dispatch_request_raw_response(
      make_api_versions_request(api_versions_api::max_valid),
      api_versions_api::max_valid);
    /**
     * Peek for the API versions response error code.
     */
    protocol::decoder reader(response_buffer.share());
    auto resp_ec = kafka::error_code(reader.read_int16());
    /**
     * If the broker does not support the requested version of the API versions
     * request, it will respond with UNSUPPORTED_VERSION error code and the
     * ApiVersionResponse version 0.
     *
     * For more details see: KIP-511
     */
    api_versions_response response;
    if (resp_ec == error_code::none) {
        response.decode(
          std::move(response_buffer), api_versions_api::max_valid);
    } else {
        // TODO: handle the returned supported version here
        if (resp_ec == error_code::unsupported_version) {
            response.decode(std::move(response_buffer), api_version(0));
            auto fallback_version = find_api_versions_request_version(
              response.data.api_keys);
            vlog(
              _logger.info,
              "Broker does not support API version request version {}, falling "
              "back to version {}",
              api_versions_api::max_valid,
              fallback_version);
            response = co_await _transport->dispatch(
              make_api_versions_request(fallback_version), fallback_version);
        } else {
            vlog(
              _logger.warn,
              "Unable to initialize the API versions - {}",
              resp_ec);
            throw broker_error(
              _node_id, resp_ec, "Failed to initialize API versions");
        }
    }

    for (auto& api : response.data.api_keys) {
        _supported_versions.insert_or_assign(
          api_key(api.api_key),
          api_version_range{
            .min = api_version(api.min_version),
            .max = api_version(api.max_version),
          });
    }
    vlog(_logger.trace, "Supported API versions: {}", _supported_versions);
}

ss::future<response_t> remote_broker::dispatch(
  request_t r,
  api_version version,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    auto holder = _gate.hold();
    try {
        co_await maybe_initialize_connection(as);
        auto response = co_await ss::visit(
          std::move(r), [this, version](auto req) {
              return do_dispatch<>(std::move(req), version).then([](auto resp) {
                  return response_t(std::move(resp));
              });
          });
        co_return response;
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

remote_broker_factory::remote_broker_factory(
  const connection_configuration& config, prefix_logger& logger)
  : _config(config)
  , _logger(&logger)
  , _client_id(config.client_id.value_or("redpanda-client")) {}

ss::future<shared_broker_t> remote_broker_factory::create_broker(
  model::node_id node_id, net::unresolved_address addr) {
    net::base_transport::configuration transport_cfg{
      .server_addr = addr,
    };
    vlog(
      _logger->debug,
      "Creating transport for broker {} - {}:{}",
      node_id,
      addr.host(),
      addr.port());
    if (_config.broker_tls) {
        transport_cfg.credentials
          = co_await _config.broker_tls->build_credentials();
        if (_config.broker_tls->provide_sni_hostname) {
            transport_cfg.tls_sni_hostname = addr.host();
        }
    }
    auto broker_transport = std::make_unique<transport>(
      std::move(transport_cfg), _config.client_id);

    co_return ss::make_shared<remote_broker>(
      node_id, _config, std::move(broker_transport));
}

std::ostream& operator<<(std::ostream& os, const api_version_range& r) {
    fmt::print(os, "{{min: {}, max: {}}}", r.min, r.max);
    return os;
}

} // namespace kafka::client
