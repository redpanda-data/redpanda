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
#include "kafka/client/sasl_client.h"
#include "net/connection.h"
#include "thirdparty/c-ares/ares.h"
#include "utils/unresolved_address.h"

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

broker::broker(
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

ss::future<> broker::connect(model::timeout_clock::time_point deadline) {
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

ss::future<> broker::maybe_initialize_connection(
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    if (_transport->is_valid() && !needs_authentication()) {
        co_return;
    }
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
    co_await maybe_authenticate();
}

ss::future<> broker::connect_with_retries(
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    if (_transport->is_valid()) {
        co_return;
    }
    auto deadline = model::timeout_clock::now() + _config->connection_timeout;

    // Every time broker is reconnected its authentication state is reset
    // to `none` so that it can be re-authenticated if needed.
    _authentication_state = auth_state::none;

    while (!_gate.is_closed()) {
        if (as) {
            as->get().check();
        }
        if (model::timeout_clock::now() >= deadline) {
            vlog(
              _logger.warn,
              "Connection attempted timed out after {} seconds",
              _config->connection_timeout / 1s);
            // todo: change error handing
            throw broker_error(_node_id, error_code::broker_not_available);
        }
        try {
            co_await connect(deadline);
            vlog(_logger.debug, "Broker connection established");
            co_return;
        } catch (...) {
            vlog(
              _logger.warn, "Connection error - {}", std::current_exception());
        }
        // todo: use more sophisticated retry policy
        co_await ss::sleep(100ms);
    }
}

ss::future<> broker::maybe_authenticate() {
    if (!needs_authentication()) {
        co_return;
    }
    _authentication_state = auth_state::in_progress;
    try {
        vlog(_logger.debug, "Authenticating broker");
        co_await do_authenticate(
          shared_from_this(), _config->sasl_cfg.value(), _logger);
        _authentication_state = auth_state::authenticated;
    } catch (...) {
        vlog(
          _logger.warn, "Authentication error - {}", std::current_exception());
        throw;
    }
}

api_version broker::api_version_for(api_key key) const {
    switch (key) {
    case offset_fetch_api::key:
        return api_version(4);
    case fetch_api::key:
        return api_version(10);
    case list_offsets_api::key:
        return api_version(3);
    case produce_api::key:
        return api_version(7);
    case offset_commit_api::key:
        return api_version(7);
    case describe_groups_api::key:
        return api_version(2);
    case heartbeat_api::key:
        return api_version(3);
    case join_group_api::key:
        return api_version(4);
    case sync_group_api::key:
        return api_version(3);
    case leave_group_api::key:
        return api_version(2);
    case metadata_api::key:
        return api_version(8);
    case find_coordinator_api::key:
        return api_version(2);
    case list_groups_api::key:
        return api_version(2);
    case create_topics_api::key:
        return api_version(6);
    case sasl_handshake_api::key:
        return api_version(1);
    case delete_records_api::key:
        return api_version(2);
    case offset_for_leader_epoch_api::key:
        return api_version(2);
    case sasl_authenticate_api::key:
        return api_version(1);
    case describe_configs_api::key:
        return api_version(4);
    default:
        throw std::runtime_error(
          fmt::format("Unsupported API key: {}", to_string(key)));
    }
}

broker_factory::broker_factory(
  const connection_configuration& config, prefix_logger& logger)
  : _config(config)
  , _logger(&logger)
  , _client_id(_config.client_id.value_or("redpanda-client")) {}

ss::future<shared_broker_t> broker_factory::create_broker(
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
    }
    auto broker_transport = std::make_unique<transport>(
      std::move(transport_cfg), _config.client_id);

    co_return ss::make_lw_shared<broker>(
      node_id, _config, std::move(broker_transport));
}

} // namespace kafka::client
