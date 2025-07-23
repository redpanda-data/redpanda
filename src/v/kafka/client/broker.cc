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
    try {
        vlog(
          _logger->debug,
          "connecting to {} - {}:{}",
          node_id,
          addr.host(),
          addr.port());
        co_await broker_transport->connect();
    } catch (const std::system_error& ex) {
        if (net::is_reconnect_error(ex) || is_dns_failure_error(ex)) {
            throw broker_error(node_id, error_code::network_exception);
        }
        vlog(_logger->warn, "std::system_error: {}", ex.what());
        throw;
    }
    vlog(
      _logger->info,
      "connected to broker:{} - {}:{}",
      node_id,
      addr.host(),
      addr.port());
    auto connected_broker = ss::make_lw_shared<broker>(
      node_id, std::move(broker_transport));
    if (!_config.sasl_cfg) {
        vlog(
          _logger->debug,
          "broker {} - {}:{}, doesn't require authentication",
          node_id,
          addr.host(),
          addr.port());
        co_return connected_broker;
    }
    auto f = co_await ss::coroutine::as_future(
      do_authenticate(connected_broker, _config.sasl_cfg.value(), *_logger));
    if (f.failed()) {
        auto ex = f.get_exception();
        vlog(
          _logger->warn,
          "broker {} - {}:{}, error during authentication: {}",
          node_id,
          addr.host(),
          addr.port(),
          ex);
        co_await connected_broker->stop();
        std::rethrow_exception(ex);
    }
    vlog(
      _logger->trace,
      "broker {} - {}:{} authenticated",
      node_id,
      addr.host(),
      addr.port());
    co_return connected_broker;
}

} // namespace kafka::client
