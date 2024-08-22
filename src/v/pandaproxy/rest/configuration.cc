// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "configuration.h"

#include "config/base_property.h"
#include "config/broker_endpoint.h"
#include "config/endpoint_tls_config.h"
#include "config/rest_authn_endpoint.h"
#include "model/metadata.h"

#include <chrono>

namespace pandaproxy::rest {
using namespace std::chrono_literals;

configuration::configuration(const YAML::Node& cfg)
  : configuration() {
    _config_errors = read_yaml(cfg);
}

configuration::configuration()
  : pandaproxy_api(
      *this,
      "pandaproxy_api",
      "Rest API listen address and port",
      {},
      {config::rest_authn_endpoint{
        .address = net::unresolved_address("0.0.0.0", 8082),
        .authn_method = std::nullopt}})
  , pandaproxy_api_tls(
      *this,
      "pandaproxy_api_tls",
      "TLS configuration for Pandaproxy api.",
      {},
      {},
      config::endpoint_tls_config::validate_many)
  , advertised_pandaproxy_api(
      *this,
      "advertised_pandaproxy_api",
      "Network address for the HTTP Proxy API server to publish to clients.",
      {},
      {},
      model::broker_endpoint::validate_many)
  , api_doc_dir(
      *this,
      "api_doc_dir",
      "Path to the API specifications for the HTTP Proxy API.",
      {},
      "/usr/share/redpanda/proxy-api-doc")
  , consumer_instance_timeout(
      *this,
      "consumer_instance_timeout_ms",
      "How long to wait for an idle consumer before removing it. A consumer is "
      "considered idle when it's not making requests or heartbeats.",
      {},
      std::chrono::minutes{5})
  , client_cache_max_size(
      *this,
      "client_cache_max_size",
      "The maximum number of Kafka client connections that Redpanda can cache "
      "in the LRU (least recently used) cache. The LRU cache helps optimize "
      "resource utilization by keeping the most recently used clients in "
      "memory, facilitating quicker reconnections for frequent clients while "
      "limiting memory usage.",
      {.needs_restart = config::needs_restart::yes},
      10,
      [](const size_t max_size) {
          std::optional<ss::sstring> msg{std::nullopt};
          if (max_size == 0) {
              msg = ss::sstring{"Client cache max size must not be zero"};
          }
          return msg;
      })
  , client_keep_alive(
      *this,
      "client_keep_alive",
      "Time, in milliseconds, that an idle client connection may remain open "
      "to the HTTP Proxy API.",
      {.needs_restart = config::needs_restart::yes, .example = "300000"},
      5min,
      [](const std::chrono::milliseconds& keep_alive) {
          std::optional<ss::sstring> msg{std::nullopt};
          if (keep_alive <= 0s) {
              msg = ss::sstring{"Client keep alive must be greater than 0"};
          }
          return msg;
      }) {}
} // namespace pandaproxy::rest
