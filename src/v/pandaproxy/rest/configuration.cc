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
#include "config/configuration.h"
#include "config/endpoint_tls_config.h"
#include "model/metadata.h"
#include "units.h"

#include <chrono>

namespace pandaproxy::rest {
using namespace std::chrono_literals;

configuration::configuration(const YAML::Node& cfg)
  : configuration() {
    read_yaml(cfg);
}

configuration::configuration()
  : pandaproxy_api(
    *this,
    "pandaproxy_api",
    "Rest API listen address and port",
    {},
    {model::broker_endpoint(net::unresolved_address("0.0.0.0", 8082))})
  , pandaproxy_api_tls(
      *this,
      "pandaproxy_api_tls",
      "TLS configuration for Pandaproxy api",
      {},
      {},
      config::endpoint_tls_config::validate_many)
  , advertised_pandaproxy_api(
      *this,
      "advertised_pandaproxy_api",
      "Rest API address and port to publish to client")
  , api_doc_dir(
      *this,
      "api_doc_dir",
      "API doc directory",
      {},
      "/usr/share/redpanda/proxy-api-doc")
  , consumer_instance_timeout(
      *this,
      "consumer_instance_timeout_ms",
      "How long to wait for an idle consumer before removing it",
      {},
      std::chrono::minutes{5}) {}

} // namespace pandaproxy::rest
