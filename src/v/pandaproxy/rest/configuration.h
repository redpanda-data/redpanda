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
#include "config/broker_endpoint.h"
#include "config/config_store.h"
#include "config/endpoint_tls_config.h"
#include "config/property.h"
#include "config/rest_authn_endpoint.h"
#include "model/metadata.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/socket_defs.hh>

#include <chrono>

namespace pandaproxy::rest {

/// Pandaproxy configuration
///
/// All application modules depend on configuration. The configuration module
/// can not depend on any other module to prevent cyclic dependencies.
struct configuration final : public config::config_store {
    config::one_or_many_property<config::rest_authn_endpoint> pandaproxy_api;
    config::one_or_many_property<config::endpoint_tls_config>
      pandaproxy_api_tls;
    config::one_or_many_property<model::broker_endpoint>
      advertised_pandaproxy_api;
    config::property<ss::sstring> api_doc_dir;
    config::property<std::chrono::milliseconds> consumer_instance_timeout;
    config::property<size_t> client_cache_max_size;
    config::property<std::chrono::milliseconds> client_keep_alive;

    configuration();
    explicit configuration(const YAML::Node& cfg);

    const error_map_t& errors() const { return _config_errors; }

private:
    error_map_t _config_errors;
};

} // namespace pandaproxy::rest
