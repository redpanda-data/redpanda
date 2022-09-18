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
#include "config/config_store.h"
#include "config/property.h"
#include "config/rest_authn_endpoint.h"
#include "config/tls_config.h"
#include "model/metadata.h"
#include "model/timestamp.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/socket_defs.hh>

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
    config::property<int64_t> client_cache_max_size;
    config::property<int64_t> client_keep_alive;

    configuration();
    explicit configuration(const YAML::Node& cfg);

    // A helper method that searches for the address within
    // list of pandaproxy_api endpoints.
    // Returns the authn type if the address is found.
    // Returns none type otherwise
    config::rest_authn_type authn_type(net::unresolved_address& address);
};

} // namespace pandaproxy::rest
