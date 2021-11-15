/*
 * Copyright 2020 Vectorized, Inc.
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
#include "config/tls_config.h"
#include "model/metadata.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/socket_defs.hh>

namespace pandaproxy::rest {

/// Pandaproxy configuration
///
/// All application modules depend on configuration. The configuration module
/// can not depend on any other module to prevent cyclic dependencies.
struct configuration final : public config::config_store {
    config::one_or_many_property<model::broker_endpoint> pandaproxy_api;
    config::one_or_many_property<config::endpoint_tls_config>
      pandaproxy_api_tls;
    config::one_or_many_property<model::broker_endpoint>
      advertised_pandaproxy_api;
    config::property<ss::sstring> api_doc_dir;

    configuration();
    explicit configuration(const YAML::Node& cfg);
};

} // namespace pandaproxy::rest
