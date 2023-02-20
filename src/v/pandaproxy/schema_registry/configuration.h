/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "config/configuration.h"
#include "config/rest_authn_endpoint.h"
#include "config/tls_config.h"
#include "model/metadata.h"

namespace pandaproxy::schema_registry {

/// Schema Registry configuration
///
struct configuration final : public config::config_store {
    configuration();
    explicit configuration(const YAML::Node& cfg);

    config::one_or_many_property<config::rest_authn_endpoint>
      schema_registry_api;
    config::one_or_many_property<config::endpoint_tls_config>
      schema_registry_api_tls;
    config::property<std::optional<int16_t>> schema_registry_replication_factor;
    config::property<ss::sstring> api_doc_dir;
};

} // namespace pandaproxy::schema_registry
