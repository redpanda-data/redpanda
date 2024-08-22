// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/configuration.h"

namespace pandaproxy::schema_registry {

configuration::configuration(const YAML::Node& cfg)
  : configuration() {
    read_yaml(cfg);
}

configuration::configuration()
  : schema_registry_api(
      *this,
      "schema_registry_api",
      "Schema Registry API listener address and port",
      {},
      {config::rest_authn_endpoint{
        .address = net::unresolved_address("0.0.0.0", 8081),
        .authn_method = std::nullopt}})
  , schema_registry_api_tls(
      *this,
      "schema_registry_api_tls",
      "TLS configuration for Schema Registry API.",
      {},
      {},
      config::endpoint_tls_config::validate_many)
  , mode_mutability(
      *this,
      "mode_mutability",
      "Enable modifications to the read-only `mode` of the Schema "
      "Registry.When set to `true`, the entire Schema Registry or its subjects "
      "can be switched to `READONLY` or `READWRITE`. This property is useful "
      "for preventing unwanted changes to the entire Schema Registry or "
      "specific subjects.",
      {},
      true)
  , schema_registry_replication_factor(
      *this,
      "schema_registry_replication_factor",
      "Replication factor for internal `_schemas` topic.  If unset, defaults "
      "to `default_topic_replication`.",
      {},
      std::nullopt)
  , api_doc_dir(
      *this,
      "api_doc_dir",
      "API doc directory",
      {},
      "/usr/share/redpanda/proxy-api-doc") {}

} // namespace pandaproxy::schema_registry
