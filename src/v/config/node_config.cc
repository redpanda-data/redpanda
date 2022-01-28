// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "node_config.h"

#include "config/configuration.h"

namespace config {

node_config::node_config() noexcept
  : data_directory(
    *this,
    "data_directory",
    "Place where redpanda will keep the data",
    required::yes)
  , node_id(
      *this,
      "node_id",
      "Unique id identifying a node in the cluster",
      required::yes)
  , rack(*this, "rack", "Rack identifier", required::no, std::nullopt)
  , seed_servers(
      *this,
      "seed_servers",
      "List of the seed servers used to join current cluster. If the "
      "seed_server list is empty the node will be a cluster root and it will "
      "form a new cluster",
      required::no,
      {})
  , rpc_server(
      *this,
      "rpc_server",
      "IpAddress and port for RPC server",
      required::no,
      net::unresolved_address("127.0.0.1", 33145))
  , rpc_server_tls(
      *this,
      "rpc_server_tls",
      "TLS configuration for RPC server",
      required::no,
      tls_config(),
      tls_config::validate)
  , kafka_api(
      *this,
      "kafka_api",
      "Address and port of an interface to listen for Kafka API requests",
      required::no,
      {model::broker_endpoint(net::unresolved_address("127.0.0.1", 9092))})
  , kafka_api_tls(
      *this,
      "kafka_api_tls",
      "TLS configuration for Kafka API endpoint",
      required::no,
      {},
      endpoint_tls_config::validate_many)
  , admin(
      *this,
      "admin",
      "Address and port of admin server",
      required::no,
      {model::broker_endpoint(net::unresolved_address("127.0.0.1", 9644))})
  , admin_api_tls(
      *this,
      "admin_api_tls",
      "TLS configuration for admin HTTP server",
      required::no,
      {},
      endpoint_tls_config::validate_many)
  , coproc_supervisor_server(
      *this,
      "coproc_supervisor_server",
      "IpAddress and port for supervisor service",
      required::no,
      net::unresolved_address("127.0.0.1", 43189))
  , admin_api_doc_dir(
      *this,
      "admin_api_doc_dir",
      "Admin API doc directory",
      required::no,
      "/usr/share/redpanda/admin-api-doc")
  , dashboard_dir(
      *this,
      "dashboard_dir",
      "serve http dashboard on / url",
      required::no,
      std::nullopt)
  , cloud_storage_cache_directory(
      *this,
      "cloud_storage_cache_directory",
      "Directory for archival cache. Should be present when "
      "`cloud_storage_enabled` is present",
      required::no,
      std::nullopt)
  , enable_central_config(
      *this,
      "enable_central_config",
      "Enable central storage + sync of cluster configuration",
      required::no,
      false)
  , _advertised_rpc_api(
      *this,
      "advertised_rpc_api",
      "Address of RPC endpoint published to other cluster members",
      required::no,
      std::nullopt)
  , _advertised_kafka_api(
      *this,
      "advertised_kafka_api",
      "Address of Kafka API published to the clients",
      required::no,
      {}) {}

node_config::error_map_t node_config::load(const YAML::Node& root_node) {
    if (!root_node["redpanda"]) {
        throw std::invalid_argument("'redpanda' root is required");
    }

    const auto& ignore = shard_local_cfg().property_names();

    return config_store::read_yaml(root_node["redpanda"], ignore);
}

/// Get a shard local copy of the node_config.
///
/// This has a terse name because it is used so many places,
/// usually as config::node().<property>
node_config& node() {
    static thread_local node_config cfg;
    return cfg;
}

} // namespace config
