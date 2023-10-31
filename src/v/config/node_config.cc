// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "node_config.h"

#include "config/configuration.h"
#include "net/unresolved_address.h"

namespace config {

node_config::node_config() noexcept
  : developer_mode(
    *this,
    "developer_mode",
    "Flag to enable developer mode, which skips most of the checks performed at startup. Not recommended for production use."
    {.visibility = visibility::tunable},
    false)
  , data_directory(
      *this,
      "data_directory",
      "Path to the directory for storing Redpanda's streaming data files.",
      {.required = required::yes, .visibility = visibility::user})
  , node_id(
      *this,
      "node_id",
      "A number that uniquely identifies the broker within the cluster. If `null` (the default value), Redpanda automatically assigns an ID. If set, it must be non-negative value. "
      {.visibility = visibility::user},
      std::nullopt,
      [](std::optional<model::node_id> id) -> std::optional<ss::sstring> {
          if (id && (*id)() < 0) {
              return fmt::format("Negative node_id ({}) not allowed", *id);
          }
          return std::nullopt;
      })
  , rack(
      *this,
      "rack",
      "A label that identifies a failure zone. Apply the same label to all brokers in the same failure zone. When enable_rack_awareness is set to `true` at the cluster level, the system uses the rack labels to spread partition replicas across different failure zones.",
      {.visibility = visibility::user},
      std::nullopt)
  , seed_servers(
      *this,
      "seed_servers",
      "List of the seed servers used to join current cluster. If the "
      "seed_server list is empty the node will be a cluster root and it will "
      "form a new cluster",
      {.visibility = visibility::user},
      {},
      [](std::vector<seed_server> s) -> std::optional<ss::sstring> {
          std::sort(s.begin(), s.end());
          const auto s_dupe_i = std::adjacent_find(s.cbegin(), s.cend());
          if (s_dupe_i != s.cend()) {
              return fmt::format(
                "Duplicate items in seed_servers: {}", *s_dupe_i);
          }
          return std::nullopt;
      })
  , empty_seed_starts_cluster(
      *this,
      "empty_seed_starts_cluster",
      "Controls how a new cluster is formed. This property must have the same value in all brokers in a cluster. If `true`, an empty seed_servers list denotes that this broker should form a cluster. At most, one broker in the cluster should be configured with an empty seed_servers list. If no such configured broker exists, or if configured to be `false`, then all brokers denoted by the seed_servers list must be identical in their configurations, and those brokers form the initial cluster. "
      {.visibility = visibility::user},
      true)
  , rpc_server(
      *this,
      "rpc_server",
      "IP address and port for the Remote Procedure Call (RPC) server.",
      {.visibility = visibility::user},
      net::unresolved_address("127.0.0.1", 33145))
  , rpc_server_tls(
      *this,
      "rpc_server_tls",
      "TLS configuration for the RPC server.",
      {.visibility = visibility::user},
      tls_config(),
      tls_config::validate)
  , kafka_api(
      *this,
      "kafka_api",
      "IP address and port of the Kafka API endpoint that handles requests.",
      {.visibility = visibility::user},
      {config::broker_authn_endpoint{
        .address = net::unresolved_address("127.0.0.1", 9092),
        .authn_method = std::nullopt}})
  , kafka_api_tls(
      *this,
      "kafka_api_tls",
      "Transport Layer Security (TLS) configuration for the Kafka API endpoint.",
      {.visibility = visibility::user},
      {},
      endpoint_tls_config::validate_many)
  , admin(
      *this,
      "admin",
      "IP address and port of the admin server.",
      {.visibility = visibility::user},
      {model::broker_endpoint(net::unresolved_address("127.0.0.1", 9644))})
  , admin_api_tls(
      *this,
      "admin_api_tls",
      "TLS configuration for the Admin API.",
      {.visibility = visibility::user},
      {},
      endpoint_tls_config::validate_many)
  , coproc_supervisor_server(
      *this, 
      "coproc_supervisor_server",
      "IP address and port for supervisor service.")
  , emergency_disable_data_transforms(
      *this,
      "emergency_disable_data_transforms",
      "Override the cluster enablement setting and disable WebAssembly powered "
      "data transforms. Only used as an emergency shutoff button.",
      {.visibility = visibility::user},
      false)
  , admin_api_doc_dir(
      *this,
      "admin_api_doc_dir",
      "Path to the admin API documentation directory.",
      {.visibility = visibility::user},
      "/usr/share/redpanda/admin-api-doc")
  , dashboard_dir(
      *this,
      "dashboard_dir",
      "Path to the directory where the HTTP dashboard is located.")
  , cloud_storage_cache_directory(
      *this,
      "cloud_storage_cache_directory",
      "The directory where the cache archive is stored. This property is mandatory when cloud_storage_enabled is set to `true`."
      {.visibility = visibility::user},
      std::nullopt)
  , enable_central_config(*this, "enable_central_config")
  , crash_loop_limit(
      *this,
      "crash_loop_limit",
      "A limit on the number of consecutive times a broker can crash within one hour before its crash-tracking logic is reset. This limit prevents a broker from getting stuck in an infinite cycle of crashes. If `null`, the property is disabled and no limit is applied. The crash-tracking logic is reset (to zero consecutive crashes) by any of the following conditions:
       The broker shuts down cleanly.
       One hour passes since the last crash.
       The broker configuration file, `redpanda.yaml`, is updated.
       The `startup_log` file in the broker's data_directory is manually deleted."
      {.visibility = visibility::user},
      5)
  , upgrade_override_checks(
      *this,
      "upgrade_override_checks",
      "Whether to violate safety checks when starting a redpanda version newer "
      "than the cluster's consensus version",
      {.visibility = visibility::tunable},
      false)
  , memory_allocation_warning_threshold(
      *this,
      "memory_allocation_warning_threshold",
      "Enables log messages for allocations greater than the given size.",
      {.visibility = visibility::tunable},
      std::nullopt)
  , storage_failure_injection_enabled(
      *this,
      "storage_failure_injection_enabled",
      "If true, inject low level storage failures on the write path. **Not** "
      "for production usage.",
      {.visibility = visibility::tunable},
      false)
  , recovery_mode_enabled(
      *this,
      "recovery_mode_enabled",
      "If true, start redpanda in \"metadata only\" mode, skipping "
      "loading user partitions and allowing only metadata operations.",
      {.visibility = visibility::user},
      false)
  , storage_failure_injection_config_path(
      *this,
      "storage_failure_injection_config_path",
      "Path to the configuration file used for low level storage failure "
      "injection",
      {.visibility = visibility::tunable},
      std::nullopt)
  , _advertised_rpc_api(
      *this,
      "advertised_rpc_api",
      "Address of RPC endpoint published to other cluster members",
      {.visibility = visibility::user},
      std::nullopt)
  , _advertised_kafka_api(
      *this,
      "advertised_kafka_api",
      "Address of Kafka API published to the clients",
      {.visibility = visibility::user},
      {}) {}

void validate_multi_node_property_config(
  std::map<ss::sstring, ss::sstring>& errors) {
    auto const& cfg = config::node();
    const auto& kafka_api = cfg.kafka_api.value();
    for (auto const& ep : kafka_api) {
        const auto& n = ep.name;
        auto authn_method = ep.authn_method;
        if (authn_method.has_value()) {
            if (authn_method == config::broker_authn_method::mtls_identity) {
                auto kafka_api_tls = config::node().kafka_api_tls();
                auto tls_it = std::find_if(
                  kafka_api_tls.begin(),
                  kafka_api_tls.end(),
                  [&n](const config::endpoint_tls_config& ep) {
                      return ep.name == n;
                  });
                if (
                  tls_it == kafka_api_tls.end() || !tls_it->config.is_enabled()
                  || !tls_it->config.get_require_client_auth()) {
                    errors.emplace(
                      "kafka_api.authentication_method",
                      ssx::sformat(
                        "kafka_api {} configured with {}, but there is not an "
                        "equivalent kafka_api_tls config that requires client "
                        "auth.",
                        n,
                        to_string_view(*authn_method)));
                }
            }
        }
    }

    for (auto const& ep : cfg.advertised_kafka_api()) {
        auto err = model::broker_endpoint::validate_not_is_addr_any(ep);
        if (err) {
            errors.emplace("advertised_kafka_api", ssx::sformat("{}", *err));
        }
    }

    auto rpc_err = model::broker_endpoint::validate_not_is_addr_any(
      model::broker_endpoint{"", cfg.advertised_rpc_api()});

    if (rpc_err) {
        errors.emplace("advertised_rpc_api", ssx::sformat("{}", *rpc_err));
    }
}

node_config::error_map_t node_config::load(const YAML::Node& root_node) {
    if (!root_node["redpanda"]) {
        throw std::invalid_argument("'redpanda' root is required");
    }

    const auto& ignore = shard_local_cfg().property_names();

    auto errors = config_store::read_yaml(root_node["redpanda"], ignore);
    validate_multi_node_property_config(errors);
    return errors;
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
