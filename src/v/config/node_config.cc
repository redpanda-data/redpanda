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
#include "config/types.h"
#include "utils/unresolved_address.h"

namespace config {

node_config::node_config() noexcept
  : developer_mode(
      *this,
      "developer_mode",
      "Skips most of the checks performed at startup, not recomended for "
      "production use",
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
      "A number that uniquely identifies the broker within the cluster. If "
      "`null` (the default value), Redpanda automatically assigns an ID. If "
      "set, it must be non-negative value. The `node_id` property must not be "
      "changed after a broker joins the cluster.",
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
      "A label that identifies a failure zone. Apply the same label to all "
      "brokers in the same failure zone. When `enable_rack_awareness` is set "
      "to `true` at the cluster level, the system uses the rack labels to "
      "spread partition replicas across different failure zones.",
      {.visibility = visibility::user},
      std::nullopt)
  , seed_servers(
      *this,
      "seed_servers",
      "List of the seed servers used to join current cluster. If the "
      "`seed_servers` list is empty the node will be a cluster root and it "
      "will form a new cluster. When `empty_seed_starts_cluster` is `true`, "
      "Redpanda enables one broker with an empty `seed_servers` list to "
      "initiate a new cluster. The broker with an empty `seed_servers` becomes "
      "the cluster root, to which other brokers must connect to join the "
      "cluster.  Brokers looking to join the cluster should have their "
      "`seed_servers` populated with the cluster root's address, facilitating "
      "their connection to the cluster. Only one broker, the designated "
      "cluster root, should have an empty `seed_servers` list during the "
      "initial cluster bootstrapping. This ensures a single initiation point "
      "for cluster formation. When `empty_seed_starts_cluster` is `false`, "
      "Redpanda requires all brokers to start with a known set of brokers "
      "listed in `seed_servers`. The `seed_servers` list must not be empty and "
      "should be identical across these initial seed brokers, containing the "
      "addresses of all seed brokers. Brokers not included in the "
      "`seed_servers` list use it to discover and join the cluster, allowing "
      "for expansion beyond the foundational members. The `seed_servers` list "
      "must be consistent across all seed brokers to prevent cluster "
      "fragmentation and ensure stable cluster formation.",
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
      "Controls how a new cluster is formed. All brokers in a cluster must "
      "have the same value. See how the `empty_seed_starts_cluster` setting "
      "works with the `seed_servers` setting to form a cluster. For backward "
      "compatibility, `true` is the default. Redpanda recommends using `false` "
      "in production environments to prevent accidental cluster formation.",
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
      "Transport Layer Security (TLS) configuration for the Kafka API "
      "endpoint.",
      {.visibility = visibility::user},
      {},
      endpoint_tls_config::validate_many)
  , admin(
      *this,
      "admin",
      "Network address for the Admin API[] server.",
      {.visibility = visibility::user},
      {model::broker_endpoint(net::unresolved_address("127.0.0.1", 9644))})
  , admin_api_tls(
      *this,
      "admin_api_tls",
      "Specifies the TLS configuration for the HTTP Admin API.",
      {.visibility = visibility::user},
      {},
      endpoint_tls_config::validate_many)
  , coproc_supervisor_server(*this, "coproc_supervisor_server")
  , emergency_disable_data_transforms(
      *this,
      "emergency_disable_data_transforms",
      "Override the cluster property `data_transforms_enabled` and disable "
      "Wasm-powered data transforms. This is an emergency shutoff button.",
      {.visibility = visibility::user},
      false)
  , admin_api_doc_dir(
      *this,
      "admin_api_doc_dir",
      "Path to the API specifications for the Admin API.",
      {.visibility = visibility::user},
      "/usr/share/redpanda/admin-api-doc")
  , dashboard_dir(*this, "dashboard_dir")
  , cloud_storage_cache_directory(
      *this,
      "cloud_storage_cache_directory",
      "Directory for archival cache. Should be present when "
      "`cloud_storage_enabled` is present",
      {.visibility = visibility::user},
      std::nullopt)
  , cloud_storage_inventory_hash_store(
      *this,
      "cloud_storage_inventory_hash_path_directory",
      "Directory to store inventory report hashes for use by cloud storage "
      "scrubber",
      {.visibility = visibility::user},
      std::nullopt)
  , enable_central_config(*this, "enable_central_config")
  , crash_loop_limit(
      *this,
      "crash_loop_limit",
      "A limit on the number of consecutive times a broker can crash within "
      "one hour before its crash-tracking logic is reset. This limit prevents "
      "a broker from getting stuck in an infinite cycle of crashes. For more "
      "information see "
      "https://docs.redpanda.com/current/reference/properties/"
      "broker-properties/#crash_loop_limit.",
      {.visibility = visibility::user},
      5) // default value
  , upgrade_override_checks(
      *this,
      "upgrade_override_checks",
      "Whether to violate safety checks when starting a Redpanda version newer "
      "than the cluster's consensus version.",
      {.visibility = visibility::tunable},
      false)
  , memory_allocation_warning_threshold(
      *this,
      "memory_allocation_warning_threshold",
      "Threshold for log messages that contain a larger memory allocation than "
      "specified.",
      {.visibility = visibility::tunable},
      128_KiB + 1) // 128 KiB is the largest allowed allocation size
  , storage_failure_injection_enabled(
      *this,
      "storage_failure_injection_enabled",
      "If `true`, inject low level storage failures on the write path. Do "
      "_not_ use for production instances.",
      {.visibility = visibility::tunable},
      false)
  , recovery_mode_enabled(
      *this,
      "recovery_mode_enabled",
      "If `true`, start Redpanda in recovery mode, where user partitions are "
      "not loaded and only administrative operations are allowed.",
      {.visibility = visibility::user},
      false)
  , storage_failure_injection_config_path(
      *this,
      "storage_failure_injection_config_path",
      "Path to the configuration file used for low level storage failure "
      "injection.",
      {.visibility = visibility::tunable},
      std::nullopt)
  , verbose_logging_timeout_sec_max(
      *this,
      "verbose_logging_timeout_sec_max",
      "Maximum duration in seconds for verbose (`TRACE` or `DEBUG`) logging. "
      "Values configured above this will be clamped. If null (the default) "
      "there is no limit. Can be overridden in the Admin API on a per-request "
      "basis.",
      {.visibility = visibility::tunable},
      std::nullopt,
      {.min = 1s})
  , fips_mode(
      *this,
      "fips_mode",
      "Controls whether Redpanda starts in FIPS mode.  This property allows "
      "for three values: `disabled` - Redpanda does not start in FIPS mode. "
      "`permissive` - Redpanda performs the same check as enabled, but a "
      "warning is logged, and Redpanda continues to run. Redpanda loads the "
      "OpenSSL FIPS provider into the OpenSSL library. After this completes, "
      "Redpanda is operating in FIPS mode, which means that the TLS cipher "
      "suites available to users are limited to the TLSv1.2 and TLSv1.3 "
      "NIST-approved cryptographic methods. `enabled` - Redpanda verifies that "
      "the operating system is enabled for FIPS by checking "
      "`/proc/sys/crypto/fips_enabled`. If the file does not exist or does not "
      "return `1`, Redpanda immediately exits.",
      {.visibility = visibility::user},
      fips_mode_flag::disabled,
      {fips_mode_flag::disabled,
       fips_mode_flag::enabled,
       fips_mode_flag::permissive})
  , openssl_config_file(
      *this,
      "openssl_config_file",
      "Path to the configuration file used by OpenSSL to properly load the "
      "FIPS-compliant module.",
      {.visibility = visibility::user},
      std::nullopt)
  , openssl_module_directory(
      *this,
      "openssl_module_directory",
      "Path to the directory that contains the OpenSSL FIPS-compliant module. "
      "The filename that Redpanda looks for is `fips.so`.",
      {.visibility = visibility::user},
      std::nullopt)
  , node_id_overrides(
      *this,
      "node_id_overrides",
      "List of node ID and UUID overrides to be applied at broker startup. "
      "Each entry includes the current UUID and desired ID and UUID. Each "
      "entry applies to a given node if and only if 'current' matches that "
      "node's current UUID.",
      {.visibility = visibility::user},
      {})
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
    const auto& cfg = config::node();
    const auto& kafka_api = cfg.kafka_api.value();
    for (const auto& ep : kafka_api) {
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

    for (const auto& ep : cfg.advertised_kafka_api()) {
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

    auto ignore = shard_local_cfg().property_names_and_aliases();

    auto errors = config_store::read_yaml(
      root_node["redpanda"], std::move(ignore));
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
