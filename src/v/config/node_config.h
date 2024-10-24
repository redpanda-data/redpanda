// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "config/bounded_property.h"
#include "config/broker_authn_endpoint.h"
#include "config/broker_endpoint.h"
#include "config/convert.h"
#include "config/data_directory_path.h"
#include "config/node_overrides.h"
#include "config/property.h"
#include "config/seed_server.h"
#include "config_store.h"
#include "model/fundamental.h"

#include <algorithm>
#include <iterator>

namespace config {

struct node_config final : public config_store {
public:
    property<bool> developer_mode;
    property<data_directory_path> data_directory;

    // NOTE: during the normal runtime of a cluster, it is safe to assume that
    // the value of the node ID has been determined, and that there is a value
    // set for this property.
    property<std::optional<model::node_id>> node_id;

    property<std::optional<model::rack_id>> rack;
    property<std::vector<seed_server>> seed_servers;
    property<bool> empty_seed_starts_cluster;

    // Internal RPC listener
    property<net::unresolved_address> rpc_server;
    property<tls_config> rpc_server_tls;

    // Kafka RPC listener
    one_or_many_property<config::broker_authn_endpoint> kafka_api;
    one_or_many_property<endpoint_tls_config> kafka_api_tls;

    // Admin API listener
    one_or_many_property<model::broker_endpoint> admin;
    one_or_many_property<endpoint_tls_config> admin_api_tls;

    // Coproc/wasm
    deprecated_property coproc_supervisor_server;

    // Data transforms
    property<bool> emergency_disable_data_transforms;

    // HTTP server content dirs
    property<ss::sstring> admin_api_doc_dir;
    deprecated_property dashboard_dir;

    // Shadow indexing/S3 cache location
    property<std::optional<ss::sstring>> cloud_storage_cache_directory;

    // Path to store inventory file hashes for cloud storage scrubber
    property<std::optional<ss::sstring>> cloud_storage_inventory_hash_store;

    deprecated_property enable_central_config;

    property<std::optional<uint32_t>> crash_loop_limit;

    // If true, permit any version of redpanda to start, even
    // if potentially incompatible with existing system state.
    property<bool> upgrade_override_checks;
    property<std::optional<size_t>> memory_allocation_warning_threshold;

    // If true, inject low level failures in the storage layer according
    // to the configuration at `storage_failure_injection_config_path`.
    property<bool> storage_failure_injection_enabled;

    // If true, start redpanda in "metadata only" mode, skipping loading
    // user partitions and allowing only metadata operations.
    property<bool> recovery_mode_enabled;

    // Path to the configuration file for low level storage failure injection.
    property<std::optional<std::filesystem::path>>
      storage_failure_injection_config_path;

    // Timeout upper-bound for setting verbose (>=DEBUG) logging.
    bounded_property<std::optional<std::chrono::seconds>>
      verbose_logging_timeout_sec_max;

    // Flag indicating whether or not Redpanda will start in FIPS mode
    enum_property<fips_mode_flag> fips_mode;

    // Path to the OpenSSL config file
    property<std::optional<std::filesystem::path>> openssl_config_file;

    // Path to the directory that holds the OpenSSL FIPS module
    property<std::optional<std::filesystem::path>> openssl_module_directory;

    property<std::vector<config::node_id_override>> node_id_overrides;

    // build pidfile path: `<data_directory>/pid.lock`
    std::filesystem::path pidfile_path() const {
        return data_directory().path / "pid.lock";
    }

    std::filesystem::path strict_data_dir_file_path() const {
        return data_directory().path / ".redpanda_data_dir";
    }

    std::filesystem::path disk_benchmark_path() const {
        return data_directory().path / "syschecks";
    }

    // This file tracks the metadata needed to detect crash loops
    std::filesystem::path crash_loop_tracker_path() const {
        return data_directory().path / "startup_log";
    }

    /**
     * Return the configured cache path if set, otherwise a default
     * path within the data directory.
     */
    std::filesystem::path cloud_storage_cache_path() const {
        if (cloud_storage_cache_directory().has_value()) {
            return std::string(cloud_storage_cache_directory().value());
        } else {
            return data_directory().path / "cloud_storage_cache";
        }
    }

    std::filesystem::path cloud_storage_inventory_hash_path() const {
        if (cloud_storage_inventory_hash_store().has_value()) {
            return std::filesystem::path{
              cloud_storage_inventory_hash_store().value()};
        }
        return data_directory().path / "cloud_storage_inventory";
    }

    std::vector<model::broker_endpoint> advertised_kafka_api() const {
        if (_advertised_kafka_api().empty()) {
            std::vector<model::broker_endpoint> eps;
            auto api = kafka_api();
            eps.reserve(api.size());
            std::transform(
              std::make_move_iterator(api.begin()),
              std::make_move_iterator(api.end()),
              std::back_inserter(eps),
              [](auto ep) {
                  return model::broker_endpoint{ep.name, ep.address};
              });
            return eps;
        }
        return _advertised_kafka_api();
    }

    const one_or_many_property<model::broker_endpoint>&
    advertised_kafka_api_property() {
        return _advertised_kafka_api;
    }

    net::unresolved_address advertised_rpc_api() const {
        return _advertised_rpc_api().value_or(rpc_server());
    }

    node_config() noexcept;
    error_map_t load(const YAML::Node& root_node);
    error_map_t load(
      const std::filesystem::path& loaded_from, const YAML::Node& root_node) {
        _cfg_file_path = loaded_from;
        return load(root_node);
    }

    const std::filesystem::path& get_cfg_file_path() const {
        return _cfg_file_path;
    }

private:
    property<std::optional<net::unresolved_address>> _advertised_rpc_api;
    one_or_many_property<model::broker_endpoint> _advertised_kafka_api;
    std::filesystem::path _cfg_file_path;
};

node_config& node();

} // namespace config
