// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "config/broker_endpoint.h"
#include "config/convert.h"
#include "config/data_directory_path.h"
#include "config/property.h"
#include "config/seed_server.h"
#include "config_store.h"

namespace config {

struct node_config final : public config_store {
public:
    property<data_directory_path> data_directory;
    property<model::node_id> node_id;
    property<std::optional<ss::sstring>> rack;
    property<std::vector<seed_server>> seed_servers;

    // Internal RPC listener
    property<net::unresolved_address> rpc_server;
    property<tls_config> rpc_server_tls;

    // Kafka RPC listener
    one_or_many_property<model::broker_endpoint> kafka_api;
    one_or_many_property<endpoint_tls_config> kafka_api_tls;

    // Admin API listener
    one_or_many_property<model::broker_endpoint> admin;
    one_or_many_property<endpoint_tls_config> admin_api_tls;

    // Coproc/wasm
    property<net::unresolved_address> coproc_supervisor_server;

    // HTTP server content dirs
    property<ss::sstring> admin_api_doc_dir;
    property<std::optional<ss::sstring>> dashboard_dir;

    // Shadow indexing/S3 cache location
    property<std::optional<ss::sstring>> cloud_storage_cache_directory;

    deprecated_property enable_central_config;

    // build pidfile path: `<data_directory>/pid.lock`
    std::filesystem::path pidfile_path() const {
        return data_directory().path / "pid.lock";
    }

    const std::vector<model::broker_endpoint>& advertised_kafka_api() const {
        if (_advertised_kafka_api().empty()) {
            return kafka_api();
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
      std::filesystem::path const& loaded_from, const YAML::Node& root_node) {
        _cfg_file_path = loaded_from;
        return load(root_node);
    }

    std::filesystem::path const& get_cfg_file_path() const {
        return _cfg_file_path;
    }

private:
    property<std::optional<net::unresolved_address>> _advertised_rpc_api;
    one_or_many_property<model::broker_endpoint> _advertised_kafka_api;
    std::filesystem::path _cfg_file_path;
};

node_config& node();

} // namespace config
