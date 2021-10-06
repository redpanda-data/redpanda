// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "config/data_directory_path.h"
#include "config/property.h"
#include "config_store.h"

namespace config {

struct node_config final : public config_store {
public:
    property<data_directory_path> data_directory;
    property<model::node_id> node_id;

    property<unresolved_address> rpc_server;
    property<tls_config> rpc_server_tls;

    property<std::optional<ss::sstring>> cloud_storage_cache_directory;

    // build pidfile path: `<data_directory>/pid.lock`
    std::filesystem::path pidfile_path() const {
        return data_directory().path / "pid.lock";
    }

    unresolved_address advertised_rpc_api() const {
        return _advertised_rpc_api().value_or(rpc_server());
    }

    node_config() noexcept;
    void load(const YAML::Node& root_node);

private:
    property<std::optional<unresolved_address>> _advertised_rpc_api;
};

node_config& node();

} // namespace config
