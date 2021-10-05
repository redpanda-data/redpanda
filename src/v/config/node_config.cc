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
  , cloud_storage_cache_directory(
      *this,
      "cloud_storage_cache_directory",
      "Directory for archival cache. Should be present when "
      "`cloud_storage_enabled` is present",
      required::no,
      (data_directory.value().path / "archival_cache").native()) {}

void node_config::load(const YAML::Node& root_node) {
    if (!root_node["redpanda"]) {
        throw std::invalid_argument("'redpanda'root is required");
    }

    const auto& ignore = shard_local_cfg().property_names();

    config_store::read_yaml(root_node["redpanda"], ignore);
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
