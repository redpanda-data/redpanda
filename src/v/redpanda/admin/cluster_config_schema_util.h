/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/configuration.h"
#include "config/node_config.h"

#include <seastar/json/json_elements.hh>

namespace util {

/**
 * @brief Generates json schema for a given configuration.
 * @return ss::json::json_return_type
 */
ss::json::json_return_type
generate_json_schema(const config::configuration& conf);

/**
 * @brief Generates json schema for node (broker-scoped) configuration.
 * Same mechanism as generate_json_schema, for config::node_config instead
 * of config::configuration.
 * @return ss::json::json_return_type
 */
ss::json::json_return_type
generate_node_config_json_schema(const config::node_config& conf);

} // namespace util
