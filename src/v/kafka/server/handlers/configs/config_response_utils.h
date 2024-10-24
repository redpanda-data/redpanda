/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/types.h"
#include "container/fragmented_vector.h"
#include "kafka/protocol/describe_configs.h"
#include "kafka/protocol/schemata/create_topics_response.h"

#include <iterator>
#include <optional>

namespace kafka {

struct config_response {
    ss::sstring name{};
    std::optional<ss::sstring> value{};
    bool read_only{};
    bool is_default{};
    kafka::describe_configs_source config_source{-1};
    bool is_sensitive{};
    std::vector<describe_configs_synonym> synonyms{};
    kafka::describe_configs_type config_type{0};
    std::optional<ss::sstring> documentation{};

    describe_configs_resource_result to_describe_config();
    creatable_topic_configs to_create_config();
};

using config_response_container_t = chunked_vector<config_response>;
using config_key_t = std::optional<chunked_vector<ss::sstring>>;

config_response_container_t make_topic_configs(
  const cluster::metadata_cache& metadata_cache,
  const cluster::topic_properties& topic_properties,
  const config_key_t& config_keys,
  bool include_synonyms,
  bool include_documentation);

config_response_container_t make_broker_configs(
  const config_key_t& config_keys,
  bool include_synonyms,
  bool include_documentation);

} // namespace kafka
