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
#include "kafka/protocol/describe_configs.h"
#include "kafka/protocol/create_topics.h"

namespace kafka {

void report_topic_config(
  const describe_configs_resource& resource,
  describe_configs_result& result,
  const cluster::metadata_cache& metadata_cache,
  const cluster::topic_properties& topic_properties,
  bool include_synonyms,
  bool include_documentation);

void report_broker_config(
  const describe_configs_resource& resource,
  describe_configs_result& result,
  bool include_synonyms,
  bool include_documentation);

std::vector<creatable_topic_configs> make_configs(
  const cluster::metadata_cache& metadata_cache,
  const cluster::topic_properties& topic_config);

} // namespace kafka
