/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster_link/model/types.h"
#include "kafka/protocol/types.h"

namespace cluster_link::model {
/**
 * @brief Determines whether the given topic matches the filter criteria
 *
 * This method iterates through the provided pattern list to determine if the
 * topic matches any include filters. If no include filters match, or if an
 * exclude filter matches, the topic is not selected.
 *
 * @param topic The name of the topic to evaluate
 * @param patterns The list of include/exclude filter patterns
 * @return True if the topic passes the filter criteria; false otherwise
 */
bool select_topic(
  ::model::topic_view topic,
  const chunked_vector<resource_name_filter_pattern>& patterns);

/**
 * @brief Determines whether a topic is in scope for a default-include filter
 *
 * Unlike select_topic (opt-in: a topic is only selected if some include
 * pattern matches it), this uses default-include semantics: the topic is
 * in scope unless an exclude pattern matches it, and, when include patterns
 * are present, at least one of them also matches. An empty pattern list
 * scopes every topic. An exclude match always wins over an include match.
 *
 * @param topic The name of the topic to evaluate
 * @param patterns The list of include/exclude filter patterns
 * @return True if the topic is in scope; false otherwise
 */
bool select_topic_default_include(
  ::model::topic_view topic,
  const chunked_vector<resource_name_filter_pattern>& patterns);

bool select_group(
  const kafka::group_id& group_id,
  const chunked_vector<resource_name_filter_pattern>& patterns);

bool select_role(
  std::string_view role_name,
  const chunked_vector<resource_name_filter_pattern>& patterns);

} // namespace cluster_link::model
