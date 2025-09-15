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

bool select_group(
  const kafka::group_id& group_id,
  const chunked_vector<resource_name_filter_pattern>& patterns);

} // namespace cluster_link::model
