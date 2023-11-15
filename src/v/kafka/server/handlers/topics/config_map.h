/*
 * Copyright 2023 Redpanda Data, Inc.
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
#include "kafka/protocol/schemata/create_topics_request.h"
#include "kafka/protocol/schemata/create_topics_response.h"
#include "kafka/server/handlers/topics/types.h"

#include <boost/lexical_cast.hpp>

namespace kafka {

inline creatable_topic_result
from_cluster_topic_result(const cluster::topic_result& err) {
    return {.name = err.tp_ns.tp, .error_code = map_topic_error_code(err.ec)};
}

config_map_t config_map(const std::vector<createable_topic_config>& config);
config_map_t config_map(const std::vector<creatable_topic_configs>& config);

cluster::custom_assignable_topic_configuration
to_cluster_type(const creatable_topic& t);

config_map_t from_cluster_type(const cluster::topic_properties&);
} // namespace kafka
