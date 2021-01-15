/*
 * Copyright 2020 Vectorized, Inc.
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
#include "kafka/errors.h"
#include "kafka/requests/schemata/create_topics_request.h"
#include "kafka/requests/schemata/create_topics_response.h"
#include "model/fundamental.h"
#include "model/namespace.h"

#include <absl/container/flat_hash_map.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

namespace kafka {

/// \brief Type representing Kafka protocol response from
/// CreateTopics, DeleteTopics and CreatePartitions requests
/// the types used here match the types used in Kafka protocol specification
struct topic_op_result {
    model::topic topic;
    error_code ec;
    std::optional<ss::sstring> err_msg;
};

inline creatable_topic_result
from_cluster_topic_result(const cluster::topic_result& err) {
    return {.name = err.tp_ns.tp, .error_code = map_topic_error_code(err.ec)};
}

absl::flat_hash_map<ss::sstring, ss::sstring>
config_map(const std::vector<createable_topic_config>& config);

cluster::topic_configuration to_cluster_type(const creatable_topic& t);

} // namespace kafka
