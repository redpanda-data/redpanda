#pragma once
#include "cluster/namespace.h"
#include "cluster/types.h"
#include "kafka/errors.h"
#include "model/fundamental.h"

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
    static topic_op_result
    from_cluster_topic_result(const cluster::topic_result& err) {
        return {.topic = err.tp_ns.tp, .ec = map_topic_error_code(err.ec)};
    }
};

/// \brief Partition assignment used in Kafka protocol.
/// It maps the partition id to node_id
struct partition_assignment {
    model::partition_id partition;
    std::vector<model::node_id> assignments;
};

struct config_entry {
    ss::sstring name;
    ss::sstring value;
};

/// \brief Part of CreateTopics request from Kafka API
/// that describes single topic configuration
struct new_topic_configuration {
    model::topic topic;
    int32_t partition_count;
    int16_t replication_factor;
    std::vector<partition_assignment> assignments;
    std::vector<config_entry> config;

    absl::flat_hash_map<ss::sstring, ss::sstring> config_map() const;

    cluster::topic_configuration to_cluster_type() const;
};
} // namespace kafka
