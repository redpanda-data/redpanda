#pragma once
#include "cluster/types.h"
#include "model/fundamental.h"
#include "redpanda/kafka/default_namespace.h"
#include "redpanda/kafka/errors/errors.h"
#include "redpanda/kafka/errors/mapping.h"

#include <boost/lexical_cast.hpp>

namespace kafka::requests {

/// \brief Type representing Kafka protocol response from
/// CreateTopics, DeleteTopics and CreatePartitions requests
/// the types used here match the types used in Kafka protocol specification
struct topic_op_result {
    model::topic topic;
    error_code error_code;
    std::optional<sstring> err_msg;
    static topic_op_result
    from_cluster_topic_result(const cluster::topic_result& err) {
        return {.topic = err.topic,
                .error_code = map_topic_error_code(err.error_code)};
    }
};

/// \brief Partition assignment used in Kafka protocol.
/// It maps the partition id to node_id
struct partition_assignment {
    model::partition_id partition;
    std::vector<model::node_id> assignments;
};
/// \brief Part of CreateTopics request from Kafka API
/// that describes single topic configuration
struct new_topic_configuration {
    model::topic_view topic;
    // using signed int as Kafka protocol defines it to be signed integer
    int32_t partition_count;
    // using signed int as Kafka protocol defines it to be signed integer
    int16_t replication_factor;
    std::vector<partition_assignment> assignments;
    std::unordered_map<sstring, sstring> config_entries;

    cluster::topic_configuration to_cluster_type() const {
        cluster::topic_configuration cfg(
          default_namespace(),
          model::topic{sstring(topic())},
          partition_count,
          replication_factor);
        if (auto it = config_entries.find("compression.type");
            it != config_entries.end()) {
            cfg.compression = boost::lexical_cast<model::compression>(
              (*it).second);
        }
        if (auto it = config_entries.find("cleanup.policy");
            it != config_entries.end()) {
            if ((*it).second == "compact") {
                cfg.compaction = model::topic_partition::compaction::yes;
            }
        }
        if (auto it = config_entries.find("retention.bytes");
            it != config_entries.end()) {
            cfg.retention_bytes = boost::lexical_cast<uint64_t>((*it).second);
        }
        if (auto it = config_entries.find("retention.ms");
            it != config_entries.end()) {
            cfg.retention = std::chrono::milliseconds(
              boost::lexical_cast<uint64_t>((*it).second));
        }
        return cfg;
    }
};
} // namespace kafka::requests
