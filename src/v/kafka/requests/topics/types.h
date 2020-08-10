#pragma once
#include "cluster/namespace.h"
#include "cluster/types.h"
#include "kafka/errors.h"
#include "kafka/requests/schemata/create_topics_request.h"
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

absl::flat_hash_map<ss::sstring, ss::sstring>
config_map(const std::vector<createable_topic_config>& config);
cluster::topic_configuration to_cluster_type(const creatable_topic& t);

} // namespace kafka
