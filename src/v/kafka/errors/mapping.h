#pragma once
#include "cluster/types.h"
#include "kafka/errors/errors.h"

namespace kafka {

static constexpr inline error_code
map_topic_error_code(cluster::topic_error_code code) {
    switch (code) {
    case cluster::topic_error_code::no_error:
        return error_code::none;
    case cluster::topic_error_code::unknown_error:
        return error_code::unknown_server_error;
    case cluster::topic_error_code::invalid_config:
        return error_code::invalid_config;
    case cluster::topic_error_code::invalid_partitions:
        return error_code::invalid_partitions;
    case cluster::topic_error_code::invalid_replication_factor:
        return error_code::invalid_replication_factor;
    case cluster::topic_error_code::time_out:
        return error_code::request_timed_out;
    default:
        return error_code::unknown_server_error;
    }
}

} // namespace kafka
