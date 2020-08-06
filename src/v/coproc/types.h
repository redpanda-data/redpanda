#pragma once

#include "model/adl_serde.h"
#include "model/fundamental.h"

#include <vector>

namespace coproc {

/// \brief per topic a client will recieve a response code on the
/// registration status of the topic
enum class enable_response_code : int8_t {
    success = 0,
    topic_already_enabled,
    topic_does_not_exist,
    invalid_topic,
    materialized_topic,
    internal_error
};

/// \brief per topic a client will recieve a response code on the
/// deregistration status of the topic
enum class disable_response_code : int8_t {
    success = 0,
    topic_never_enabled,
    invalid_topic,
    materialized_topic,
    internal_error
};

/// \brief type to use for registration/deregistration of a topic
struct metadata_info {
    std::vector<model::topic> inputs;
};

/// \brief registration acks per topic, responses are organized in the
/// same order as the list of topics in the 'inputs' array
struct enable_topics_reply {
    std::vector<enable_response_code> acks;
};

/// \brief deregistration acks per topic, responses are organized in the
/// same order as the list of topics in the 'inputs' array
struct disable_topics_reply {
    std::vector<disable_response_code> acks;
};

} // namespace coproc
