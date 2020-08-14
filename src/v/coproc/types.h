#pragma once

#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "utils/named_type.h"

#include <optional>
#include <string_view>
#include <vector>

namespace coproc {

/// \brief View wrapper for a materialized topic format
/// The format for a materialized_topic will not pass kafka topic validators
struct materialized_topic {
    model::topic_view src;
    model::topic_view dest;
};

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

/// Parses a topic formatted as a materialized topic
/// \return materialized_topic view or std::nullopt if parameter fails to be
/// parsed correctly
std::optional<materialized_topic> make_materialized_topic(const model::topic&);

/// \brief Returns true if the schema obeys the $<src>.<dest>$ pattern
inline bool is_materialized_topic(const model::topic& t) {
    return make_materialized_topic(t).has_value();
}

} // namespace coproc
