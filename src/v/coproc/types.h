#pragma once

#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "reflection/async_adl.h"
#include "utils/named_type.h"

#include <optional>
#include <string_view>
#include <vector>

namespace coproc {

using script_id = named_type<uint64_t, struct script_id_tag>;

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
    invalid_ingestion_policy,
    script_id_already_exists,
    topic_does_not_exist,
    invalid_topic,
    materialized_topic,
    internal_error
};

/// \brief per topic a client will recieve a response code on the
/// deregistration status of the topic
enum class disable_response_code : int8_t {
    success = 0,
    script_id_does_not_exist,
    internal_error
};

enum class topic_ingestion_policy : int8_t { earliest = 0, stored, latest };

inline bool is_valid_ingestion_policy(topic_ingestion_policy p) {
    // Other ingestion policies to be implemented at a later time
    return p == topic_ingestion_policy::latest;
}

/// \brief type to use for registration/deregistration of a topic
struct enable_copros_request {
    struct data {
        using topic_mode = std::pair<model::topic, topic_ingestion_policy>;
        script_id id;
        std::vector<topic_mode> topics;
    };
    std::vector<data> inputs;
};

/// \brief registration acks per copro, responses are organized in the
/// same order as the list of topics in the 'topics' array
struct enable_copros_reply {
    using ack_id_pair = std::pair<script_id, std::vector<enable_response_code>>;
    std::vector<ack_id_pair> acks;
};

/// \brief deregistration request, remove all topics registered to a coprocessor
/// with id 'script_id'.
struct disable_copros_request {
    std::vector<script_id> ids;
};

/// \brief deregistration acks per topic, responses are organized in the
/// same order as the list of topics in the 'ids' array
struct disable_copros_reply {
    std::vector<disable_response_code> acks;
};

/// \brief Request that co-processors with the given script ids, process batches
/// from the reader whose source topic is the given ntp
struct process_batch_request {
    struct data {
        std::vector<script_id> ids;
        model::ntp ntp;
        model::record_batch_reader reader;
    };
    std::vector<data> reqs;
};

/// \brief Response from the above request, acks from script ids that have
/// processed the record and produce new batches on a new materialized ntp
struct process_batch_reply {
    struct data {
        script_id id;
        model::ntp ntp;
        model::record_batch_reader reader;
    };
    std::vector<data> resps;
};

/// Parses a topic formatted as a materialized topic
/// \return materialized_topic view or std::nullopt if parameter fails to be
/// parsed correctly
std::optional<materialized_topic> make_materialized_topic(const model::topic&);

/// \brief Returns true if the schema obeys the $<src>.<dest>$ pattern
inline bool is_materialized_topic(const model::topic& t) {
    return make_materialized_topic(t).has_value();
}

std::ostream&
operator<<(std::ostream& os, enable_copros_request::data::topic_mode);

} // namespace coproc

namespace reflection {

template<>
struct async_adl<coproc::process_batch_request> {
    ss::future<> to(iobuf& out, coproc::process_batch_request&&);
    ss::future<coproc::process_batch_request> from(iobuf_parser&);
};

template<>
struct async_adl<coproc::process_batch_request::data> {
    ss::future<> to(iobuf& out, coproc::process_batch_request::data&&);
    ss::future<coproc::process_batch_request::data> from(iobuf_parser&);
};

template<>
struct async_adl<coproc::process_batch_reply> {
    ss::future<> to(iobuf& out, coproc::process_batch_reply&&);
    ss::future<coproc::process_batch_reply> from(iobuf_parser&);
};

template<>
struct async_adl<coproc::process_batch_reply::data> {
    ss::future<> to(iobuf& out, coproc::process_batch_reply::data&&);
    ss::future<coproc::process_batch_reply::data> from(iobuf_parser&);
};

} // namespace reflection
