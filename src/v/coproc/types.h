/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "coproc/errc.h"
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

/// \brief per topic a client will recieve a response code on the
/// registration status of the topic
enum class enable_response_code : int8_t {
    success = 0,
    internal_error,
    invalid_ingestion_policy,
    script_id_already_exists,
    script_contains_invalid_topic,
    script_contains_no_topics
};

/// \brief per topic a client will recieve a response code on the
/// deregistration status of the topic
enum class disable_response_code : int8_t {
    success = 0,
    internal_error,
    script_id_does_not_exist,
};

enum class topic_ingestion_policy : int8_t { earliest = 0, stored, latest };

inline bool is_valid_ingestion_policy(topic_ingestion_policy p) {
    // Other ingestion policies to be implemented at a later time
    return p == topic_ingestion_policy::latest;
}

/// \brief type to use for registration/deregistration of a topic
struct enable_copros_request {
    struct data {
        script_id id;
        iobuf source_code;
    };
    std::vector<data> inputs;
};

/// \brief registration acks per copro, responses are organized in the
/// same order as the list of topics in the 'topics' array
struct enable_copros_reply {
    using topic_policy = std::pair<model::topic, topic_ingestion_policy>;
    struct script_metadata {
        script_id id;
        std::vector<topic_policy> input_topics;
    };
    struct data {
        enable_response_code ack;
        script_metadata script_meta;
    };
    std::vector<data> acks;
};

/// Stub for what should be 0 parameter method, 'disable_all_coprocessors'
using empty_request = named_type<int8_t, struct empty_req_tag>;

/// \brief deregistration request, remove all topics registered to a coprocessor
/// with id 'script_id'.
struct disable_copros_request {
    std::vector<script_id> ids;
};

/// \brief deregistration acks per topic, responses are organized in the
/// same order as the list of topics in the 'ids' array
struct disable_copros_reply {
    using ack = std::pair<script_id, disable_response_code>;
    std::vector<ack> acks;
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

struct topic_namespace_policy {
    model::topic_namespace tn;
    topic_ingestion_policy policy;
};

std::ostream& operator<<(std::ostream& os, const enable_response_code);

std::ostream& operator<<(std::ostream& os, const disable_response_code);

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
