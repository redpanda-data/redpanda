/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/coordinator/types.h"

#include "utils/to_string.h"

namespace datalake::coordinator {

std::ostream& operator<<(std::ostream& o, const errc& errc) {
    switch (errc) {
    case errc::ok:
        return o << "errc::ok";
    case errc::coordinator_topic_not_exists:
        return o << "errc::coordinator_topic_not_exists";
    case errc::not_leader:
        return o << "errc::not_leader";
    case errc::timeout:
        return o << "errc::timeout";
    case errc::fenced:
        return o << "errc::fenced";
    case errc::stale:
        return o << "errc::stale";
    case errc::concurrent_requests:
        return o << "errc::concurrent_requests";
    case errc::revision_mismatch:
        return o << "errc::revision_mismatch";
    case errc::incompatible_schema:
        return o << "errc::incompatible_schema";
    case errc::failed:
        return o << "errc::failed";
    }
    return o << fmt::format("errc::unknown({})", static_cast<int16_t>(errc));
}

std::ostream& operator<<(std::ostream& o, const ensure_table_exists_reply& r) {
    fmt::print(o, "{{errc: {}}}", r.errc);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const ensure_table_exists_request& r) {
    fmt::print(
      o, "{{topic: {}, topic_revision: {}}}", r.topic, r.topic_revision);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const add_translated_data_files_reply& reply) {
    fmt::print(o, "{{errc: {}}}", reply.errc);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const add_translated_data_files_request& request) {
    fmt::print(
      o,
      "{{partition: {}, topic_revision: {}, files: {}, translation term: {}}}",
      request.tp,
      request.topic_revision,
      request.ranges,
      request.translator_term);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const fetch_latest_translated_offset_reply& reply) {
    fmt::print(
      o,
      "{{errc: {}, offset: {}, backpressure: {}}}",
      reply.errc,
      reply.last_added_offset,
      reply.backpressure);
    return o;
}

std::ostream& operator<<(
  std::ostream& o, const fetch_latest_translated_offset_request& request) {
    fmt::print(
      o,
      "{{partition: {}, topic_revision: {}}}",
      request.tp,
      request.topic_revision);
    return o;
}

std::ostream& operator<<(std::ostream& o, const per_topic_usage_stats& stats) {
    fmt::print(
      o,
      "{{topic: {}, revision: {}, total_kafka_bytes_processed: {}}}",
      stats.topic,
      stats.revision,
      stats.total_kafka_bytes_processed);
    return o;
}

std::ostream& operator<<(std::ostream& o, const datalake_usage_stats& stats) {
    fmt::print(o, "{{topic_usages: {} }}", stats.topic_usages);
    return o;
}

std::ostream& operator<<(std::ostream& o, const usage_stats_reply& resp) {
    fmt::print(o, "{{errc: {}, stats: {}}}", resp.errc, resp.stats);
    return o;
}

std::ostream& operator<<(std::ostream& o, const usage_stats_request& req) {
    fmt::print(o, "{{coordinator_partition: {}}}", req.coordinator_partition);
    return o;
}

std::ostream& operator<<(std::ostream& o, const get_topic_state_reply& reply) {
    fmt::print(
      o,
      "{{errc: {}, topic_states size: {}}}",
      reply.errc,
      reply.topic_states.size());
    return o;
}

std::ostream&
operator<<(std::ostream& o, const get_topic_state_request& request) {
    fmt::print(
      o,
      "{{coordinator_partition: {}, topics_filter: {}}}",
      request.coordinator_partition,
      request.topics_filter);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const reset_topic_state_reply& reply) {
    fmt::print(o, "{{errc: {}}}", reply.errc);
    return o;
}

} // namespace datalake::coordinator
