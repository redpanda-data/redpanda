/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "kafka/server/datalake_usage_api.h"

#include "utils/to_string.h"

namespace kafka {

datalake_usage_api::usage_stats::usage_stats(const usage_stats& other) {
    if (other.topic_stats) {
        topic_stats = other.topic_stats->copy();
    } else {
        topic_stats.reset();
    }
    missing_reason = other.missing_reason;
}

datalake_usage_api::usage_stats& datalake_usage_api::usage_stats::operator=(
  const datalake_usage_api::usage_stats& other) {
    if (this != &other) {
        if (other.topic_stats) {
            topic_stats = other.topic_stats->copy();
        } else {
            topic_stats.reset();
        }
        missing_reason = other.missing_reason;
    }
    return *this;
}

std::ostream& operator<<(
  std::ostream& os, const datalake_usage_api::stats_missing_reason& r) {
    switch (r) {
    case datalake_usage_api::stats_missing_reason::none:
        return os << "none";
    case datalake_usage_api::stats_missing_reason::feature_disabled:
        return os << "feature_disabled";
    case datalake_usage_api::stats_missing_reason::collection_error:
        return os << "collection_error";
    case datalake_usage_api::stats_missing_reason::not_controller_leader:
        return os << "not_controller_leader";
    }
}

std::ostream&
operator<<(std::ostream& os, const datalake_usage_api::topic_usage& u) {
    fmt::print(
      os,
      "{{ topic: {} revision: {} kafka_bytes_processed: {} }}",
      u.topic,
      u.revision,
      u.kafka_bytes_processed);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const datalake_usage_api::usage_stats& u) {
    fmt::print(
      os,
      "{{ topic_stats: {}, missing_stats_reason: {} }}",
      u.topic_stats,
      u.missing_reason);
    return os;
}

} // namespace kafka
