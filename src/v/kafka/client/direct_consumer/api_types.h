/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "base/outcome.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/schemata/fetch_response.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/util/bool_class.hh>
namespace kafka::client {

using subscription_epoch = named_type<uint64_t, struct subscription_epoch_tag>;

enum class offset_reset_policy : int8_t {
    // reset to the earliest offset
    earliest,
    // reset to the latest offset
    latest,
};

inline std::ostream& operator<<(std::ostream& os, offset_reset_policy p) {
    switch (p) {
    case offset_reset_policy::earliest:
        return os << "earliest";
    case offset_reset_policy::latest:
        return os << "latest";
    }
}

template<typename T>
using kafka_result = result<T, kafka::error_code>;

/**
 * Control for assigned topic/partition
 */
struct partition_assignment {
    model::partition_id partition_id;
    std::optional<kafka::offset> next_offset;
};

struct topic_assignment {
    model::topic topic;
    chunked_vector<partition_assignment> partitions;
};

/**
 * Data exposed to the `fetch_next()` caller
 */
struct fetched_partition_data {
    // these should always be set, initialize to invalid
    model::partition_id partition_id{-1};
    kafka::leader_epoch leader_epoch{-1};
    kafka::offset start_offset{-1};
    kafka::offset high_watermark{-1};
    kafka::offset last_stable_offset{model::invalid_lso};

    // the following have reasonable defaults
    chunked_vector<model::record_batch> data{};
    kafka::error_code error = kafka::error_code::none;
    std::optional<chunked_vector<aborted_transaction>> aborted_transactions{
      std::nullopt};
    subscription_epoch
      subscription_epoch; // should always be set but no invalid value
    size_t size_bytes{0};
};

struct fetched_topic_data {
    model::topic topic;

    chunked_vector<fetched_partition_data> partitions;
    size_t total_bytes{0};
};

using fetches = kafka_result<chunked_vector<fetched_topic_data>>;

// Offsets of a source partition, reported by fetch responses
struct source_partition_offsets {
    // The start of the source partition's log
    kafka::offset log_start_offset{-1};
    // The source partition's log high watermark
    kafka::offset high_watermark{-1};
    // The source partition's log last stable offset
    kafka::offset last_stable_offset{model::offset_cast(model::invalid_lso)};
    // The timestamp that the fetch response was received by the client
    ss::lowres_clock::time_point last_offset_update_timestamp{};

    // check all except timestamp, useful for checking if offsets have changed
    // over time
    bool are_offsets_equal(const source_partition_offsets& other) const {
        return log_start_offset == other.log_start_offset
               && high_watermark == other.high_watermark
               && last_stable_offset == other.last_stable_offset;
    }
};

struct partition_offset {
    model::partition_id partition_id;
    kafka::error_code error_code = kafka::error_code::none;
    kafka::leader_epoch leader_epoch;
    kafka::offset offset;
};

struct topic_partition_offsets {
    model::topic topic;
    kafka::error_code error_code = kafka::error_code::none;
    chunked_vector<partition_offset> offsets;
};

template<typename Element>
using topic_partition_map = chunked_hash_map<
  model::topic,
  chunked_hash_map<model::partition_id, Element>>;

using fetch_sessions_enabled
  = ss::bool_class<struct fetch_sessions_enabled_tag>;

} // namespace kafka::client
