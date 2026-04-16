/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "datalake/coordinator/partition_state_override.h"
#include "datalake/coordinator/state.h"
#include "datalake/coordinator/translated_offset_range.h"
#include "datalake/errors.h"
#include "datalake/schema_identifier.h"
#include "model/fundamental.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/map.h"

namespace datalake::coordinator {

enum class errc : int16_t {
    ok,
    coordinator_topic_not_exists,
    not_leader,
    timeout,
    fenced,
    stale,
    concurrent_requests,
    revision_mismatch,
    incompatible_schema,
    failed,
};

constexpr bool is_retriable(errc errc) {
    return errc == errc::coordinator_topic_not_exists
           || errc == errc::not_leader || errc == errc::timeout
           || errc == errc::concurrent_requests;
}

std::ostream& operator<<(std::ostream&, const errc&);

struct ensure_table_exists_reply
  : serde::envelope<
      ensure_table_exists_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    ensure_table_exists_reply() = default;
    explicit ensure_table_exists_reply(errc err)
      : errc(err) {}

    friend std::ostream&
    operator<<(std::ostream&, const ensure_table_exists_reply&);

    errc errc;

    auto serde_fields() { return std::tie(errc); }
};
struct ensure_table_exists_request
  : serde::envelope<
      ensure_table_exists_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = ensure_table_exists_reply;

    ensure_table_exists_request() = default;
    ensure_table_exists_request(
      model::topic topic,
      model::revision_id topic_revision,
      record_schema_components schema_components)
      : topic(std::move(topic))
      , topic_revision(topic_revision)
      , schema_components(std::move(schema_components)) {}

    model::topic topic;
    model::revision_id topic_revision;
    record_schema_components schema_components;

    friend std::ostream&
    operator<<(std::ostream&, const ensure_table_exists_request&);

    const model::topic& get_topic() const { return topic; }

    auto serde_fields() {
        return std::tie(topic, topic_revision, schema_components);
    }
};

struct ensure_dlq_table_exists_reply
  : serde::envelope<
      ensure_dlq_table_exists_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    ensure_dlq_table_exists_reply() = default;
    explicit ensure_dlq_table_exists_reply(errc err)
      : errc(err) {}

    friend std::ostream&
    operator<<(std::ostream&, const ensure_dlq_table_exists_reply&);

    errc errc;

    auto serde_fields() { return std::tie(errc); }
};

struct ensure_dlq_table_exists_request
  : serde::envelope<
      ensure_dlq_table_exists_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = ensure_dlq_table_exists_reply;

    ensure_dlq_table_exists_request() = default;
    ensure_dlq_table_exists_request(
      model::topic topic, model::revision_id topic_revision)
      : topic(std::move(topic))
      , topic_revision(topic_revision) {}

    model::topic topic;
    model::revision_id topic_revision;

    friend std::ostream&
    operator<<(std::ostream&, const ensure_dlq_table_exists_request&);

    const model::topic& get_topic() const { return topic; }

    auto serde_fields() { return std::tie(topic, topic_revision); }
};

struct add_translated_data_files_reply
  : serde::envelope<
      add_translated_data_files_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    add_translated_data_files_reply() = default;
    explicit add_translated_data_files_reply(errc err)
      : errc(err) {}

    friend std::ostream&
    operator<<(std::ostream&, const add_translated_data_files_reply&);

    errc errc;

    auto serde_fields() { return std::tie(errc); }
};
struct add_translated_data_files_request
  : serde::envelope<
      add_translated_data_files_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = add_translated_data_files_reply;

    add_translated_data_files_request() = default;

    model::topic_partition tp;
    model::revision_id topic_revision;
    // Translated data files, expected to be contiguous, with no gaps or
    // overlaps, ordered in increasing offset order.
    chunked_vector<translated_offset_range> ranges;
    model::term_id translator_term;

    add_translated_data_files_request(
      model::topic_partition tp,
      model::revision_id topic_revision,
      chunked_vector<translated_offset_range> ranges,
      model::term_id translator_term)
      : tp(std::move(tp))
      , topic_revision(topic_revision)
      , ranges(std::move(ranges))
      , translator_term(translator_term) {}

    add_translated_data_files_request copy() const {
        chunked_vector<translated_offset_range> copied_ranges;
        for (auto& range : ranges) {
            copied_ranges.push_back(range.copy());
        }
        return {
          tp,
          topic_revision,
          std::move(copied_ranges),
          translator_term,
        };
    }

    friend std::ostream&
    operator<<(std::ostream&, const add_translated_data_files_request&);

    const model::topic& get_topic() const { return tp.topic; }

    auto serde_fields() {
        return std::tie(tp, topic_revision, ranges, translator_term);
    }
};

struct fetch_latest_translated_offset_reply
  : serde::envelope<
      fetch_latest_translated_offset_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    fetch_latest_translated_offset_reply() = default;
    explicit fetch_latest_translated_offset_reply(errc err)
      : errc(err) {}
    explicit fetch_latest_translated_offset_reply(
      std::optional<kafka::offset> last_added,
      std::optional<kafka::offset> last_committed)
      : last_added_offset(last_added)
      , last_iceberg_committed_offset(last_committed)
      , errc(errc::ok) {}

    // The offset of the latest data file added to the coordinator.
    std::optional<kafka::offset> last_added_offset;

    std::optional<kafka::offset> last_iceberg_committed_offset;

    // If not ok, the request processing has a problem.
    errc errc;

    friend std::ostream&
    operator<<(std::ostream&, const fetch_latest_translated_offset_reply&);

    auto serde_fields() {
        return std::tie(last_added_offset, errc, last_iceberg_committed_offset);
    }
};

// For a given topic/partition fetches the latest translated offset from
// the coordinator.
struct fetch_latest_translated_offset_request
  : serde::envelope<
      fetch_latest_translated_offset_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = fetch_latest_translated_offset_reply;

    fetch_latest_translated_offset_request() = default;

    model::topic_partition tp;
    model::revision_id topic_revision;

    const model::topic& get_topic() const { return tp.topic; }

    friend std::ostream&
    operator<<(std::ostream&, const fetch_latest_translated_offset_request&);

    auto serde_fields() { return std::tie(tp, topic_revision); }
};

struct stm_snapshot
  : public serde::
      envelope<stm_snapshot, serde::version<0>, serde::compat_version<0>> {
    topics_state topics;

    auto serde_fields() { return std::tie(topics); }
};

struct per_topic_usage_stats
  : serde::envelope<
      per_topic_usage_stats,
      serde::version<0>,
      serde::compat_version<0>> {
    per_topic_usage_stats() = default;
    explicit per_topic_usage_stats(
      model::topic topic,
      model::revision_id revision,
      uint64_t kafka_bytes_processed)
      : topic(std::move(topic))
      , revision(revision)
      , total_kafka_bytes_processed(kafka_bytes_processed) {}

    model::topic topic;
    model::revision_id revision;
    uint64_t total_kafka_bytes_processed{0};

    friend std::ostream&
    operator<<(std::ostream&, const per_topic_usage_stats&);

    auto serde_fields() {
        return std::tie(topic, revision, total_kafka_bytes_processed);
    }
};

struct datalake_usage_stats
  : serde::envelope<
      datalake_usage_stats,
      serde::version<0>,
      serde::compat_version<0>> {
    friend std::ostream& operator<<(std::ostream&, const datalake_usage_stats&);

    chunked_vector<per_topic_usage_stats> topic_usages;

    auto serde_fields() { return std::tie(topic_usages); }
};

struct usage_stats_reply
  : serde::
      envelope<usage_stats_reply, serde::version<0>, serde::compat_version<0>> {
    usage_stats_reply() = default;
    explicit usage_stats_reply(errc err)
      : errc(err) {}

    friend std::ostream& operator<<(std::ostream&, const usage_stats_reply&);

    errc errc;
    // only valid if errc == errc::ok
    datalake_usage_stats stats;

    auto serde_fields() { return std::tie(errc, stats); }
};

// Request to fetch usage stats for all topics coordinated by a given
// coordinator topic partition.
struct usage_stats_request
  : serde::envelope<
      usage_stats_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = usage_stats_reply;

    model::partition_id coordinator_partition;

    usage_stats_request() = default;
    explicit usage_stats_request(model::partition_id coordinator_partition)
      : coordinator_partition(coordinator_partition) {}
    friend std::ostream& operator<<(std::ostream&, const usage_stats_request&);

    model::partition_id get_coordinator_partition() {
        return coordinator_partition;
    }

    auto serde_fields() { return std::tie(coordinator_partition); }
};

struct get_topic_state_reply
  : serde::envelope<
      get_topic_state_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    get_topic_state_reply() = default;
    explicit get_topic_state_reply(errc err)
      : errc(err) {}

    friend std::ostream&
    operator<<(std::ostream&, const get_topic_state_reply&);

    errc errc;
    // Map from topic to its state. Only valid if errc == errc::ok
    chunked_hash_map<model::topic, topic_state> topic_states;

    auto serde_fields() { return std::tie(errc, topic_states); }
};

struct get_topic_state_request
  : serde::envelope<
      get_topic_state_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = get_topic_state_reply;

    get_topic_state_request() = default;
    explicit get_topic_state_request(
      model::partition_id coordinator_partition,
      chunked_vector<model::topic> topics_filter)
      : coordinator_partition(coordinator_partition)
      , topics_filter(std::move(topics_filter)) {}

    model::partition_id coordinator_partition;

    // Topics to return. If empty, returns all topics.
    chunked_vector<model::topic> topics_filter;

    model::partition_id get_coordinator_partition() const {
        return coordinator_partition;
    }

    friend std::ostream&
    operator<<(std::ostream&, const get_topic_state_request&);

    auto serde_fields() {
        return std::tie(coordinator_partition, topics_filter);
    }
};

struct reset_topic_state_reply
  : serde::envelope<
      reset_topic_state_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    reset_topic_state_reply() = default;
    explicit reset_topic_state_reply(errc err)
      : errc(err) {}
    friend std::ostream&
    operator<<(std::ostream&, const reset_topic_state_reply&);
    errc errc;
    auto serde_fields() { return std::tie(errc); }
};

struct reset_topic_state_request
  : serde::envelope<
      reset_topic_state_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using resp_t = reset_topic_state_reply;

    model::partition_id coordinator_partition;
    model::topic topic;
    model::revision_id topic_revision;
    bool reset_all_partitions{false};
    chunked_hash_map<model::partition_id, partition_state_override>
      partition_overrides;

    reset_topic_state_request() = default;

    explicit reset_topic_state_request(
      model::partition_id coordinator_partition,
      model::topic topic,
      model::revision_id topic_revision,
      bool reset_all_partitions = false,
      chunked_hash_map<model::partition_id, partition_state_override>
        partition_overrides = {})
      : coordinator_partition(coordinator_partition)
      , topic(std::move(topic))
      , topic_revision(topic_revision)
      , reset_all_partitions(reset_all_partitions)
      , partition_overrides(std::move(partition_overrides)) {}

    model::partition_id get_coordinator_partition() const {
        return coordinator_partition;
    }

    friend std::ostream&
    operator<<(std::ostream& o, const reset_topic_state_request& req) {
        fmt::print(
          o,
          "{{coordinator_partition: {}, topic: {}, topic_revision: {}, "
          "reset_all_partitions: {}, partition_overrides: {} entries}}",
          req.coordinator_partition,
          req.topic,
          req.topic_revision,
          req.reset_all_partitions,
          req.partition_overrides.size());
        return o;
    }

    auto serde_fields() {
        return std::tie(
          coordinator_partition,
          topic,
          topic_revision,
          reset_all_partitions,
          partition_overrides);
    }
};

} // namespace datalake::coordinator
