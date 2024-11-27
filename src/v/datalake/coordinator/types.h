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

#include "container/fragmented_vector.h"
#include "datalake/coordinator/state.h"
#include "datalake/coordinator/translated_offset_range.h"
#include "datalake/errors.h"
#include "datalake/schema_identifier.h"
#include "model/fundamental.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"

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
    using rpc_adl_exempt = std::true_type;

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
    using rpc_adl_exempt = std::true_type;
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

struct add_translated_data_files_reply
  : serde::envelope<
      add_translated_data_files_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

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
    using rpc_adl_exempt = std::true_type;
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
    using rpc_adl_exempt = std::true_type;

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

    auto serde_fields() { return std::tie(last_added_offset, errc); }
};

// For a given topic/partition fetches the latest translated offset from
// the coordinator.
struct fetch_latest_translated_offset_request
  : serde::envelope<
      fetch_latest_translated_offset_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
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

} // namespace datalake::coordinator
