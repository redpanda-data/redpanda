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

#include "base/outcome.h"
#include "container/fragmented_vector.h"
#include "datalake/coordinator/state.h"
#include "datalake/coordinator/translated_offset_range.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "utils/named_type.h"

namespace datalake::coordinator {

// Represents the deterministic updates to the coordinator STM state.

enum class update_key : uint8_t {
    add_files = 0,
    mark_files_committed = 1,
    topic_lifecycle_update = 2,
};
std::ostream& operator<<(std::ostream&, const update_key&);

using stm_update_error = named_type<ss::sstring, struct update_error_tag>;

// An update to add files for a given Kafka partition.
struct add_files_update
  : public serde::
      envelope<add_files_update, serde::version<0>, serde::compat_version<0>> {
    static constexpr auto key{update_key::add_files};
    static checked<add_files_update, stm_update_error> build(
      const topics_state&,
      const model::topic_partition&,
      model::revision_id topic_revision,
      chunked_vector<translated_offset_range>);
    auto serde_fields() { return std::tie(tp, topic_revision, entries); }

    checked<std::nullopt_t, stm_update_error> can_apply(const topics_state&);
    checked<std::nullopt_t, stm_update_error>
    apply(topics_state&, model::offset);
    friend std::ostream& operator<<(std::ostream&, const add_files_update&);

    model::topic_partition tp;
    model::revision_id topic_revision;
    // Expected to be ordered from lowest offset to highest offset.
    chunked_vector<translated_offset_range> entries;
};

// An update to untrack pending files, e.g. after committing them to Iceberg.
struct mark_files_committed_update
  : public serde::envelope<
      mark_files_committed_update,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr auto key{update_key::mark_files_committed};
    static checked<mark_files_committed_update, stm_update_error> build(
      const topics_state&,
      const model::topic_partition&,
      model::revision_id topic_revision,
      kafka::offset);
    auto serde_fields() { return std::tie(tp, topic_revision, new_committed); }

    checked<std::nullopt_t, stm_update_error> can_apply(const topics_state&);
    checked<std::nullopt_t, stm_update_error> apply(topics_state&);
    friend std::ostream&
    operator<<(std::ostream&, const mark_files_committed_update&);

    model::topic_partition tp;
    model::revision_id topic_revision;

    // All pending entries whose offset range falls entirely below this offset
    // (inclusive) should be removed.
    kafka::offset new_committed;
};

// An update to change topic lifecycle state after it has been deleted.
struct topic_lifecycle_update
  : public serde::envelope<
      topic_lifecycle_update,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr auto key{update_key::topic_lifecycle_update};
    auto serde_fields() { return std::tie(topic, revision, new_state); }

    // returns true if the update actually changes anything
    checked<bool, stm_update_error> can_apply(const topics_state&);
    checked<bool, stm_update_error> apply(topics_state&);

    friend std::ostream&
    operator<<(std::ostream&, const topic_lifecycle_update&);

    model::topic topic;
    model::revision_id revision;
    topic_state::lifecycle_state_t new_state;
};

} // namespace datalake::coordinator
