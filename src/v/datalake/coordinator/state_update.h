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
      chunked_vector<translated_offset_range>);
    auto serde_fields() { return std::tie(tp, entries); }

    checked<std::nullopt_t, stm_update_error> can_apply(const topics_state&);
    checked<std::nullopt_t, stm_update_error> apply(topics_state&);

    model::topic_partition tp;

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
    static checked<mark_files_committed_update, stm_update_error>
    build(const topics_state&, const model::topic_partition&, kafka::offset);
    auto serde_fields() { return std::tie(tp, new_committed); }

    checked<std::nullopt_t, stm_update_error> can_apply(const topics_state&);
    checked<std::nullopt_t, stm_update_error> apply(topics_state&);

    model::topic_partition tp;

    // All pending entries whose offset range falls entirely below this offset
    // (inclusive) should be removed.
    kafka::offset new_committed;
};

} // namespace datalake::coordinator
