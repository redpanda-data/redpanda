/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "cloud_topics/level_one/metastore/lsm/state_reader.h"
#include "cloud_topics/level_one/metastore/lsm/write_batch_row.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <expected>

namespace cloud_topics::l1 {

using db_update_error = named_type<ss::sstring, struct db_update_error_tag>;

struct add_objects_db_update {
    ss::future<std::expected<void, db_update_error>> build_rows(
      state_reader&,
      chunked_vector<write_batch_row>&,
      chunked_hash_map<model::topic_id_partition, kafka::offset>* = nullptr)
      const;

    // - There are new objects
    // - There are new terms for each tidp
    // - The terms are all increasing
    // - Input extents are in order and contiguous
    // - New terms match the range fo the extents
    std::expected<void, db_update_error> validate_inputs() const;

    // For tests.
    static add_objects_db_update from(add_objects_update);

    chunked_vector<new_object> new_objects;
    term_state_update_t new_terms;
};

struct replace_objects_db_update {
    ss::future<std::expected<void, db_update_error>>
    build_rows(state_reader&, chunked_vector<write_batch_row>&) const;

    // - There are new objects
    // - Input extents are in order and form contiguous intervals
    // - Compaction updates align with new extents
    std::expected<void, db_update_error> validate_inputs() const;

    // For tests.
    static replace_objects_db_update from(replace_objects_update);

    chunked_vector<new_object> new_objects;
    chunked_hash_map<
      model::topic_id,
      chunked_hash_map<model::partition_id, compaction_state_update>>
      compaction_updates;
};

} // namespace cloud_topics::l1
