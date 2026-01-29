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

// TODO: might be nice to make this be some kind of variant that contains
// state_reader::errc instead of duplicating error codes.
enum class db_update_errc {
    io_error,
    corruption,
    shutting_down,

    // The input is invalid, indicating a bug in the caller. Worth logging at
    // ERROR level, as these are unexpected.
    invalid_input,

    // The request cannot be applied to the current state because it would
    // break an invariant. These errors may be caused by races and therefore
    // may not necessarily be unexpected.
    invalid_update,
};
using db_update_error = detailed_error<db_update_errc>;

struct add_objects_db_update {
    ss::future<std::expected<void, db_update_error>> build_rows(
      state_reader&,
      chunked_vector<write_batch_row>&,
      chunked_hash_map<model::topic_id_partition, kafka::offset>* = nullptr)
      const;

    // Validates the given update is well-formed:
    // - There are new objects
    // - There are new terms for each tidp
    // - The terms are all increasing
    // - Input extents are in order and contiguous
    // - New terms match the range fo the extents
    std::expected<void, db_update_error> validate_inputs() const;

    chunked_vector<new_object> new_objects;
    term_state_update_t new_terms;
};

struct replace_objects_db_update {
    ss::future<std::expected<void, db_update_error>>
    build_rows(state_reader&, chunked_vector<write_batch_row>&) const;

    // Validates the given update is well-formed:
    // - There are new objects
    // - Input extents are in order and form contiguous intervals
    // - Compaction updates align with new extents
    std::expected<void, db_update_error> validate_inputs() const;

    chunked_vector<new_object> new_objects;
    chunked_hash_map<
      model::topic_id,
      chunked_hash_map<model::partition_id, compaction_state_update>>
      compaction_updates;
};

struct set_start_offset_db_update {
    ss::future<std::expected<void, db_update_error>> build_rows(
      state_reader&,
      chunked_vector<write_batch_row>&,
      bool* is_no_op = nullptr) const;

    model::topic_id_partition tp;
    kafka::offset new_start_offset;
};

struct remove_topics_db_update {
    ss::future<std::expected<void, db_update_error>>
    build_rows(state_reader&, chunked_vector<write_batch_row>&) const;

    chunked_vector<model::topic_id> topics;
};

struct remove_objects_db_update {
    // Removes objects that are no longer referenced by any extents.
    // An object is considered unreferenced when removed_data_size >=
    // total_data_size.
    ss::future<std::expected<void, db_update_error>>
    build_rows(state_reader&, chunked_vector<write_batch_row>&) const;

    chunked_vector<object_id> objects;
};

} // namespace cloud_topics::l1

template<>
struct fmt::formatter<cloud_topics::l1::db_update_errc> final
  : fmt::formatter<std::string_view> {
    template<typename FormatContext>
    auto format(
      const cloud_topics::l1::db_update_errc& e, FormatContext& ctx) const {
        using enum cloud_topics::l1::db_update_errc;
        std::string_view name;
        switch (e) {
        case io_error:
            name = "db_update_errc::io_error";
            break;
        case corruption:
            name = "db_update_errc::corruption";
            break;
        case shutting_down:
            name = "db_update_errc::shutting_down";
            break;
        case invalid_input:
            name = "db_update_errc::invalid_input";
            break;
        case invalid_update:
            name = "db_update_errc::invalid_update";
            break;
        }
        return fmt::format_to(ctx.out(), "{}", name);
    }
};
