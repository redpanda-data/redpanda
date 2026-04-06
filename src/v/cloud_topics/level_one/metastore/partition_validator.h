/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/level_one/metastore/lsm/state_reader.h"
#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "utils/detailed_error.h"

#include <seastar/core/future.hh>

#include <cstdint>
#include <expected>
#include <optional>

namespace cloud_io {
class remote;
} // namespace cloud_io

namespace cloud_topics::l1 {

/// Values match
/// the proto AnomalyType enum for direct casting.
enum class anomaly_type : int {
    extent_overlap = 1,
    extent_gap = 2,
    next_offset_mismatch = 3,
    start_offset_mismatch = 4,
    object_not_found = 5,
    object_preregistered = 6,
    object_not_in_storage = 7,
    compaction_range_below_start = 8,
    compaction_range_above_next = 9,
    compaction_tombstone_overlap = 10,
    compaction_tombstone_outside_cleaned = 11,
    term_ordering = 12,
    compaction_state_unexpected = 13,
};

std::string_view to_string_view(anomaly_type t);

inline auto format_as(anomaly_type t) { return to_string_view(t); }

struct metastore_anomaly {
    anomaly_type type;
    ss::sstring description;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "[{}]: {}", type, description);
    }
};

struct validate_partition_options {
    model::topic_id_partition tidp;

    /// If true, verify each extent's object_id exists in the metastore
    /// object table and is not a preregistration.
    bool check_object_metadata{false};

    /// If true, check that each extent's L1 object exists in cloud
    /// storage via HEAD request.
    bool check_object_storage{false};

    cloud_io::remote* remote{nullptr};
    const cloud_storage_clients::bucket_name* bucket{nullptr};
    ss::abort_source* as{nullptr};

    /// Resume validation at this base_offset (inclusive). The extent
    /// at this offset is re-read to verify cross-page contiguity but
    /// is not counted toward max_extents.
    std::optional<kafka::offset> resume_at_offset;

    /// Max extents to validate per validation call.
    uint32_t max_extents{0};
};

struct partition_validation_result {
    chunked_vector<metastore_anomaly> anomalies;

    /// The base_offset of the last extent validated. If nullopt, all
    /// extents have been validated (no more pages).
    std::optional<kafka::offset> resume_at_offset;

    /// Number of extents validated in this call.
    uint32_t extents_validated{0};

    template<typename... Args>
    void
    record(anomaly_type t, fmt::format_string<Args...> msg, Args&&... args) {
        anomalies.emplace_back(
          metastore_anomaly{
            .type = t,
            .description = fmt::format(
              std::move(msg), std::forward<Args>(args)...),
          });
    }
};

/// Validates metastore invariants for a single partition using a state_reader
/// snapshot. Each call validates a single page (up to max_extents) of extents.
/// Non-extent invariants are only checked when validating the first page,
/// after which only extent contiguity is checked.
///
/// Invariants checked:
/// - Extent contiguity: no gaps or overlaps between consecutive extents
/// - Offset bounds: start_offset and next_offset align with extent range
/// - Term starts: align with offset bounds and are monotonically increasing
/// - Compaction state: cleaned/tombstone ranges within bounds
/// - Object metadata: extent object_ids exist and are not preregistrations
/// - Object storage: L1 objects exist in cloud storage
class partition_validator {
public:
    enum class errc {
        io_error,
        shutting_down,
    };
    using error = detailed_error<errc>;

    explicit partition_validator(state_reader& reader);

    ss::future<std::expected<partition_validation_result, error>>
    validate(validate_partition_options opts);

private:
    /// Validate extent contiguity and offset bounds. Returns the set
    /// of unique object_ids referenced by validated extents.
    ss::future<std::expected<chunked_hash_set<object_id>, error>>
    validate_extents(
      const validate_partition_options& opts,
      const metadata_row_value& metadata,
      partition_validation_result& result);

    ss::future<std::expected<std::monostate, error>> validate_terms(
      const validate_partition_options& opts,
      const metadata_row_value& metadata,
      partition_validation_result& result);

    ss::future<std::expected<std::monostate, error>> validate_compaction(
      const validate_partition_options& opts,
      const metadata_row_value& metadata,
      partition_validation_result& result);

    ss::future<std::expected<std::monostate, error>> validate_objects(
      const validate_partition_options& opts,
      chunked_hash_set<object_id> seen_objects,
      partition_validation_result& result);

    state_reader& reader_;
};

std::string_view to_string_view(partition_validator::errc e);

inline auto format_as(partition_validator::errc e) { return to_string_view(e); }

} // namespace cloud_topics::l1
