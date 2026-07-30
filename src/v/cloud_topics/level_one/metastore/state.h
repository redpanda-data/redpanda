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

#include "absl/container/btree_set.h"
#include "base/seastarx.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/envelope.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/optional.h"
#include "serde/rw/set.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <expected>

namespace cloud_topics::l1 {

// Represents the state managed by the replicated state machine that serves L1
// metadata.

// The metastore stores an imported tiered-storage segment's descriptor
// (l1::imported_ts_info) decomposed by row ownership: the path is an object
// property, the delta/term are extent properties. The interface and IO path
// recompose the two into an imported_ts_info; only the storage layer (and its
// debug-serde encoding) sees this split.

/// Where a referenced tiered-storage segment lives. An object property: the
/// imported analog of a native object's oid -> bucket path, independent of
/// which Kafka offsets any extent over it covers.
struct imported_ts_object_location
  : public serde::envelope<
      imported_ts_object_location,
      serde::version<0>,
      serde::compat_version<0>> {
    friend bool operator==(
      const imported_ts_object_location&,
      const imported_ts_object_location&) = default;
    auto serde_fields() { return std::tie(ts_path); }

    /// Opaque tiered-storage segment object path (sname_format path).
    ts_segment_path ts_path;
};

/// How to interpret the data within an extent imported from a tiered-storage
/// segment. An extent property, alongside the extent's Kafka offset bounds
/// (base_offset/last_offset).
struct imported_ts_segment_info
  : public serde::envelope<
      imported_ts_segment_info,
      serde::version<0>,
      serde::compat_version<0>> {
    friend bool
    operator==(const imported_ts_segment_info&, const imported_ts_segment_info&)
      = default;
    std::strong_ordering
    operator<=>(const imported_ts_segment_info&) const = default;
    auto serde_fields() { return std::tie(segment_term, delta_base, tx_state); }

    /// Raft term the segment was written in. Stamped onto every imported batch
    /// as its partition leader epoch (a tiered-storage segment is single-term),
    /// so a Kafka fetch of the imported region reports the original leader
    /// epoch rather than -1.
    model::term_id segment_term{};
    /// Offset-translation delta at the segment's base (source segment_meta's
    /// delta_offset). Seeds the reader's running delta at the segment start so
    /// translation survives a compacted front hole. See l1::imported_ts_info.
    model::offset_delta delta_base{};
    /// Whether the segment's aborted-transaction (.tx) manifest exists,
    /// resolved at import time so the read path can skip the probe. See
    /// l1::tx_manifest_state.
    tx_manifest_state tx_state{tx_manifest_state::unknown};
};

// Translate between the storage split (object-owned location + extent-owned
// segment descriptor) and the unified imported_ts_info the interface and IO
// path use.
inline imported_ts_object_location
to_object_location(const imported_ts_info& i) {
    return imported_ts_object_location{.ts_path = i.ts_path};
}
inline imported_ts_segment_info to_segment_info(const imported_ts_info& i) {
    return imported_ts_segment_info{
      .segment_term = i.segment_term,
      .delta_base = i.delta_base,
      .tx_state = i.tx_state};
}
inline imported_ts_info to_imported_ts_info(
  const imported_ts_object_location& loc, const imported_ts_segment_info& seg) {
    return imported_ts_info{
      .ts_path = loc.ts_path,
      .segment_term = seg.segment_term,
      .delta_base = seg.delta_base,
      .tx_state = seg.tx_state};
}
// Recompose a read response's imported_ts_info from the object row's location
// and the extent row's segment descriptor (set together for an imported extent,
// both nullopt for native L1). The extent's Kafka offset bounds live on the
// extent itself (base_offset/last_offset); the read path does not need them
// here.
inline std::optional<imported_ts_info> to_imported_ts_info(
  const std::optional<imported_ts_object_location>& loc,
  const std::optional<imported_ts_segment_info>& seg) {
    if (loc.has_value() && seg.has_value()) {
        return to_imported_ts_info(*loc, *seg);
    }
    return std::nullopt;
}

// A description of a portion of an object that points at data for a contiguous
// range of batches belonging to a single partition. The object itself may
// contain ranges of data from many partitions.
struct extent
  : public serde::
      envelope<extent, serde::version<1>, serde::compat_version<0>> {
    friend bool operator==(const extent&, const extent&) = default;
    std::strong_ordering operator<=>(const extent&) const = default;
    auto serde_fields() {
        return std::tie(
          base_offset,
          last_offset,
          max_timestamp,
          filepos,
          len,
          oid,
          imported_ts_info);
    }

    kafka::offset base_offset;
    kafka::offset last_offset;
    model::timestamp max_timestamp;
    size_t filepos;
    size_t len;
    // TODO: avoid duplicating the UUIDs with some indirection.
    object_id oid;
    // Properties of the data in this extent when it is imported from a
    // tiered-storage segment (nullopt for native L1). The segment's Kafka
    // bounds are base_offset/last_offset above; ts_path lives on the object.
    std::optional<imported_ts_segment_info> imported_ts_info;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(
          it,
          "{{offsets:({}~{}), max_timestamp:{}, filepos:{}, len:{}, oid:{}",
          base_offset,
          last_offset,
          max_timestamp,
          filepos,
          len,
          oid);
    }
};

// Offset that corresponds to the start of a given term.
struct term_start
  : public serde::
      envelope<term_start, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const term_start&, const term_start&) = default;
    auto serde_fields() { return std::tie(term_id, start_offset); }
    auto operator<=>(const term_start&) const = default;

    model::term_id term_id;
    kafka::offset start_offset;
};

struct compaction_state
  : public serde::
      envelope<compaction_state, serde::version<0>, serde::compat_version<0>> {
    struct cleaned_range_with_tombstones
      : public serde::envelope<
          cleaned_range_with_tombstones,
          serde::version<0>,
          serde::compat_version<0>> {
        friend bool operator==(
          const cleaned_range_with_tombstones&,
          const cleaned_range_with_tombstones&) = default;
        auto operator<=>(const cleaned_range_with_tombstones&) const = default;
        auto serde_fields() {
            return std::tie(
              base_offset, last_offset, cleaned_with_tombstones_at);
        }

        kafka::offset base_offset;
        kafka::offset last_offset;

        // Timestamp at which this clean range was generated.
        // This is important to track to be able to schedule tombstone removal
        // some time (delete.retention.ms) after cleaning.
        model::timestamp cleaned_with_tombstones_at;
    };
    using tombstone_range_set_t
      = absl::btree_set<cleaned_range_with_tombstones>;

    friend bool
    operator==(const compaction_state&, const compaction_state&) = default;
    auto serde_fields() {
        return std::tie(cleaned_ranges, cleaned_ranges_with_tombstones);
    }
    compaction_state copy() const;

    // Returns false if the input range overlaps with another existing range
    // with tombstones.
    bool may_add(const cleaned_range_with_tombstones&) const;

    // Adds the input range to the set of cleaned ranges with tombstones.
    bool add(const cleaned_range_with_tombstones&);

    // Returns true if the input inclusive range is fully covered by a set of
    // cleaned ranges with tombstones.
    bool
      has_contiguous_range_with_tombstones(kafka::offset, kafka::offset) const;

    // Removes the input inclusive range from the set of cleaned ranges with
    // tombstones. The input range doesn't need to align exactly with any of
    // `cleaned_ranges_with_tombstones`, but it must be fully covered.
    //
    // For example, let's say our cleaned ranges with tombstones were:
    // ┌───────────┬───────────┐
    // │           │           │
    // │10..99     │100..199   │
    // │ts=900     │ts=1000    │
    // └───────────┴───────────┘
    //
    // Even though it doesn't align with the bounds of any range, we could
    // erase [80, 129] because that entire range is covered.
    // ┌─────────┬────┬────────┐
    // │         │    │        │
    // │10..79   │    │130..199│
    // │ts=900   │    │ts=1000 │
    // └─────────┴────┴────────┘
    //
    // We are not able to erase [0, 79], because [0, 9] are not covered.
    bool erase_contiguous_range_with_tombstones(kafka::offset, kafka::offset);

    // Prefix truncates the cleaned_ranges and cleaned_ranges_with_tombstones
    // such that all ranges below the new start are removed and any range that
    // overlaps with the new start is truncated to start at the given offset.
    void truncate_with_new_start_offset(kafka::offset);

private:
    struct tombstone_range_iters {
        tombstone_range_set_t::const_iterator begin;
        tombstone_range_set_t::const_iterator last;
    };
    // Returns iterators that span the contiguous, minimal set that fully cover
    // the given inclusive offset range. If no such set of contiguous ranges
    // exist, returns std::nullopt.
    std::optional<tombstone_range_iters>
      get_contiguous_range_with_tombstones(kafka::offset, kafka::offset) const;

public:
    // Ranges of the log whose keys have been deduplicated from the _beginning
    // of the log_ (NOT from the cleaned range's start offset!) to and
    // including the interval's last offset.
    //
    // While extents that overlap with a cleaned range may be replaced when
    // cleaning a dirty range, there is no point in recompacting an offset
    // range that is cleaned (because all the records are already deduplicated)
    // unless it contains tombstones that are eligible for compaction.
    offset_interval_set cleaned_ranges;

    // Cleaned offset ranges that contain tombstones, tracked separately to
    // avoid complicating reasoning about cleaned ranges. Ordered, and
    // maintained to be non-overlapping.
    //
    // These must overlap with `cleaned_ranges`.
    //
    // For a tombstone record to be elegible for removal, all offsets at and
    // below it must have been cleaned for at least delete.retention.ms.
    tombstone_range_set_t cleaned_ranges_with_tombstones;
};

// State tracked per Kafka partition. The extents added to this state must have
// no overlaps and no gaps in order to ensure there is no data loss.
struct partition_state
  : public serde::
      envelope<partition_state, serde::version<1>, serde::compat_version<0>> {
    friend bool
    operator==(const partition_state&, const partition_state&) = default;
    auto serde_fields() {
        return std::tie(
          extents,
          start_offset,
          next_offset,
          compaction_state,
          compaction_epoch,
          term_starts,
          migrating);
    }

    partition_state copy() const;

    // The list of extents that comprise this partition. The ordering here
    // allows us to perform efficient lookups by offset.
    // Using a set here allows us to:
    // - perform efficient lookups by offset
    // - remove elements from the beginning e.g. for prefix truncation
    // - replace ranges in the middle with new extents e.g. for compaction
    using extent_set_t = absl::btree_set<extent>;
    extent_set_t extents;

    // Brute force calculation of the byte size of all the extents for this
    // partition. This is used for the simple metastore which isn't used in
    // production, so efficiency isn't critical.
    size_t calculate_size() const {
        size_t total = 0;
        for (const auto& ext : extents) {
            total += ext.len;
        }
        return total;
    }

    // The start offset of the partition. This may not align with the front of
    // `extents` if the offset was set through the Kafka API.
    kafka::offset start_offset{0};

    // The next offset expected to be added to this partition. In general this
    // should align with the back of `extents`, but is required when `extents`
    // is empty.
    kafka::offset next_offset{0};

    // Describes the current state of compacted regions of this partition's
    // log, if it belongs to a compacted topic.
    //
    // Empty iff compaction has not been run for this partition.
    // TODO: should we remove this if cleanup policy is switched?
    std::optional<compaction_state> compaction_state;

    using compaction_epoch_t
      = named_type<int64_t, struct state_compaction_epoch>;
    // The current epoch of this partition's compacted log. Effectively
    // describes the number of logical changes made to the log's underlying
    // data. Incremented when a compaction or partial compaction has been
    // applied to this partition.
    // This is a useful field for optimistic concurrency control, preventing a
    // compaction job based on stale data from overwriting an updated log.
    compaction_epoch_t compaction_epoch{0};

    // A mapping of terms (used instead of Kafka epochs) to the starting Kafka
    // offset for that term. Both the term and offsets are maintained to be
    // monotonically strictly increasing.
    //
    // TODO: when we implement set_start_offset(), we should retain enough
    // information to return a value for the start_offset, even when the log
    // has been prefix truncated to be empty. I.e. this list should never be
    // empty once there has been data in the log.
    absl::btree_set<term_start> term_starts;

    // True while the partition is mid tiered->cloud migration. For
    // offline/remote consumers (cluster recovery, read-replica) that cannot
    // observe the live archival-STM routing signal. Defaults to false, so a
    // snapshot predating this field restores as a native cloud topic.
    bool migrating{};
};

// Tracks the state managed for each partition of a Kafka topic.
struct topic_state
  : public serde::
      envelope<topic_state, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const topic_state&, const topic_state&) = default;
    auto serde_fields() { return std::tie(pid_to_state); }
    chunked_hash_map<model::partition_id, partition_state> pid_to_state;

    topic_state copy() const;
};

// Metadata about a given object that is not specific to any partition.
struct object_entry
  : public serde::
      envelope<object_entry, serde::version<2>, serde::compat_version<0>> {
    friend bool operator==(const object_entry&, const object_entry&) = default;
    auto serde_fields() {
        return std::tie(
          total_data_size,
          removed_data_size,
          footer_pos,
          object_size,
          last_updated,
          is_preregistration,
          imported_ts_location);
    }
    size_t total_data_size{0};
    size_t removed_data_size{0};
    size_t footer_pos{0};
    size_t object_size{0};
    model::timestamp last_updated;
    bool is_preregistration{false};
    // Location of the backing tiered-storage segment when this object is
    // imported by reference (nullopt for native L1). Used by GC to delete the
    // segment + .tx + .index; the per-data fields live on the extent.
    std::optional<imported_ts_object_location> imported_ts_location;
};

// Tracks the state of each topic revision.
struct state
  : public serde::envelope<state, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const state&, const state&) = default;
    auto serde_fields() { return std::tie(topic_to_state, objects); }
    chunked_hash_map<model::topic_id, topic_state> topic_to_state;
    chunked_hash_map<object_id, object_entry> objects;

    state copy() const;

    std::optional<std::reference_wrapper<const partition_state>>
    partition_state(const model::topic_id_partition&) const;
};

struct stm_snapshot
  : public serde::
      envelope<stm_snapshot, serde::version<0>, serde::compat_version<0>> {
    state state;

    auto serde_fields() { return std::tie(state); }
};

} // namespace cloud_topics::l1
