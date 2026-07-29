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

#include "base/format_to.h"
#include "container/chunked_hash_map.h"
#include "datalake/coordinator/translated_offset_range.h"
#include "iceberg/manifest_entry.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

#include <seastar/util/bool_class.hh>

#include <deque>

namespace datalake::coordinator {

using drift_detected = ss::bool_class<struct drift_detected_tag>;

// Represents the state to be managed by the datalake coordinator's replicated
// state machine.

struct pending_entry
  : public serde::
      envelope<pending_entry, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(data, added_pending_at); }

    translated_offset_range data;

    // Offset of the control topic partition at which this data entry was added
    // to the state machine as a pending entry.
    model::offset added_pending_at;

    pending_entry copy() const;
};

// State tracked per Kafka partition. Groups of files get added added to this
// state, each group corresponding to an offset range. The ranges added to this
// state must have no overlaps and no gaps in order to ensure exactly once
// delivery of files to the Iceberg table.
//
// Files are added to this state as "pending entries". Once the files are
// committed to Iceberg, we cease tracking of the files and instead keep track
// of the highest Kafka offset of the committed files.
//
// By tracking pending files only, we rely on the Iceberg catalog to be the
// source of truth of existing metadata. This allows Redpanda to maintain a
// much smaller memory footprint per table, and to tolerate concurrent updates
// to the table more easily (e.g. consider reconciling an external writer to
// the table if we tracked all files in the table instead of just pending
// files).
struct partition_state
  : public serde::
      envelope<partition_state, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(pending_entries, last_committed); }

    // Files that have yet to be added to the Iceberg catalog. Ordered in
    // increasing offset order.
    //
    // It is expected that files are only added to this list if they form a
    // contiguous offset range.
    std::deque<pending_entry> pending_entries;

    // The last (inclusive) Kafka offset confirmed to be sent to the Iceberg
    // catalog for a given partition.
    //
    // When set, is expected that this corresponds to the end of a pending
    // entry, and upon setting, that all entries up to and including that entry
    // are removed from pending entries.
    //
    // Is nullopt iff we have never committed any files to the table.
    std::optional<kafka::offset> last_committed;

    partition_state copy() const;
};

// Tracks the state managed for each Kafka partition. Since data workers are
// run per partition, this separation allows us to bookkeep progress of each
// worker.
struct topic_state
  : public serde::
      envelope<topic_state, serde::version<2>, serde::compat_version<0>> {
    auto serde_fields() {
        return std::tie(
          revision,
          pid_to_pending_files,
          lifecycle_state,
          total_kafka_bytes_processed,
          last_committed_snapshot_id);
    }

    enum class lifecycle_state_t {
        // ready to accept new files
        live,
        // topic deleted, new files can't be accepted (but already accepted
        // files will be committed)
        closed,
        // all state related to this revision of the topic has been purged,
        // files for new revisions of this topic can be accepted.
        // TODO: GC purged topic states
        purged,
    };
    friend fmt::iterator
    format_to(topic_state::lifecycle_state_t s, fmt::iterator out) {
        switch (s) {
        case topic_state::lifecycle_state_t::live:
            return fmt::format_to(out, "live");
        case topic_state::lifecycle_state_t::closed:
            return fmt::format_to(out, "closed");
        case topic_state::lifecycle_state_t::purged:
            return fmt::format_to(out, "purged");
        }
        __builtin_unreachable();
    }

    bool has_pending_entries() const;
    bool has_pending_main_entries() const;
    bool has_pending_dlq_entries() const;
    void add_kafka_bytes_processed(uint64_t bytes) {
        total_kafka_bytes_processed += bytes;
    }

    // Topic revision
    model::revision_id revision;
    // Map from Redpanda partition id to the files pending per partition.
    chunked_hash_map<model::partition_id, partition_state> pid_to_pending_files;
    lifecycle_state_t lifecycle_state = lifecycle_state_t::live;

    // Total number of kafka bytes processed so far that have been successfully
    // committed to iceberg. Includes bytes from DLQ table as well.
    uint64_t total_kafka_bytes_processed{0};

    // Snapshot id of the last committed main table iceberg snapshot.
    std::optional<iceberg::snapshot_id> last_committed_snapshot_id;
    topic_state copy() const;

    // Like copy() but limits the pending entries to only include up to the
    // given number of files. The entries in the returned state are consistent
    // with respect to a coordinator offset upper bound: if an entry at offset
    // O is included, every entry (in any partition) at offset <= O is as well.
    // This is required for Iceberg commit dedup, which uses the coordinator
    // offset as a cursor.
    //
    // The copy also stops once the copied entries' estimated in-memory bytes
    // exceed `max_bytes`, whichever cap is hit first.
    //
    // `was_bounded` is set to true if the limit left some pending entries out
    // of the copy, so the caller knows a subsequent copy is needed to drain the
    // remainder.
    topic_state
    copy_bounded(size_t max_files, size_t max_bytes, bool& was_bounded) const;

    // TODO: add table-wide metadata like Kafka schema id, Iceberg table uuid,
    // etc.
};

// Tracks the state of each topic.
struct topics_state
  : public serde::
      envelope<topics_state, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(topic_to_state); }

    // Map from the Redpanda topic to the state managed per topic, e.g. pending
    // files per partition.
    chunked_hash_map<model::topic, topic_state> topic_to_state;

    topics_state copy() const;

    // Returns the state for the given partition.
    std::optional<std::reference_wrapper<const partition_state>>
    partition_state(const model::topic_partition&) const;

    size_t pending_files() const { return pending_files_; }
    size_t pending_bytes() const { return pending_bytes_; }

    void note_added(const translated_offset_range&);
    // Reports drift if the totals were already too low to cover the removal,
    // in which case the caller must recompute them once the entries are gone.
    [[nodiscard]] drift_detected note_removed(const translated_offset_range&);

    std::pair<size_t, size_t> compute_pending() const;

    // Rebuild the totals from topic_to_state, which a snapshot install restores
    // without them.
    void recompute_pending();

    // Crashes debug builds if an update changed pending entries without
    // note_added() or note_removed().
    void dassert_pending_totals() const;

    // Running totals across all topics. Derived from topic_to_state, so left
    // out of serde_fields(): a cache, not replicated state.
    size_t pending_files_ = 0;
    size_t pending_bytes_ = 0;
};

} // namespace datalake::coordinator
