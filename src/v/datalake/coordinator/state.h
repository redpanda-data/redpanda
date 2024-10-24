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
#include "datalake/coordinator/translated_offset_range.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

#include <deque>

namespace datalake::coordinator {

// Represents the state to be managed by the datalake coordinator's replicated
// state machine.

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
    std::deque<translated_offset_range> pending_entries;

    // The last (inclusive) Kafka offset confirmed to be sent to the Iceberg
    // catalog for a given partition.
    //
    // When set, is expected that this corresponds to the end of a pending
    // entry, and upon setting, that all entries up to and including that entry
    // are removed from pending entries.
    //
    // Is nullopt iff we have never committed any files to the table.
    std::optional<kafka::offset> last_committed;
};

// Tracks the state managed for each Kafka partition. Since data workers are
// run per partition, this separation allows us to bookkeep progress of each
// worker.
struct topic_state
  : public serde::
      envelope<topic_state, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(pid_to_pending_files); }

    // Map from Redpanda partition id to the files pending per partition.
    chunked_hash_map<model::partition_id, partition_state> pid_to_pending_files;

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

    // Returns the state for the given partition.
    std::optional<std::reference_wrapper<const partition_state>>
    partition_state(const model::topic_partition&) const;
};

} // namespace datalake::coordinator
