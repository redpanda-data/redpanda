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
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

#include <optional>

namespace archival {

/// Sink for mirroring a partition's tiered-storage segments into the
/// cloud-topics L1 metastore during a tiered->cloud migration.
///
/// The migration mirror runs as an archiver housekeeping job (leader-only,
/// co-located with the manifest), but the L1 metastore client lives in
/// cloud_topics, which depends on cluster -- not the other way round. So the
/// archiver reaches the metastore through this abstract interface, implemented
/// in cloud_topics and injected into the archiver. The interface deliberately
/// uses only model/basic types (no cloud_topics types) to keep the dependency
/// one-way.
class migration_metastore {
public:
    enum class errc {
        ok,
        // Transient: the request could not be sent/applied (not leader,
        // timeout, transport). Retryable.
        retry,
        // The metastore rejected the request as invalid.
        invalid,
    };

    /// Whether the segment has an aborted-transaction (.tx) manifest, resolved
    /// from the source segment_meta at mirror time so the L1 read path can skip
    /// the object-storage probe where the answer is already known. Mirrors what
    /// native tiered storage knows without a probe (see remote_segment.cc): a
    /// v3 segment records its .tx size in metadata_size_hint (0 => none), and a
    /// compacted segment has no aborted batches by construction; only v1/v2
    /// non-compacted segments are unknowable without looking. Duplicated here
    /// (rather than reusing the cloud_topics l1 enum) to keep cluster/archival
    /// independent of cloud_topics; the sink maps it onto
    /// l1::tx_manifest_state.
    enum class tx_manifest_state { unknown, absent, present };

    /// One tiered-storage segment to register as an imported L1 extent. Carries
    /// the segment's location (ts_path) and data descriptor (delta/term) plus
    /// the per-segment timestamp/size/offset bounds needed to build the
    /// metastore object and extent. The owning partition is passed alongside
    /// (the archiver addresses by ntp; the sink resolves ntp -> the L1
    /// topic_id_partition).
    struct imported_segment {
        model::term_id term;
        model::timestamp max_timestamp;
        size_t size_bytes{0};
        // Path of the tiered-storage segment object (the imported extent points
        // at this object by reference; no L1 object is written).
        ss::sstring ts_path;
        kafka::offset base_kafka_offset;
        kafka::offset last_kafka_offset;
        // Offset-translation delta at the segment's base (segment_meta's
        // delta_offset). Carried so the reader translates log offsets directly
        // rather than inferring the delta from the first batch, which is wrong
        // for a compacted front hole (base_kafka_offset stays put while the
        // first surviving batch sits past it).
        model::offset_delta delta_base;
        // Whether this segment has a .tx manifest (resolved from segment_meta).
        tx_manifest_state tx_state{tx_manifest_state::unknown};
    };

    /// The partition's current L1 offsets -- the mirror's durable progress
    /// cursor. next_offset is the next offset a forward append must connect at.
    struct offsets {
        kafka::offset start_offset;
        kafka::offset next_offset;
    };

    virtual ~migration_metastore() = default;

    /// Forward-append imported segments at the partition's L1 tail. The first
    /// append to a fresh partition seeds its start/next at the first segment's
    /// base and marks it migrating. Segments must be contiguous and connect at
    /// next_offset; idempotent against already-present extents.
    virtual ss::future<errc>
    append_imported(const model::ntp&, chunked_vector<imported_segment>) = 0;

    /// The partition's current L1 offsets, or nullopt if it has no L1 state
    /// yet.
    virtual ss::future<std::optional<offsets>>
    get_offsets(const model::ntp&) = 0;

    /// Clear the partition's offline migrating flag (cutover
    /// finalize). The migrating phase is set implicitly by the first
    /// append_imported.
    virtual ss::future<errc> mark_complete(const model::ntp&) = 0;
};

} // namespace archival
