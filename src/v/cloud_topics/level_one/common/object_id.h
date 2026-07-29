/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/format_to.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "serde/rw/enum.h"
#include "utils/named_type.h"
#include "utils/uuid.h"

#include <seastar/core/sstring.hh>

#include <cstdint>
#include <optional>

namespace cloud_topics::l1 {

// An object ID is a unique identifier for a cloud topic L1 object.
using object_id = named_type<uuid_t, struct l1_object_id_tag>;

inline object_id create_object_id() { return object_id{uuid_t::create()}; }

/// The cloud-storage object path of a tiered-storage segment imported into L1
/// by reference (an opaque sname_format path).
using ts_segment_path = named_type<ss::sstring, struct ts_segment_path_tag>;

/// Whether an imported tiered-storage segment has an aborted-transaction (.tx)
/// manifest. Resolved once at import time from the source segment_meta so the
/// read path need not probe object storage to find out -- preserving a
/// capability native tiered storage already has (see remote_segment.cc): a v3
/// segment records its .tx manifest size in metadata_size_hint (0 => none), and
/// a compacted segment has no aborted batches by construction. Only v1/v2
/// non-compacted segments are genuinely unknowable without looking.
enum class tx_manifest_state : uint8_t {
    /// Presence not encoded in the source metadata (v1/v2, non-compacted): the
    /// read path probes object storage and tolerates a missing .tx. The
    /// default, so a value that was never resolved degrades to the safe
    /// (probe-and-tolerate) behavior rather than silently skipping a real .tx.
    unknown = 0,
    /// Known empty -- compacted (aborted batches removed by compaction) or v3
    /// with metadata_size_hint == 0. The read path skips the .tx download and
    /// treats the segment as having no aborted transactions.
    absent = 1,
    /// Known present -- v3 with metadata_size_hint > 0. The read path downloads
    /// the .tx. If it is unexpectedly missing (a lost/partial upload), the read
    /// path warns but tolerates it (reads committed-only), matching tiered
    /// storage, which treats a notfound .tx as empty.
    present = 2,
};

/// The full descriptor of a tiered-storage segment imported into L1 by
/// reference, as the metastore *interface* and the IO read path see it: where
/// the segment lives plus how to interpret its data.
///
/// The metastore *storage* layer decomposes this by ownership -- the path is an
/// object property and the delta/term are extent properties (see
/// `imported_ts_object_location` / `imported_ts_segment_info` in state.h) --
/// but the API and the reader treat it as one unit. The extent's Kafka offset
/// bounds (carried separately by the API/IO types) give the segment's offset
/// range.
struct imported_ts_info
  : public serde::
      envelope<imported_ts_info, serde::version<0>, serde::compat_version<0>> {
    friend bool
    operator==(const imported_ts_info&, const imported_ts_info&) = default;
    auto serde_fields() {
        return std::tie(ts_path, segment_term, delta_base, tx_state);
    }

    /// Opaque tiered-storage segment object path (sname_format path).
    ts_segment_path ts_path;
    /// Raft term the segment was written in. Stamped onto every imported batch
    /// as its partition leader epoch (a tiered-storage segment is single-term),
    /// so a Kafka fetch of the imported region reports the original leader
    /// epoch rather than -1.
    model::term_id segment_term{};
    /// Offset-translation delta at the segment's base (the source
    /// segment_meta's delta_offset: base log offset minus base Kafka offset).
    /// Seeds the reader's running delta when a seek scans from the segment
    /// start, so translation is correct even when compaction has removed the
    /// leading records -- the first surviving batch may sit past the declared
    /// base, and inferring the delta from it would renumber survivors down into
    /// the hole.
    model::offset_delta delta_base{};
    /// Whether the segment's aborted-transaction (.tx) manifest exists,
    /// resolved at import time so the read path can skip the probe where the
    /// answer is known. See tx_manifest_state.
    tx_manifest_state tx_state{tx_manifest_state::unknown};
};

/// An L1 object or imported TS segment, as seen by the IO read path. For an
/// imported extent `imported` is the segment descriptor (path + term + delta).
///
/// `position` and `size` carry format-specific semantics:
///   native (imported == nullopt):
///     position = footer byte offset within the object
///     size     = footer length in bytes (= object_size - footer_pos)
///   imported (imported.has_value()):
///     position = 0 (unused)
///     size     = total segment size in bytes
struct object_extent {
    object_id id;
    size_t position = 0;
    size_t size = 0;
    /// Set for an imported extent (nullopt for native L1); carries the
    /// segment's path, term, and offset-translation delta.
    std::optional<imported_ts_info> imported;

    fmt::iterator format_to(fmt::iterator it) const;
};

/// One object to delete via io::delete_objects. A native L1 object (ts_path
/// nullopt) is deleted by its id-derived path; an imported tiered-storage
/// segment (ts_path set) is deleted at that path (the segment plus its .tx and
/// .index). Both reside in the one configured object bucket.
struct object_location {
    object_id id;
    std::optional<ts_segment_path> ts_path;
};

} // namespace cloud_topics::l1
