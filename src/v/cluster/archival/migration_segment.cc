/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cluster/archival/migration_segment.h"

#include "model/fundamental.h"

namespace archival {

std::optional<migration_metastore::imported_segment> make_imported_segment(
  const cloud_storage::segment_meta& meta, ss::sstring ts_path) {
    // Skip segments with no Kafka-addressable records (see the header): an
    // inverted extent would be rejected by the metastore and stall the mirror.
    if (meta.base_kafka_offset() >= meta.next_kafka_offset()) {
        return std::nullopt;
    }

    // Resolve the .tx manifest presence from segment_meta exactly as the native
    // read path would (remote_segment.cc): a compacted segment has no aborted
    // batches by construction, a v3 segment records its .tx size in
    // metadata_size_hint (0 => none), and v1/v2 don't encode it. Carrying the
    // resolved answer lets the L1 read path skip the .tx probe wherever it is
    // already known.
    using tx_state_t = migration_metastore::tx_manifest_state;
    auto tx_state = tx_state_t::unknown;
    if (
      meta.is_compacted
      || (meta.sname_format == cloud_storage::segment_name_format::v3 && meta.metadata_size_hint == 0)) {
        tx_state = tx_state_t::absent;
    } else if (meta.sname_format == cloud_storage::segment_name_format::v3) {
        tx_state = tx_state_t::present;
    }

    // Guard the pre-offset-translation sentinel: segments uploaded before
    // offset translation existed carry delta_offset == offset_delta::min() (the
    // "absent" sentinel), which the segment_meta kafka-offset helpers clamp to
    // 0. The extent's base/last bounds below already go through those clamped
    // helpers; delta_base must match, or the reader would translate by
    // INT64_MIN while the bounds say delta 0. Treat the sentinel as "no
    // translation" (delta 0), i.e. kafka offset == log offset.
    auto delta_base = meta.delta_offset == model::offset_delta::min()
                        ? model::offset_delta(0)
                        : meta.delta_offset;

    return migration_metastore::imported_segment{
      .term = meta.segment_term,
      .max_timestamp = meta.max_timestamp,
      .size_bytes = meta.size_bytes,
      .ts_path = std::move(ts_path),
      .base_kafka_offset = meta.base_kafka_offset(),
      .last_kafka_offset = kafka::prev_offset(meta.next_kafka_offset()),
      .delta_base = delta_base,
      .tx_state = tx_state,
    };
}

} // namespace archival
