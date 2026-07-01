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
#include "cloud_storage/types.h"
#include "cluster/archival/migration_metastore.h"

#include <seastar/core/sstring.hh>

#include <optional>

namespace archival {

/// Resolve the imported-extent descriptor for a single source tiered-storage
/// segment during the migration mirror, from its `segment_meta`. `ts_path` is
/// supplied by the caller (generating it needs the owning submanifest and the
/// archiver's remote_path_provider); every other field derives purely from
/// `meta`, which is what makes this unit-testable across the segment-format
/// matrix.
///
/// Returns nullopt for a segment with no Kafka-addressable records (a
/// fully-compacted-away or control/config-only segment), i.e.
/// base_kafka_offset >= next_kafka_offset. L1 is keyed on Kafka offsets, so
/// such a segment imports nothing; building an extent for it would yield
/// last_kafka_offset = base_kafka_offset - 1 (inverted), which the metastore
/// rejects -- failing the whole append and stalling the mirror. The mirror
/// skips it instead.
std::optional<migration_metastore::imported_segment> make_imported_segment(
  const cloud_storage::segment_meta& meta, ss::sstring ts_path);

} // namespace archival
