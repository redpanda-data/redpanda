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

#include "bytes/iobuf.h"
#include "cloud_storage/fwd.h"
#include "cloud_storage_clients/types.h"
#include "cluster/archival/staging_uploader.h"
#include "model/fundamental.h"
#include "utils/retry_chain_node.h"

#include <absl/container/node_hash_map.h>

#include <filesystem>

namespace archival {

/// Reference to one partition extent inside a staging object.
struct staged_extent_ref {
    model::term_id term;
    model::offset base;
    model::offset last;
    ss::sstring key;
    uint64_t byte_offset{0};
    uint64_t byte_len{0};
    bool compacted{false};
    model::initial_revision_id revision{};
};

/// Catalog of every staged extent in a bucket, built once per recovery by
/// listing staging/ and reading each object's inline index (two ranged GETs
/// per object: footer, then index). Shared by all partition downloads.
class staging_recovery_catalog {
public:
    /// Never throws: an unreadable staging object is skipped with a warning
    /// (recovery then simply stops at the canonical tier for the ranges it
    /// carried).
    static ss::future<staging_recovery_catalog> build(
      cloud_storage::remote& remote,
      const cloud_storage_clients::bucket_name& bucket,
      retry_chain_node& parent,
      std::optional<model::cluster_uuid> source_cluster = std::nullopt);

    const std::vector<staged_extent_ref>* find(const model::ntp& ntp) const {
        auto it = _extents.find(ntp);
        return it == _extents.end() ? nullptr : &it->second;
    }

    bool empty() const { return _extents.empty(); }
    size_t partitions() const { return _extents.size(); }
    /// Objects whose index could not be read even after retries. When
    /// non-zero the catalog may be missing extents and every replay decision
    /// made from it is best-effort — callers log this loudly.
    size_t skipped_downloads() const { return _skipped_downloads; }

private:
    absl::node_hash_map<model::ntp, std::vector<staged_extent_ref>> _extents;
    size_t _skipped_downloads{0};
};

struct staged_tail_result {
    // last offset after replay; equals last_canonical when nothing applied
    model::offset max_offset;
    size_t batches_applied{0};
    size_t bytes_written{0};
    // raft term of the last applied extent; meaningful only when
    // batches_applied > 0
    model::term_id last_term{};
    // [base, last] offset ranges of applied batches that are invisible to
    // kafka clients (offset-translator gaps), in offset order
    std::vector<std::pair<model::offset, model::offset>> translator_gaps;
};

/// Materialize the staged tail of \p ntp above \p last_canonical as a raft
/// log segment file in \p dir ("{base}-{term}-v1.log", raw disk-format
/// batches).
///
/// Correctness rules (the "canonical-first" contract, safe for compacted
/// topics):
///  - only batches with base_offset strictly above last_canonical are kept,
///    at batch granularity — staged data overlapping the canonical range is
///    always discarded, so replay can never resurrect records the canonical
///    tier has compacted away below its last offset;
///  - extents are applied in offset order with term-fenced dedupe (highest
///    term wins on overlap);
///  - the tail must be contiguous with the canonical end; the first gap stops
///    the replay (staged data past a gap is unrecoverable-in-order and is
///    ignored, loudly).
ss::future<staged_tail_result> materialize_staged_tail(
  cloud_storage::remote& remote,
  const cloud_storage_clients::bucket_name& bucket,
  const staging_recovery_catalog& catalog,
  const model::ntp& ntp,
  model::initial_revision_id revision,
  model::offset last_canonical,
  const std::filesystem::path& dir,
  retry_chain_node& parent);

} // namespace archival
