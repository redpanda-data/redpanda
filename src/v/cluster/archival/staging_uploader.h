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

#include "base/units.h"
#include "bytes/iobuf.h"
#include "cloud_storage/fwd.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "cluster/fwd.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "storage/fwd.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/node_hash_map.h>

#include <deque>

namespace archival {

/// Byte range of one partition's committed data inside a staging object.
/// Offsets are raft (log) domain offsets: the payload is the raw disk-format
/// byte range [base, last] of the partition's log, so a staging object is
/// replayable without any external metadata.
struct staging_extent
  : serde::envelope<
      staging_extent,
      serde::version<2>, // v1: + compacted; v2: + revision
      serde::compat_version<0>> {
    model::ntp ntp;
    model::term_id term;
    model::offset base;
    model::offset last;
    uint64_t byte_offset{0};
    uint64_t byte_len{0};
    // Source range contained compacted segments: offsets inside/after it may
    // legitimately be non-contiguous. Consumed by staged-tail replay.
    bool compacted{false};
    // Topic incarnation (remote revision): replay only applies extents whose
    // revision matches the topic being recovered, so staged data from an
    // earlier cluster/topic incarnation that reused the same ntp name can
    // never leak into a restored log.
    model::initial_revision_id revision{};

    auto serde_fields() {
        return std::tie(
          ntp, term, base, last, byte_offset, byte_len, compacted, revision);
    }
};

/// Trailing index of a staging object. Serialized with serde and followed by
/// the fixed footer, so the object is self-describing:
///   [data region][serde staging_index][u64 index_size][u32 version][u32 magic]
struct staging_index
  : serde::
      envelope<staging_index, serde::version<0>, serde::compat_version<0>> {
    std::vector<staging_extent> extents;

    auto serde_fields() { return std::tie(extents); }
};

/// Asynchronous tiered-storage dual-write staging uploader (one per shard).
///
/// Coalesces committed-but-not-yet-staged data across all local leader
/// partitions into a single offset-stamped staging object per round and
/// uploads it to the primary bucket (and, when configured, a secondary bucket)
/// strictly off the produce ack path. A failed or slow upload never affects
/// produces; it surfaces as staging lag.
class staging_uploader {
public:
    static constexpr uint32_t footer_magic = 0x54535331; // "TSS1"
    static constexpr uint32_t footer_version = 1;

    staging_uploader(
      ss::sharded<cloud_storage::remote>& remote,
      ss::sharded<cluster::partition_manager>& pm,
      ss::sharded<storage::api>& storage,
      cloud_storage_clients::bucket_name primary_bucket);

    // The source-cluster staging prefix: staging/{cluster_uuid}/. The uuid
    // is the stable identity a whole-cluster restore names via remote_label,
    // unlike cluster_id which regenerates on the replacement cluster.
    ss::sstring cluster_label() const;

    ss::future<> start();
    ss::future<> stop();

    /// Serialize index + footer onto the end of a staging object payload.
    static iobuf make_object(iobuf data, staging_index index);

private:
    ss::future<> run_loop();
    /// One staging round. Returns true when a partition was deferred because
    /// the round byte budget was exhausted (caller loops again immediately).
    ss::future<bool> round();
    ss::future<> maybe_gc();
    ss::future<cloud_storage::upload_result>
    put(cloud_storage_clients::bucket_name bucket, ss::sstring key, iobuf buf);
    ss::future<> drain_secondary_queue();
    /// Anti-entropy: list this node+shard's staging prefix in both buckets
    /// and re-upload objects missing from the secondary. Covers losses the
    /// in-memory retry queue cannot (e.g. restarts mid-outage).
    ss::future<> reconcile_secondary();
    void setup_metrics();

    ss::sharded<cloud_storage::remote>& _remote;
    ss::sharded<storage::api>& _storage;
    ss::sharded<cluster::partition_manager>& _pm;
    cloud_storage_clients::bucket_name _primary_bucket;

    // ntp -> last staged (raft) offset
    absl::node_hash_map<model::ntp, model::offset> _cursors;
    ss::lowres_clock::time_point _last_gc{};
    uint64_t _seq{0};
    // Unique per process start: staging keys embed it so a restored cluster
    // incarnation can never overwrite (or race the replay reads of) the dead
    // incarnation's staging objects, despite sharing cluster_id/node/shard.
    ss::sstring _boot_id;

    struct pending_secondary {
        ss::sstring key;
        iobuf payload;
    };
    std::deque<pending_secondary> _secondary_queue;
    size_t _secondary_queue_bytes{0};
    static constexpr size_t max_secondary_queue_objects = 128;
    static constexpr size_t max_secondary_queue_bytes = 256_MiB;

    // counters exposed via metrics
    uint64_t _uploads_total{0};
    uint64_t _staged_bytes_total{0};
    uint64_t _upload_errors_primary{0};
    uint64_t _upload_errors_secondary{0};
    uint64_t _secondary_dropped{0};
    uint64_t _secondary_reconciled{0};
    ss::lowres_clock::time_point _last_reconcile{};
    uint64_t _staging_gaps{0};
    int64_t _lag_offsets{0};

    ss::abort_source _as;
    ss::gate _gate;
    retry_chain_node _rtc;
    metrics::internal_metric_groups _metrics;
};

} // namespace archival
