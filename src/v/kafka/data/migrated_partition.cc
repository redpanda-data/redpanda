/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/data/migrated_partition.h"

#include "cloud_storage/async_manifest_view.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/types.h"
#include "cluster/partition.h"
#include "model/fundamental.h"
#include "storage/offset_translator_state.h"

#include <algorithm>

namespace kafka {

migrated_partition::migrated_partition(
  ss::lw_shared_ptr<cluster::partition> partition,
  kafka::offset boundary,
  std::unique_ptr<kafka::partition_proxy::impl> ts,
  std::unique_ptr<kafka::partition_proxy::impl> ct) noexcept
  : _partition(std::move(partition))
  , _boundary(boundary)
  , _ts(std::move(ts))
  , _ct(std::move(ct)) {}

migrated_partition::~migrated_partition() = default;

const model::ntp& migrated_partition::ntp() const { return _ct->ntp(); }

ss::future<result<model::offset, error_code>>
migrated_partition::sync_effective_start(model::timeout_clock::duration t) {
    // The post-migration partition is fundamentally a cloud topic; the TS range
    // below the boundary is exposed via start_offset(). Fetch validation for
    // offsets in the TS range is routed to the TS path in
    // validate_fetch_offset.
    return _ct->sync_effective_start(t);
}

model::offset migrated_partition::local_start_offset() const {
    return _ct->local_start_offset();
}

model::offset migrated_partition::start_offset() const {
    // Expose the earliest readable offset across both paths so a Kafka client
    // can fetch the pre-migration TS range from the start.
    return std::min(_ts->start_offset(), _ct->start_offset());
}

model::offset migrated_partition::high_watermark() const {
    return _ct->high_watermark();
}

checked<model::offset, error_code>
migrated_partition::last_stable_offset() const {
    return _ct->last_stable_offset();
}

kafka::leader_epoch migrated_partition::leader_epoch() const {
    return _ct->leader_epoch();
}

ss::future<std::optional<model::offset>>
migrated_partition::get_leader_epoch_last_offset(
  kafka::leader_epoch epoch) const {
    return _ct->get_leader_epoch_last_offset(epoch);
}

bool migrated_partition::is_leader() const { return _ct->is_leader(); }

ss::future<std::error_code> migrated_partition::linearizable_barrier() {
    return _ct->linearizable_barrier();
}

ss::future<error_code> migrated_partition::prefix_truncate(
  model::offset o, ss::lowres_clock::time_point deadline) {
    return _ct->prefix_truncate(o, deadline);
}

ss::future<storage::translating_reader>
migrated_partition::make_reader(kafka::log_reader_config cfg) {
    if (cfg.start_offset <= _boundary) {
        // Pre-migration range: serve from the tiered-storage path, capped at
        // the boundary. The cap matters because after migration the TS archiver
        // may upload CT-phase raft segments (ctp_placeholder batches) alongside
        // the original TS data; the remote-segment reader silently skips
        // non-raft_data batches, so a reader crossing the boundary would drop
        // committed CT records. The consumer's next fetch starts above the
        // boundary and is routed to the CT path.
        cfg.max_offset = std::min(cfg.max_offset, _boundary);
        return _ts->make_reader(cfg);
    }
    return _ct->make_reader(cfg);
}

ss::future<std::optional<storage::timequery_result>>
migrated_partition::timequery(storage::timequery_config cfg) {
    // Timestamp-based lookups are served by the CT path. A timequery for a
    // timestamp that falls in the pre-migration TS range is not specially
    // handled (it resolves to the CT start); offset-based reads from 0 do reach
    // the TS data via make_reader. Revisit if timestamp lookups into the TS
    // range become a requirement.
    return _ct->timequery(cfg);
}

ss::future<std::vector<model::tx_range>>
migrated_partition::aborted_transactions(
  model::offset base,
  model::offset last,
  ss::lw_shared_ptr<const storage::offset_translator_state> ot_state) {
    // base/last are Kafka-space offsets here (model::offset typed but holding
    // Kafka values, matching the partition_proxy convention).
    const model::offset boundary{_boundary()};
    if (base > boundary) {
        // Post-migration range: pre-filtered committed-only CT data; delegate
        // to the CT path.
        co_return co_await _ct->aborted_transactions(
          base, last, std::move(ot_state));
    }
    if (!ot_state) {
        // No translator => reading from L1, where aborts are already filtered.
        co_return std::vector<model::tx_range>{};
    }

    // Pre-migration range: the local rm_stm may have discarded (or the local
    // log been prefix-truncated past) the raft offsets covering this range, so
    // read the abort index from the per-segment .tx files in cloud storage and
    // translate the raft offsets to Kafka via the fetch's offset-translator.
    //
    // .tx files store raft offsets. For an abort-control batch beyond the
    // current fetch window the OT state underestimates the delta (it has not
    // seen the non-data batches written after the window end), so look up the
    // segment containing the offset in the STM manifest and use its
    // delta_offset_end. For a transaction straddling the TS->CT boundary the
    // abort-control batch lives in CT raft space; cap the translated Kafka
    // offset at the boundary so committed CT records are never over-filtered.
    const auto base_k = model::offset_cast(base);
    const auto last_k = std::min(model::offset_cast(last), _boundary);
    cloud_storage::offset_range offsets{
      .begin = base_k,
      .end = last_k,
      .begin_rp = ot_state->to_log_offset(kafka::offset_cast(base_k)),
      .end_rp = ot_state->to_log_offset(kafka::offset_cast(last_k)),
    };

    auto source = co_await _partition->aborted_transactions_cloud(offsets);

    const cloud_storage::partition_manifest* stm_manifest = nullptr;
    auto manifest_view = _partition->get_cloud_storage_manifest_view();
    if (manifest_view) {
        stm_manifest = &manifest_view->stm_manifest();
    }

    std::vector<model::tx_range> target;
    target.reserve(source.size());
    for (const auto& range : source) {
        // tx_range first/last are model::offset typed but hold Kafka-space
        // values (the result of from_log_offset). Same convention here.
        model::offset translated_last;
        if (range.last <= offsets.end_rp) {
            // Within OT state coverage: exact translation.
            translated_last = ot_state->from_log_offset(range.last);
        } else {
            translated_last = ot_state->from_log_offset(range.last);
            if (stm_manifest) {
                auto seg_it = stm_manifest->segment_containing(range.last);
                if (seg_it != stm_manifest->end()) {
                    const auto& meta = *seg_it;
                    if (meta.delta_offset_end != model::offset_delta::min()) {
                        translated_last = kafka::offset_cast(
                          range.last - meta.delta_offset_end);
                    }
                }
            }
            if (translated_last > kafka::offset_cast(_boundary)) {
                translated_last = kafka::offset_cast(_boundary);
            }
        }
        auto translated_first = ot_state->from_log_offset(
          std::max(offsets.begin_rp, range.first));
        target.emplace_back(range.pid, translated_first, translated_last);
    }
    co_return target;
}

ss::future<error_code> migrated_partition::validate_fetch_offset(
  model::offset o,
  bool reading_from_follower,
  model::timeout_clock::time_point deadline) {
    // Accept offsets across the whole [TS start, CT high watermark] range by
    // routing validation to the path that owns the offset. o is a Kafka-space
    // value in a model::offset, matching the partition_proxy convention.
    if (o <= model::offset{_boundary()}) {
        return _ts->validate_fetch_offset(o, reading_from_follower, deadline);
    }
    return _ct->validate_fetch_offset(o, reading_from_follower, deadline);
}

ss::future<result<model::offset>> migrated_partition::replicate(
  chunked_vector<model::record_batch> batches, raft::replicate_options opts) {
    // All post-migration writes are cloud-topics writes.
    return _ct->replicate(std::move(batches), opts);
}

raft::replicate_stages migrated_partition::replicate(
  model::batch_identity bi,
  model::record_batch batch,
  raft::replicate_options opts) {
    return _ct->replicate(bi, std::move(batch), opts);
}

std::unique_ptr<exact_offset_replicator>
migrated_partition::make_exact_offset_replicator() && {
    return std::move(*_ct).make_exact_offset_replicator();
}

result<partition_info> migrated_partition::get_partition_info() const {
    return _ct->get_partition_info();
}

size_t migrated_partition::estimate_size_between(
  kafka::offset base, kafka::offset last) const {
    return _ct->estimate_size_between(base, last);
}

cluster::partition_probe& migrated_partition::probe() { return _ct->probe(); }

size_t migrated_partition::local_size_bytes() const {
    return _ct->local_size_bytes();
}

ss::future<std::optional<size_t>> migrated_partition::cloud_size_bytes() const {
    return _ct->cloud_size_bytes();
}

model::offset migrated_partition::offset_lag() const {
    return _ct->offset_lag();
}

ss::future<cluster::partition_cloud_storage_status>
migrated_partition::get_cloud_storage_status() const {
    return _ct->get_cloud_storage_status();
}

} // namespace kafka
