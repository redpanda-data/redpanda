/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/migration/metastore_sink.h"

// metastore.h transitively provides l1::imported_ts_info (via
// common/object_id.h) and the migrating flag (via metastore/state.h),
// whose packages are not directly visible here.
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cluster/metadata_cache.h"

#include <map>

namespace cloud_topics {

namespace {
using errc = archival::migration_metastore::errc;

errc to_errc(l1::metastore::errc e) {
    switch (e) {
    case l1::metastore::errc::transport_error:
        return errc::retry;
    case l1::metastore::errc::missing_ntp:
    case l1::metastore::errc::invalid_request:
    case l1::metastore::errc::out_of_range:
        return errc::invalid;
    }
    return errc::invalid;
}

// Map the archiver-side .tx-presence state onto the l1 read-path enum. The two
// enums are kept separate so cluster/archival need not depend on cloud_topics;
// this is the single point that bridges them.
l1::tx_manifest_state
to_l1_tx_state(archival::migration_metastore::tx_manifest_state s) {
    using src = archival::migration_metastore::tx_manifest_state;
    switch (s) {
    case src::unknown:
        return l1::tx_manifest_state::unknown;
    case src::absent:
        return l1::tx_manifest_state::absent;
    case src::present:
        return l1::tx_manifest_state::present;
    }
    return l1::tx_manifest_state::unknown;
}
} // namespace

std::optional<model::topic_id_partition>
migration_metastore_sink::resolve(const model::ntp& ntp) const {
    auto cfg = _md->get_topic_cfg(
      model::topic_namespace_view{ntp.ns, ntp.tp.topic});
    if (!cfg || !cfg->tp_id.has_value()) {
        return std::nullopt;
    }
    return model::topic_id_partition{*cfg->tp_id, ntp.tp.partition};
}

ss::future<errc> migration_metastore_sink::append_imported(
  const model::ntp& ntp, chunked_vector<imported_segment> segs) {
    auto tidp = resolve(ntp);
    if (!tidp.has_value()) {
        co_return errc::invalid;
    }
    // Mark the partition migrating before importing: the metastore only adopts
    // a fresh partition's log at the imported extents' (non-zero) base when it
    // is migrating. Idempotent, so it is safe to repeat on every batch (only
    // the first import, against an empty partition, depends on it).
    if (auto res = co_await _ms.set_migrating(*tidp, true); !res.has_value()) {
        co_return to_errc(res.error());
    }
    // Register the imported segments through the normal object-builder /
    // add_objects path. Imported objects reference existing TS segments (no L1
    // write to reserve), so they go through the builder's add_imported entry,
    // which skips pre-registration.
    auto builder_res = co_await _ms.object_builder();
    if (!builder_res.has_value()) {
        co_return to_errc(builder_res.error());
    }
    auto& builder = *builder_res.value();
    // Term starts must be strictly monotonic; collapse to one entry per
    // distinct term at its lowest offset.
    std::map<model::term_id, kafka::offset> term_min;
    for (auto& s : segs) {
        auto it = term_min.find(s.term);
        if (it == term_min.end() || s.base_kafka_offset < it->second) {
            term_min[s.term] = s.base_kafka_offset;
        }
        auto added = builder.add_imported(
          l1::metastore::object_metadata::ntp_metadata{
            .tidp = *tidp,
            .base_offset = s.base_kafka_offset,
            .last_offset = s.last_kafka_offset,
            .max_timestamp = s.max_timestamp,
            .pos = 0,
            .size = s.size_bytes,
            .imported = l1::imported_ts_info{
              .ts_path = l1::ts_segment_path{std::move(s.ts_path)},
              .segment_term = s.term,
              .delta_base = s.delta_base,
              .tx_state = to_l1_tx_state(s.tx_state),
            },
          });
        if (!added.has_value()) {
            co_return errc::invalid;
        }
    }
    l1::metastore::term_offset_map_t terms;
    for (const auto& [term, off] : term_min) {
        terms[*tidp].push_back(
          l1::metastore::term_offset{.term = term, .first_offset = off});
    }
    auto res = co_await _ms.add_objects(builder, terms);
    if (!res.has_value()) {
        co_return to_errc(res.error());
    }
    if (!res->corrected_next_offsets.empty()) {
        // The imported batch did not connect at the partition tail (the
        // partition must be migrating, and the mirror forward-appends), so
        // surface it rather than silently dropping the extents.
        co_return errc::invalid;
    }
    co_return errc::ok;
}

ss::future<std::optional<archival::migration_metastore::offsets>>
migration_metastore_sink::get_offsets(const model::ntp& ntp) {
    auto tidp = resolve(ntp);
    if (!tidp.has_value()) {
        co_return std::nullopt;
    }
    auto res = co_await _ms.get_offsets(*tidp);
    if (!res.has_value()) {
        co_return std::nullopt;
    }
    co_return offsets{
      .start_offset = res->start_offset,
      .next_offset = res->next_offset,
    };
}

ss::future<errc>
migration_metastore_sink::mark_complete(const model::ntp& ntp) {
    auto tidp = resolve(ntp);
    if (!tidp.has_value()) {
        co_return errc::invalid;
    }
    // Cutover clears the migrating flag: the partition is now a cloud topic.
    auto res = co_await _ms.set_migrating(*tidp, false);
    if (!res.has_value()) {
        co_return to_errc(res.error());
    }
    co_return errc::ok;
}

} // namespace cloud_topics
