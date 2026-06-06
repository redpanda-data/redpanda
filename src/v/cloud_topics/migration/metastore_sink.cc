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
} // namespace

ss::future<errc> migration_metastore_sink::append_imported(
  chunked_vector<imported_segment> segs) {
    // Register the imported segments through the normal object-builder /
    // add_objects path. Imported objects reference existing TS segments (no L1
    // write to reserve), so they go through the builder's add_imported entry,
    // which skips pre-registration.
    auto builder_res = co_await _ms.object_builder();
    if (!builder_res.has_value()) {
        co_return to_errc(builder_res.error());
    }
    auto& builder = *builder_res.value();
    // Term starts must be strictly monotonic per partition; collapse to one
    // entry per distinct term at its lowest offset.
    chunked_hash_map<
      model::topic_id_partition,
      std::map<model::term_id, kafka::offset>>
      term_min;
    for (auto& s : segs) {
        auto& tmap = term_min[s.tidp];
        auto it = tmap.find(s.term);
        if (it == tmap.end() || s.base_kafka_offset < it->second) {
            tmap[s.term] = s.base_kafka_offset;
        }
        auto added = builder.add_imported(
          l1::metastore::object_metadata::ntp_metadata{
            .tidp = s.tidp,
            .base_offset = s.base_kafka_offset,
            .last_offset = s.last_kafka_offset,
            .max_timestamp = s.max_timestamp,
            .pos = 0,
            .size = s.size_bytes,
            .imported = l1::imported_ts_info{
              .ts_path = l1::ts_segment_path{std::move(s.ts_path)},
              .segment_term = s.term,
              .delta_base = s.delta_base,
            },
          });
        if (!added.has_value()) {
            co_return errc::invalid;
        }
    }
    l1::metastore::term_offset_map_t terms;
    for (const auto& [tidp, tmap] : term_min) {
        for (const auto& [term, off] : tmap) {
            terms[tidp].push_back(
              l1::metastore::term_offset{.term = term, .first_offset = off});
        }
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
migration_metastore_sink::get_offsets(const model::topic_id_partition& tidp) {
    auto res = co_await _ms.get_offsets(tidp);
    if (!res.has_value()) {
        co_return std::nullopt;
    }
    co_return offsets{
      .start_offset = res->start_offset,
      .next_offset = res->next_offset,
    };
}

ss::future<errc>
migration_metastore_sink::mark_complete(const model::topic_id_partition& tidp) {
    // Cutover clears the migrating flag: the partition is now a cloud topic.
    auto res = co_await _ms.set_migrating(tidp, false);
    if (!res.has_value()) {
        co_return to_errc(res.error());
    }
    co_return errc::ok;
}

} // namespace cloud_topics
