/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/types.h"

#include "absl/container/node_hash_set.h"
#include "utils/to_string.h" // IWYU pragma: keep

namespace cloud_storage {

std::ostream& operator<<(std::ostream& o, const segment_meta& s) {
    fmt::print(
      o,
      "{{is_compacted: {}, size_bytes: {}, base_offset: {}, committed_offset: "
      "{}, base_timestamp: {}, max_timestamp: {}, delta_offset: {}, "
      "ntp_revision: {}, archiver_term: {}, segment_term: {}, "
      "delta_offset_end: {}, sname_format: {}, metadata_size_hint: {}}}",
      s.is_compacted,
      s.size_bytes,
      s.base_offset,
      s.committed_offset,
      s.base_timestamp,
      s.max_timestamp,
      s.delta_offset,
      s.ntp_revision,
      s.archiver_term,
      s.segment_term,
      s.delta_offset_end,
      s.sname_format,
      s.metadata_size_hint);
    return o;
}

std::ostream& operator<<(std::ostream& o, const segment_name_format& r) {
    switch (r) {
    case segment_name_format::v1:
        o << "{v1}";
        break;
    case segment_name_format::v2:
        o << "{v2}";
        break;
    case segment_name_format::v3:
        o << "{v3}";
        break;
    }
    return o;
}

std::ostream&
operator<<(std::ostream& o, const spillover_manifest_path_components& c) {
    fmt::print(
      o,
      "{{base: {}, last: {}, base_kafka: {}, next_kafka: {}, base_ts: {}, "
      "last_ts: {}}}",
      c.base,
      c.last,
      c.base_kafka,
      c.next_kafka,
      c.base_ts,
      c.last_ts);
    return o;
}

std::ostream& operator<<(std::ostream& o, const scrub_status& s) {
    switch (s) {
    case scrub_status::full:
        o << "{full}";
        break;
    case scrub_status::partial:
        o << "{partial}";
        break;
    case scrub_status::failed:
        o << "{failed}";
        break;
    }
    return o;
}

std::ostream& operator<<(std::ostream& o, const anomaly_type& t) {
    switch (t) {
    case anomaly_type::missing_delta:
        o << "{missing_delta}";
        break;
    case anomaly_type::non_monotonical_delta:
        o << "{non_monotonical_delta}";
        break;
    case anomaly_type::end_delta_smaller:
        o << "{end_delta_smaller}";
        break;
    case anomaly_type::committed_smaller:
        o << "{committed_smaller}";
        break;
    case anomaly_type::offset_gap:
        o << "{offset_gap}";
        break;
    case anomaly_type::offset_overlap:
        o << "{offset_overlap}";
        break;
    }
    return o;
}

std::ostream& operator<<(std::ostream& o, const anomaly_meta& meta) {
    fmt::print(
      o,
      "{{type: {}, at: {}, previous: {}}}",
      meta.type,
      meta.at,
      meta.previous);
    return o;
}

void scrub_segment_meta(
  const segment_meta& current,
  const std::optional<segment_meta>& previous,
  segment_meta_anomalies& detected) {
    // After one segment has a delta offset, all subsequent segments
    // should have a delta offset too.
    if (
      previous && previous->delta_offset != model::offset_delta{}
      && current.delta_offset == model::offset_delta{}) {
        detected.insert(anomaly_meta{
          .type = anomaly_type::missing_delta,
          .at = current,
          .previous = previous});
    }

    // The delta offset field of a segment should always be greater or
    // equal to that of the previous one.
    if (
      previous && previous->delta_offset != model::offset_delta{}
      && current.delta_offset != model::offset_delta{}
      && previous->delta_offset > current.delta_offset) {
        detected.insert(anomaly_meta{
          .type = anomaly_type::non_monotonical_delta,
          .at = current,
          .previous = previous});
    }

    // The committed offset of a segment should always be greater or equal
    // to the base offset.
    if (current.committed_offset < current.base_offset) {
        detected.insert(
          anomaly_meta{.type = anomaly_type::committed_smaller, .at = current});
    }

    // The end delta offset of a segment should always be greater or equal
    // to the base delta offset.
    if (
      current.delta_offset != model::offset_delta{}
      && current.delta_offset_end != model::offset_delta{}
      && current.delta_offset_end < current.delta_offset) {
        detected.insert(
          anomaly_meta{.type = anomaly_type::end_delta_smaller, .at = current});
    }

    // The base offset of a given segment should be equal to the committed
    // offset of the previous segment plus one. Otherwise, if the base offset is
    // greater, we have a gap in the log.
    if (
      previous
      && model::next_offset(previous->committed_offset) < current.base_offset) {
        detected.insert(anomaly_meta{
          .type = anomaly_type::offset_gap,
          .at = current,
          .previous = previous});
    }

    // The base offset of a given segment should be equal to the committed
    // offset of the previous segment plus one. Otherwise, if the base offset is
    // lower, we have overlapping segments in the log.
    if (
      previous
      && model::next_offset(previous->committed_offset) > current.base_offset) {
        detected.insert(anomaly_meta{
          .type = anomaly_type::offset_overlap,
          .at = current,
          .previous = previous});
    }
}

// Limit on number of anomalies that can be stored in the manifest
static constexpr size_t max_number_of_manifest_anomalies = 100;

bool anomalies::has_value() const {
    return missing_partition_manifest || missing_spillover_manifests.size() > 0
           || missing_segments.size() > 0
           || segment_metadata_anomalies.size() > 0;
}

size_t anomalies::count_segment_meta_anomaly_type(anomaly_type type) const {
    auto begin = segment_metadata_anomalies.begin();
    auto end = segment_metadata_anomalies.end();
    const auto count = std::count_if(begin, end, [&type](const auto& anomaly) {
        return anomaly.type == type;
    });

    return static_cast<size_t>(count);
}

/// Returns number of discarded elements
template<class T, size_t size_limit = max_number_of_manifest_anomalies>
inline size_t insert_with_size_limit(
  absl::node_hash_set<T>& dest, const absl::node_hash_set<T>& to_add) {
    if (dest.size() + to_add.size() <= size_limit) {
        dest.insert(
          std::make_move_iterator(to_add.begin()),
          std::make_move_iterator(to_add.end()));
        return 0;
    }
    auto total_size = dest.size() + to_add.size();
    auto to_remove = total_size - size_limit;
    size_t num_removed = 0;
    if (dest.size() <= to_remove) {
        to_remove -= dest.size();
        num_removed += dest.size();
        dest.clear();
    } else {
        auto it = dest.begin();
        std::advance(it, to_remove);
        dest.erase(dest.begin(), it);
        num_removed += to_remove;
        to_remove = 0;
    }
    auto begin = to_add.begin();
    auto end = to_add.end();
    std::advance(begin, to_remove);
    num_removed += to_remove;
    dest.insert(std::make_move_iterator(begin), std::make_move_iterator(end));
    return num_removed;
}

anomalies& anomalies::operator+=(anomalies&& other) {
    missing_partition_manifest |= other.missing_partition_manifest;

    // Keep only last 'max_number_of_manifest_anomalies' elements. The
    // scrubber moves from smaller offsets to larger offsets and 'other'
    // is supposed to contain larger offsets. Because of that we want
    // to add all elements from 'other' and truncate the prefix.
    // We also want to progress the scrub because we want to populate the
    // anomaly counters even if there are too many of them.
    num_discarded_missing_spillover_manifests += insert_with_size_limit(
      missing_spillover_manifests, other.missing_spillover_manifests);
    num_discarded_missing_segments += insert_with_size_limit(
      missing_segments, other.missing_segments);
    num_discarded_metadata_anomalies += insert_with_size_limit(
      segment_metadata_anomalies, other.segment_metadata_anomalies);

    last_complete_scrub = std::max(
      last_complete_scrub, other.last_complete_scrub);

    return *this;
}

std::ostream& operator<<(std::ostream& o, const anomalies& a) {
    if (!a.has_value()) {
        return o << "{}";
    }

    fmt::print(
      o,
      "{{missing_partition_manifest: {}, missing_spillover_manifests: {}, "
      "missing_segments: {}, segment_metadata_anomalies: {}}}",
      a.missing_partition_manifest,
      a.missing_spillover_manifests.size()
        + a.num_discarded_missing_spillover_manifests,
      a.missing_segments.size() + a.num_discarded_missing_segments,
      a.segment_metadata_anomalies.size() + a.num_discarded_metadata_anomalies);

    return o;
}

std::ostream& operator<<(std::ostream& os, upload_type upload) {
    return os << to_string(upload);
}

std::ostream& operator<<(std::ostream& os, download_type download) {
    return os << to_string(download);
}

std::ostream& operator<<(std::ostream& os, existence_check_type head) {
    switch (head) {
        using enum cloud_storage::existence_check_type;
    case object:
        return os << "object";
    case segment:
        return os << "segment";
    case manifest:
        return os << "manifest";
    }
}

} // namespace cloud_storage
