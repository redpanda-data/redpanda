/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "segment_reupload.h"

#include "cloud_storage/partition_manifest.h"
#include "logger.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"

namespace archival {
segment_collector::segment_collector(
  model::offset begin_inclusive,
  const cloud_storage::partition_manifest* manifest,
  const storage::disk_log_impl* log,
  size_t max_uploaded_segment_size)
  : _begin_inclusive(begin_inclusive)
  , _manifest(manifest)
  , _log(log)
  , _max_uploaded_segment_size(max_uploaded_segment_size) {}

void segment_collector::collect_segments() {
    if (_manifest->size() == 0) {
        vlog(
          archival_log.warn,
          "No segments to collect for ntp {}",
          _manifest->get_ntp());
        return;
    }

    align_begin_offset_to_manifest();

    if (_begin_inclusive >= _manifest->get_last_offset()) {
        vlog(
          archival_log.warn,
          "Start offset {} is ahead of manifest last offset {} for ntp {}",
          _begin_inclusive,
          _manifest->get_last_offset(),
          _manifest->get_ntp());
        return;
    }

    do_collect();
}

segment_collector::segment_seq segment_collector::segments() {
    return _segments;
}

void segment_collector::do_collect() {
    auto replace_boundary = find_replacement_boundary();
    auto start = _begin_inclusive;
    model::offset current_segment_end{0};
    size_t collected_size = 0;
    while (current_segment_end < _manifest->get_last_offset()) {
        auto result = find_next_compacted_segment(start);
        if (result.segment.get() == nullptr) {
            break;
        }

        if (unlikely(!_ntp_cfg)) {
            _ntp_cfg = result.ntp_conf;
        }

        auto segment_size = result.segment->size_bytes();
        if (collected_size + segment_size > _max_uploaded_segment_size) {
            vlog(
              archival_log.debug,
              "Compacted segment collect for ntp {} stopping collection, total "
              "size: {} will overflow max allowed upload size: {}, current "
              "collected size: {}",
              _manifest->get_ntp(),
              collected_size + segment_size,
              _max_uploaded_segment_size,
              collected_size);
            break;
        }

        // For the first compacted segment found, begin offset needs to be
        // re-aligned if it falls inside manifest segment.
        if (_segments.empty()) {
            _begin_inclusive = result.segment->offsets().base_offset;
            align_begin_offset_to_manifest();
        }
        _segments.push_back(result.segment);
        current_segment_end = result.segment->offsets().committed_offset;
        start = current_segment_end + model::offset{1};
        collected_size += segment_size;
    }

    if (current_segment_end >= replace_boundary) {
        _can_replace_manifest_segment = true;
    }

    align_end_offset_to_manifest(current_segment_end);
}

model::offset segment_collector::find_replacement_boundary() const {
    auto it = _manifest->segment_containing(_begin_inclusive);

    // Crossing this boundary means that the collection can replace at least one
    // segment in manifest.
    model::offset replace_boundary;

    // manifest: 10-19, 25-29
    // _begin_inclusive (in gap): 22.
    if (it == _manifest->end()) {
        // first segment after gap: 25-29
        it = std::find_if(
          _manifest->begin(), _manifest->end(), [this](const auto& entry) {
              return entry.first.base_offset > _begin_inclusive;
          });

        // The collection is valid if it can reach the end of the gap: 24
        replace_boundary = it->first.base_offset - model::offset{1};
    } else {
        replace_boundary = it->second.committed_offset;
    }

    return replace_boundary;
}

void segment_collector::align_end_offset_to_manifest(
  model::offset compacted_segment_end) {
    if (compacted_segment_end >= _manifest->get_last_offset()) {
        vlog(
          archival_log.debug,
          "Compacted segment collect for ntp {} offset {} advanced "
          "ahead of manifest, clamping to {}",
          _manifest->get_ntp(),
          compacted_segment_end,
          _manifest->get_last_offset());
        _end_inclusive = _manifest->get_last_offset();
    } else {
        // Align the end offset to the nearest segment ending in manifest.
        auto it = _manifest->segment_containing(compacted_segment_end);
        if (it == _manifest->end()) {
            // compacted_segment_end is in a gap in the manifest.
            if (
              compacted_segment_end >= _manifest->get_start_offset().value()) {
                vlog(
                  archival_log.info,
                  "Compacted segment collect for ntp {}: collection ended at "
                  "gap in manifest: {}",
                  _manifest->get_ntp(),
                  compacted_segment_end);

                // try to fill the manifest gap with the data locally available.
                _end_inclusive = compacted_segment_end;
            }
            return;
        }

        // If the compacted segment end is not aligned to manifest segment, then
        // pull back to the end of the previous segment.
        if (it->second.committed_offset == compacted_segment_end) {
            _end_inclusive = compacted_segment_end;
        } else {
            _end_inclusive = it->first.base_offset - model::offset{1};
        }
    }
}

segment_collector::lookup_result
segment_collector::find_next_compacted_segment(model::offset start_offset) {
    const auto& segment_set = _log->segments();
    auto it = segment_set.lower_bound(start_offset);
    // start_offset < log start due to eviction of compacted segments before
    // they could be uploaded, skip forward to log start.
    if (start_offset < _log->offsets().start_offset) {
        vlog(
          archival_log.debug,
          "Finding next compacted segment for {}: start_offset: {} behind log "
          "start: {}, skipping forward",
          _manifest->get_ntp(),
          start_offset,
          _log->offsets().start_offset);
        it = segment_set.begin();
    }

    if (it == segment_set.end()) {
        vlog(
          archival_log.debug,
          "Finding next compacted segment for {}: can't find segment after "
          "offset: {}",
          _manifest->get_ntp(),
          start_offset);
        return {};
    }

    const auto& segment = *it;
    if (segment->finished_self_compaction()) {
        vlog(
          archival_log.trace,
          "Found compacted segment for ntp {}: {}",
          _manifest->get_ntp(),
          segment);
        return {.segment = segment, .ntp_conf = &_log->config()};
    }
    vlog(
      archival_log.debug,
      "Finding next compacted segment for {}: no compacted "
      "segments after offset: {}",
      _manifest->get_ntp(),
      start_offset);
    return {};
}

model::offset segment_collector::begin_inclusive() const {
    return _begin_inclusive;
}

model::offset segment_collector::end_inclusive() const {
    return _end_inclusive;
}

const storage::ntp_config* segment_collector::ntp_cfg() const {
    return _ntp_cfg;
}

bool segment_collector::can_replace_manifest_segment() const {
    return _can_replace_manifest_segment && _begin_inclusive < _end_inclusive;
}

cloud_storage::segment_name segment_collector::adjust_segment_name() const {
    vassert(
      !_segments.empty(), "Cannot calculate segment name with no segments");

    auto first = _segments.front();
    auto file_name = first->filename();
    auto meta = storage::segment_path::parse_segment_filename(file_name);
    auto version = meta ? meta->version : storage::record_version_type::v1;

    cloud_storage::segment_name name{};
    if (_begin_inclusive == first->offsets().base_offset) {
        auto orig_path = std::filesystem::path(file_name);
        name = cloud_storage::segment_name(orig_path.filename().string());
        vlog(archival_log.debug, "Using original segment name: {}", name);
    } else {
        auto path = storage::segment_path::make_segment_path(
          *_ntp_cfg, _begin_inclusive, first->offsets().term, version);
        name = cloud_storage::segment_name(path.filename().string());
        vlog(archival_log.debug, "Using adjusted segment name: {}", name);
    }

    return name;
}

void segment_collector::align_begin_offset_to_manifest() {
    if (_begin_inclusive >= _manifest->get_last_offset()) {
        return;
    }

    if (_begin_inclusive < _manifest->get_start_offset().value()) {
        vlog(
          archival_log.debug,
          "_begin_inclusive is behind manifest for ntp: {}, skipping forward "
          "to "
          "start of manifest from: {} to: {}",
          _manifest->get_ntp(),
          _begin_inclusive,
          _manifest->get_start_offset().value());

        // manifest: 10-40
        // _begin_inclusive: before: 5, after: 10
        _begin_inclusive = _manifest->get_start_offset().value();
        return;
    }

    auto it = _manifest->find(_begin_inclusive);

    // If iterator points to a segment, it means that _begin_inclusive is
    // aligned on manifest segment boundary, so do nothing. Otherwise, skip
    // _begin_inclusive to the start of the next manifest segment.
    if (it == _manifest->end()) {
        it = _manifest->segment_containing(_begin_inclusive);

        // manifest: 10-19, 25-29
        // _begin_inclusive (in gap): before: 22, after: 22
        if (it == _manifest->end()) {
            vlog(
              archival_log.debug,
              "_begin_inclusive lies in manifest gap for ntp: {} "
              "value: {}",
              _manifest->get_ntp(),
              _begin_inclusive);
            return;
        }

        // manifest: 10-19, 20-29
        // _begin_inclusive: before: 15, after: 20 OR
        // _begin_inclusive: before: 25, after: 30
        _begin_inclusive = it->second.committed_offset + model::offset{1};
        vlog(
          archival_log.debug,
          "_begin_inclusive skipped to start of next segment for ntp: {} "
          "to: {}",
          _manifest->get_ntp(),
          _begin_inclusive);
    }
}

} // namespace archival
