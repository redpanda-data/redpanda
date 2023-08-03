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
#include "storage/offset_to_filepos.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>

namespace archival {
segment_collector::segment_collector(
  model::offset begin_inclusive,
  const cloud_storage::partition_manifest& manifest,
  const storage::log& log,
  size_t max_uploaded_segment_size,
  std::optional<model::offset> end_inclusive)
  : _begin_inclusive(begin_inclusive)
  , _manifest(manifest)
  , _log(log)
  , _max_uploaded_segment_size(max_uploaded_segment_size)
  , _target_end_inclusive(end_inclusive)
  , _collected_size(0) {}

void segment_collector::collect_segments(segment_collector_mode mode) {
    if (_manifest.size() == 0) {
        vlog(
          archival_log.debug,
          "No segments to collect for ntp {}, manifest empty",
          _manifest.get_ntp());
        return;
    }

    // start_offset < log start due to eviction of segments before
    // they could be uploaded, skip forward to log start.
    if (_begin_inclusive < _log.offsets().start_offset) {
        if (mode == segment_collector_mode::collect_compacted) {
            vlog(
              archival_log.debug,
              "Provided start offset is below the start offset of the local "
              "log: "
              "{} < {} for ntp {}. Advancing to the beginning of the local "
              "log.",
              _begin_inclusive,
              _log.offsets().start_offset,
              _manifest.get_ntp());
            _begin_inclusive = _log.offsets().start_offset;
        } else {
            vlog(
              archival_log.debug,
              "Provided start offset is below the start offset of the local "
              "log: "
              "{} < {} for ntp {}. Exiting early.",
              _begin_inclusive,
              _log.offsets().start_offset,
              _manifest.get_ntp());
            return;
        }
    }

    if (mode == segment_collector_mode::collect_compacted) {
        align_begin_offset_to_manifest();
    } else if (_manifest.find(_begin_inclusive) == _manifest.end()) {
        vlog(
          archival_log.debug,
          "Provided start offset {} is not aligned to a segment in the "
          "manifest: "
          "for ntp {}. Exiting early.",
          _begin_inclusive,
          _manifest.get_ntp());
        return;
    }

    if (_begin_inclusive >= _manifest.get_last_offset()) {
        vlog(
          archival_log.debug,
          "Start offset {} is ahead of manifest last offset {} for ntp {}",
          _begin_inclusive,
          _manifest.get_last_offset(),
          _manifest.get_ntp());
        return;
    }
    if (_target_end_inclusive.has_value()) {
        if (_target_end_inclusive.value() < _log.offsets().start_offset) {
            vlog(
              archival_log.debug,
              "Provided end offset is below the start offset of the local log: "
              "{} < {} for ntp {}. Advancing to the beginning of the local "
              "log.",
              _target_end_inclusive.value(),
              _log.offsets().start_offset,
              _manifest.get_ntp());
            return;
        }
        if (_target_end_inclusive.value() > _manifest.get_last_offset()) {
            vlog(
              archival_log.debug,
              "Target end offset {} is ahead of manifest last offset {} for "
              "ntp {}",
              _target_end_inclusive.value(),
              _manifest.get_last_offset(),
              _manifest.get_ntp());
            return;
        }
    }

    do_collect(mode);
}

segment_collector::segment_seq segment_collector::segments() {
    return _segments;
}

void segment_collector::do_collect(segment_collector_mode mode) {
    auto replace_boundary = _target_end_inclusive.value_or(
      find_replacement_boundary());
    auto start = _begin_inclusive;
    model::offset current_segment_end{0};
    bool done = false;
    while (!done && current_segment_end < _manifest.get_last_offset()) {
        auto result = find_next_segment(start, mode);
        if (result.segment.get() == nullptr) {
            break;
        }

        if (unlikely(!_ntp_cfg)) {
            _ntp_cfg = result.ntp_conf;
        }

        auto segment_size = result.segment->size_bytes();
        if (
          _target_end_inclusive.has_value()
          && result.segment->offsets().committed_offset
               >= _target_end_inclusive.value()) {
            // In this case the collected size may overflow
            // _max_uploaded_segment_size a bit so we could actually find
            // _target_end_inclusive inside the last segment.
            vlog(
              archival_log.debug,
              "Segment collect for ntp {} stopping collection, total size: {} "
              "reached target end offset: {}, current collected size: {}",
              _manifest.get_ntp(),
              _collected_size + segment_size,
              _target_end_inclusive.value(),
              _collected_size);
            // Current segment has to be added to the list of results
            done = true;
        } else if (
          _collected_size + segment_size > _max_uploaded_segment_size) {
            vlog(
              archival_log.debug,
              "Segment collect for ntp {} stopping collection, total "
              "size: {} will overflow max allowed upload size: {}, current "
              "collected size: {}",
              _manifest.get_ntp(),
              _collected_size + segment_size,
              _max_uploaded_segment_size,
              _collected_size);
            break;
        }

        // For the first segment found, begin offset needs to be
        // re-aligned if it falls inside manifest segment.
        if (
          _segments.empty()
          && mode == segment_collector_mode::collect_compacted) {
            _begin_inclusive = result.segment->offsets().base_offset;
            align_begin_offset_to_manifest();
        }
        _segments.push_back(result.segment);
        current_segment_end = result.segment->offsets().committed_offset;
        start = current_segment_end + model::offset{1};
        _collected_size += segment_size;
    }

    if (current_segment_end >= replace_boundary) {
        _can_replace_manifest_segment = true;
    }

    align_end_offset_to_manifest(
      _target_end_inclusive.value_or(current_segment_end));
}

model::offset segment_collector::find_replacement_boundary() const {
    auto it = _manifest.segment_containing(_begin_inclusive);

    // Crossing this boundary means that the collection can replace at least one
    // segment in manifest.
    model::offset replace_boundary;

    // manifest: 10-19, 25-29
    // _begin_inclusive (in gap): 22.
    if (it == _manifest.end()) {
        // first segment after gap: 25-29
        for (it = _manifest.begin(); it != _manifest.end(); ++it) {
            const auto& entry = *it;
            if (entry.base_offset > _begin_inclusive) {
                break;
            }
        }
        // The collection is valid if it can reach the end of the gap: 24
        vassert(it != _manifest.end(), "Trying to dereference end iterator");
        replace_boundary = it->base_offset - model::offset{1};
    } else {
        replace_boundary = it->committed_offset;
    }

    return replace_boundary;
}

void segment_collector::align_end_offset_to_manifest(
  model::offset segment_end) {
    if (segment_end == _manifest.get_last_offset()) {
        _end_inclusive = _manifest.get_last_offset();
    } else if (segment_end > _manifest.get_last_offset()) {
        vlog(
          archival_log.debug,
          "Segment collect for ntp {} offset {} advanced "
          "ahead of manifest, clamping to {}",
          _manifest.get_ntp(),
          segment_end,
          _manifest.get_last_offset());
        _end_inclusive = _manifest.get_last_offset();
    } else {
        // Align the end offset to the nearest segment ending in manifest.
        auto it = _manifest.segment_containing(segment_end);
        if (it == _manifest.end()) {
            // segment_end is in a gap in the manifest.
            if (segment_end >= _manifest.get_start_offset().value()) {
                vlog(
                  archival_log.debug,
                  "Segment collect for ntp {}: collection ended at "
                  "gap in manifest: {}",
                  _manifest.get_ntp(),
                  segment_end);

                // try to fill the manifest gap with the data locally available.
                _end_inclusive = segment_end;
            }
            return;
        }

        // If the segment end is not aligned to manifest segment, then
        // pull back to the end of the previous segment.
        if (it->committed_offset == segment_end) {
            _end_inclusive = segment_end;
        } else {
            _end_inclusive = it->base_offset - model::offset{1};
        }
    }
}

segment_collector::lookup_result segment_collector::find_next_segment(
  model::offset start_offset, segment_collector_mode mode) {
    // 'start_offset' should always be above the start offset of the local log
    // as we skip to it in the calling code (`collect_segments`).
    if (start_offset < _log.offsets().start_offset) {
        vlog(
          archival_log.warn,
          "Finding next segment for {}: can't find segments below the local "
          "log start offset ({} < {})",
          _manifest.get_ntp(),
          start_offset,
          _log.offsets().start_offset);
        return {};
    }

    const auto& segment_set = _log.segments();
    auto it = segment_set.lower_bound(start_offset);
    if (it == segment_set.end()) {
        vlog(
          archival_log.debug,
          "Finding next segment for {}: can't find segment after "
          "offset: {}",
          _manifest.get_ntp(),
          start_offset);
        return {};
    }

    const auto& segment = *it;
    auto segment_is_compacted = segment->finished_self_compaction();
    auto compacted_segment_expected
      = mode == segment_collector_mode::collect_compacted;
    if (segment_is_compacted == compacted_segment_expected) {
        vlog(
          archival_log.trace,
          "Found segment for ntp {}: {}",
          _manifest.get_ntp(),
          segment);
        return {.segment = segment, .ntp_conf = &_log.config()};
    }
    vlog(
      archival_log.debug,
      "Finding next segment for {}: no "
      "segments after offset: {}",
      _manifest.get_ntp(),
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

bool segment_collector::should_replace_manifest_segment() const {
    const bool valid_collection = _can_replace_manifest_segment
                                  && _begin_inclusive < _end_inclusive;

    // If we have selected only one segment for collection, ensure
    // that its compacted size is smaller than the size associated
    // with the selected segment in the manifest. This guards against,
    // name clashing between the existing segment and the re-uploaded segment.
    if (valid_collection && _segments.size() == 1) {
        auto segment_to_replace = _manifest.segment_containing(
          _begin_inclusive);

        // This branch is *not* taken if the begin and/or end offsets of the
        // collected range are in a manifest gap. In that scenario, a name
        // clash is not possible.
        if (
          segment_to_replace != _manifest.end()
          && _begin_inclusive == segment_to_replace->base_offset
          && _end_inclusive == segment_to_replace->committed_offset) {
            const bool should = _collected_size
                                < segment_to_replace->size_bytes;

            if (!should) {
                vlog(
                  archival_log.debug,
                  "Skipping re-upload of compacted segment as its size has "
                  "not decreased as a result of self-compaction: {}",
                  *(_segments.front()));
            }

            return should;
        }
    }

    return valid_collection;
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
    if (_begin_inclusive >= _manifest.get_last_offset()) {
        return;
    }

    if (_begin_inclusive < _manifest.get_start_offset().value()) {
        vlog(
          archival_log.debug,
          "_begin_inclusive is behind manifest for ntp: {}, skipping forward "
          "to "
          "start of manifest from: {} to: {}",
          _manifest.get_ntp(),
          _begin_inclusive,
          _manifest.get_start_offset().value());

        // manifest: 10-40
        // _begin_inclusive: before: 5, after: 10
        _begin_inclusive = _manifest.get_start_offset().value();
        return;
    }

    auto it = _manifest.find(_begin_inclusive);

    // If iterator points to a segment, it means that _begin_inclusive is
    // aligned on manifest segment boundary, so do nothing. Otherwise, skip
    // _begin_inclusive to the start of the next manifest segment.
    if (it == _manifest.end()) {
        it = _manifest.segment_containing(_begin_inclusive);

        // manifest: 10-19, 25-29
        // _begin_inclusive (in gap): before: 22, after: 22
        if (it == _manifest.end()) {
            vlog(
              archival_log.debug,
              "_begin_inclusive lies in manifest gap for ntp: {} "
              "value: {}",
              _manifest.get_ntp(),
              _begin_inclusive);
            return;
        }

        // manifest: 10-19, 20-29
        // _begin_inclusive: before: 15, after: 20 OR
        // _begin_inclusive: before: 25, after: 30
        _begin_inclusive = it->committed_offset + model::offset{1};
        vlog(
          archival_log.debug,
          "_begin_inclusive skipped to start of next segment for ntp: {} "
          "to: {}",
          _manifest.get_ntp(),
          _begin_inclusive);
    }
}

ss::future<upload_candidate_with_locks>
segment_collector::make_upload_candidate(
  ss::io_priority_class io_priority_class,
  ss::lowres_clock::duration segment_lock_duration) {
    if (_segments.empty()) {
        vlog(
          archival_log.debug,
          "No segments to reupload for {}",
          _manifest.get_ntp());
        co_return upload_candidate_with_locks{upload_candidate{}, {}};
    } else {
        if (archival_log.is_enabled(ss::log_level::debug)) {
            std::stringstream seg;
            for (const auto& s : _segments) {
                fmt::print(
                  seg,
                  "{}-{}/{}; ",
                  s->offsets().base_offset,
                  s->offsets().committed_offset,
                  s->size_bytes());
            }
            vlog(archival_log.debug, "Collected segments: {}", seg.str());
        }
    }

    auto last = _segments.back();
    auto last_size_bytes = last->size_bytes();

    // Take the locks before opening any readers on the segments.
    auto deadline = std::chrono::steady_clock::now() + segment_lock_duration;
    std::vector<ss::future<ss::rwlock::holder>> locks;
    locks.reserve(_segments.size());
    std::transform(
      _segments.begin(),
      _segments.end(),
      std::back_inserter(locks),
      [&deadline](auto& seg) { return seg->read_lock(deadline); });

    auto locks_resolved = co_await ss::when_all_succeed(
      locks.begin(), locks.end());

    auto first = _segments.front();
    auto head_seek_result = co_await storage::convert_begin_offset_to_file_pos(
      _begin_inclusive,
      first,
      first->index().base_timestamp(),
      io_priority_class);

    if (head_seek_result.has_error()) {
        co_return upload_candidate_with_locks{upload_candidate{}, {}};
    }

    auto tail_seek_result = co_await storage::convert_end_offset_to_file_pos(
      _end_inclusive, last, last->index().max_timestamp(), io_priority_class);

    if (tail_seek_result.has_error()) {
        co_return upload_candidate_with_locks{upload_candidate{}, {}};
    }

    auto head_seek = head_seek_result.value();
    auto tail_seek = tail_seek_result.value();

    if (head_seek.offset_inside_batch || tail_seek.offset_inside_batch) {
        vlog(
          archival_log.warn,
          "The upload candidate boundaries lie inside batch, skipping upload. "
          "begin inclusive: {}, is inside batch: {}, seek result: {}, end "
          "inclusive: {}, is "
          "inside batch: {}, seek result: {}",
          _begin_inclusive,
          head_seek.offset_inside_batch,
          head_seek.offset,
          _end_inclusive,
          tail_seek.offset_inside_batch,
          tail_seek.offset);
        co_return upload_candidate_with_locks{
          upload_candidate{
            .exposed_name = {},
            .starting_offset = _begin_inclusive,
            .file_offset = 0,
            .content_length = 0,
            .final_offset = _end_inclusive,
            .final_file_offset = 0,
            .base_timestamp = {},
            .max_timestamp = {},
            .term = {},
            .sources = {},
          },
          {}};
    }

    vlog(
      archival_log.debug,
      "collected size: {}, last segment {}-{}/{}, head seek bytes: {}, tail "
      "seek bytes: {}",
      _collected_size,
      last->offsets().base_offset,
      last->offsets().committed_offset,
      last_size_bytes,
      head_seek.bytes,
      tail_seek.bytes);

    size_t content_length = _collected_size
                            - (head_seek.bytes + last_size_bytes);
    content_length += tail_seek.bytes;

    auto starting_offset = head_seek.offset;
    if (starting_offset != _begin_inclusive) {
        vlog(
          archival_log.debug,
          "adjusting begin offset of upload candidate from {} to {}",
          starting_offset,
          _begin_inclusive);
        starting_offset = _begin_inclusive;
    }

    auto final_offset = tail_seek.offset;
    if (final_offset != _end_inclusive) {
        vlog(
          archival_log.debug,
          "adjusting end offset of upload candidate from {} to {}",
          final_offset,
          _end_inclusive);
        final_offset = _end_inclusive;
    }

    co_return upload_candidate_with_locks{
      upload_candidate{
        .exposed_name = adjust_segment_name(),
        .starting_offset = starting_offset,
        .file_offset = head_seek.bytes,
        .content_length = content_length,
        .final_offset = final_offset,
        .final_file_offset = tail_seek.bytes,
        .base_timestamp = head_seek.ts,
        .max_timestamp = tail_seek.ts,
        .term = first->offsets().term,
        .sources = _segments,
      },
      std::move(locks_resolved)};
}

size_t segment_collector::collected_size() const { return _collected_size; }

} // namespace archival
