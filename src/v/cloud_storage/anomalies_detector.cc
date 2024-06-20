/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/anomalies_detector.h"

#include "cloud_storage/base_manifest.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/partition_manifest_downloader.h"
#include "cloud_storage/remote.h"

namespace cloud_storage {

anomalies_detector::anomalies_detector(
  cloud_storage_clients::bucket_name bucket,
  model::ntp ntp,
  model::initial_revision_id initial_rev,
  const remote_path_provider& path_provider,
  remote& remote,
  retry_chain_logger& logger,
  ss::abort_source& as)
  : _bucket(std::move(bucket))
  , _ntp(std::move(ntp))
  , _initial_rev(initial_rev)
  , _remote_path_provider(path_provider)
  , _remote(remote)
  , _logger(logger)
  , _as(as) {}

ss::future<anomalies_detector::result> anomalies_detector::run(
  retry_chain_node& rtc_node,
  quota_limit quota_total,
  std::optional<model::offset> scrub_from) {
    _result = result{};
    _received_quota = quota_total;

    vlog(_logger.debug, "Downloading partition manifest ...");

    partition_manifest_downloader dl(
      _bucket, _remote_path_provider, _ntp, _initial_rev, _remote);
    partition_manifest manifest(_ntp, _initial_rev);
    auto dl_result = co_await dl.download_manifest(rtc_node, &manifest);
    ++_result.ops;
    if (dl_result.has_error()) {
        vlog(_logger.debug, "Failed downloading partition manifest ...");
        _result.status = scrub_status::failed;
        co_return _result;
    }
    if (
      dl_result.value()
      == find_partition_manifest_outcome::no_matching_manifest) {
        _result.detected.missing_partition_manifest = true;
        co_return _result;
    }

    std::deque<ss::sstring> spill_manifest_paths;
    const auto& spillovers = manifest.get_spillover_map();
    for (auto iter = spillovers.begin(); iter != spillovers.end(); ++iter) {
        spillover_manifest_path_components comp{
          .base = iter->base_offset,
          .last = iter->committed_offset,
          .base_kafka = iter->base_kafka_offset(),
          .next_kafka = iter->next_kafka_offset(),
          .base_ts = iter->base_timestamp,
          .last_ts = iter->max_timestamp,
        };
        auto spill_path = _remote_path_provider.spillover_manifest_path(
          manifest, comp);
        auto exists_result = co_await _remote.segment_exists(
          _bucket, remote_segment_path{spill_path}, rtc_node);
        ++_result.ops;
        if (exists_result == download_result::notfound) {
            _result.detected.missing_spillover_manifests.emplace(comp);
        } else if (exists_result != download_result::success) {
            vlog(
              _logger.debug,
              "Failed to check existence of spillover manifest {}",
              spill_path);
            _result.status = scrub_status::partial;
        } else {
            spill_manifest_paths.emplace_front(spill_path);
        }
    }

    const auto stop_at_stm = co_await check_manifest(
      manifest, scrub_from, rtc_node);
    if (stop_at_stm == stop_detector::yes) {
        _result.status = scrub_status::partial;
        co_return _result;
    }

    std::optional<segment_meta> first_seg_previous_manifest;
    if (!manifest.empty()) {
        first_seg_previous_manifest = *manifest.begin();
    }

    for (auto const& spill_path : spill_manifest_paths) {
        if (should_stop()) {
            _result.status = scrub_status::partial;
            co_return _result;
        }

        const auto spill = co_await download_spill_manifest(
          spill_path, rtc_node);
        if (spill) {
            // Check adjacent segments which have a manifest
            // boundary between them.
            if (auto last_in_spill = spill->last_segment();
                last_in_spill && first_seg_previous_manifest) {
                scrub_segment_meta(
                  *first_seg_previous_manifest,
                  last_in_spill,
                  _result.detected.segment_metadata_anomalies);
            }

            const auto stop_at_spill = co_await check_manifest(
              *spill, scrub_from, rtc_node);
            if (stop_at_spill == stop_detector::yes) {
                _result.status = scrub_status::partial;
                co_return _result;
            }

            if (!spill->empty()) {
                first_seg_previous_manifest = *spill->begin();
            } else {
                vlog(
                  _logger.warn, "Empty spillover manifest at {}", spill_path);
            }
        } else {
            _result.status = scrub_status::partial;
            first_seg_previous_manifest = std::nullopt;
        }
    }

    _result.last_scrubbed_offset = std::nullopt;
    if (scrub_from) {
        _result.status = scrub_status::partial;
    }

    co_return _result;
}

ss::future<std::optional<spillover_manifest>>
anomalies_detector::download_spill_manifest(
  const ss::sstring& path, retry_chain_node& rtc_node) {
    vlog(_logger.debug, "Downloading spillover manifest {}", path);

    ++_result.ops;

    spillover_manifest spill{_ntp, _initial_rev};
    auto manifest_get_result = co_await _remote.download_manifest(
      _bucket,
      {manifest_format::serde, remote_manifest_path{path}},
      spill,
      rtc_node);

    if (manifest_get_result != download_result::success) {
        vlog(_logger.debug, "Failed downloading spillover manifest {}", path);

        co_return std::nullopt;
    }

    co_return spill;
}

ss::future<anomalies_detector::stop_detector>
anomalies_detector::check_manifest(
  const partition_manifest& manifest,
  std::optional<model::offset> scrub_from,
  retry_chain_node& rtc_node) {
    vlog(
      _logger.debug,
      "Checking manifest {}",
      manifest.get_manifest_path(_remote_path_provider));
    if (
      scrub_from
      && (manifest.get_start_offset() > *scrub_from || manifest.get_last_offset() == scrub_from)) {
        vlog(
          _logger.debug,
          "Manifest with offset range [{}, {}] ({}) is above the scrub "
          "starting offset ({}), so it has been scrubed already. "
          "Skipping ...",
          manifest.get_start_offset(),
          manifest.get_last_offset(),
          manifest.get_manifest_path(_remote_path_provider),
          scrub_from);

        co_return stop_detector::no;
    }

    // limit the total number (per partition) of visited segments in a run:
    // first compute how many segments we can visit with the
    // get_visitable_segments(),
    auto visitable_tail_segments = std::min(
      manifest.size(), get_visitable_segments());
    // to visit only the last #visitable_tail_segments,
    // take begin iterator and skip the first `size() - visitable_tail_segments`
    // elements.
    auto start_index = manifest.size() - visitable_tail_segments;
    auto seg_iter = std::next(manifest.begin(), ptrdiff_t(start_index));
    if (seg_iter.is_end()) {
        // can't visit any segments. this probably means that we already had
        // some progress and we terminated the quota. stop early
        vlog(
          _logger.debug,
          "Manifest with offset range [{}, {}] num segments {} ({}) skipped, "
          "not enough object quota to visit any segment. Skipping ...",
          manifest.get_start_offset(),
          manifest.get_last_offset(),
          manifest.size(),
          manifest.get_manifest_path(_remote_path_provider));
        co_return stop_detector::no;
    }
    std::optional<segment_meta> previous_seg_meta;
    if (scrub_from && manifest.get_last_offset() > scrub_from) {
        if (auto iter = manifest.segment_containing(*scrub_from);
            iter != manifest.end()) {
            previous_seg_meta = *iter;
            seg_iter = std::move(++iter);
        }
    }

    for (; seg_iter != manifest.end(); ++seg_iter) {
        if (should_stop()) {
            _result.status = scrub_status::partial;
            co_return stop_detector::yes;
        }

        const auto seg_meta = *seg_iter;

        const auto segment_path = _remote_path_provider.segment_path(
          manifest, seg_meta);
        const auto exists_result = co_await _remote.segment_exists(
          _bucket, remote_segment_path{segment_path}, rtc_node);
        _result.ops += 1;
        _result.segments_visited += 1;

        if (exists_result == download_result::notfound) {
            _result.detected.missing_segments.emplace(seg_meta);
        } else if (exists_result != download_result::success) {
            vlog(
              _logger.debug,
              "Failed to check existence of segment at {}",
              segment_path);

            _result.status = scrub_status::partial;
        }

        scrub_segment_meta(
          seg_meta,
          previous_seg_meta,
          _result.detected.segment_metadata_anomalies);
        previous_seg_meta = seg_meta;

        _result.last_scrubbed_offset = seg_meta.committed_offset;
    }

    vlog(
      _logger.debug,
      "Finished checking manifest {}",
      manifest.get_manifest_path(_remote_path_provider));
    co_return stop_detector::no;
}

size_t anomalies_detector::get_visitable_segments() const {
    if (!_result.last_scrubbed_offset.has_value()) {
        // Allow the scrubbing of one segment even if that means
        // going above the quota in order to ensure some forward
        // progress in all quota cases.
        return size_t(std::max(
          1, _received_quota.max_num_segments() - _result.segments_visited));
    }
    return size_t(std::max(
      0, _received_quota.max_num_segments() - _result.segments_visited));
}

bool anomalies_detector::should_stop() const {
    if (_as.abort_requested()) {
        return true;
    }

    if (
      archival::run_quota_t{_result.ops} > _received_quota.max_num_operations
      || segment_depth_t{_result.segments_visited}
           >= _received_quota.max_num_segments) {
        // Allow the scrubbing of one segment even if that means
        // going above the quota in order to ensure some forward
        // progress in all quota cases.
        return _result.last_scrubbed_offset.has_value();
    }

    return false;
}

anomalies_detector::result&
anomalies_detector::result::operator+=(anomalies_detector::result&& other) {
    if (
      status == scrub_status::failed || other.status == scrub_status::failed) {
        status = scrub_status::failed;
    } else if (
      status == scrub_status::partial
      || other.status == scrub_status::partial) {
        status = scrub_status::partial;
    } else {
        status = scrub_status::full;
    }

    ops += other.ops;
    segments_visited += other.segments_visited;
    last_scrubbed_offset = other.last_scrubbed_offset;
    detected += std::move(other.detected);

    return *this;
}

} // namespace cloud_storage
