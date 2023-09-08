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
#include "cloud_storage/remote.h"
#include "cloud_storage/spillover_manifest.h"

namespace cloud_storage {

anomalies_detector::anomalies_detector(
  cloud_storage_clients::bucket_name bucket,
  model::ntp ntp,
  model::initial_revision_id initial_rev,
  remote& remote,
  retry_chain_logger& logger,
  ss::abort_source& as)
  : _bucket(std::move(bucket))
  , _ntp(std::move(ntp))
  , _initial_rev(initial_rev)
  , _remote(remote)
  , _logger(logger)
  , _as(as) {}

ss::future<anomalies_detector::result>
anomalies_detector::run(retry_chain_node& rtc_node) {
    anomalies detected{};
    scrub_status status{scrub_status::full};
    size_t ops = 0;

    vlog(_logger.debug, "Downloading partition manifest ...");

    partition_manifest manifest(_ntp, _initial_rev);
    auto [dl_result, format] = co_await _remote.try_download_partition_manifest(
      _bucket, manifest, rtc_node);
    ++ops;

    if (dl_result == download_result::notfound) {
        detected.missing_partition_manifest = true;
        co_return result{
          .status = status, .detected = std::move(detected), .ops = ops};
    } else if (dl_result != download_result::success) {
        vlog(_logger.debug, "Failed downloading partition manifest ...");
        co_return result{.status = scrub_status::failed, .ops = ops};
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

        auto spill_path = generate_spillover_manifest_path(
          _ntp, _initial_rev, comp);
        auto exists_result = co_await _remote.segment_exists(
          _bucket, remote_segment_path{spill_path()}, rtc_node);
        ++ops;
        if (exists_result == download_result::notfound) {
            detected.missing_spillover_manifests.emplace(comp);
        } else if (dl_result != download_result::success) {
            vlog(
              _logger.debug,
              "Failed to check existence of spillover manifest {}",
              spill_path());
            status = scrub_status::partial;
        } else {
            spill_manifest_paths.emplace_front(spill_path());
        }
    }

    // Binary manifest encoding and spillover manifests were both added
    // in the same release. Hence, it's an anomaly to have a JSON
    // encoded manifest and spillover manifests.
    if (format == manifest_format::json && spill_manifest_paths.size() > 0) {
        detected.missing_partition_manifest = true;
    }

    result final_res{
      .status = status, .detected = std::move(detected), .ops = ops};

    auto stm_manifest_check_res = co_await check_manifest(manifest, rtc_node);
    final_res += std::move(stm_manifest_check_res);

    for (auto iter = spill_manifest_paths.begin();
         iter != spill_manifest_paths.end();
         ++iter) {
        if (_as.abort_requested()) {
            final_res.status = scrub_status::partial;
            co_return final_res;
        }

        auto manifest_res = co_await download_and_check_spill_manifest(
          *iter, rtc_node);
        final_res += std::move(manifest_res);
    }

    co_return final_res;
}

ss::future<anomalies_detector::result>
anomalies_detector::download_and_check_spill_manifest(
  const ss::sstring& path, retry_chain_node& rtc_node) {
    result res{};

    vlog(_logger.debug, "Downloading spillover manifest {}", path);

    spillover_manifest spill{_ntp, _initial_rev};
    auto manifest_get_result = co_await _remote.download_manifest(
      _bucket,
      {manifest_format::serde, remote_manifest_path{path}},
      spill,
      rtc_node);
    res.ops += 1;

    if (manifest_get_result != download_result::success) {
        vlog(_logger.debug, "Failed downloading spillover manifest {}", path);

        res.status = scrub_status::partial;
        co_return res;
    }

    res += co_await check_manifest(spill, rtc_node);

    co_return res;
}

ss::future<anomalies_detector::result> anomalies_detector::check_manifest(
  const partition_manifest& manifest, retry_chain_node& rtc_node) {
    result res{};

    vlog(_logger.debug, "Checking manifest {}", manifest.get_manifest_path());

    for (auto seg_iter = manifest.begin(); seg_iter != manifest.end();
         ++seg_iter) {
        if (_as.abort_requested()) {
            res.status = scrub_status::partial;
            co_return res;
        }

        auto segment_path = manifest.generate_segment_path(*seg_iter);
        auto exists_result = co_await _remote.segment_exists(
          _bucket, segment_path, rtc_node);
        res.ops += 1;

        if (exists_result == download_result::notfound) {
            res.detected.missing_segments.emplace(*seg_iter);
        } else if (exists_result != download_result::success) {
            vlog(
              _logger.debug,
              "Failed to check existence of segment at {}",
              segment_path());

            res.status = scrub_status::partial;
        }
    }

    vlog(
      _logger.debug,
      "Finished checking manifest {}",
      manifest.get_manifest_path());

    co_return res;
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
    detected += std::move(other.detected);

    return *this;
}

} // namespace cloud_storage
