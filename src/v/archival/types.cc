/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/types.h"

#include "archival/logger.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/types.h"
#include "config/configuration.h"

#include <fmt/chrono.h>
#include <fmt/format.h>

namespace archival {

std::ostream&
operator<<(std::ostream& o, const std::optional<segment_time_limit>& tl) {
    if (tl) {
        fmt::print(
          o,
          "{}",
          std::chrono::duration_cast<std::chrono::milliseconds>(tl.value()()));
    } else {
        fmt::print(o, "N/A");
    }
    return o;
}

std::ostream& operator<<(std::ostream& o, const configuration& cfg) {
    fmt::print(
      o,
      "{{bucket_name: {}, initial_backoff: {}, "
      "segment_upload_timeout: {}, "
      "manifest_upload_timeout: {}, time_limit: {}}}",
      cfg.bucket_name,
      std::chrono::duration_cast<std::chrono::milliseconds>(
        cfg.cloud_storage_initial_backoff),
      std::chrono::duration_cast<std::chrono::milliseconds>(
        cfg.segment_upload_timeout),
      std::chrono::duration_cast<std::chrono::milliseconds>(
        cfg.manifest_upload_timeout),
      cfg.time_limit);
    return o;
}

static ss::sstring get_value_or_throw(
  const config::property<std::optional<ss::sstring>>& prop,
  std::string_view name) {
    auto opt = prop.value();
    if (!opt) {
        vlog(
          archival_log.error,
          "Configuration property {} is required to enable archival storage",
          name);
        throw std::runtime_error(
          fmt::format("configuration property {} is not set", name));
    }
    return *opt;
}

archival::configuration
get_archival_service_config(ss::scheduling_group sg, ss::io_priority_class p) {
    vlog(archival_log.debug, "Generating archival configuration");
    auto disable_metrics = net::metrics_disabled(
      config::shard_local_cfg().disable_metrics());

    auto time_limit = config::shard_local_cfg()
                        .cloud_storage_segment_max_upload_interval_sec.value();
    if (time_limit and time_limit.value() == 0s) {
        vlog(
          archival_log.error,
          "Configuration property "
          "cloud_storage_segment_max_upload_interval_sec can't be 0");
        throw std::runtime_error(
          "cloud_storage_segment_max_upload_interval_sec is invalid");
    }
    auto time_limit_opt = time_limit ? std::make_optional(
                            segment_time_limit(*time_limit))
                                     : std::nullopt;

    const auto& bucket_config
      = cloud_storage::configuration::get_bucket_config();
    archival::configuration cfg{
      .bucket_name = cloud_storage_clients::bucket_name(
        get_value_or_throw(bucket_config, bucket_config.name())),
      .cloud_storage_initial_backoff
      = config::shard_local_cfg().cloud_storage_initial_backoff_ms.value(),
      .segment_upload_timeout
      = config::shard_local_cfg()
          .cloud_storage_segment_upload_timeout_ms.value(),
      .manifest_upload_timeout
      = config::shard_local_cfg()
          .cloud_storage_manifest_upload_timeout_ms.value(),
      .upload_loop_initial_backoff
      = config::shard_local_cfg()
          .cloud_storage_upload_loop_initial_backoff_ms.value(),
      .upload_loop_max_backoff
      = config::shard_local_cfg()
          .cloud_storage_upload_loop_max_backoff_ms.value(),
      .svc_metrics_disabled = service_metrics_disabled(
        static_cast<bool>(disable_metrics)),
      .ntp_metrics_disabled = per_ntp_metrics_disabled(
        static_cast<bool>(disable_metrics)),
      .time_limit = time_limit_opt,
      .upload_scheduling_group = sg,
      .upload_io_priority = p};
    vlog(archival_log.debug, "Archival configuration generated: {}", cfg);
    return cfg;
}

bool adjacent_segment_run::maybe_add_segment(
  const cloud_storage::segment_meta& s, size_t max_size) {
    vlog(
      archival_log.debug,
      "{} Segments collected, looking at segment meta: {}, current run meta: "
      "{}",
      num_segments,
      s,
      meta);
    if (num_segments == 1 && meta.size_bytes + s.size_bytes > max_size) {
        // Corner case, we hit a segment which is smaller than the max_size
        // but it's larger than max_size when combined with its neighbor. In
        // this case we need to skip previous the segment.
        num_segments = 0;
        segments.clear();
        meta = {};
    }
    if (num_segments == 0) {
        // Find the begining of the small segment
        // run.
        if (s.size_bytes < max_size) {
            meta = s;
            num_segments = 1;
            segments.push_back(
              cloud_storage::partition_manifest::generate_remote_segment_path(
                ntp, s));
        }
    } else {
        if (meta.size_bytes + s.size_bytes <= max_size) {
            if (model::next_offset(meta.committed_offset) != s.base_offset) {
                // In case if we're dealing with one of the old manifests with
                // inconsistencies (overlapping offsets, etc).
                num_segments = 0;
                meta = {};
                segments.clear();
                vlog(
                  archival_log.debug,
                  "Reseting the upload, current committed offset: {}, next "
                  "base offset: {}, meta: {}",
                  meta.committed_offset,
                  s.base_offset,
                  meta);
                return false;
            }
            // Move the end of the small segment run forward
            meta.committed_offset = s.committed_offset;
            meta.max_timestamp = s.max_timestamp;
            num_segments++;
            meta.size_bytes += s.size_bytes;
            segments.push_back(
              cloud_storage::partition_manifest::generate_remote_segment_path(
                ntp, s));
        } else {
            return num_segments > 1;
        }
    }
    return false;
}

std::ostream& operator<<(std::ostream& os, const adjacent_segment_run& run) {
    std::vector<ss::sstring> names;
    names.reserve(run.segments.size());
    std::transform(
      run.segments.begin(),
      run.segments.end(),
      std::back_inserter(names),
      [](const cloud_storage::remote_segment_path& rsp) {
          return rsp().native();
      });
    fmt::print(
      os,
      "{{meta: {}, num_segments: {}, segments: {}}}",
      run.meta,
      run.num_segments,
      names);
    return os;
}

} // namespace archival
