/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/archival/types.h"

#include "cloud_storage/configuration.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/types.h"
#include "cluster/archival/logger.h"
#include "config/configuration.h"
#include "net/types.h"

#include <fmt/chrono.h>
#include <fmt/format.h>

#include <chrono>

using namespace std::chrono_literals;

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
      cfg.cloud_storage_initial_backoff(),
      cfg.segment_upload_timeout(),
      cfg.manifest_upload_timeout(),
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
      = config::shard_local_cfg().cloud_storage_initial_backoff_ms.bind(),
      .segment_upload_timeout
      = config::shard_local_cfg()
          .cloud_storage_segment_upload_timeout_ms.bind(),
      .manifest_upload_timeout
      = config::shard_local_cfg()
          .cloud_storage_manifest_upload_timeout_ms.bind(),
      .garbage_collect_timeout
      = config::shard_local_cfg()
          .cloud_storage_garbage_collect_timeout_ms.bind(),
      .upload_loop_initial_backoff
      = config::shard_local_cfg()
          .cloud_storage_upload_loop_initial_backoff_ms.bind(),
      .upload_loop_max_backoff
      = config::shard_local_cfg()
          .cloud_storage_upload_loop_max_backoff_ms.bind(),
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

} // namespace archival
