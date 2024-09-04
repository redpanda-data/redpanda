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

#include "base/vlog.h"
#include "cloud_storage/configuration.h"
#include "cloud_storage/logger.h"
#include "config/node_config.h"
#include "config/types.h"

#include <absl/container/node_hash_set.h>

namespace {
cloud_storage_clients::default_overrides get_default_overrides() {
    // Set default overrides
    cloud_storage_clients::default_overrides overrides;
    overrides.max_idle_time
      = config::shard_local_cfg()
          .cloud_storage_max_connection_idle_time_ms.value();
    if (auto optep
        = config::shard_local_cfg().cloud_storage_api_endpoint.value();
        optep.has_value()) {
        overrides.endpoint = cloud_storage_clients::endpoint_url(*optep);
    }
    overrides.disable_tls = config::shard_local_cfg().cloud_storage_disable_tls;
    if (auto cert = config::shard_local_cfg().cloud_storage_trust_file.value();
        cert.has_value()) {
        overrides.trust_file = cloud_storage_clients::ca_trust_file(
          std::filesystem::path(*cert));
    }
    overrides.port = config::shard_local_cfg().cloud_storage_api_endpoint_port;

    return overrides;
}

} // namespace

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

std::ostream& operator<<(std::ostream& o, const configuration& cfg) {
    fmt::print(
      o,
      "{{connection_limit: {}, client_config: {}, "
      "bucket_name: {}, cloud_credentials_source: {}}}",
      cfg.connection_limit,
      cfg.client_config,
      cfg.bucket_name,
      cfg.cloud_credentials_source);
    return o;
}

static ss::sstring get_value_or_throw(
  const config::property<std::optional<ss::sstring>>& prop, const char* name) {
    auto opt = prop.value();
    if (!opt) {
        vlog(
          cst_log.error,
          "Configuration property {} is required to enable archival storage",
          name);
        throw std::runtime_error(
          fmt::format("configuration property {} is not set", name));
    }
    return *opt;
}

ss::future<configuration> configuration::get_config() {
    if (config::shard_local_cfg().cloud_storage_azure_storage_account()) {
        co_return co_await get_abs_config();
    } else {
        co_return co_await get_s3_config();
    }
}

ss::future<configuration> configuration::get_s3_config() {
    vlog(cst_log.debug, "Generating S3 cloud storage configuration");

    auto cloud_credentials_source
      = config::shard_local_cfg().cloud_storage_credentials_source.value();

    std::optional<cloud_roles::private_key_str> secret_key;
    std::optional<cloud_roles::public_key_str> access_key;

    // If the credentials are sourced from config file, the keys must be present
    // in the file. If the credentials are sourced from infrastructure APIs, the
    // keys must be absent in the file.
    // TODO (abhijat) validate and fail if source is not static file and the
    //  keys are still supplied with the config.
    if (
      cloud_credentials_source
      == model::cloud_credentials_source::config_file) {
        secret_key = cloud_roles::private_key_str(get_value_or_throw(
          config::shard_local_cfg().cloud_storage_secret_key,
          "cloud_storage_secret_key"));
        access_key = cloud_roles::public_key_str(get_value_or_throw(
          config::shard_local_cfg().cloud_storage_access_key,
          "cloud_storage_access_key"));
    }

    auto region = cloud_roles::aws_region_name(get_value_or_throw(
      config::shard_local_cfg().cloud_storage_region, "cloud_storage_region"));
    auto url_style = config::shard_local_cfg().cloud_storage_url_style.value();

    auto disable_metrics = net::metrics_disabled(
      config::shard_local_cfg().disable_metrics());
    auto disable_public_metrics = net::public_metrics_disabled(
      config::shard_local_cfg().disable_public_metrics());
    auto bucket_name = cloud_storage_clients::bucket_name(get_value_or_throw(
      config::shard_local_cfg().cloud_storage_bucket, "cloud_storage_bucket"));

    auto s3_conf
      = co_await cloud_storage_clients::s3_configuration::make_configuration(
        access_key,
        secret_key,
        region,
        bucket_name,
        cloud_storage_clients::from_config(url_style),
        config::fips_mode_enabled(config::node().fips_mode.value()),
        get_default_overrides(),
        disable_metrics,
        disable_public_metrics);

    configuration cfg{
      .client_config = std::move(s3_conf),
      .connection_limit = cloud_storage::connection_limit(
        config::shard_local_cfg().cloud_storage_max_connections.value()),
      .bucket_name = bucket_name,
      .cloud_credentials_source = cloud_credentials_source,
    };

    vlog(cst_log.debug, "S3 cloud storage configuration generated: {}", cfg);
    co_return cfg;
}

ss::future<configuration> configuration::get_abs_config() {
    vlog(cst_log.debug, "Generating ABS cloud storage configuration");

    const auto storage_account = cloud_roles::storage_account{
      get_value_or_throw(
        config::shard_local_cfg().cloud_storage_azure_storage_account,
        "cloud_storage_azure_storage_account")};
    const auto container = get_value_or_throw(
      config::shard_local_cfg().cloud_storage_azure_container,
      "cloud_storage_azure_container");
    const auto shared_key =
      []() -> std::optional<cloud_roles::private_key_str> {
        auto opt
          = config::shard_local_cfg().cloud_storage_azure_shared_key.value();
        if (opt.has_value()) {
            return cloud_roles::private_key_str(opt.value());
        }
        return std::nullopt;
    }();

    const auto cloud_credentials_source
      = config::shard_local_cfg().cloud_storage_credentials_source.value();
    using enum model::cloud_credentials_source;
    if (!(cloud_credentials_source == config_file
          || cloud_credentials_source == azure_aks_oidc_federation
          || cloud_credentials_source == azure_vm_instance_metadata)) {
        vlog(
          cst_log.error,
          "Configuration property cloud_storage_credentials_source must be set "
          "to 'config_file' or 'azure_aks_oidc_federation' or "
          "'azure_vm_instance_metadata'");
        throw std::runtime_error(
          "configuration property cloud_storage_credentials_source is not set "
          "to a value allowed for ABS");
    }

    auto disable_metrics = net::metrics_disabled(
      config::shard_local_cfg().disable_metrics());
    auto disable_public_metrics = net::public_metrics_disabled(
      config::shard_local_cfg().disable_public_metrics());

    auto abs_conf
      = co_await cloud_storage_clients::abs_configuration::make_configuration(
        shared_key,
        storage_account,
        get_default_overrides(),
        disable_metrics,
        disable_public_metrics);

    configuration cfg{
      .client_config = std::move(abs_conf),
      .connection_limit = cloud_storage::connection_limit(
        config::shard_local_cfg().cloud_storage_max_connections.value()),
      .bucket_name = cloud_storage_clients::bucket_name(get_value_or_throw(
        config::shard_local_cfg().cloud_storage_azure_container,
        "cloud_storage_azure_container")),
      .cloud_credentials_source = cloud_credentials_source,
    };

    vlog(cst_log.debug, "ABS cloud storage configuration generated: {}", cfg);
    co_return cfg;
}

const config::property<std::optional<ss::sstring>>&
configuration::get_bucket_config() {
    if (config::shard_local_cfg().cloud_storage_azure_storage_account()) {
        return config::shard_local_cfg().cloud_storage_azure_container;
    } else {
        return config::shard_local_cfg().cloud_storage_bucket;
    }
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
