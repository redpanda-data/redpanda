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

#include "cloud_storage/logger.h"
#include "vlog.h"

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

std::ostream& operator<<(std::ostream& o, const download_result& r) {
    switch (r) {
    case download_result::success:
        o << "{success}";
        break;
    case download_result::notfound:
        o << "{key_not_found}";
        break;
    case download_result::timedout:
        o << "{timed_out}";
        break;
    case download_result::failed:
        o << "{failed}";
        break;
    };
    return o;
}

std::ostream& operator<<(std::ostream& o, const upload_result& r) {
    switch (r) {
    case upload_result::success:
        o << "{success}";
        break;
    case upload_result::timedout:
        o << "{timed_out}";
        break;
    case upload_result::failed:
        o << "{failed}";
        break;
    case upload_result::cancelled:
        o << "{cancelled}";
        break;
    };
    return o;
}

std::ostream& operator<<(std::ostream& o, const configuration& cfg) {
    fmt::print(
      o,
      "{{connection_limit: {}, client_config: {}, metrics_disabled: {}, "
      "bucket_name: {}, cloud_credentials_source: {}}}",
      cfg.connection_limit,
      cfg.client_config,
      cfg.metrics_disabled,
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
    auto disable_metrics = net::metrics_disabled(
      config::shard_local_cfg().disable_metrics());
    auto disable_public_metrics = net::public_metrics_disabled(
      config::shard_local_cfg().disable_public_metrics());

    auto s3_conf
      = co_await cloud_storage_clients::s3_configuration::make_configuration(
        access_key,
        secret_key,
        region,
        get_default_overrides(),
        disable_metrics,
        disable_public_metrics);

    configuration cfg{
      .client_config = std::move(s3_conf),
      .connection_limit = cloud_storage::connection_limit(
        config::shard_local_cfg().cloud_storage_max_connections.value()),
      .metrics_disabled = remote_metrics_disabled(
        static_cast<bool>(disable_metrics)),
      .bucket_name = cloud_storage_clients::bucket_name(get_value_or_throw(
        config::shard_local_cfg().cloud_storage_bucket,
        "cloud_storage_bucket")),
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
    const auto shared_key = cloud_roles::private_key_str{get_value_or_throw(
      config::shard_local_cfg().cloud_storage_azure_shared_key,
      "cloud_storage_azure_shared_key")};

    const auto cloud_credentials_source
      = config::shard_local_cfg().cloud_storage_credentials_source.value();
    if (
      cloud_credentials_source
      != model::cloud_credentials_source::config_file) {
        vlog(
          cst_log.error,
          "Configuration property cloud_storage_credentials_source must be set "
          "to 'config_file' as only Shared Key Authorization is supported for "
          "now.");
        throw std::runtime_error(
          "configuration property cloud_storage_credentials_source is not "
          "equal to 'config_file'");
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
      .metrics_disabled = remote_metrics_disabled(
        static_cast<bool>(disable_metrics)),
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

} // namespace cloud_storage
