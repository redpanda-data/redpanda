/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/configuration.h"

#include "base/vlog.h"
#include "cloud_storage/logger.h"
#include "config/configuration.h"
#include "config/node_config.h"

namespace cloud_storage {

namespace {
cloud_storage_clients::default_overrides get_default_overrides() {
    // Set default overrides
    cloud_storage_clients::default_overrides overrides;
    overrides.max_idle_time
      = config::shard_local_cfg()
          .cloud_storage_max_connection_idle_time_ms.value();
    if (
      auto optep = config::shard_local_cfg().cloud_storage_api_endpoint.value();
      optep.has_value()) {
        overrides.endpoint = cloud_storage_clients::endpoint_url(*optep);
    }
    overrides.disable_tls = config::shard_local_cfg().cloud_storage_disable_tls;
    if (
      auto cert = config::shard_local_cfg().cloud_storage_trust_file.value();
      cert.has_value()) {
        overrides.trust_file = cloud_storage_clients::ca_trust_file(
          std::filesystem::path(*cert));
    }
    overrides.port = config::shard_local_cfg().cloud_storage_api_endpoint_port;

    return overrides;
}

} // namespace

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
        cloud_credentials_source,
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
        cloud_credentials_source,
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

}; // namespace cloud_storage
