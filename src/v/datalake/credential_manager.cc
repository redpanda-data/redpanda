/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/credential_manager.h"

#include "cloud_roles/types.h"
#include "cloud_storage_clients/configuration.h"
#include "cloud_storage_clients/types.h"
#include "config/configuration.h"
#include "datalake/logger.h"
#include "hashing/secure.h"
#include "net/types.h"

namespace datalake {

namespace {

// Build the client configuration for refreshing AWS credentials for Iceberg.
// Uses iceberg-specific configuration if available, otherwise falls back
// to cloud storage configuration.
cloud_storage_clients::client_configuration
create_aws_sigv4_configuration(const config::configuration& cfg) {
    // The bg refresh op is closely tied to S3. It'd be
    // nice to untangle it more.
    cloud_storage_clients::s3_configuration s3_config{};

    // Prefer iceberg-specific configuration but accept cloud storage
    // configuration, for convenience in the common case where glue and
    // S3 use the same credentials.
    auto access_key = cfg.iceberg_rest_catalog_aws_access_key().has_value()
                        ? cfg.iceberg_rest_catalog_aws_access_key()
                        : cfg.cloud_storage_access_key();
    if (access_key.has_value()) {
        s3_config.access_key = cloud_roles::public_key_str{access_key.value()};
    }

    auto secret_key = cfg.iceberg_rest_catalog_aws_secret_key().has_value()
                        ? cfg.iceberg_rest_catalog_aws_secret_key()
                        : cfg.cloud_storage_secret_key();
    if (secret_key.has_value()) {
        s3_config.secret_key = cloud_roles::private_key_str{secret_key.value()};
    }

    // Service name defaults to "glue".
    s3_config.service = cloud_roles::aws_service_name{
      cfg.iceberg_rest_catalog_aws_service_name()};

    auto region = cfg.iceberg_rest_catalog_aws_region().has_value()
                    ? cfg.iceberg_rest_catalog_aws_region()
                    : cfg.cloud_storage_region();
    s3_config.region = cloud_roles::aws_region_name{region.value_or("")};

    return cloud_storage_clients::client_configuration{std::move(s3_config)};
}

// Build the client configuration for refreshing GCP credentials for Iceberg.
cloud_storage_clients::client_configuration
create_gcp_configuration(const config::configuration&) {
    // We're are (abu)sing cloud_storage_clients to reach out to the GCP
    // credential refreshing mechanism. It is always used with the
    // gcp_instance_metadata credential source which requires no additional
    // configuration.
    cloud_storage_clients::s3_configuration s3_config{};
    return cloud_storage_clients::client_configuration{s3_config};
}

model::cloud_credentials_source
get_credentials_source(const config::configuration& cfg) {
    if (
      cfg.iceberg_rest_catalog_authentication_mode()
      == config::datalake_catalog_auth_mode::gcp) {
        return model::cloud_credentials_source::gcp_instance_metadata;
    }

    return cfg.iceberg_rest_catalog_aws_credentials_source().has_value()
             ? cfg.iceberg_rest_catalog_aws_credentials_source().value()
             : cfg.cloud_storage_credentials_source();
}

// Build the client configuration for refreshing credentials.
// If the return is empty, no credential refresh is needed.
std::optional<cloud_storage_clients::client_configuration>
create_auth_refresh_configuration(const config::configuration& cfg) {
    if (cfg.iceberg_catalog_type() != config::datalake_catalog_type::rest) {
        return std::nullopt;
    }

    switch (cfg.iceberg_rest_catalog_authentication_mode()) {
    case config::datalake_catalog_auth_mode::none:
        return std::nullopt;
    case config::datalake_catalog_auth_mode::bearer:
        return std::nullopt;
    case config::datalake_catalog_auth_mode::oauth2:
        // TODO: Implement OAuth2 auth refresh via the bg op.
        // The client will handle refresh for now.
        return std::nullopt;
    case config::datalake_catalog_auth_mode::aws_sigv4:
        return create_aws_sigv4_configuration(cfg);
    case config::datalake_catalog_auth_mode::gcp:
        return create_gcp_configuration(cfg);
    }
}

ss::sstring compute_sha256_hex(const iobuf& data) {
    hash_sha256 hasher;
    hasher.update(data);
    auto hash = hasher.reset();
    return to_hex(hash);
}

} // anonymous namespace

credential_manager::credential_manager() = default;

credential_manager::~credential_manager() = default;

ss::future<> credential_manager::start() {
    start_auth_refresh_if_needed();
    co_return;
}

ss::future<> credential_manager::stop() {
    auth_refresh_as_.request_abort();
    credentials_available_cv_.broken();
    if (auth_refresh_bg_op_) {
        co_await auth_refresh_bg_op_->stop();
        auth_refresh_bg_op_.reset();
    }
    if (!gate_.is_closed()) {
        co_await gate_.close();
    }
}

ss::future<result<std::monostate>> credential_manager::wait_for_credentials() {
    if (apply_credentials_) {
        co_return std::monostate{};
    }

    vlog(datalake_log.info, "Waiting for credentials to become available");

    try {
        co_await credentials_available_cv_.wait(
          std::chrono::seconds(5), [this] {
              return apply_credentials_ != nullptr || gate_.is_closed()
                     || auth_refresh_as_.abort_requested();
          });
    } catch (const ss::condition_variable_timed_out&) {
        vlog(
          datalake_log.warn, "Timeout waiting for credentials after 5 seconds");
        co_return std::make_error_code(std::errc::timed_out);
    } catch (const ss::broken_condition_variable&) {
        co_return std::make_error_code(std::errc::operation_canceled);
    }

    if (apply_credentials_) {
        // Don't log if interrupted during shutdown.
        vlog(datalake_log.info, "Credentials are now available");
        co_return std::monostate{};
    }

    // Interrupted during shutdown.
    co_return std::make_error_code(std::errc::operation_canceled);
}

ss::future<result<std::monostate>> credential_manager::maybe_sign(
  const std::optional<iobuf>& payload,
  boost::beast::http::request_header<>& request) {
    const auto& cfg = config::shard_local_cfg();
    if (
      cfg.iceberg_rest_catalog_authentication_mode()
        != config::datalake_catalog_auth_mode::aws_sigv4
      && cfg.iceberg_rest_catalog_authentication_mode()
           != config::datalake_catalog_auth_mode::gcp) {
        co_return std::monostate{};
    }

    auto wait_result = co_await wait_for_credentials();
    if (wait_result.has_error()) {
        co_return wait_result.error();
    }

    if (
      cfg.iceberg_rest_catalog_authentication_mode()
      == config::datalake_catalog_auth_mode::aws_sigv4) {
        // Clear the Authorization and sha headers to ensure clean signing.
        constexpr auto amz_sha_header = "x-amz-content-sha256";
        request.erase(boost::beast::http::field::authorization);
        request.erase(amz_sha_header);

        if (payload.has_value()) {
            // Set the payload sha if there is a payload.
            // Otherwise, signing will handle adding the sha for empty.
            auto hash = compute_sha256_hex(payload.value());
            request.set(amz_sha_header, std::string_view(hash));
        }
    }

    if (
      cfg.iceberg_rest_catalog_authentication_mode()
        == config::datalake_catalog_auth_mode::gcp
      && config::shard_local_cfg()
           .iceberg_rest_catalog_gcp_user_project()
           .has_value()) {
        constexpr auto gcp_project_header = "x-goog-user-project";

        request.set(
          gcp_project_header,
          std::string_view(*config::shard_local_cfg()
                              .iceberg_rest_catalog_gcp_user_project()));
    }

    auto ec = apply_credentials_->add_auth(request);
    if (ec) {
        co_return ec;
    }

    co_return std::monostate{};
}

void credential_manager::start_auth_refresh_if_needed() {
    if (ss::this_shard_id() != cloud_roles::auth_refresh_shard_id) {
        return;
    }

    const auto& cfg = config::shard_local_cfg();
    auto client_config = create_auth_refresh_configuration(cfg);
    if (!client_config.has_value()) {
        return;
    }

    auto config_source
      = cloud_storage_clients::build_refresh_credentials_source(
        *client_config, cfg.cloud_storage_credentials_source);

    auth_refresh_bg_op_.emplace(
      datalake_log,
      gate_,
      auth_refresh_as_,
      get_credentials_source(cfg),
      std::move(config_source));

    auth_refresh_bg_op_->maybe_start_auth_refresh_op(
      [this](cloud_roles::credentials creds) -> ss::future<> {
          return propagate_credentials(std::move(creds));
      },
      "datalake");
}

ss::future<>
credential_manager::propagate_credentials(cloud_roles::credentials creds) {
    vlog(datalake_log.info, "Propagating credentials to all shards");
    return container().invoke_on_all(
      [c = std::move(creds)](credential_manager& mgr) mutable {
          if (!mgr.apply_credentials_) {
              vlog(
                datalake_log.debug,
                "Creating new credentials applier on shard {}",
                ss::this_shard_id());
              mgr.apply_credentials_ = ss::make_lw_shared(
                cloud_roles::make_credentials_applier(c));
              mgr.credentials_available_cv_.broadcast();
          } else {
              vlog(
                datalake_log.debug,
                "Updating existing credentials on shard {}",
                ss::this_shard_id());
              mgr.apply_credentials_->reset_creds(c);
          }
      });
}

} // namespace datalake
