/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "cloud_roles/refresh_credentials.h"
#include "model/metadata.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

namespace cloud_roles {

inline constexpr ss::shard_id auth_refresh_shard_id = 0;

/// Helper class to start the background operations to periodically refresh
/// authentication. Selects the implementation for fetch based on the
/// cloud_credentials_source property.
class auth_refresh_bg_op {
public:
    struct s3_compat_config {
        cloud_roles::aws_service_name service;
        cloud_roles::aws_region_name region;
    };
    struct abs_config {};

    using credentials_source_config = std::variant<
      // Static credentials from config file.
      cloud_roles::credentials,
      // S3 like providers (AWS, GCP).
      s3_compat_config,
      // Azure Blob Storage.
      abs_config>;

public:
    auth_refresh_bg_op(
      ss::logger& logger,
      ss::gate& gate,
      ss::abort_source& as,
      model::cloud_credentials_source cloud_credentials_source,
      credentials_source_config source_config);

    /// Helper to decide if credentials will be regularly fetched from
    /// infrastructure APIs or loaded once from config file.
    bool is_static_config() const;

    /// Builds a set of static AWS compatible credentials, reading values from
    /// the S3 configuration passed to us.
    cloud_roles::credentials build_static_credentials() const;

    /// Update static config.
    void set_source_config(credentials_source_config config) {
        if (!is_static_config()) {
            throw std::runtime_error(
              "cannot set static config when using dynamic credentials");
        }
        _source_config = std::move(config);
    }

    /// Start a background refresh operation, accepting a callback which is
    /// called with newly fetched credentials periodically. The operation is
    /// started on auth_refresh_shard_id and credentials are copied to other
    /// shards using the callback.
    void maybe_start_auth_refresh_op(
      cloud_roles::credentials_update_cb_t credentials_update_cb,
      ss::sstring metrics_tag = "");

    ss::future<> stop();

    // Trigger credentials refresh (in case if IAM is used).
    // Also, apply some basic rate limiting.
    void maybe_refresh_credentials();

    // The method returns a counter which is incremented every
    // time the token refresh is requested.
    uint64_t token_refresh_count() const noexcept;

private:
    void do_start_auth_refresh_op(
      cloud_roles::credentials_update_cb_t credentials_update_cb,
      ss::sstring metrics_tag);

    ss::logger& _log;
    ss::gate& _gate;
    ss::abort_source& _as;

    model::cloud_credentials_source _cloud_credentials_source;
    credentials_source_config _source_config;

    std::optional<cloud_roles::refresh_credentials> _refresh_credentials;
    std::optional<ss::lowres_clock::time_point> _last_refresh_time;
    uint64_t _refresh_cnt{0};
};

} // namespace cloud_roles
