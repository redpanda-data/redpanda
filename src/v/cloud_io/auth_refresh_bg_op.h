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
#include "cloud_storage_clients/configuration.h"
#include "model/metadata.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

namespace cloud_io {

inline constexpr ss::shard_id auth_refresh_shard_id = 0;

/// Helper class to start the background operations to periodically refresh
/// authentication. Selects the implementation for fetch based on the
/// cloud_credentials_source property.
class auth_refresh_bg_op {
public:
    auth_refresh_bg_op(
      ss::gate& gate,
      ss::abort_source& as,
      cloud_storage_clients::client_configuration client_conf,
      model::cloud_credentials_source cloud_credentials_source);

    /// Helper to decide if credentials will be regularly fetched from
    /// infrastructure APIs or loaded once from config file.
    bool is_static_config() const;

    /// Builds a set of static AWS compatible credentials, reading values from
    /// the S3 configuration passed to us.
    cloud_roles::credentials build_static_credentials() const;

    /// Start a background refresh operation, accepting a callback which is
    /// called with newly fetched credentials periodically. The operation is
    /// started on auth_refresh_shard_id and credentials are copied to other
    /// shards using the callback.
    void maybe_start_auth_refresh_op(
      cloud_roles::credentials_update_cb_t credentials_update_cb);

    cloud_storage_clients::client_configuration get_client_config() const;
    void set_client_config(cloud_storage_clients::client_configuration conf);

    ss::future<> stop();

private:
    void do_start_auth_refresh_op(
      cloud_roles::credentials_update_cb_t credentials_update_cb);

    ss::gate& _gate;
    ss::abort_source& _as;
    cloud_storage_clients::client_configuration _client_conf;
    model::cloud_credentials_source _cloud_credentials_source;
    std::optional<cloud_roles::refresh_credentials> _refresh_credentials;
};

} // namespace cloud_io
