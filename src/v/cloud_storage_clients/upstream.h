/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/format_to.h"
#include "base/seastarx.h"
#include "cloud_roles/apply_credentials.h"
#include "cloud_storage_clients/bucket_params.h"
#include "cloud_storage_clients/client.h"
#include "cloud_storage_clients/credential_manager.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/weak_ptr.hh>

namespace cloud_storage_clients {

class upstream_key {
public:
    explicit upstream_key(
      cloud_roles::aws_region_name region = {},
      cloud_storage_clients::endpoint_url endpoint = {}) noexcept
      : _region(std::move(region))
      , _endpoint(std::move(endpoint)) {}

public:
    const cloud_roles::aws_region_name& region() const { return _region; }
    const cloud_storage_clients::endpoint_url& endpoint() const {
        return _endpoint;
    }

public:
    fmt::iterator format_to(fmt::iterator it) const;

    auto operator<=>(const upstream_key&) const = default;

private:
    cloud_roles::aws_region_name _region;
    cloud_storage_clients::endpoint_url _endpoint;
};

// Non-const params because ada::url_search_params doesn't have const methods.
upstream_key create_upstream_key(const client_configuration&, bucket_params);

static inline const upstream_key default_upstream_key{};

class upstream
  : public ss::peering_sharded_service<upstream>
  , public ss::weakly_referencable<upstream> {
    using client_ptr = ss::shared_ptr<client>;

public:
    explicit upstream(client_configuration config);

public:
    ss::future<> start();
    ss::future<> stop();

public:
    ss::future<> client_self_configure();

    /// Performs the dual functions of loading refreshed credentials into
    /// apply_credentials object, as well as initializing the client pool
    /// the first time this function is called.
    void load_credentials(cloud_roles::credentials credentials);

    void maybe_refresh_credentials();
    uint64_t token_refresh_count() const noexcept;

    /// TODO: Async with timeouts barriers etc.
    client_ptr make_client() noexcept;

private:
    ss::future<
      std::optional<cloud_storage_clients::client_self_configuration_output>>
    do_client_self_configure(client_ptr client);

    ss::future<> accept_self_configure_result(
      std::optional<client_self_configuration_output> result);

    ///  Wait for credentials to be acquired. Once credentials are acquired,
    ///  based on the policy, optionally wait for client pool to initialize.
    ss::future<> wait_for_credentials();

private:
    ss::abort_source _as;
    ss::gate _gate;

    client_configuration _config;
    net::base_transport::configuration _transport_config;

    ss::shared_ptr<client_probe> _probe;

    /// Holds and applies the credentials for requests to S3. Shared pointer to
    /// enable rotating credentials to all clients.
    ss::lw_shared_ptr<cloud_roles::apply_credentials> _apply_credentials;
    ss::condition_variable _credentials_var;

    credential_manager _credential_manager;

    ssx::semaphore _self_config_barrier{0, "self_config_barrier"};
};

} // namespace cloud_storage_clients
