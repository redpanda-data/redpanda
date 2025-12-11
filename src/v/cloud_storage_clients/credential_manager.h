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

#include "cloud_roles/auth_refresh_bg_op.h"
#include "cloud_storage_clients/configuration.h"

namespace cloud_storage_clients {

class upstream;

/// Credential manager is responsible for managing authentication credentials
/// for the client pool.
class credential_manager {
public:
    credential_manager(
      upstream&,
      cloud_storage_clients::client_configuration,
      model::cloud_credentials_source);

public:
    ss::future<> start();
    ss::future<> stop();

public:
    void maybe_refresh_credentials();
    uint64_t token_refresh_count() const noexcept;

private:
    upstream& _upstream;
    cloud_storage_clients::client_configuration _client_conf;
    cloud_roles::auth_refresh_bg_op _auth_refresh_bg_op;
    config::binding<std::optional<ss::sstring>> _azure_shared_key_binding;

    ss::gate _gate;
    ss::abort_source _as;
};

}; // namespace cloud_storage_clients
