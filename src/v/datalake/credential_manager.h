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

#include "base/outcome.h"
#include "cloud_roles/apply_credentials.h"
#include "cloud_roles/auth_refresh_bg_op.h"
#include "cloud_roles/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <boost/beast/http/message.hpp>

namespace datalake {

// Service responsible for managing credential refresh for datalake components.
// Provides shared credential management for both datalake_manager and
// coordinator_manager to avoid duplication of credential refresh logic.
class credential_manager
  : public ss::peering_sharded_service<credential_manager> {
public:
    credential_manager();
    ~credential_manager();

    ss::future<> start();
    ss::future<> stop();

    ss::future<result<std::monostate>> maybe_sign(
      const std::optional<iobuf>& payload,
      boost::beast::http::request_header<>& request);

private:
    // Waits until credentials are available. Returns immediately if credentials
    // are already populated. Only waits on the first call when credentials are
    // not yet available. Times out after 5 seconds.
    ss::future<result<std::monostate>> wait_for_credentials();

    void start_auth_refresh_if_needed();
    ss::future<> propagate_credentials(cloud_roles::credentials creds);

    ss::gate gate_;

    // Background operation for refreshing AWS credentials for Iceberg/Glue
    std::optional<cloud_roles::auth_refresh_bg_op> auth_refresh_bg_op_;

    // Shared credentials applier for Iceberg REST clients
    ss::lw_shared_ptr<cloud_roles::apply_credentials> apply_credentials_;

    // Synchronization for credential availability
    ss::condition_variable credentials_available_cv_;

    ss::abort_source auth_refresh_as_;

    friend class credential_manager_tester;
};

} // namespace datalake
