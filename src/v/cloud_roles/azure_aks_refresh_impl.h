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

#include "cloud_roles/refresh_credentials.h"

namespace cloud_roles {

class azure_aks_refresh_impl final : public refresh_credentials::impl {
public:
    // for AKS, the login host is set via an env variable. to conform to the
    // refresh_credentials::impl interface, expose an empty string as
    // default_host. this is used to build the address constructor parameter and
    // to pass overrides for testings
    constexpr static std::string_view default_host = {};
    constexpr static uint16_t default_port = 443;

    azure_aks_refresh_impl(
      net::unresolved_address address,
      aws_region_name region,
      ss::abort_source& as,
      retry_params retry_params);

    /// Fetches credentials from api, the result can be iobuf or an error
    /// encountered during the fetch operation
    ss::future<api_response> fetch_credentials() override;

    std::ostream& print(std::ostream& os) const override;

protected:
    /// Helper to parse the iobuf returned from API into a credentials
    /// object, customized to API response structure
    api_response_parse_result parse_response(iobuf resp) override;

private:
    ss::sstring client_id_;
    ss::sstring tenant_id_;
    ss::sstring federated_token_file_;

    friend class aks_test_helper;
};

} // namespace cloud_roles
