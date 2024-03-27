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

class azure_vm_refresh_impl final : public refresh_credentials::impl {
public:
    // this is the local-only address of the instance metadata service, that
    // will be used to get an authorization token
    // https://learn.microsoft.com/en-us/azure/virtual-machines/instance-metadata-service
    static constexpr std::string_view default_host = "169.254.169.254";
    static constexpr uint16_t default_port = 80;

    azure_vm_refresh_impl(
      net::unresolved_address address,
      aws_region_name region,
      ss::abort_source& as,
      retry_params retry_params = default_retry_params);
    ss::future<api_response> fetch_credentials() override;

    std::ostream& print(std::ostream& os) const override;

protected:
    api_response_parse_result parse_response(iobuf response) override;
};

} // namespace cloud_roles
