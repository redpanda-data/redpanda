/*
 * Copyright 2022 Redpanda Data, Inc.
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

class aws_refresh_impl final : public refresh_credentials::impl {
public:
    static constexpr std::string_view default_host = "169.254.169.254";
    static constexpr uint16_t default_port = 80;

    aws_refresh_impl(
      net::unresolved_address address,
      aws_region_name region,
      ss::abort_source& as,
      retry_params retry_params = default_retry_params);
    ss::future<api_response> fetch_credentials() override;

    std::ostream& print(std::ostream& os) const override;

protected:
    api_response_parse_result parse_response(iobuf resp) override;

    /// Fetches the IAM role name from EC2 instance metadata API. This should
    /// only be required once , we can then cache the role name and use it for
    /// the duration of the application run
    ss::future<api_response>
    fetch_role_name(std::optional<std::string_view> token);

    /// Fetches the IMDSv2 instance metadata API token by issuing a PUT request.
    /// This token is fetched for each fetch credential attempt. If the request
    /// fails due to the following error responses: 403, 404, 405; then we
    /// permanently switch to fallback mode and make IMDSv1 requests.
    ss::future<api_response> fetch_instance_metadata_token();

    /// Issues request to instance metadata API with a token injected as a
    /// header, if the token is present. In fallback mode this is effectively a
    /// no-op.
    ss::future<api_response> make_request_with_token(
      http::client::request_header req, std::optional<std::string_view> token);

    bool is_fallback_required(const api_request_error& response);

private:
    std::optional<ss::sstring> _role;
    using fallback_engaged = ss::bool_class<struct fallback_engaged_t>;
    // Indicates if we have switched to IMDSv1 mode.
    fallback_engaged _fallback_engaged{fallback_engaged::no};
};

} // namespace cloud_roles
