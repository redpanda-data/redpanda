/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "config/rest_authn_endpoint.h"
#include "pandaproxy/types.h"
#include "utils/request_auth.h"

#include <seastar/http/request.hh>

#include <vector>

namespace pandaproxy {
// Authenticate the request when HTTP Basic Auth is enabled and return the
// authenticated user. Otherwise return the default user.
inline credential_t maybe_authenticate_request(
  config::rest_authn_method authn_method,
  request_authenticator& authenticator,
  const ss::http::request& req) {
    credential_t user;

    if (authn_method == config::rest_authn_method::http_basic) {
        // Will throw 400 & 401 if auth fails
        auto auth_result = authenticator.authenticate(req);
        // Will throw 403 if user enabled HTTP Basic Auth but
        // did not give the authorization header.
        auth_result.require_authenticated();
        user = credential_t{
          auth_result.get_username(), auth_result.get_password()};
    }

    return user;
}
} // namespace pandaproxy
