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
#include "security/request_auth.h"

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
          auth_result.get_username(),
          auth_result.get_password(),
          auth_result.get_sasl_mechanism()};
    }

    return user;
}

enum class auth_level {
    // Unauthenticated endpoint (not a typo, 'public' is a keyword)
    publik = 0,
    // Requires authentication (if enabled) but not superuser status
    user = 1,
    // Requires authentication (if enabled) and superuser status
    superuser = 2
};

inline credential_t maybe_authorize_request(
  config::rest_authn_method authn_method,
  auth_level lvl,
  request_authenticator& authenticator,
  const ss::http::request& req) {
    credential_t user;

    if (authn_method != config::rest_authn_method::none) {
        // Will throw 400 & 401 if auth fails
        auto auth_result = authenticator.authenticate(req);
        // Will throw 403 if user enabled HTTP Basic Auth but
        // did not give the authorization header.
        switch (lvl) {
        case auth_level::superuser:
            auth_result.require_superuser();
            break;
        case auth_level::user:
            auth_result.require_authenticated();
            break;
        case auth_level::publik:
            auth_result.pass();
            break;
        }

        user = credential_t{
          auth_result.get_username(),
          auth_result.get_password(),
          auth_result.get_sasl_mechanism()};
    }

    return user;
}

} // namespace pandaproxy
