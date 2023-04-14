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

#include "cluster/fwd.h"
#include "config/property.h"
#include "security/fwd.h"
#include "security/types.h"

#include <seastar/http/request.hh>

/**
 * Helper for HTTP request handlers that would like to enforce
 * authentication and authorization rules.
 *
 * Authentication is done on construction: either basic auth or mtls is
 * accepted.  If neither succeeds, an http response exception will be thrown.
 *
 * Authorization is done using one of the require_* methods: e.g.
 * require_superuser.
 */
class [[nodiscard]] request_auth_result {
public:
    using authenticated = ss::bool_class<struct authenticated_tag>;
    using superuser = ss::bool_class<struct superuser_tag>;

    /**
     * Authenticated user.  They have passed authentication so we know
     * their identity, and whether that identity is a superuser.
     *
     * @param username
     * @param is_superuser
     */
    request_auth_result(
      security::credential_user username,
      security::credential_password password,
      superuser is_superuser)
      : _username(std::move(username))
      , _password(std::move(password))
      , _authenticated(true)
      , _superuser(is_superuser){};

    /**
     * Anonymous user.  They may still be considered authenticated/superuser
     * if the global require_auth property is set to false (i.e. anonymous
     * users have all powers)
     */
    request_auth_result(authenticated is_authenticated, superuser is_superuser)
      : _authenticated(is_authenticated)
      , _superuser(is_superuser){};

    ~request_auth_result() noexcept(false);

    /**
     * Raise 403 if not a superuser
     */
    void require_superuser();

    /**
     * Raise 403 if not authenticated
     */
    void require_authenticated();

    /**
     * Do nothing.  Hook for logging access on un-authenticated API endpoints.
     */
    void pass();

    ss::sstring const& get_username() const { return _username; }
    ss::sstring const& get_password() const { return _password; }

private:
    security::credential_user _username;
    security::credential_password _password;
    bool _authenticated{false};
    bool _superuser{false};
    bool _checked{false};
};

class request_authenticator {
public:
    request_authenticator(
      config::binding<bool> require_auth,
      config::binding<std::vector<ss::sstring>> superusers,
      cluster::controller*);

    request_auth_result authenticate(const ss::http::request& req);

private:
    request_auth_result do_authenticate(
      ss::http::request const& req,
      security::credential_store const& cred_store,
      bool require_auth);

    cluster::controller* _controller{nullptr};
    config::binding<bool> _require_auth;
    config::binding<std::vector<ss::sstring>> _superusers;
};
