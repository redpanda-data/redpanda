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
#include "security/acl.h"
#include "security/fwd.h"
#include "security/types.h"

#include <seastar/http/exception.hh>
#include <seastar/http/request.hh>

class unauthorized_user_exception : public ss::httpd::base_exception {
public:
    unauthorized_user_exception(
      security::credential_user username, const std::string& msg)
      : ss::httpd::base_exception(
          msg, ss::http::reply::status_type::unauthorized)
      , _username(std::move(username)) {}
    const security::credential_user& get_username() const { return _username; }

private:
    security::credential_user _username;
};

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
    using auth_required = ss::bool_class<struct auth_required_tag>;

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
      ss::sstring sasl_mechanism,
      superuser is_superuser,
      chunked_vector<security::acl_principal> groups)
      : _username(std::move(username))
      , _password(std::move(password))
      , _sasl_mechanism(std::move(sasl_mechanism))
      , _groups(std::move(groups))
      , _authenticated(true)
      , _superuser(is_superuser)
      , _auth_required(true) {};

    /**
     * Anonymous user.  They may still be considered authenticated/superuser
     * if the global require_auth property is set to false (i.e. anonymous
     * users have all powers)
     */
    request_auth_result(
      authenticated is_authenticated,
      superuser is_superuser,
      auth_required is_auth_required)
      : _authenticated(is_authenticated)
      , _superuser(is_superuser)
      , _auth_required(is_auth_required) {};

    request_auth_result operator=(request_auth_result&&) = delete;
    request_auth_result operator=(const request_auth_result&) = delete;

    request_auth_result(const request_auth_result& rhs)
      : _username(rhs._username)
      , _password(rhs._password)
      , _sasl_mechanism(rhs._sasl_mechanism)
      , _groups(rhs._groups.copy())
      , _authenticated(rhs._authenticated)
      , _superuser(rhs._superuser)
      , _auth_required(rhs._auth_required)
      , _checked(rhs._checked) {}

    request_auth_result(request_auth_result&&) noexcept;
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

    const ss::sstring& get_username() const { return _username; }
    const ss::sstring& get_password() const { return _password; }
    const ss::sstring& get_sasl_mechanism() const { return _sasl_mechanism; }
    const chunked_vector<security::acl_principal>& get_groups() const {
        return _groups;
    }

    bool is_authenticated() const { return _authenticated; };
    bool is_superuser() const { return _superuser; }
    bool is_auth_required() const { return _auth_required; }

private:
    security::credential_user _username;
    security::credential_password _password;
    ss::sstring _sasl_mechanism;
    chunked_vector<security::acl_principal> _groups;
    bool _authenticated{false};
    bool _superuser{false};
    bool _auth_required{false};
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
      const ss::http::request& req,
      const security::credential_store& cred_store,
      bool require_auth);

    cluster::controller* _controller{nullptr};
    config::binding<bool> _require_auth;
    config::binding<std::vector<ss::sstring>> _superusers;
};

inline constexpr std::string_view authz_basic_prefix = "Basic ";
inline constexpr std::string_view authz_bearer_prefix = "Bearer ";
