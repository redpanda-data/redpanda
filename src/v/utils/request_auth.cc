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

#include "utils/request_auth.h"

#include "cluster/controller.h"
#include "seastar/http/exception.hh"
#include "security/credential_store.h"
#include "security/scram_algorithm.h"
#include "vlog.h"

static ss::logger logger{"request_auth"};

request_authenticator::request_authenticator(
  config::binding<bool> require_auth,
  config::binding<std::vector<ss::sstring>> superusers,
  cluster::controller* controller)
  : _controller(controller)
  , _require_auth(std::move(require_auth))
  , _superusers(std::move(superusers)) {}

/**
 * Attempt to authenticate the request.
 *
 * The returned object **must be used** via one of its authorization
 * helpers (e.g. require_superuser) or it will throw an exception
 * on destruction.
 *
 * @param req
 * @return
 */
request_auth_result
request_authenticator::authenticate(const ss::http::request& req) {
    if (_controller == nullptr) {
        // We are running outside of an environment with credentials, e.g.
        // a unit test or a standalone pandaproxy/schema_registry
        return request_auth_result(
          request_auth_result::authenticated::yes,
          request_auth_result::superuser::yes);
    }

    const auto& cred_store = _controller->get_credential_store().local();
    try {
        return do_authenticate(req, cred_store, _require_auth());
    } catch (ss::httpd::base_exception const& e) {
        if (e.status() == ss::http::reply::status_type::unauthorized) {
            if (_require_auth()) {
                throw;
            } else {
                // Auth is disabled: give this user full access, but
                // treat them as anonymous.
                return request_auth_result(
                  request_auth_result::authenticated::yes,
                  request_auth_result::superuser::yes);
            }
        } else {
            throw;
        }
    }
}

request_auth_result request_authenticator::do_authenticate(
  ss::http::request const& req,
  security::credential_store const& cred_store,
  bool require_auth) {
    security::credential_user username;

    auto auth_hdr = req.get_header("authorization");
    if (auth_hdr.substr(0, 5) == "Basic") {
        // Minimal length: Basic, a space, 1 or more bytes
        if (auth_hdr.size() < 7) {
            throw ss::httpd::bad_request_exception(
              "Malformed Authorization header");
        }

        auto base64 = auth_hdr.substr(6);
        ss::sstring decoded_bytes;
        try {
            decoded_bytes = base64_to_string(base64);
        } catch (base64_decoder_exception const&) {
            vlog(logger.info, "Client auth failure: bad BASE64 encoding");
            throw ss::httpd::bad_request_exception(
              "Malformed Authorization header");
        }

        auto colon = decoded_bytes.find(":");
        if (colon == ss::sstring::npos || colon == decoded_bytes.size() - 1) {
            vlog(logger.info, "Client auth failure: malformed 'user:password'");
            throw ss::httpd::bad_request_exception(
              "Malformed Authorization header");
        }
        username = security::credential_user{decoded_bytes.substr(0, colon)};
        security::credential_password password{decoded_bytes.substr(colon + 1)};

        const auto cred_opt = cred_store.get<security::scram_credential>(
          username);
        if (!cred_opt.has_value()) {
            // User not found
            vlog(
              logger.warn,
              "Client auth failure: user '{}' not found",
              username);
            throw ss::httpd::base_exception(
              "Unauthorized", ss::http::reply::status_type::unauthorized);
        } else {
            const auto& cred = cred_opt.value();
            bool is_valid = (
              security::scram_sha256::validate_password(
                password, cred.stored_key(), cred.salt(), cred.iterations())
              || security::scram_sha512::validate_password(
                password, cred.stored_key(), cred.salt(), cred.iterations()));
            if (!is_valid) {
                // User found, password doesn't match
                vlog(
                  logger.warn,
                  "Client auth failure: user '{}' wrong password",
                  username);
                throw ss::httpd::base_exception(
                  "Unauthorized", ss::http::reply::status_type::unauthorized);
            } else {
                vlog(logger.trace, "Authenticated user {}", username);
                const auto& superusers = _superusers();
                auto found = std::find(
                  superusers.begin(), superusers.end(), username);
                bool superuser = (found != superusers.end()) || (!require_auth);
                return request_auth_result(
                  std::move(username),
                  std::move(password),
                  request_auth_result::superuser(superuser));
            }
        }
    } else if (!auth_hdr.empty()) {
        throw ss::httpd::bad_request_exception(
          "Unsupported Authorization method");
    } else {
        // No Authorization header: user is anonymous
        if (require_auth) {
            return request_auth_result(
              request_auth_result::authenticated::no,
              request_auth_result::superuser::no);
        } else {
            return request_auth_result(
              request_auth_result::authenticated::yes,
              request_auth_result::superuser::yes);
        }
    }
}

void request_auth_result::require_superuser() {
    _checked = true;
    if (!_superuser) {
        vlog(
          logger.info,
          "Client authorization failure: {} is not a superuser",
          _username);
        throw ss::httpd::base_exception(
          "Forbidden (superuser role required)",
          ss::http::reply::status_type::forbidden);
    }
}

void request_auth_result::require_authenticated() {
    _checked = true;
    if (!_authenticated) {
        vlog(
          logger.info,
          "Client authorization failure: user must be authenticated");
        throw ss::httpd::base_exception(
          "Forbidden (authentication is required)",
          ss::http::reply::status_type::unauthorized);
    }
}

void request_auth_result::pass() { _checked = true; }

/**
 * It is important to protect against someone calling authenticate()
 * but then not calling any of the authorization helpers: this indicates
 * a request handler that might be unintentionally allowing unchecked
 * access.
 *
 * This is a rare case of a throwing destructor.  It is made safe by
 * checking if there is already an exception in flight first, and by
 * knowing that all our member objects have nothrow destructors.
 */
request_auth_result::~request_auth_result() noexcept(false) {
    if (!_checked && !std::current_exception()) {
        vlog(
          logger.error, "request_auth_result destroyed without being checked!");

        // In this case, it is essential that we do not send any data
        // in a response: they get a 500 instead.  Since this is security
        // code, we do not tell them why.
        throw ss::httpd::server_error_exception("Internal Error");
    }
}
