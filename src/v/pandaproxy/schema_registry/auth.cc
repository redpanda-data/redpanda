// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/auth.h"

#include "config/configuration.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/authorization.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/service.h"
#include "pandaproxy/server.h"
#include "security/audit/audit_log_manager.h"
#include "security/audit/types.h"
#include "security/request_auth.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>

#include <boost/algorithm/string/predicate.hpp>

namespace pandaproxy::schema_registry {

namespace {

using server = pandaproxy::ctx_server<pandaproxy::schema_registry::service>;

security::audit::authentication::used_cleartext
is_cleartext(const ss::sstring& protocol) {
    return boost::iequals(protocol, "https")
             ? security::audit::authentication::used_cleartext::no
             : security::audit::authentication::used_cleartext::yes;
}
security::audit::authentication_event_options
make_authn_event_options(const server::request_t& rq) {
    return {
      .auth_protocol = rq.user.sasl_mechanism,
      .server_addr = net::unresolved_address{rq.req->get_server_address()},
      .svc_name = audit_svc_name,
      .client_addr = net::unresolved_address{rq.req->get_client_address()},
      .is_cleartext = is_cleartext(rq.req->get_protocol_name()),
      .user = {
        .name = rq.user.name.empty() ? "{{anonymous}}" : rq.user.name,
        .type_id = rq.user.name.empty() ? security::audit::user::type::unknown
                                        : security::audit::user::type::user}};
}

security::audit::authentication_event_options make_authn_event_options(
  const server::request_t& rq, const request_auth_result& auth_result) {
    return {
      .auth_protocol = auth_result.get_sasl_mechanism(),
      .server_addr = net::unresolved_address{rq.req->get_server_address()},
      .svc_name = audit_svc_name,
      .client_addr = net::unresolved_address{rq.req->get_client_address()},
      .is_cleartext = is_cleartext(rq.req->get_protocol_name()),
      .user = {
        .name = auth_result.get_username().empty() ? "{{anonymous}}"
                                                   : auth_result.get_username(),
        .type_id = auth_result.is_authenticated()
                     ? (auth_result.is_superuser()
                          ? security::audit::user::type::admin
                          : security::audit::user::type::user)
                     : security::audit::user::type::unknown,
        .groups = security::acl_principals_to_audit_groups(
          auth_result.get_groups())}};
}
security::audit::authentication_event_options make_authn_event_error(
  const server::request_t& rq,
  std::string_view username,
  std::string_view reason) {
    return {
      .server_addr = net::unresolved_address{rq.req->get_server_address()},
      .svc_name = audit_svc_name,
      .client_addr = net::unresolved_address{rq.req->get_client_address()},
      .is_cleartext = is_cleartext(rq.req->get_protocol_name()),
      .user
      = {.name = ss::sstring{username}, .type_id = security::audit::user::type::unknown},
      .error_reason = ss::sstring{reason}};
}

void do_audit_authn(
  const server::request_t& rq,
  security::audit::authentication_event_options options) {
    vlog(srlog.trace, "Attempting to audit authn for {}", rq.req->format_url());
    auto success = rq.service().audit_mgr().enqueue_authn_event(
      std::move(options));
    if (!success) {
        vlog(
          srlog.error,
          "Failed to audit authentication request for endpoint: {}",
          rq.req->format_url());
        throw ss::httpd::base_exception(
          "Failed to audit authentication request",
          ss::http::reply::status_type::service_unavailable);
    }
}

void do_audit_authz(const server::request_t& rq) {
    vlog(srlog.trace, "Attempting to audit authz for {}", rq.req->format_url());
    auto success = rq.service().audit_mgr().enqueue_api_activity_event(
      security::audit::event_type::schema_registry,
      *rq.req,
      rq.user.name,
      audit_svc_name);

    if (!success) {
        vlog(
          srlog.error,
          "Failed to audit authorization request for endpoint: {}",
          rq.req->format_url());
        throw ss::httpd::base_exception(
          "Failed to audit authorization request",
          ss::http::reply::status_type::service_unavailable);
    }
}

void audit_authn_failure(
  const server::request_t& rq,
  std::string_view username,
  std::string_view reason) {
    do_audit_authn(rq, make_authn_event_error(rq, username, reason));
}

void audit_authn_success(const server::request_t& rq) {
    do_audit_authn(rq, make_authn_event_options(rq));
}

void audit_authn_success(
  const server::request_t& rq, const request_auth_result& auth_result) {
    do_audit_authn(rq, make_authn_event_options(rq, auth_result));
}

void audit_authz_success(const server::request_t& rq) { do_audit_authz(rq); }

void audit_authz_failure(
  const server::request_t& rq,
  const request_auth_result& auth_result,
  ss::sstring reason) {
    vlog(srlog.trace, "Attempting to audit authz for {}", rq.req->format_url());
    auto success = rq.service().audit_mgr().enqueue_api_activity_event(
      security::audit::event_type::schema_registry,
      *rq.req,
      auth_result,
      audit_svc_name,
      false,
      std::move(reason));

    if (!success) {
        vlog(
          srlog.error,
          "Failed to audit authorization request for endpoint: {}",
          rq.req->format_url());
        throw ss::httpd::base_exception(
          "Failed to audit authorization request",
          ss::http::reply::status_type::service_unavailable);
    }
}

void handle_authz(
  const server::request_t& rq,
  auth::level lvl,
  request_auth_result& auth_result) {
    try {
        switch (lvl) {
        case auth::level::superuser:
            auth_result.require_superuser();
            break;
        case auth::level::user:
            auth_result.require_authenticated();
            break;
        case auth::level::publik:
            auth_result.pass();
            break;
        }
    } catch (const ss::httpd::base_exception& e) {
        audit_authz_failure(rq, auth_result, e.what());
        throw;
    }
    audit_authz_success(rq);
}

} // namespace

ss::lw_shared_ptr<request_auth_result> auth::handle_auth(
  server::request_t& rq, std::string_view operation_name) const {
    rq.authn_method = config::get_authn_method(
      rq.service().config().schema_registry_api.value(),
      rq.req->get_listener_idx());

    if (rq.authn_method != config::rest_authn_method::none) {
        // Will throw 400 & 401 if auth fails
        auto auth_result = [&rq]() {
            try {
                return rq.service().authenticator().authenticate(*rq.req);
            } catch (const unauthorized_user_exception& e) {
                audit_authn_failure(rq, e.get_username()(), e.what());
                throw;
            } catch (const ss::httpd::base_exception& e) {
                audit_authn_failure(rq, "", e.what());
                throw;
            }
        }();

        rq.user = credential_t{
          auth_result.get_username(),
          auth_result.get_password(),
          auth_result.get_sasl_mechanism()};
        audit_authn_success(rq, auth_result);

        // Will throw 403 if user enabled HTTP Basic Auth but
        // did not give the authorization header.
        if (config::shard_local_cfg().schema_registry_enable_authorization) {
            if (is_deferred()) {
                // Defer the authorization handling to the method handler
                return ss::make_lw_shared<request_auth_result>(
                  std::move(auth_result));
            } else {
                enterprise::handle_authz(
                  rq, operation_name, *this, auth_result);
            }
        } else {
            handle_authz(rq, _lvl, auth_result);
        }
    } else {
        rq.user = credential_t{};
        audit_authn_success(rq);
        audit_authz_success(rq);
    }
    return nullptr;
}

ss::future<server::reply_t> enforce_deferred_authz(
  ss::future<server::reply_t> handler_result,
  ss::lw_shared_ptr<request_auth_result> auth_result,
  std::string_view operation_name) {
    auto rp = co_await ss::coroutine::as_future(std::move(handler_result));
    if (auth_result) {
        if (rp.failed()) {
            // The request failed before returning any data, so it is
            // acceptable that its authorization check may not have run.
            auth_result->pass();
        } else if (!auth_result->is_checked()) {
            vlog(
              srlog.error,
              "'{}' handler replied without performing its deferred "
              "authorization check",
              operation_name);
            auth_result->pass();
            // It is essential that the reply is not sent: the client gets a
            // 500 instead. Since this is security code, do not tell them why.
            co_await ss::coroutine::return_exception(
              ss::httpd::server_error_exception("Internal Error"));
        }
    }
    // Yields the handler's reply, or rethrows its original exception.
    co_return co_await std::move(rp);
}

} // namespace pandaproxy::schema_registry
