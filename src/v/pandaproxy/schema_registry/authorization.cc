/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "pandaproxy/schema_registry/authorization.h"

#include "pandaproxy/parsing/httpd.h"
#include "pandaproxy/schema_registry/service.h"
#include "pandaproxy/schema_registry/types.h"
#include "security/acl.h"
#include "security/authorizer.h"

#include <seastar/util/variant_utils.hh>

namespace pandaproxy::schema_registry::enterprise {

namespace detail {

template<typename T>
concept no_auth = std::is_same_v<T, auth::none>
                  || std::is_same_v<T, auth::deferred>;

template<typename T>
concept requires_auth = !no_auth<T>;

struct auth_params {
    security::acl_principal principal;
    security::acl_host host;
    security::acl_operation op;

    auth_params(const server::request_t& rq, const auth& auth)
      : principal{security::principal_type::user, rq.user.name}
      , host{rq.req->get_client_address().addr()}
      , op{auth.get_op().value_or(security::acl_operation::all)} {}
};

} // namespace detail

namespace {

auth::resource
extract_resource_from_request(const server::request_t& rq, const auth& auth) {
    auto resource = auth.get_resource();
    ss::visit(
      resource,
      [&rq](subject& sub) {
          sub = parse::request_param<subject>(*rq.req, "subject");
      },
      [](const auto&) {});
    return resource;
}

void handle_authz_result(const security::auth_result& authz_result) {
    if (authz_result.is_authorized()) {
        // TODO(CORE-12275): audit success
    } else {
        // TODO(CORE-12275): audit failure
        throw ss::httpd::base_exception(
          "Forbidden (missing required ACLs)",
          ss::http::reply::status_type::forbidden);
    }
}

} // namespace

void handle_authz(
  const server::request_t& rq,
  const auth& auth,
  request_auth_result& auth_result) {
    auth_result.pass();

    detail::auth_params params{rq, auth};

    auto resource = extract_resource_from_request(rq, auth);

    // Check Authorization
    auto authz_result = ss::visit(
      resource,
      [&](const detail::requires_auth auto& resource_name) {
          return rq.service().authorizor().authorized(
            resource_name, params.op, params.principal, params.host);
      },
      [&](const detail::no_auth auto&) {
          return security::auth_result::authz_disabled(
            params.principal, params.host, params.op, registry_resource{});
      });

    handle_authz_result(authz_result);
}

void handle_deferred_authz(
  const server::request_t& rq,
  const auth& auth,
  request_auth_result& auth_result,
  const chunked_vector<subject>& resource_names) {
    auth_result.pass();

    detail::auth_params params{rq, auth};

    auto authz_result = rq.service().authorizor().any_authorized(
      resource_names, params.op, params.principal, params.host);

    handle_authz_result(authz_result);
}

} // namespace pandaproxy::schema_registry::enterprise
