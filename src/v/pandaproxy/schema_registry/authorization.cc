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

} // namespace detail

void handle_authz(
  const server::request_t& rq,
  const auth& auth,
  request_auth_result& auth_result) {
    auth_result.pass();

    // Extract the resource from the request
    ss::visit(
      auth.get_resource(),
      [&rq](subject& sub) {
          sub = parse::request_param<subject>(*rq.req, "subject");
      },
      [](const auto&) {});

    security::acl_principal principal{
      security::principal_type::user, rq.user.name};
    security::acl_host host{rq.req->get_client_address().addr()};

    // Check Authorization
    auto authz_result = ss::visit(
      auth.get_resource(),
      [&](const detail::requires_auth auto& resource_name) {
          return rq.service().authorizor().authorized(
            resource_name, auth.get_op(), principal, host);
      },
      [&](const detail::no_auth auto&) {
          return security::auth_result::authz_disabled(
            principal, host, auth.get_op(), global_resource{});
      });

    if (authz_result.is_authorized()) {
        // audit success
    } else {
        // audit failre
    }
}

} // namespace pandaproxy::schema_registry::enterprise
