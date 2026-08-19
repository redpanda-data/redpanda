/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "pandaproxy/schema_registry/fwd.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/server.h"
#include "security/acl.h"

#include <variant>

namespace pandaproxy::schema_registry {

using server = ctx_server<service>;

constexpr auto audit_svc_name = "Redpanda Schema Registry Service";

class auth {
public:
    enum class level {
        // Unauthenticated endpoint (not a typo, 'public' is a keyword)
        publik,
        // Requires authentication (if enabled) but not superuser status
        user,
        // Requires authentication (if enabled) and superuser status
        superuser
    };

    using global = registry_resource;
    using subject = subject;
    using cluster = security::acl_cluster_name;
    // AuthZ not required
    using none = named_type<std::monostate, class none_tag>;
    // AuthZ is required to be performed in the handler as the resource is
    // unknown
    using deferred = named_type<std::monostate, class deferred_tag>;

    using op = security::acl_operation;
    using resource = std::variant<none, deferred, global, subject, cluster>;

    using regular_function_handler = ss::noncopyable_function<
      ss::future<server::reply_t>(server::request_t, server::reply_t)>;
    using deferred_function_handler
      = ss::noncopyable_function<ss::future<server::reply_t>(
        server::request_t,
        server::reply_t,
        ss::lw_shared_ptr<request_auth_result>)>;
    using function_handler
      = std::variant<regular_function_handler, deferred_function_handler>;

    auth(level lvl, std::optional<op> op, resource res)
      : _lvl{lvl}
      , _op{op}
      , _res{std::move(res)} {}

    level get_level() const { return _lvl; }
    std::optional<op> get_op() const { return _op; }
    const resource& get_resource() const { return _res; }
    bool is_deferred() const {
        return std::holds_alternative<auth::deferred>(get_resource());
    }

    // Handle authentication and authorization.
    // The presence of a returned authentication result indicates that the
    // authorization check was deferred and has to be done inside the method
    // handler. The result is shared with the handler so the caller can
    // verify, once the handler completes, that the check was performed.
    ss::lw_shared_ptr<request_auth_result>
    handle_auth(server::request_t& rq, std::string_view operation_name) const;

private:
    level _lvl;
    std::optional<op> _op;
    resource _res;
};

/// Await a deferred-authorization handler's reply and enforce that the
/// handler performed its check.
///
/// A handler that fails before reaching its check is acceptable: the client
/// receives the original error, and no data is returned. A handler that
/// produces a reply without having checked indicates a handler bug that may
/// be allowing unchecked access, so the reply is discarded and replaced with
/// a 500.
///
/// A null auth_result means authorization was not deferred for this request
/// (e.g. it is disabled) and there is nothing to enforce.
ss::future<server::reply_t> enforce_deferred_authz(
  ss::future<server::reply_t> handler_result,
  ss::lw_shared_ptr<request_auth_result> auth_result,
  std::string_view operation_name);

} // namespace pandaproxy::schema_registry
