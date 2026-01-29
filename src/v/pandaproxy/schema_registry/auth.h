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
    using cluster = security::acl_cluster_name;
    // AuthZ not required
    using none = named_type<std::monostate, class none_tag>;
    // AuthZ is required to be performed in the handler as the resource is
    // unknown
    using deferred = named_type<std::monostate, class deferred_tag>;

    using op = security::acl_operation;
    using resource
      = std::variant<none, deferred, global, context_subject, cluster>;

    using regular_function_handler = ss::noncopyable_function<
      ss::future<server::reply_t>(server::request_t, server::reply_t)>;
    using deferred_function_handler = ss::noncopyable_function<ss::future<
      server::reply_t>(
      server::request_t, server::reply_t, std::optional<request_auth_result>)>;
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
    // handler
    std::optional<request_auth_result>
    handle_auth(server::request_t& rq, std::string_view operation_name) const;

private:
    level _lvl;
    std::optional<op> _op;
    resource _res;
};

} // namespace pandaproxy::schema_registry
