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

#include <variant>

namespace pandaproxy::schema_registry {

using server = ctx_server<service>;

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

    using global = global_resource;
    using subject = subject;
    // AuthZ not required
    using none = named_type<std::monostate, class none_tag>;
    // AuthZ is required to be performed in the handler as the resource is
    // unknown
    using deferred = named_type<std::monostate, class deferred_tag>;

    using op = security::acl_operation;
    using resource = std::variant<none, deferred, global, subject>;

    // temporary constructor
    explicit auth(level lvl)
      : _lvl{lvl}
      , _op{op::all}
      , _res{global{}} {}

    auth(level lvl, op op, resource res)
      : _lvl{lvl}
      , _op{op}
      , _res{std::move(res)} {}

    level get_level() const { return _lvl; }
    op get_op() const { return _op; }
    const resource& get_resource() const { return _res; }

    void handle_auth(server::request_t& rq) const;

private:
    level _lvl;
    op _op;
    resource _res;
};

} // namespace pandaproxy::schema_registry
