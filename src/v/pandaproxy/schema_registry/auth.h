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
#include "pandaproxy/server.h"

namespace pandaproxy::schema_registry {

using server = ctx_server<service>;

struct auth {
    enum class level {
        // Unauthenticated endpoint (not a typo, 'public' is a keyword)
        publik,
        // Requires authentication (if enabled) but not superuser status
        user,
        // Requires authentication (if enabled) and superuser status
        superuser
    };

    explicit auth(level lvl)
      : _lvl{lvl} {}

    void handle_auth(server::request_t& rq) const;

    level _lvl;
};

} // namespace pandaproxy::schema_registry
