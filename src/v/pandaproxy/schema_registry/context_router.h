// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/seastarx.h"
#include "pandaproxy/schema_registry/errors.h"

#include <seastar/core/sstring.hh>
#include <seastar/http/request.hh>

#include <fmt/format.h>

#include <string_view>

namespace pandaproxy::schema_registry {

/// \brief Normalize a context name from a URL path parameter.
inline ss::sstring normalize_context(std::string_view ctx) {
    if (ctx.starts_with(':')) {
        ctx.remove_prefix(1);
    }

    if (ctx.ends_with(':')) {
        ctx.remove_suffix(1);
    }

    if (ctx.find(':') != std::string_view::npos) {
        throw as_exception(context_invalid(ctx));
    }

    if (!ctx.starts_with('.')) {
        return {fmt::format(".{}", ctx)};
    }
    return ss::sstring(ctx);
}

/// \brief Check if a string already has a context prefix.
inline bool starts_with_context(std::string_view s) {
    return s.starts_with(":.") || s.starts_with(":*:");
}

} // namespace pandaproxy::schema_registry
