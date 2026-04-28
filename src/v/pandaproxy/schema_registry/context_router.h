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
#include "pandaproxy/parsing/httpd.h"
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

/// \brief Parse the "context" path parameter and normalize it.
inline ss::sstring parse_normalized_context(const ss::http::request& req) {
    return normalize_context(parse::request_param<ss::sstring>(req, "context"));
}

/// \brief Check if a string already has a context prefix.
inline bool starts_with_context(std::string_view s) {
    return s.starts_with(":.") || s.starts_with(":*:");
}

/// \brief Scope the "subject" path parameter by prepending the context.
///
/// ctx must already be normalized (in the form ".name"). The resulting
/// subject is ":.ctx:subject".
inline void scope_subject_param(ss::http::request& req, std::string_view ctx) {
    auto sub = req.get_path_param("subject");
    if (!starts_with_context(sub)) {
        req.param.set(
          ss::sstring("subject"),
          ss::sstring(fmt::format("/:{0}:{1}", ctx, sub)));
    }
}

/// \brief Inject or prepend context into the "subject" query parameter.
///
/// ctx must already be normalized.
inline void scope_subject_query(ss::http::request& req, std::string_view ctx) {
    auto existing = req.get_query_param("subject");
    if (existing.empty()) {
        req.set_query_param("subject", fmt::format(":{0}:", ctx));
    } else if (!starts_with_context(existing)) {
        req.set_query_param("subject", fmt::format(":{0}:{1}", ctx, existing));
    }
}

/// \brief Inject or prepend context into the "subjectPrefix" query parameter.
///
/// ctx must already be normalized.
inline void
scope_subject_prefix_query(ss::http::request& req, std::string_view ctx) {
    auto existing = req.get_query_param("subjectPrefix");
    if (existing.empty()) {
        req.set_query_param("subjectPrefix", fmt::format(":{0}:", ctx));
    } else if (!starts_with_context(existing)) {
        req.set_query_param(
          "subjectPrefix", fmt::format(":{0}:{1}", ctx, existing));
    }
}

/// \brief Inject the context as a context-only qualified subject path
/// parameter.
///
/// ctx must already be normalized.
inline void
inject_context_as_subject(ss::http::request& req, std::string_view ctx) {
    req.param.set(
      ss::sstring("subject"), ss::sstring(fmt::format("/:{0}:", ctx)));
}

} // namespace pandaproxy::schema_registry
