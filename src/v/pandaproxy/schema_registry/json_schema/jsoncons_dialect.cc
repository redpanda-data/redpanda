/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/schema_registry/json_schema/jsoncons_dialect.h"

#include <fmt/core.h>

namespace pandaproxy::schema_registry::json_schema {

result<json_schema_dialect> validate_json_schema(
  json_schema_dialect dialect, const jsoncons::ojson& schema) {
    {
        // validation pre-step: get metaschema for json draft
        const auto& metaschema_doc = [=]() -> const auto& {
            using enum json_schema_dialect;
            switch (dialect) {
            case draft4:
                return get_metaschema<draft4>();
            case draft6:
                return get_metaschema<draft6>();
            case draft7:
                return get_metaschema<draft7>();
            case draft201909:
                return get_metaschema<draft201909>();
            case draft202012:
                return get_metaschema<draft202012>();
            }
        }();

        // validation of schema: validate it against metaschema
        try {
            // Throws when the schema is invalid with details about the failure
            metaschema_doc.validate(schema);
        } catch (const std::exception& e) {
            return error_info{
              error_code::schema_invalid,
              fmt::format(
                "Invalid json schema: '{}'. Error: '{}'",
                schema.to_string(),
                e.what())};
        }

        // schema is a syntactically valid json schema, where $schema ==
        // Dialect.
        // TODO AB cross validate "$ref" fields, this is not done automatically
        // TODO validate that "pattern" and "patternProperties" are valid regex
        return dialect;
    }
}

result<json_schema_dialect>
try_validate_json_schema(const jsoncons::ojson& schema) {
    using enum json_schema_dialect;

    // no explicit $schema: try to validate from newest to oldest draft
    auto first_error = std::optional<error_info>{};
    for (auto d : {draft202012, draft201909, draft7, draft6, draft4}) {
        auto res = validate_json_schema(d, schema);
        if (res.has_value()) {
            return res;
        }
        // failed to validated with dialect d. save error for reporting
        if (!first_error.has_value()) {
            first_error = res.error();
        }
    }

    // A json without a "$schema" member is likely meant to use the latest
    // dialect, so the first failure message is likely more insightful.
    // Also, except for draft4, the other schemas are mostly compatible,
    // only adding rules.
    return first_error.value();
}

} // namespace pandaproxy::schema_registry::json_schema
