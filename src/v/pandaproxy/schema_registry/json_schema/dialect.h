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

#include "strings/string_switch.h"

#include <optional>
#include <string_view>

namespace pandaproxy::schema_registry::json_schema {

// List of supported JSON Schema dialects by Schema Registry.
enum class json_schema_dialect {
    draft4,
    draft6,
    draft7,
    draft201909,
    draft202012,
};

constexpr std::string_view
dialect_to_uri(json_schema_dialect draft, bool strip = false) {
    using enum json_schema_dialect;
    auto dialect_str = [&]() -> std::string_view {
        switch (draft) {
        case draft4:
            return "http://json-schema.org/draft-04/schema#";
        case draft6:
            return "http://json-schema.org/draft-06/schema#";
        case draft7:
            return "http://json-schema.org/draft-07/schema#";
        case draft201909:
            return "https://json-schema.org/draft/2019-09/schema#";
        case draft202012:
            return "https://json-schema.org/draft/2020-12/schema#";
        }
    }();

    if (strip) {
        // strip final # from uri
        dialect_str.remove_suffix(1);
    }

    return dialect_str;
}

constexpr std::optional<json_schema_dialect>
dialect_from_uri(std::string_view uri) {
    using enum json_schema_dialect;
    return string_switch<std::optional<json_schema_dialect>>{uri}
      .match_all(dialect_to_uri(draft4), dialect_to_uri(draft4, true), draft4)
      .match_all(dialect_to_uri(draft6), dialect_to_uri(draft6, true), draft6)
      .match_all(dialect_to_uri(draft7), dialect_to_uri(draft7, true), draft7)
      .match_all(
        dialect_to_uri(draft201909),
        dialect_to_uri(draft201909, true),
        draft201909)
      .match_all(
        dialect_to_uri(draft202012),
        dialect_to_uri(draft202012, true),
        draft202012)
      .default_match(std::nullopt);
}

}; // namespace pandaproxy::schema_registry::json_schema
