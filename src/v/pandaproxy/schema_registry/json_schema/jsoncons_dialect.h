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
#pragma once

#include "pandaproxy/schema_registry/json_schema/dialect.h"
#include "pandaproxy/schema_registry/result.h"

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonschema/jsonschema.hpp>

namespace pandaproxy::schema_registry::json_schema {

template<json_schema_dialect Dialect>
const jsoncons::jsonschema::json_schema<jsoncons::ojson>& get_metaschema() {
    static const auto meteschema_doc = [] {
        auto metaschema = [] {
            switch (Dialect) {
            case json_schema_dialect::draft4:
                return jsoncons::jsonschema::draft4::schema_draft4<
                  jsoncons::ojson>::get_schema();
            case json_schema_dialect::draft6:
                return jsoncons::jsonschema::draft6::schema_draft6<
                  jsoncons::ojson>::get_schema();
            case json_schema_dialect::draft7:
                return jsoncons::jsonschema::draft7::schema_draft7<
                  jsoncons::ojson>::get_schema();
            case json_schema_dialect::draft201909:
                return jsoncons::jsonschema::draft201909::schema_draft201909<
                  jsoncons::ojson>::get_schema();
            case json_schema_dialect::draft202012:
                return jsoncons::jsonschema::draft202012::schema_draft202012<
                  jsoncons::ojson>::get_schema();
            }
        }();

        // Throws if the metaschema can't be parsed (which should never happen
        // and if it does, it would be detected by unit tests)
        return jsoncons::jsonschema::make_json_schema(metaschema);
    }();

    return meteschema_doc;
}

result<json_schema_dialect> validate_json_schema(
  json_schema_dialect dialect, const jsoncons::ojson& schema);

result<json_schema_dialect>
try_validate_json_schema(const jsoncons::ojson& schema);

} // namespace pandaproxy::schema_registry::json_schema
