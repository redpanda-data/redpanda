/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "outcome.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/types.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace pandaproxy::schema_registry {

///\brief Create a schema definition from raw input.
///
/// Validates the JSON and minify it.
/// TODO(Ben): Validate that it's a valid schema
///
/// Returns error_code::schema_invalid on failure
template<typename Encoding>
result<schema_definition> make_schema_definition(std::string_view sv) {
    // Validate and minify
    // TODO (Ben): Minify. e.g.:
    // "schema": "{\"type\": \"string\"}" -> "schema": "\"string\""
    rapidjson::GenericDocument<Encoding> doc;
    doc.Parse(sv.data(), sv.size());
    if (doc.HasParseError()) {
        return error_code::schema_invalid;
    }
    rapidjson::GenericStringBuffer<Encoding> str_buf;
    str_buf.Reserve(sv.size());
    rapidjson::Writer<rapidjson::StringBuffer> w{str_buf};
    doc.Accept(w);
    return schema_definition{
      ss::sstring{str_buf.GetString(), str_buf.GetSize()}};
}

} // namespace pandaproxy::schema_registry
