/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "json/document.h"
#include "json/json.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "likely.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/types.h"
#include "seastarx.h"
#include "ssx/semaphore.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <rapidjson/error/en.h>

#include <exception>
#include <utility>

namespace pandaproxy::schema_registry {

///\brief Create a schema definition from raw input.
///
/// Validates the JSON and minify it.
/// TODO(Ben): Validate that it's a valid schema
///
/// Returns error_code::schema_invalid on failure
template<typename Encoding>
result<unparsed_schema_definition::raw_string>
make_schema_definition(std::string_view sv) {
    // Validate and minify
    // TODO (Ben): Minify. e.g.:
    // "schema": "{\"type\": \"string\"}" -> "schema": "\"string\""
    ::json::GenericDocument<Encoding> doc;
    doc.Parse(sv.data(), sv.size());
    if (doc.HasParseError()) {
        return error_info{
          error_code::schema_invalid,
          fmt::format(
            "Invalid schema: {} at offset {}",
            rapidjson::GetParseError_En(doc.GetParseError()),
            doc.GetErrorOffset())};
    }
    ::json::GenericStringBuffer<Encoding> str_buf;
    str_buf.Reserve(sv.size());
    ::json::Writer<::json::GenericStringBuffer<Encoding>> w{str_buf};
    doc.Accept(w);
    return unparsed_schema_definition::raw_string{
      ss::sstring{str_buf.GetString(), str_buf.GetSize()}};
}

} // namespace pandaproxy::schema_registry
