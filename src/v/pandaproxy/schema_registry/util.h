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

#include "base/seastarx.h"
#include "bytes/iobuf_parser.h"
#include "json/chunked_buffer.h"
#include "json/document.h"
#include "json/writer.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/types.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <rapidjson/error/en.h>

namespace pandaproxy::schema_registry {

///\brief Create a schema definition from raw input.
///
/// Validates the JSON and minify it.
/// TODO(Ben): Validate that it's a valid schema
///
/// Returns error_code::schema_invalid on failure
template<typename Encoding>
result<canonical_schema_definition::raw_string>
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
    ::json::chunked_buffer buf;
    ::json::Writer<::json::chunked_buffer> w{buf};
    doc.Accept(w);
    return canonical_schema_definition::raw_string{std::move(buf).as_iobuf()};
}

template<typename Tag>
ss::sstring to_string(named_type<iobuf, Tag> def) {
    iobuf_parser p{std::move(def)};
    return p.read_string(p.bytes_left());
}

} // namespace pandaproxy::schema_registry
