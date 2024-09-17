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

#include "iceberg/rest_client/parsers.h"

#include "json/stringbuffer.h"
#include "json/writer.h"

namespace {

constexpr auto oauth_token_schema = R"JSON({"type": "object",
    "properties": {"access_token": {"type": "string"},"scope": {"type": "string"},
        "token_type": {"type": "string"},"expires_in": {"type": "integer"}}, 
    "required":["access_token","expires_in"]})JSON";
}

namespace iceberg::rest_client {

ss::sstring get_schema_validation_error(const json::SchemaValidator& v) {
    json::StringBuffer sb;
    json::Writer<json::StringBuffer> w{sb};
    v.GetError().Accept(w);
    return sb.GetString();
}

expected<oauth_token> parse_oauth_token(json::Document&& doc) {
    json::Document schema;
    if (schema.Parse(oauth_token_schema).HasParseError()) {
        return tl::unexpected(json_parse_error{
          .context = "parse_oauth_token::invalid_schema_validator_input",
          .error = GetParseError_En(schema.GetParseError())});
    }

    json::SchemaDocument schema_doc{schema};
    json::SchemaValidator v{schema_doc};

    if (!doc.Accept(v)) {
        return tl::unexpected(json_parse_error{
          .context = "parse_oauth_token::response_does_not_match_schema",
          .error = get_schema_validation_error(v)});
    }

    auto expires_in_sec = std::chrono::seconds{doc["expires_in"].GetInt()};
    return oauth_token{
      .token = doc["access_token"].GetString(),
      .expiry = expires_in_sec,
      .expires_at = ss::lowres_clock::now() + expires_in_sec};
}

} // namespace iceberg::rest_client
