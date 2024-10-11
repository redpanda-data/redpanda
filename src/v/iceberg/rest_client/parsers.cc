/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/rest_client/parsers.h"

#include "json/stringbuffer.h"
#include "json/writer.h"

namespace {

constexpr auto oauth_token_schema = R"JSON({
    "type": "object",
    "properties": {
        "access_token": {
            "type": "string"
        },
        "scope": {
            "type": "string"
        },
        "token_type": {
            "type": "string"
        },
        "expires_in": {
            "type": "integer"
        }
    },
    "required": [
        "access_token",
        "expires_in"
    ]
})JSON";
}

namespace iceberg::rest_client {

parse_error_msg get_schema_validation_error(const json::SchemaValidator& v) {
    json::StringBuffer sb;
    json::Writer<json::StringBuffer> w{sb};
    v.GetError().Accept(w);
    return parse_error_msg{sb.GetString()};
}

expected<oauth_token> parse_oauth_token(json::Document&& doc) {
    json::Document schema;
    if (schema.Parse(oauth_token_schema).HasParseError()) {
        return tl::unexpected(json_parse_error{
          .context = "parse_oauth_token::invalid_schema_validator_input",
          .error = parse_error_msg{GetParseError_En(schema.GetParseError())}});
    }

    json::SchemaDocument schema_doc{schema};

    if (json::SchemaValidator v{schema_doc}; !doc.Accept(v)) {
        return tl::unexpected(json_parse_error{
          .context = "parse_oauth_token::response_does_not_match_schema",
          .error = get_schema_validation_error(v)});
    }

    auto expires_in_sec = std::chrono::seconds{doc["expires_in"].GetInt()};
    return oauth_token{
      .token = doc["access_token"].GetString(),
      .expires_at = ss::lowres_clock::now() + expires_in_sec};
}

} // namespace iceberg::rest_client
