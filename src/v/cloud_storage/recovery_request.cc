/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/recovery_request.h"

#include "json/stringbuffer.h"
#include "json/validator.h"
#include "json/writer.h"

namespace {
constexpr std::string_view request_schema = R"(
{
    "type": "object",
    "properties": {
        "topic_names_pattern": {
            "type": "string"
        },
        "retention_bytes": {
            "type": "integer"
        },
        "retention_ms": {
            "type": "integer"
        }
    },
    "additionalProperties": false,
    "anyOf":  [
        { "required": ["topic_names_pattern"] },
        { "required": ["retention_bytes"] },
        { "required": ["retention_ms"] }
    ],
    "not": {
        "anyOf": [
            {"required": ["retention_bytes", "retention_ms"]}
        ]
    }
}
)";
}

namespace cloud_storage {

recovery_request::recovery_request(const ss::http::request& req) {
    parse_request_body(req);
}

std::optional<ss::sstring> recovery_request::topic_names_pattern() const {
    return _topic_names_pattern;
}

std::optional<size_t> recovery_request::retention_bytes() const {
    return _retention_bytes;
}

std::optional<std::chrono::milliseconds>
recovery_request::retention_ms() const {
    return _retention_ms;
}

template<typename headers>
static std::optional<ss::sstring> find_content_type(const headers& h) {
    if (h.contains("Content-Type")) {
        return h.at("Content-Type");
    }

    if (h.contains("content-type")) {
        return h.at("content-type");
    }

    return std::nullopt;
}

void recovery_request::parse_request_body(const ss::http::request& request) {
    if (request.content_length > 0) {
        auto content_type = find_content_type(request._headers);
        if (!content_type) {
            throw bad_request{"missing content type"};
        }
        if (content_type.value() != "application/json") {
            throw bad_request{
              fmt::format("invalid content type {}", content_type.value())};
        }
        json::Document document;
        document.Parse(request.content.data());
        if (document.HasParseError()) {
            throw bad_request{fmt::format(
              "{}", rapidjson::GetParseError_En(document.GetParseError()))};
        }

        auto validator = json::validator(std::string{request_schema});
        if (!document.Accept(validator.schema_validator)) {
            json::StringBuffer sbuf;
            json::Writer<json::StringBuffer> w{sbuf};
            validator.schema_validator.GetError().Accept(w);
            throw bad_request{
              fmt::format("invalid request body: {}", sbuf.GetString())};
        }

        if (document.HasMember("topic_names_pattern")) {
            _topic_names_pattern = document["topic_names_pattern"].GetString();
        }

        if (document.HasMember("retention_bytes")) {
            _retention_bytes = document["retention_bytes"].GetInt64();
        }

        if (document.HasMember("retention_ms")) {
            _retention_ms = std::chrono::milliseconds{
              document["retention_ms"].GetInt64()};
        }
    }
}

std::ostream& operator<<(std::ostream& os, const recovery_request& r) {
    fmt::print(
      os,
      "{{topic_names_pattern: {}, retention_bytes: {}, retention_ms: {}}}",
      r.topic_names_pattern().value_or("none"),
      r.retention_bytes().has_value()
        ? std::to_string(r.retention_bytes().value())
        : "none",
      r.retention_ms().has_value() ? std::to_string(r.retention_ms()->count())
                                   : "none");
    return os;
}

} // namespace cloud_storage
