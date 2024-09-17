/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "bytes/streambuf.h"
#include "cloud_roles/request_response_helpers.h"

#include <boost/test/unit_test.hpp>

static auto parse(std::string_view schema, std::string_view data) {
    auto buf = iobuf{};
    auto os_buf = iobuf_ostreambuf{buf};
    std::ostream{&os_buf} << data;
    return cloud_roles::parse_json_response_and_validate(
      schema, std::move(buf));
}

BOOST_AUTO_TEST_CASE(schema_parsing_error) {
    constexpr static auto schema_string = R"(
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
        "name": {
            "type": "string"
        },
        "age": {
            "type": "string",
        }
    },
    "required": ["name", "age"],
}
  )";

    // schema is not a valid json (stray , at the end of the schema)
    auto parse_schema_fail = parse(
      schema_string, R"({"name": "John", "age":"42"})");
    BOOST_CHECK(std::holds_alternative<cloud_roles::api_response_parse_error>(
      parse_schema_fail));
}

auto to_str(const cloud_roles::validate_and_parse_res& val) {
    return ss::visit(
      val,
      [](const json::Document&) -> ss::sstring { return "json::Document"; },
      [](const cloud_roles::malformed_api_response_error& mal) {
          return ssx::sformat(
            "malformed_api_response_error: {}", mal.missing_fields);
      },
      [](const cloud_roles::api_response_parse_error& pe) {
          return ssx::sformat("api_response_parse_error: {}", pe.reason);
      });
}

BOOST_AUTO_TEST_CASE(schema_parsing_test) {
    // rapidjson support for regex is just a subset of ecmascript, basically no
    // character classes like \d or \w. when writing a pattern, fall back to a
    // simple regex syntax
    constexpr static auto schema_string = R"(
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
        "name": {
            "type": "string"
        },
        "age": {
            "type": "string",
            "pattern": "^ *\\+?[0-9]{1,18} *$"
        }
    },
    "required": ["name", "age"]
}
  )";

    BOOST_TEST_CONTEXT("good data in") {
        auto parse_ok = parse(
          schema_string, R"({"name": "John", "age": "42"})");
        BOOST_REQUIRE_MESSAGE(
          std::holds_alternative<json::Document>(parse_ok), to_str(parse_ok));
        auto& doc = std::get<json::Document>(parse_ok);
        BOOST_CHECK(doc["name"].GetString() == std::string_view{"John"});
        BOOST_CHECK(doc["age"].GetString() == std::string_view{"42"});
    }

    BOOST_TEST_CONTEXT("missing fields are reported") {
        auto parse_missing = parse(schema_string, R"({"name": "John"})");
        BOOST_CHECK_MESSAGE(
          std::holds_alternative<cloud_roles::malformed_api_response_error>(
            parse_missing),
          to_str(parse_missing));
        auto& doc = std::get<cloud_roles::malformed_api_response_error>(
          parse_missing);
        BOOST_CHECK(doc.missing_fields == std::vector<ss::sstring>{"age"});
    }

    BOOST_TEST_CONTEXT(
      "missing fields takes precedence over malformed response (except for "
      "`patter: regex` feature of rapidjson)") {
        auto parse_missing_and_invalid = parse(
          schema_string, R"({"age": "-42"})");
        BOOST_CHECK_MESSAGE(
          std::holds_alternative<cloud_roles::api_response_parse_error>(
            parse_missing_and_invalid),
          to_str(parse_missing_and_invalid));
    }

    BOOST_TEST_CONTEXT("malformed response is reported") {
        auto parse_invalid = parse(
          schema_string, R"({"name": "John", "age": "-42"})");
        BOOST_CHECK_MESSAGE(
          std::holds_alternative<cloud_roles::api_response_parse_error>(
            parse_invalid),
          to_str(parse_invalid));

        auto parse_invalid_type = parse(
          schema_string, R"({"name": "John", "age": 42})");
        BOOST_CHECK_MESSAGE(
          std::holds_alternative<cloud_roles::api_response_parse_error>(
            parse_invalid_type),
          to_str(parse_invalid_type));
    }

    BOOST_TEST_CONTEXT("empty is handled") {
        auto parse_empty = parse(schema_string, R"({})");
        BOOST_CHECK_MESSAGE(
          std::holds_alternative<cloud_roles::malformed_api_response_error>(
            parse_empty),
          to_str(parse_empty));
    }

    BOOST_TEST_CONTEXT("malformed is handled") {
        auto parse_malformed = parse(
          schema_string, R"({"name": "John", "age":})");
        BOOST_CHECK_MESSAGE(
          std::holds_alternative<cloud_roles::api_response_parse_error>(
            parse_malformed),
          to_str(parse_malformed));
    }
}
