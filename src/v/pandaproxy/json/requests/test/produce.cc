// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/json/requests/produce.h"

#include "base/seastarx.h"
#include "kafka/protocol/produce.h"
#include "kafka/protocol/schemata/produce_response.h"
#include "model/timestamp.h"
#include "pandaproxy/json/exceptions.h"
#include "pandaproxy/json/rjson_util.h"
#include "utils/to_string.h"

#include <seastar/testing/thread_test_case.hh>

#include <vector>

namespace ppj = pandaproxy::json;

auto make_binary_v2_handler() {
    return ppj::produce_request_handler<>(ppj::serialization_format::binary_v2);
}

auto make_json_v2_handler() {
    return ppj::produce_request_handler<>(ppj::serialization_format::json_v2);
}

SEASTAR_THREAD_TEST_CASE(test_produce_binary_request) {
    auto input = R"(
      {
        "records": [
          {
            "value": "dmVjdG9yaXplZA==",
            "partition": 0
          },
          {
            "value": "cGFuZGFwcm94eQ==",
            "partition": 1
          }
        ]
      })";

    auto records = ppj::impl::rjson_parse(input, make_binary_v2_handler());
    BOOST_TEST(records.size() == 2);
    BOOST_TEST(!!records[0].value);

    auto parser = iobuf_parser(std::move(*records[0].value));
    auto value = parser.read_string(parser.bytes_left());
    BOOST_TEST(value == "vectorized");
    BOOST_TEST(records[0].partition_id == model::partition_id(0));

    parser = iobuf_parser(std::move(*records[1].value));
    value = parser.read_string(parser.bytes_left());
    BOOST_TEST(value == "pandaproxy");
    BOOST_TEST(records[1].partition_id == model::partition_id(1));
}

SEASTAR_THREAD_TEST_CASE(test_produce_json_request) {
    auto input = R"(
      {
        "records": [
          {
            "value": 42,
            "partition": 0
          },
          {
            "key": "json_test",
            "value": {"integer": -5, "string": "str", "array": ["element"]},
            "partition": 1
          }
        ]
      })";

    auto records = ppj::impl::rjson_parse(input, make_json_v2_handler());
    BOOST_REQUIRE_EQUAL(records.size(), 2);
    BOOST_REQUIRE_EQUAL(records[0].partition_id, model::partition_id(0));
    BOOST_REQUIRE(!records[0].key);
    BOOST_REQUIRE(!!records[0].value);
    auto parser = iobuf_parser(std::move(*records[0].value));
    auto value = parser.read_string(parser.bytes_left());
    BOOST_REQUIRE_EQUAL(value, R"(42)");

    BOOST_REQUIRE_EQUAL(records[1].partition_id, model::partition_id(1));
    BOOST_REQUIRE(!!records[1].key);
    parser = iobuf_parser(std::move(*records[1].key));
    value = parser.read_string(parser.bytes_left());
    BOOST_REQUIRE_EQUAL(value, R"("json_test")");

    BOOST_REQUIRE(!!records[1].value);
    parser = iobuf_parser(std::move(*records[1].value));
    value = parser.read_string(parser.bytes_left());
    BOOST_REQUIRE_EQUAL(
      value, R"({"integer":-5,"string":"str","array":["element"]})");
}

SEASTAR_THREAD_TEST_CASE(test_produce_invalid_json_request) {
    auto input = R"(
      {
        "records": [
          {
            "value": invalid,
            "partition": 0
          }
        ]
      })";

    BOOST_CHECK_THROW(
      ppj::impl::rjson_parse(input, make_json_v2_handler()),
      pandaproxy::json::parse_error);
}

SEASTAR_THREAD_TEST_CASE(test_produce_request_empty) {
    auto input = R"(
      {
        "records": []
      })";

    auto records = ppj::impl::rjson_parse(input, make_binary_v2_handler());
    BOOST_TEST(records.size() == 0);
}

SEASTAR_THREAD_TEST_CASE(test_produce_request_error_records_name) {
    auto input = R"(
      {
        "values": [
          {
            "value": "dmVjdG9yaXplZA==",
            "partition": 0
          }
        ]
      })";

    BOOST_CHECK_EXCEPTION(
      ppj::impl::rjson_parse(input, make_binary_v2_handler()),
      ppj::parse_error,
      [](const ppj::parse_error& e) {
          return e.what() == std::string_view("parse error at offset 25");
      });
}

SEASTAR_THREAD_TEST_CASE(test_produce_request_error_partition_name) {
    auto input = R"(
      {
        "records": [
          {
            "value": "dmVjdG9yaXplZA==",
            "id": 0
          }
        ]
      })";

    BOOST_CHECK_EXCEPTION(
      ppj::impl::rjson_parse(input, make_binary_v2_handler()),
      ppj::parse_error,
      [](const ppj::parse_error& e) {
          return e.what() == std::string_view("parse error at offset 99");
      });
}

SEASTAR_THREAD_TEST_CASE(test_produce_request_error_partition_type) {
    auto input = R"(
      {
        "records": [
          {
            "value": "dmVjdG9yaXplZA==",
            "partition": "42"
          }
        ]
      })";

    BOOST_CHECK_EXCEPTION(
      ppj::impl::rjson_parse(input, make_binary_v2_handler()),
      ppj::parse_error,
      [](const ppj::parse_error& e) {
          return e.what() == std::string_view("parse error at offset 112");
      });
}

SEASTAR_THREAD_TEST_CASE(test_produce_request_error_before_records) {
    auto input = R"(
      {
        "partition": 42,
        "records": [
          {
            "value": "dmVjdG9yaXplZA==",
            "partition": 0
          }
        ]
      })";

    BOOST_CHECK_EXCEPTION(
      ppj::impl::rjson_parse(input, make_binary_v2_handler()),
      ppj::parse_error,
      [](const ppj::parse_error& e) {
          return e.what() == std::string_view("parse error at offset 28");
      });
}

SEASTAR_THREAD_TEST_CASE(test_produce_request_error_after_records) {
    auto input = R"(
      {
        "records": [
          {
            "value": "dmVjdG9yaXplZA==",
            "partition": 0
          }
        ],
        "partition": 42
      })";

    BOOST_CHECK_EXCEPTION(
      ppj::impl::rjson_parse(input, make_binary_v2_handler()),
      ppj::parse_error,
      [](const ppj::parse_error& e) {
          return e.what() == std::string_view("parse error at offset 152");
      });
}

SEASTAR_THREAD_TEST_CASE(test_produce_request_error_between_records) {
    auto input = R"(
      {
        "records": [
          {
            "value": "dmVjdG9yaXplZA==",
            "partition": 0
          },
          "partition": 42
        ]
      })";

    BOOST_CHECK_EXCEPTION(
      ppj::impl::rjson_parse(input, make_binary_v2_handler()),
      ppj::parse_error,
      [](const ppj::parse_error& e) {
          return e.what() == std::string_view("parse error at offset 144");
      });
}

SEASTAR_THREAD_TEST_CASE(test_produce_request_error_no_records) {
    auto input = R"(
      {
        "value": "dmVjdG9yaXplZA==",
        "partition": 0
      })";

    BOOST_CHECK_EXCEPTION(
      ppj::impl::rjson_parse(input, make_binary_v2_handler()),
      ppj::parse_error,
      [](const ppj::parse_error& e) {
          return e.what() == std::string_view("parse error at offset 24");
      });
}

SEASTAR_THREAD_TEST_CASE(test_produce_response) {
    auto expected
      = R"({"offsets":[{"partition":0,"offset":42},{"partition":1,"error_code":37,"offset":-1}]})";

    auto topic = kafka::produce_response::topic{
      .name = model::topic{"topic0"},
    };

    topic.partitions.emplace_back(kafka::produce_response::partition{
      .partition_index = model::partition_id{0},
      .error_code = kafka::error_code::none,
      .base_offset = model::offset{42},
      .log_append_time_ms = model::timestamp{},
      .log_start_offset = model::offset{}});
    topic.partitions.emplace_back(kafka::produce_response::partition{
      .partition_index = model::partition_id{1},
      .error_code = kafka::error_code::invalid_partitions,
      .base_offset = model::offset{-1},
      .log_append_time_ms = model::timestamp{},
      .log_start_offset = model::offset{}});

    auto output = ppj::rjson_serialize_str(topic);

    BOOST_TEST(output == expected);
}
