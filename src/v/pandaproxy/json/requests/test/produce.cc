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
#include "utils/base64.h"
#include "utils/to_string.h"

#include <seastar/testing/thread_test_case.hh>

#include <fmt/format.h>

#include <vector>

namespace ppj = pandaproxy::json;

auto make_binary_v2_handler() {
    return ppj::produce_request_handler<>(ppj::serialization_format::binary_v2);
}

auto make_json_v2_handler() {
    return ppj::produce_request_handler<>(ppj::serialization_format::json_v2);
}

// Records that the reader delivered key/value strings through the chunked
// sink protocol rather than the contiguous String() path.
struct probe_produce_request_handler : public ppj::produce_request_handler<> {
    probe_produce_request_handler(
      ppj::serialization_format fmt, bool* chunked_string_used)
      : ppj::produce_request_handler<>{fmt}
      , chunked_string_used{chunked_string_used} {}

    bool ChunkedString(::json::SizeType len) {
        *chunked_string_used = true;
        return ppj::produce_request_handler<>::ChunkedString(len);
    }

    bool* chunked_string_used;
};

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
    BOOST_TEST(records[0].value.has_value());

    auto parser = iobuf_parser(std::move(*records[0].value));
    auto value = parser.read_string(parser.bytes_left());
    BOOST_TEST(value == "vectorized");
    BOOST_TEST(records[0].partition_id == model::partition_id(0));

    parser = iobuf_parser(std::move(*records[1].value));
    value = parser.read_string(parser.bytes_left());
    BOOST_TEST(value == "pandaproxy");
    BOOST_TEST(records[1].partition_id == model::partition_id(1));
}

// Large record keys/values are decoded by the reader directly into an
// iobuf-backed sink and base64-decoded fragment-by-fragment, avoiding large
// contiguous allocations.
SEASTAR_THREAD_TEST_CASE(test_produce_binary_request_large_value) {
    constexpr size_t num_reps = 100000;
    // built via std::string, whose amortized append avoids the quadratic
    // copying of repeated ss::sstring::operator+=
    std::string raw;
    raw.reserve(16 * num_reps);
    for (size_t i = 0; i < num_reps; ++i) {
        raw += "pandaproxy!";
    }
    auto encoded = bytes_to_base64(
      bytes{reinterpret_cast<const uint8_t*>(raw.data()), raw.size()});

    auto input = fmt::format(
      R"({{
        "records": [
          {{
            "key": "{}",
            "value": "{}",
            "partition": 0
          }}
        ]
      }})",
      encoded,
      encoded);

    bool chunked_string_used = false;
    auto records = ppj::impl::rjson_parse(
      input.c_str(),
      probe_produce_request_handler{
        ppj::serialization_format::binary_v2, &chunked_string_used});
    BOOST_REQUIRE_EQUAL(records.size(), 1);
    BOOST_REQUIRE(chunked_string_used);
    BOOST_REQUIRE(records[0].key.has_value());
    BOOST_REQUIRE(records[0].value.has_value());

    auto parser = iobuf_parser(std::move(*records[0].key));
    auto key = parser.read_string(parser.bytes_left());
    BOOST_TEST(key == raw);

    parser = iobuf_parser(std::move(*records[0].value));
    auto value = parser.read_string(parser.bytes_left());
    BOOST_TEST(value == raw);
}

SEASTAR_THREAD_TEST_CASE(test_produce_binary_request_invalid_base64) {
    auto input = R"(
      {
        "records": [
          {
            "value": "!!!not-base64!!!",
            "partition": 0
          }
        ]
      })";

    bool chunked_string_used = false;
    BOOST_REQUIRE_THROW(
      ppj::impl::rjson_parse(
        input,
        probe_produce_request_handler{
          ppj::serialization_format::binary_v2, &chunked_string_used}),
      ppj::parse_error);
    // the base64 decode failure was detected inside the chunked path
    BOOST_REQUIRE(chunked_string_used);
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
    BOOST_REQUIRE(!records[0].key.has_value());
    BOOST_REQUIRE(records[0].value.has_value());
    auto parser = iobuf_parser(std::move(*records[0].value));
    auto value = parser.read_string(parser.bytes_left());
    BOOST_REQUIRE_EQUAL(value, R"(42)");

    BOOST_REQUIRE_EQUAL(records[1].partition_id, model::partition_id(1));
    BOOST_REQUIRE(records[1].key.has_value());
    parser = iobuf_parser(std::move(*records[1].key));
    value = parser.read_string(parser.bytes_left());
    BOOST_REQUIRE_EQUAL(value, R"("json_test")");

    BOOST_REQUIRE(records[1].value.has_value());
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

    topic.partitions.emplace_back(
      kafka::produce_response::partition{
        .partition_index = model::partition_id{0},
        .error_code = kafka::error_code::none,
        .base_offset = model::offset{42},
        .log_append_time_ms = model::timestamp{},
        .log_start_offset = model::offset{}});
    topic.partitions.emplace_back(
      kafka::produce_response::partition{
        .partition_index = model::partition_id{1},
        .error_code = kafka::error_code::invalid_partitions,
        .base_offset = model::offset{-1},
        .log_append_time_ms = model::timestamp{},
        .log_start_offset = model::offset{}});

    auto output = ppj::rjson_serialize_str(topic);

    BOOST_TEST(output == expected);
}
