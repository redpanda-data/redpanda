#include "pandaproxy/json/requests/produce.h"

#include "kafka/requests/produce_request.h"
#include "kafka/requests/response.h"
#include "model/timestamp.h"
#include "pandaproxy/json/rjson_util.h"
#include "seastarx.h"

#include <seastar/testing/thread_test_case.hh>

namespace ppj = pandaproxy::json;

auto make_binary_v2_handler() {
    return ppj::produce_request_handler<>(
      pandaproxy::serialization_format::binary_v2);
}

SEASTAR_THREAD_TEST_CASE(test_produce_request) {
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

    auto records = ppj::rjson_parse(input, make_binary_v2_handler());
    BOOST_TEST(records.size() == 2);
    BOOST_TEST(!!records[0].value);

    auto parser = iobuf_parser(std::move(*records[0].value));
    auto value = parser.read_string(parser.bytes_left());
    BOOST_TEST(value == "vectorized");
    BOOST_TEST(records[0].id == model::partition_id(0));

    parser = iobuf_parser(std::move(*records[1].value));
    value = parser.read_string(parser.bytes_left());
    BOOST_TEST(value == "pandaproxy");
    BOOST_TEST(records[1].id == model::partition_id(1));
}

SEASTAR_THREAD_TEST_CASE(test_produce_request_empty) {
    auto input = R"(
      {
        "records": []
      })";

    auto records = ppj::rjson_parse(input, make_binary_v2_handler());
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
      ppj::rjson_parse(input, make_binary_v2_handler()),
      ppj::parse_error,
      [](ppj::parse_error const& e) {
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
      ppj::rjson_parse(input, make_binary_v2_handler()),
      ppj::parse_error,
      [](ppj::parse_error const& e) {
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
      ppj::rjson_parse(input, make_binary_v2_handler()),
      ppj::parse_error,
      [](ppj::parse_error const& e) {
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
      ppj::rjson_parse(input, make_binary_v2_handler()),
      ppj::parse_error,
      [](ppj::parse_error const& e) {
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
      ppj::rjson_parse(input, make_binary_v2_handler()),
      ppj::parse_error,
      [](ppj::parse_error const& e) {
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
      ppj::rjson_parse(input, make_binary_v2_handler()),
      ppj::parse_error,
      [](ppj::parse_error const& e) {
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
      ppj::rjson_parse(input, make_binary_v2_handler()),
      ppj::parse_error,
      [](ppj::parse_error const& e) {
          return e.what() == std::string_view("parse error at offset 24");
      });
}

SEASTAR_THREAD_TEST_CASE(test_produce_response) {
    auto expected
      = R"({"offsets":[{"partition":0,"offset":42},{"partition":1,"error_code":37,"offset":-1}]})";

    auto topic = kafka::produce_response::topic{
      .name = model::topic{"topic0"},
      .partitions = {
        kafka::produce_response::partition{
          .id = model::partition_id{0},
          .error = kafka::error_code::none,
          .base_offset = model::offset{42},
          .log_append_time = model::timestamp{},
          .log_start_offset = model::offset{}},
        kafka::produce_response::partition{
          .id = model::partition_id{1},
          .error = kafka::error_code::invalid_partitions,
          .base_offset = model::offset{-1},
          .log_append_time = model::timestamp{},
          .log_start_offset = model::offset{}},
      }};

    auto output = ppj::rjson_serialize(topic);

    BOOST_TEST(output == expected);
}
