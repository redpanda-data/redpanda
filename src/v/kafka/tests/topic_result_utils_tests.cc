#include "kafka/requests/request_reader.h"
#include "kafka/requests/topics/topic_result_utils.h"

#include <seastar/testing/thread_test_case.hh>

namespace {
using namespace kafka;

std::vector<topic_op_result> create_non_empty_response() {
    return {{.topic = model::topic("topic1"),
             .ec = kafka::error_code::invalid_request,
             .err_msg = std::make_optional<sstring>("Invalid request")},
            {.topic = model::topic("topic2"),
             .ec = kafka::error_code::invalid_config,
             .err_msg = std::nullopt}};
}

auto read_result(request_reader& r) {
    auto topic = r.read_string();
    auto err_code = static_cast<kafka::error_code>(r.read_int16());
    auto msg = r.read_nullable_string();
    return std::make_tuple(
      std::move(topic), std::move(err_code), std::move(msg));
}

auto read_result_no_msg(request_reader& r) {
    auto topic = r.read_string();
    auto err_code = static_cast<kafka::error_code>(r.read_int16());
    return std::make_tuple(std::move(topic), std::move(err_code));
}

} // namespace

SEASTAR_THREAD_TEST_CASE(encode_empty_response_no_throttle_time) {
    auto encoded = encode_topic_results({}, -1, include_message::no);
    request_reader reader(std::move(*encoded).release());
    auto results = reader.read_array(
      [](request_reader& r) { return read_result_no_msg(r); });

    BOOST_CHECK_EQUAL(results.size(), 0);
    BOOST_CHECK_EQUAL(reader.bytes_left(), 0);
};

SEASTAR_THREAD_TEST_CASE(non_empty_response_no_throttle_time_no_msg) {
    auto encoded = encode_topic_results(
      create_non_empty_response(), -1, include_message::no);
    request_reader reader(std::move(*encoded).release());
    auto results = reader.read_array(
      [](request_reader& r) { return read_result_no_msg(r); });

    BOOST_CHECK_EQUAL(results.size(), 2);
    BOOST_CHECK_EQUAL(std::get<0>(results[0]), "topic1");
    BOOST_CHECK_EQUAL(
      std::get<1>(results[0]), kafka::error_code::invalid_request);
    BOOST_CHECK_EQUAL(std::get<0>(results[1]), "topic2");
    BOOST_CHECK_EQUAL(
      std::get<1>(results[1]), kafka::error_code::invalid_config);
    BOOST_CHECK_EQUAL(reader.bytes_left(), 0);
};

SEASTAR_THREAD_TEST_CASE(non_empty_response_throttle_time_no_msg) {
    auto encoded = encode_topic_results(
      create_non_empty_response(), 10, include_message::no);
    request_reader reader(std::move(*encoded).release());
    auto throttle_time = reader.read_int32();
    auto results = reader.read_array(
      [](request_reader& r) { return read_result_no_msg(r); });

    BOOST_CHECK_EQUAL(throttle_time, 10);
    BOOST_CHECK_EQUAL(results.size(), 2);
    BOOST_CHECK_EQUAL(std::get<0>(results[0]), "topic1");
    BOOST_CHECK_EQUAL(
      std::get<1>(results[0]), kafka::error_code::invalid_request);
    BOOST_CHECK_EQUAL(std::get<0>(results[1]), "topic2");
    BOOST_CHECK_EQUAL(
      std::get<1>(results[1]), kafka::error_code::invalid_config);
    BOOST_CHECK_EQUAL(reader.bytes_left(), 0);
};

SEASTAR_THREAD_TEST_CASE(non_empty_response_throttle_time_with_msg) {
    auto encoded = encode_topic_results(
      create_non_empty_response(), 10, include_message::yes);
    request_reader reader(std::move(*encoded).release());
    auto throttle_time = reader.read_int32();
    auto results = reader.read_array(
      [](request_reader& r) { return read_result(r); });

    BOOST_CHECK_EQUAL(throttle_time, 10);
    BOOST_CHECK_EQUAL(results.size(), 2);
    BOOST_CHECK_EQUAL(std::get<0>(results[0]), "topic1");
    BOOST_CHECK_EQUAL(
      std::get<1>(results[0]), kafka::error_code::invalid_request);
    BOOST_CHECK_EQUAL(*std::get<2>(results[0]), "Invalid request");
    BOOST_CHECK_EQUAL(std::get<0>(results[1]), "topic2");
    BOOST_CHECK_EQUAL(
      std::get<1>(results[1]), kafka::error_code::invalid_config);
    BOOST_CHECK_EQUAL(std::get<2>(results[1]).has_value(), false);
    BOOST_CHECK_EQUAL(reader.bytes_left(), 0);
};
