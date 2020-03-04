#define BOOST_TEST_MODULE utils

#include "kafka/errors.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(error_mapping_test) {
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(cluster::errc::success),
      kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(cluster::errc::topic_invalid_config),
      kafka::error_code::invalid_config);
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(cluster::errc::topic_invalid_partitions),
      kafka::error_code::invalid_partitions);
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(
        cluster::errc::topic_invalid_replication_factor),
      kafka::error_code::invalid_replication_factor);
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(cluster::errc::notification_wait_timeout),
      kafka::error_code::request_timed_out);
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(cluster::errc::not_leader_controller),
      kafka::error_code::not_controller);
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(cluster::errc::topic_already_exists),
      kafka::error_code::topic_already_exists);
};

BOOST_AUTO_TEST_CASE(mapping_unknow_error) {
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(static_cast<cluster::errc>(-66)),
      kafka::error_code::unknown_server_error);
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(static_cast<cluster::errc>(-33)),
      kafka::error_code::unknown_server_error);
};
