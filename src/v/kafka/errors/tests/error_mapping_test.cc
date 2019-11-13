#define BOOST_TEST_MODULE utils

#include "kafka/errors/mapping.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(error_mapping_test) {
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(cluster::topic_error_code::no_error),
      kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(cluster::topic_error_code::invalid_config),
      kafka::error_code::invalid_config);
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(cluster::topic_error_code::unknown_error),
      kafka::error_code::unknown_server_error);
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(
        cluster::topic_error_code::invalid_partitions),
      kafka::error_code::invalid_partitions);
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(
        cluster::topic_error_code::invalid_replication_factor),
      kafka::error_code::invalid_replication_factor);
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(cluster::topic_error_code::time_out),
      kafka::error_code::request_timed_out);
};

BOOST_AUTO_TEST_CASE(mapping_unknow_error) {
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(static_cast<cluster::topic_error_code>(-66)),
      kafka::error_code::unknown_server_error);
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(static_cast<cluster::topic_error_code>(-33)),
      kafka::error_code::unknown_server_error);
};
