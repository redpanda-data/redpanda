#define BOOST_TEST_MODULE utils
#include "cluster/types.h"
#include "kafka/requests/topics/types.h"

#include <boost/test/unit_test.hpp>

using namespace kafka; // NOLINT

BOOST_AUTO_TEST_CASE(test_no_additional_options) {
    new_topic_configuration no_options = {.topic = model::topic_view{"test_tp"},
                                          .partition_count = 5,
                                          .replication_factor = 5};

    auto cluster_tp_config = no_options.to_cluster_type();
    BOOST_REQUIRE_EQUAL(cluster_tp_config.topic, sstring(no_options.topic()));
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.partition_count, no_options.partition_count);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.replication_factor, no_options.replication_factor);
}

BOOST_AUTO_TEST_CASE(test_all_additional_options) {
    new_topic_configuration all_options = {
      .topic = model::topic_view{"test_tp"},
      .partition_count = 5,
      .replication_factor = 5,
      .config = {
        {"compression.type", "snappy"},
        {"cleanup.policy", "compact"},
        {"retention.bytes", "1000000"},
        {"retention.ms", "86400000"},
      }};

    auto cluster_tp_config = all_options.to_cluster_type();
    BOOST_REQUIRE_EQUAL(cluster_tp_config.topic, sstring(all_options.topic()));
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.partition_count, all_options.partition_count);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.replication_factor, all_options.replication_factor);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.compression, model::compression::snappy);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.compaction, model::topic_partition::compaction::yes);
    BOOST_REQUIRE_EQUAL(cluster_tp_config.retention_bytes, 1000000);
    using namespace std::chrono_literals;
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.retention.count(), (86400000ms).count());
}
