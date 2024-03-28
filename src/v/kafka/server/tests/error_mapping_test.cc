// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/errors.h"
#include "kafka/server/group.h"

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
        cluster::errc::topic_invalid_partitions_core_limit),
      kafka::error_code::invalid_partitions);
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(
        cluster::errc::topic_invalid_partitions_memory_limit),
      kafka::error_code::invalid_partitions);
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(
        cluster::errc::topic_invalid_partitions_fd_limit),
      kafka::error_code::invalid_partitions);
    BOOST_REQUIRE_EQUAL(
      kafka::map_topic_error_code(
        cluster::errc::topic_invalid_partitions_decreased),
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

BOOST_AUTO_TEST_CASE(mapping_offset_commit_error) {
    BOOST_REQUIRE_EQUAL(
      kafka::map_store_offset_error_code(raft::errc::success),
      kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(
      kafka::map_store_offset_error_code(raft::errc::shutting_down),
      kafka::error_code::request_timed_out);
    BOOST_REQUIRE_EQUAL(
      kafka::map_store_offset_error_code(raft::errc::timeout),
      kafka::error_code::request_timed_out);
    BOOST_REQUIRE_EQUAL(
      kafka::map_store_offset_error_code(raft::errc::not_leader),
      kafka::error_code::not_coordinator);
    BOOST_REQUIRE_EQUAL(
      kafka::map_store_offset_error_code(raft::errc::leader_append_failed),
      kafka::error_code::unknown_server_error);
};
