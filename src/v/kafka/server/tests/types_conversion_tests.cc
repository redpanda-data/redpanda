// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/types.h"
#include "kafka/protocol/schemata/create_topics_request.h"
#include "kafka/server/handlers/topics/topic_utils.h"
#include "kafka/server/handlers/topics/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <boost/test/unit_test.hpp>

using namespace kafka; // NOLINT

BOOST_AUTO_TEST_CASE(test_no_additional_options) {
    creatable_topic no_options = {
      .name = model::topic_view{"test_tp"},
      .num_partitions = 5,
      .replication_factor = 5};

    auto cluster_tp_config = to_cluster_type(no_options);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.tp_ns.tp, ss::sstring(no_options.name()));
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.partition_count, no_options.num_partitions);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.replication_factor, no_options.replication_factor);
}

BOOST_AUTO_TEST_CASE(test_all_additional_options) {
    creatable_topic all_options = {
      .name = model::topic_view{"test_tp"},
      .num_partitions = 5,
      .replication_factor = 5,
      .configs = {
        {"compression.type", "snappy"},
        {"cleanup.policy", "compact,delete"},
        {"retention.bytes", "-1"},
        {"retention.ms", "86400000"},
        {"compaction.strategy", "header"},
      }};

    auto cluster_tp_config = to_cluster_type(all_options);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.tp_ns.tp, ss::sstring(all_options.name()));
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.partition_count, all_options.num_partitions);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.replication_factor, all_options.replication_factor);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.compression, model::compression::snappy);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.compaction_strategy,
      model::compaction_strategy::header);
    BOOST_REQUIRE_EQUAL(cluster_tp_config.retention_bytes.is_disabled(), true);
    using namespace std::chrono_literals;
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.retention_duration.value().count(),
      (86400000ms).count());
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.cleanup_policy_bitflags,
      model::cleanup_policy_bitflags::compaction
        | model::cleanup_policy_bitflags::deletion);
}
