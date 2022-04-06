// Copyright 2020 Redpanda Data, Inc.
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

#include <vector>

using namespace kafka; // NOLINT

BOOST_AUTO_TEST_CASE(test_no_additional_options) {
    creatable_topic no_options = {
      .name = model::topic_view{"test_tp"},
      .num_partitions = 5,
      .replication_factor = 5};

    auto cluster_tp_config = to_cluster_type(no_options);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.cfg.tp_ns.tp, ss::sstring(no_options.name()));
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.cfg.partition_count, no_options.num_partitions);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.cfg.replication_factor, no_options.replication_factor);
}

BOOST_AUTO_TEST_CASE(test_with_custom_assignments) {
    creatable_topic custom_assignments = {
      .name = model::topic_view{"test_tp"},
      .num_partitions = -1,
      .replication_factor = -1};

    custom_assignments.assignments.push_back(creatable_replica_assignment{
      .partition_index = model::partition_id(0),
      .broker_ids = {model::node_id(1), model::node_id(2), model::node_id(3)},
    });

    custom_assignments.assignments.push_back(creatable_replica_assignment{
      .partition_index = model::partition_id(1),
      .broker_ids
      = {model::node_id(10), model::node_id(20), model::node_id(30)},
    });

    auto cluster_tp_config = to_cluster_type(custom_assignments);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.cfg.tp_ns.tp, ss::sstring(custom_assignments.name()));
    // derrived from custom assignments
    BOOST_REQUIRE_EQUAL(cluster_tp_config.cfg.partition_count, 2);
    BOOST_REQUIRE_EQUAL(cluster_tp_config.cfg.replication_factor, 3);

    BOOST_REQUIRE_EQUAL(cluster_tp_config.custom_assignments.size(), 2);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.custom_assignments[0].id, model::partition_id(0));
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.custom_assignments[1].id, model::partition_id(1));

    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.custom_assignments[0].replicas,
      custom_assignments.assignments[0].broker_ids);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.custom_assignments[1].replicas,
      custom_assignments.assignments[1].broker_ids);
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
      cluster_tp_config.cfg.tp_ns.tp, ss::sstring(all_options.name()));
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.cfg.partition_count, all_options.num_partitions);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.cfg.replication_factor, all_options.replication_factor);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.cfg.properties.compression, model::compression::snappy);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.cfg.properties.compaction_strategy,
      model::compaction_strategy::header);
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.cfg.properties.retention_bytes.is_disabled(), true);
    using namespace std::chrono_literals;
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.cfg.properties.retention_duration.value().count(),
      (86400000ms).count());
    BOOST_REQUIRE_EQUAL(
      cluster_tp_config.cfg.properties.cleanup_policy_bitflags,
      model::cleanup_policy_bitflags::compaction
        | model::cleanup_policy_bitflags::deletion);
}
