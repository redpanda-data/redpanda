/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/tests/topic_table_fixture.h"
#include "cluster/topic_table_partition_generator.h"

inline ss::logger test_log("test");

FIXTURE_TEST(test_successful_generation, topic_table_fixture) {
    auto cmd_1 = make_create_topic_cmd("test_tp_1", 10, 3);
    auto cmd_2 = make_create_topic_cmd("test_tp_2", 10, 3);
    auto cmd_3 = make_create_topic_cmd("test_tp_3", 10, 1);

    auto res_1 = table.local().apply(std::move(cmd_1), model::offset(1)).get();
    auto res_2 = table.local().apply(std::move(cmd_2), model::offset(2)).get();
    auto res_3 = table.local().apply(std::move(cmd_3), model::offset(3)).get();

    BOOST_REQUIRE_EQUAL(res_1, cluster::errc::success);
    BOOST_REQUIRE_EQUAL(res_2, cluster::errc::success);
    BOOST_REQUIRE_EQUAL(res_3, cluster::errc::success);

    cluster::topic_table_partition_generator gen(table, 5);

    std::unordered_map<model::ntp, std::vector<model::broker_shard>> result;

    std::optional<cluster::topic_table_partition_generator::generator_type_t>
      next_batch = std::nullopt;
    do {
        next_batch = gen.next_batch().get();
        if (next_batch) {
            BOOST_REQUIRE(next_batch->size() <= 5);
            for (auto& p_replicas : *next_batch) {
                vlog(test_log.debug, "{}", p_replicas.partition);
                result[p_replicas.partition] = std::move(p_replicas.replicas);
            }
        }
    } while (next_batch.has_value());

    BOOST_REQUIRE(result.size() == 30);

    auto node_ids = members.local().node_ids();
    for (const auto& [ntp, replicas] : result) {
        if (ntp.tp.topic() == "test_tp_3") {
            BOOST_REQUIRE_EQUAL(replicas.size(), 1);
        } else {
            BOOST_REQUIRE_EQUAL(replicas.size(), 3);

            // Check that all nodes are present for
            // ntps with replication factor equal to 3.
            for (const auto& node_id : node_ids) {
                auto iter = std::find_if(
                  replicas.begin(),
                  replicas.end(),
                  [&node_id](model::broker_shard bs) {
                      return bs.node_id == node_id;
                  });

                BOOST_REQUIRE(iter != replicas.end());
            }
        }
    }
}

FIXTURE_TEST(test_topic_table_mutated, topic_table_fixture) {
    auto cmd_1 = make_create_topic_cmd("test_tp_1", 10, 3);
    auto cmd_3 = make_create_topic_cmd("test_tp_3", 10, 1);

    auto res_1 = table.local().apply(std::move(cmd_1), model::offset(1)).get();

    BOOST_REQUIRE_EQUAL(res_1, cluster::errc::success);

    cluster::topic_table_partition_generator gen(table, 3);

    auto res = gen.next_batch().get();
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(res->size() == 3);

    auto cmd_2 = make_create_topic_cmd("test_tp_2", 10, 3);
    auto res_2 = table.local().apply(std::move(cmd_2), model::offset(2)).get();
    BOOST_REQUIRE_EQUAL(res_2, cluster::errc::success);

    // The topic table mutated, so we expected the generator to detect
    // this and throw.
    BOOST_REQUIRE_THROW(
      gen.next_batch().get(),
      cluster::topic_table_partition_generator_exception);
}
