/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/tests/cluster_test_fixture.h"
#include "errc.h"
#include "jumbo_log/metadata.h"
#include "jumbo_log/rpc.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "test_utils/fixture.h"

#include <boost/test/tools/interface.hpp>

FIXTURE_TEST(test_jumbo_log_create_write_intent, cluster_test_fixture) {
    model::node_id node0(0);
    auto app = create_node_application(node0);

    wait_for_controller_leadership(node0).get();

    auto object_uuid_1 = random_generators::get_uuid();
    auto create_reply1 = app->jumbo_log_frontend.local()
                           .create_write_intent(
                             jumbo_log::rpc::create_write_intent_request(
                               10s,
                               jumbo_log::segment_object{
                                 .id = object_uuid_1, .size_bytes = 1}),
                             30s)
                           .get();

    BOOST_TEST_REQUIRE(create_reply1.ec == cluster::errc::success);
    BOOST_TEST_REQUIRE(create_reply1.write_intent_id() > 0);

    auto object_uuid_2 = random_generators::get_uuid();
    auto create_reply2 = app->jumbo_log_frontend.local()
                           .create_write_intent(
                             jumbo_log::rpc::create_write_intent_request(
                               10s,
                               jumbo_log::segment_object{
                                 .id = object_uuid_2, .size_bytes = 1}),
                             30s)
                           .get();

    BOOST_TEST_REQUIRE(create_reply2.ec == cluster::errc::success);
    BOOST_TEST_REQUIRE(create_reply2.write_intent_id() > 0);

    auto create_reply_retry_1 = app->jumbo_log_frontend.local()
                                  .create_write_intent(
                                    jumbo_log::rpc::create_write_intent_request(
                                      10s,
                                      jumbo_log::segment_object{
                                        .id = object_uuid_1, .size_bytes = 1}),
                                    30s)
                                  .get();

    BOOST_TEST_REQUIRE(create_reply_retry_1.ec == cluster::errc::success);
    BOOST_TEST_REQUIRE(create_reply_retry_1.write_intent_id() > 0);
    BOOST_TEST_REQUIRE(
      create_reply_retry_1.write_intent_id() == create_reply1.write_intent_id(),
      "write_intent_id mismatch for retried request");

    auto get_reply_single = app->jumbo_log_frontend.local()
                              .get_write_intents(
                                jumbo_log::rpc::get_write_intents_request(
                                  10s, {create_reply1.write_intent_id}),
                                30s)
                              .get();
    BOOST_TEST_REQUIRE(get_reply_single.ec == cluster::errc::success);
    BOOST_TEST_REQUIRE(get_reply_single.write_intents.size() == 1);
    BOOST_TEST_REQUIRE(
      get_reply_single.write_intents[0].id == create_reply1.write_intent_id);
    BOOST_TEST_REQUIRE(
      get_reply_single.write_intents[0].object.id == object_uuid_1);
    BOOST_TEST_REQUIRE(
      get_reply_single.write_intents[0].object.size_bytes == 1);

    auto get_reply_multi = app->jumbo_log_frontend.local()
                             .get_write_intents(
                               jumbo_log::rpc::get_write_intents_request(
                                 10s,
                                 {create_reply1.write_intent_id,
                                  create_reply2.write_intent_id}),
                               30s)
                             .get();
    BOOST_TEST_REQUIRE(get_reply_multi.ec == cluster::errc::success);
    BOOST_TEST_REQUIRE(get_reply_multi.write_intents.size() == 2);
    BOOST_TEST_REQUIRE(
      get_reply_multi.write_intents[0].id == create_reply1.write_intent_id);
    BOOST_TEST_REQUIRE(
      get_reply_multi.write_intents[0].object.id == object_uuid_1);
    BOOST_TEST_REQUIRE(get_reply_multi.write_intents[0].object.size_bytes == 1);
    BOOST_TEST_REQUIRE(
      get_reply_multi.write_intents[1].id == create_reply2.write_intent_id);
    BOOST_TEST_REQUIRE(
      get_reply_multi.write_intents[1].object.id == object_uuid_2);
    BOOST_TEST_REQUIRE(get_reply_multi.write_intents[1].object.size_bytes == 1);
}
