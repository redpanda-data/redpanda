/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/pacemaker.h"
#include "coproc/script_manager.h"
#include "coproc/service.h"
#include "coproc/tests/utils/coproc_test_fixture.h"
#include "coproc/tests/utils/helpers.h"
#include "coproc/types.h"
#include "rpc/test/rpc_integration_fixture.h"
#include "ssx/future-util.h"
#include "test_utils/fixture.h"

class script_manager_service_fixture : public coproc_test_fixture {
public:
    // This function ensures that the internals of the coproc::active_mappings
    // caches are laid out how they are expected to be i.e. the right ntps
    // existing on the correct cores determined by the _shard_table
    ss::future<bool> coproc_validate() {
        return app.pacemaker.map_reduce0(
          [this](coproc::pacemaker& p) {
              return ssx::async_all_of(
                get_layout().cbegin(),
                get_layout().cend(),
                [this, &p](const log_layout_map::value_type& pair) {
                    auto logs = app.storage.local().log_mgr().get(pair.first);
                    return ss::do_with(
                      std::move(logs), [&p](decltype(logs)& logs) {
                          return ssx::async_all_of(
                            logs.cbegin(), logs.cend(), [&p](const auto& e) {
                                return p.ntp_is_registered(e.first);
                            });
                      });
                });
          },
          true,
          std::logical_or<>());
    }
};

FIXTURE_TEST(test_coproc_topic_dne, script_manager_service_fixture) {
    startup({{make_ts("foo"), 5}}).get();
    const auto resp = register_coprocessors(
                        sm_client(), {make_enable_req(1234, {{"bar", l}})})
                        .get0()
                        .value()
                        .data;
    BOOST_CHECK_EQUAL(resp.acks.size(), 1);
    BOOST_CHECK_EQUAL(resp.acks[0].first, coproc::script_id(1234));
    BOOST_CHECK_EQUAL(resp.acks[0].second.size(), 1);
    BOOST_CHECK_EQUAL(resp.acks[0].second[0], erc::topic_does_not_exist);
}

FIXTURE_TEST(test_coproc_no_topics, script_manager_service_fixture) {
    startup({{make_ts("foo"), 5}}).get();
    const auto resp = register_coprocessors(
                        sm_client(), {make_enable_req(54, {})})
                        .get0()
                        .value()
                        .data;
    BOOST_CHECK_EQUAL(resp.acks.size(), 1);
}

// This test fixture tests the edge cases, i.e. situations that should fail
FIXTURE_TEST(test_coproc_invalid_topics, script_manager_service_fixture) {
    startup({{make_ts("foo"), 5}, {make_ts("bar"), 3}, {make_ts("baz"), 18}})
      .get();
    // This string is more then 249 chars, should be an error to attempt to
    // register
    static const ss::sstring too_long_topic(
      "abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_"
      "abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_"
      "abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_"
      "abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_abcedgtpr_"
      "abcedgtpr_abcedgtpr_");

    const auto resp
      = register_coprocessors(
          sm_client(),
          {make_enable_req(
            999,
            {{".", l}, {"..", l}, {"foo", l}, {"", l}, {too_long_topic, l}})})
          .get0()
          .value()
          .data;
    BOOST_CHECK_EQUAL(resp.acks.size(), 1);
    const auto& first_acks = resp.acks[0].second;
    BOOST_CHECK_EQUAL(first_acks.size(), 5);
    BOOST_CHECK_EQUAL(resp.acks[0].first, coproc::script_id(999));
    BOOST_CHECK_EQUAL(first_acks[0], erc::invalid_topic);
    BOOST_CHECK_EQUAL(first_acks[1], erc::invalid_topic);
    BOOST_CHECK_EQUAL(first_acks[2], erc::success);
    BOOST_CHECK_EQUAL(first_acks[3], erc::invalid_topic);
    BOOST_CHECK_EQUAL(first_acks[4], erc::invalid_topic);
    BOOST_CHECK(coproc_validate().get0());
}

FIXTURE_TEST(
  test_coproc_reg_materialized_topic, script_manager_service_fixture) {
    startup({{make_ts("foo"), 6}, {make_ts("bar"), 3}, {make_ts("baz"), 18}})
      .get();

    const auto resp = register_coprocessors(
                        sm_client(),
                        {make_enable_req(2313, {{"foo", l}, {"foo.$bar$", l}})})
                        .get0()
                        .value()
                        .data;
    BOOST_CHECK_EQUAL(resp.acks.size(), 1);
    const auto& acks = resp.acks[0].second;
    BOOST_CHECK_EQUAL(coproc::script_id(2313), resp.acks[0].first);
    BOOST_CHECK_EQUAL(acks.size(), 2);
    BOOST_CHECK_EQUAL(acks[0], erc::success);
    BOOST_CHECK_EQUAL(acks[1], erc::materialized_topic);
    BOOST_CHECK(coproc_validate().get0());
}

FIXTURE_TEST(
  test_coproc_script_id_already_exists, script_manager_service_fixture) {
    const uint32_t script_id = 55431;
    startup({{make_ts("foo"), 5}, {make_ts("bar"), 3}, {make_ts("baz"), 18}})
      .get();

    /// First register a copro
    const auto init = register_coprocessors(
                        sm_client(), {make_enable_req(script_id, {{"foo", l}})})
                        .get0()
                        .value()
                        .data;
    BOOST_CHECK_EQUAL(init.acks.size(), 1);
    BOOST_CHECK_EQUAL(init.acks[0].first, coproc::script_id(script_id));
    BOOST_CHECK_EQUAL(init.acks[0].second.size(), 1);
    BOOST_CHECK(init.acks[0].second[0] == erc::success);

    /// Then attempt to re-register one with the same id
    const auto resp = register_coprocessors(
                        sm_client(),
                        {make_enable_req(3289, {{"foo", l}, {"bar", e}}),
                         {make_enable_req(script_id, {{"nogo", l}})}})
                        .get0()
                        .value()
                        .data;
    BOOST_CHECK(coproc_validate().get0());
    BOOST_CHECK_EQUAL(resp.acks.size(), 2);
    const auto& acks_1 = resp.acks[0].second;
    const auto& acks_2 = resp.acks[1].second;
    BOOST_CHECK_EQUAL(acks_1.size(), 2);
    BOOST_CHECK_EQUAL(acks_2.size(), 1);
    BOOST_CHECK_EQUAL(resp.acks[0].first, coproc::script_id(3289));
    BOOST_CHECK_EQUAL(resp.acks[1].first, coproc::script_id(script_id));
    BOOST_CHECK_EQUAL(acks_1[0], erc::success);
    BOOST_CHECK_EQUAL(acks_1[1], erc::invalid_ingestion_policy);
    BOOST_CHECK_EQUAL(acks_2[0], erc::script_id_already_exists);
    BOOST_CHECK(coproc_validate().get0());
}

FIXTURE_TEST(test_coproc_topics, script_manager_service_fixture) {
    startup({{make_ts("foo"), 8}, {make_ts("bar"), 2}, {make_ts("baz"), 18}})
      .get();

    // 1. Attempt to register foo, bar and baz
    const auto resp = register_coprocessors(
                        sm_client(),
                        {make_enable_req(
                           523, {{"foo", l}, {"bar", l}, {"baz", l}}),
                         make_enable_req(332, {{"foo", l}})})
                        .get0()
                        .value()
                        .data;
    // 2. ensure server replied with success for all topics
    BOOST_CHECK_EQUAL(resp.acks.size(), 2);
    const auto& first_acks = resp.acks[0].second;
    const auto& second_acks = resp.acks[1].second;
    BOOST_CHECK_EQUAL(resp.acks[0].first, coproc::script_id(523));
    BOOST_CHECK_EQUAL(resp.acks[1].first, coproc::script_id(332));
    BOOST_CHECK_EQUAL(first_acks.size(), 3);
    BOOST_CHECK_EQUAL(first_acks[0], erc::success);
    BOOST_CHECK_EQUAL(first_acks[1], erc::success);
    BOOST_CHECK_EQUAL(first_acks[2], erc::success);
    BOOST_CHECK_EQUAL(second_acks.size(), 1);
    BOOST_CHECK_EQUAL(second_acks[0], erc::success);

    // 3. Verify they actually exist in the 'source_topics' struct
    BOOST_CHECK(coproc_validate().get0());

    // 4-6. Attempt to deregister some
    const auto disable_acks
      = deregister_coprocessors(sm_client(), {523}).get0().value().data;
    BOOST_CHECK_EQUAL(disable_acks.acks.size(), 1);
    BOOST_CHECK_EQUAL(disable_acks.acks[0], drc::success);
    BOOST_CHECK(coproc_validate().get0());

    const auto disable_acks2
      = deregister_coprocessors(sm_client(), {332}).get0().value().data;
    BOOST_CHECK_EQUAL(disable_acks2.acks.size(), 1);
    BOOST_CHECK_EQUAL(disable_acks2.acks[0], drc::success);
    BOOST_CHECK(coproc_validate().get0());
}
