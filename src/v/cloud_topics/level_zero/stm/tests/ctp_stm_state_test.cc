/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/stm/ctp_stm_state.h"
#include "cloud_topics/types.h"
#include "gtest/gtest.h"
#include "test_utils/test.h"
#include "utils/uuid.h"

#include <gtest/gtest.h>

#include <algorithm>

namespace ct = cloud_topics;
namespace {

TEST(ctp_stm_state_test, initial_state) {
    ct::ctp_stm_state state;
    EXPECT_FALSE(state.get_max_epoch().has_value());
    EXPECT_FALSE(state.get_max_seen_epoch().has_value());
    EXPECT_FALSE(state.get_last_reconciled_offset().has_value());
    EXPECT_FALSE(state.get_last_reconciled_log_offset().has_value());
    EXPECT_EQ(state.get_max_collectible_offset(), model::offset::min());
}

TEST(ctp_stm_state_test, advance_max_seen_epoch) {
    ct::ctp_stm_state state;
    ct::cluster_epoch epoch1(10);
    ct::cluster_epoch epoch2(20);
    ct::cluster_epoch epoch3(5);

    state.advance_max_seen_epoch(epoch1);
    EXPECT_EQ(state.get_max_seen_epoch().value(), epoch1);

    state.advance_max_seen_epoch(epoch2);
    EXPECT_EQ(state.get_max_seen_epoch().value(), epoch2);

    // Should not go backwards
    state.advance_max_seen_epoch(epoch3);
    EXPECT_EQ(state.get_max_seen_epoch().value(), epoch2);
}

TEST(ctp_stm_state_test, advance_epoch) {
    ct::ctp_stm_state state;
    ct::cluster_epoch epoch1(15);
    ct::cluster_epoch epoch2(25);
    ct::cluster_epoch epoch3(10);

    state.advance_epoch(epoch1, model::offset(1));
    EXPECT_EQ(state.get_max_epoch().value(), epoch1);
    EXPECT_EQ(state.get_max_seen_epoch().value(), epoch1);

    state.advance_epoch(epoch2, model::offset(2));
    EXPECT_EQ(state.get_max_epoch().value(), epoch2);
    EXPECT_EQ(state.get_max_seen_epoch().value(), epoch2);

    // Should not go backwards
    state.advance_epoch(epoch3, model::offset(3));
    EXPECT_EQ(state.get_max_epoch().value(), epoch2);
    EXPECT_EQ(state.get_max_seen_epoch().value(), epoch2);
}

TEST(ctp_stm_state_test, advance_epoch_on_a_follower) {
    // On a follower the max_seen_epoch should also be updated
    ct::ctp_stm_state state;
    ct::cluster_epoch advance_epoch(20);

    state.advance_epoch(advance_epoch, model::offset(1));

    EXPECT_EQ(state.get_max_seen_epoch().value(), advance_epoch);
    EXPECT_EQ(state.get_max_epoch().value(), advance_epoch);
}

TEST(ctp_stm_state_test, advance_last_reconciled_offset) {
    ct::ctp_stm_state state;
    kafka::offset kafka_offset1(100);
    model::offset model_offset1(200);
    // Out of order offsets
    kafka::offset kafka_offset2(50);
    model::offset model_offset2(100);

    state.advance_last_reconciled_offset(kafka_offset1, model_offset1);
    EXPECT_EQ(state.get_last_reconciled_offset().value(), kafka_offset1);
    EXPECT_EQ(state.get_last_reconciled_log_offset().value(), model_offset1);

    // Should not go backwards
    state.advance_last_reconciled_offset(kafka_offset2, model_offset2);
    EXPECT_EQ(state.get_last_reconciled_offset().value(), kafka_offset1);
    EXPECT_EQ(state.get_last_reconciled_log_offset().value(), model_offset1);
}

TEST(ctp_stm_state_test, get_max_collectible_offset) {
    ct::ctp_stm_state state;

    EXPECT_EQ(state.get_max_collectible_offset(), model::offset::min());

    model::offset log_offset(500);
    state.advance_last_reconciled_offset(kafka::offset(300), log_offset);
    EXPECT_EQ(state.get_max_collectible_offset(), log_offset);
}

TEST(ctp_stm_state_test, advance_lro_updates_min_epoch) {
    ct::ctp_stm_state state;
    ct::cluster_epoch epoch1(10);
    ct::cluster_epoch epoch2(20);

    // Initially min epoch is not set
    EXPECT_FALSE(state.estimate_min_epoch().has_value());

    state.advance_epoch(epoch1, model::offset(1));
    EXPECT_TRUE(state.estimate_min_epoch().has_value());
    EXPECT_EQ(state.estimate_min_epoch().value(), epoch1);

    state.advance_epoch(epoch2, model::offset(5));
    EXPECT_EQ(state.estimate_min_epoch().value(), epoch1);

    // Advance LRO past the offset of epoch1
    state.advance_last_reconciled_offset(kafka::offset(100), model::offset(2));
    EXPECT_EQ(state.estimate_min_epoch().value(), epoch1);

    // Advance LRO past the offset of epoch2
    state.advance_last_reconciled_offset(kafka::offset(200), model::offset(6));
    EXPECT_EQ(state.estimate_min_epoch().value(), epoch2);
}

TEST(ctp_stm_state_test, advance_start_offset) {
    ct::ctp_stm_state state;

    EXPECT_EQ(state.start_offset(), kafka::offset{0});
    state.advance_last_reconciled_offset(kafka::offset(5), model::offset(5));

    state.set_start_offset(kafka::offset{3});
    EXPECT_EQ(state.start_offset(), kafka::offset{3});

    state.set_start_offset(kafka::offset{1});
    EXPECT_EQ(state.start_offset(), kafka::offset{3});

    state.set_start_offset(kafka::offset{5});
    EXPECT_EQ(state.start_offset(), kafka::offset{5});

    state.set_start_offset(kafka::offset{5});
    EXPECT_EQ(state.start_offset(), kafka::offset{5});

    state.set_start_offset(kafka::offset{2});
    EXPECT_EQ(state.start_offset(), kafka::offset{5});
}

} // anonymous namespace
