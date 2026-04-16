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
#include "model/fundamental.h"
#include "random/generators.h"
#include "test_utils/test.h"
#include "utils/uuid.h"

#include <gtest/gtest.h>

#include <algorithm>

namespace ct = cloud_topics;
namespace {

TEST(ctp_stm_state_test, initial_state) {
    ct::ctp_stm_state state;
    EXPECT_FALSE(state.get_max_applied_epoch().has_value());
    EXPECT_FALSE(state.get_max_seen_epoch(model::term_id(1)).has_value());
    EXPECT_FALSE(state.get_last_reconciled_offset().has_value());
    EXPECT_FALSE(state.get_last_reconciled_log_offset().has_value());
    EXPECT_EQ(state.get_max_collectible_offset(), model::offset::min());
}

TEST(ctp_stm_state_test, advance_max_seen_epoch) {
    ct::ctp_stm_state state;
    ct::cluster_epoch epoch1(10);
    ct::cluster_epoch epoch2(20);
    ct::cluster_epoch epoch3(5);
    model::term_id term(1);

    state.advance_max_seen_epoch(term, epoch1);
    EXPECT_EQ(state.get_max_seen_epoch(term).value(), epoch1);

    state.advance_max_seen_epoch(term, epoch2);
    EXPECT_EQ(state.get_max_seen_epoch(term).value(), epoch2);

    // Should not go backwards
    state.advance_max_seen_epoch(term, epoch3);
    EXPECT_EQ(state.get_max_seen_epoch(term).value(), epoch2);
}

TEST(ctp_stm_state_test, advance_epoch) {
    ct::ctp_stm_state state;
    ct::cluster_epoch epoch1(15);
    ct::cluster_epoch epoch2(25);
    ct::cluster_epoch epoch3(10);
    model::term_id term(1);

    state.advance_epoch(epoch1, model::offset(1));
    EXPECT_EQ(state.get_max_applied_epoch().value(), epoch1);
    // advance_epoch does not update the seen window
    EXPECT_FALSE(state.get_max_seen_epoch(term).has_value());

    state.advance_epoch(epoch2, model::offset(2));
    EXPECT_EQ(state.get_max_applied_epoch().value(), epoch2);
    EXPECT_FALSE(state.get_max_seen_epoch(term).has_value());

    // Should not go backwards
    state.advance_epoch(epoch3, model::offset(3));
    EXPECT_EQ(state.get_max_applied_epoch().value(), epoch2);
    EXPECT_FALSE(state.get_max_seen_epoch(term).has_value());
}

TEST(ctp_stm_state_test, advance_epoch_on_a_follower) {
    // On a follower, advance_epoch only updates the applied window,
    // not the seen window. The seen window is only managed through
    // advance_max_seen_epoch on the leader path.
    ct::ctp_stm_state state;
    ct::cluster_epoch advance_epoch(20);

    state.advance_epoch(advance_epoch, model::offset(1));

    EXPECT_FALSE(state.get_max_seen_epoch(model::term_id{1}).has_value());
    EXPECT_EQ(state.get_max_applied_epoch().value(), advance_epoch);
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
    ct::cluster_epoch epoch3(30);

    // Initially min epoch is not set
    EXPECT_FALSE(state.estimate_min_epoch().has_value());

    state.advance_epoch(epoch1, model::offset(1));
    EXPECT_EQ(state.estimate_min_epoch(), epoch1);

    state.advance_epoch(epoch2, model::offset(5));
    EXPECT_EQ(state.estimate_min_epoch(), epoch1);

    state.advance_epoch(epoch3, model::offset(10));
    EXPECT_EQ(state.estimate_min_epoch(), epoch1);

    // Advance LRO past the offset of epoch1
    state.advance_last_reconciled_offset(kafka::offset(100), model::offset(2));
    EXPECT_EQ(state.estimate_min_epoch(), epoch1);

    // Advance LRO past the offset of epoch2
    state.advance_last_reconciled_offset(kafka::offset(200), model::offset(6));
    EXPECT_EQ(state.estimate_min_epoch(), epoch1);

    // Advance LRO past the offset of epoch3
    state.advance_last_reconciled_offset(kafka::offset(300), model::offset(11));
    EXPECT_EQ(state.estimate_min_epoch(), epoch2);
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

ct::cluster_epoch operator""_epoch(unsigned long long v) {
    return ct::cluster_epoch(static_cast<int64_t>(v));
}

kafka::offset operator""_offset(unsigned long long v) {
    return kafka::offset(static_cast<int64_t>(v));
}

TEST(ctp_stm_state_test, sliding_window_issue) {
    ct::ctp_stm_state state;
    model::term_id term(1);

    kafka::offset hwm = 0_offset;

    auto estimate_inactive_epoch =
      [&state] -> std::optional<ct::cluster_epoch> {
        return state.estimate_inactive_epoch();
    };

    auto apply_replicated = [&state, &hwm](ct::cluster_epoch epoch) {
        state.advance_epoch(epoch, kafka::offset_cast(hwm));
        hwm++;
    };

    auto reconcile = [&state](kafka::offset offset) {
        state.advance_last_reconciled_offset(
          offset, kafka::offset_cast(offset));
    };

    // Our start state, we can't GC anything and we have to
    // get the write lock before we can start our window
    EXPECT_EQ(estimate_inactive_epoch(), std::nullopt);
    // Start our epochs at 2
    EXPECT_FALSE(state.epoch_in_window(term, 2_epoch));

    // Write lock grabbed, max epoch can be advanced!
    state.advance_max_seen_epoch(term, 2_epoch);

    // Now epoch 0 is in the window
    EXPECT_TRUE(state.epoch_in_window(term, 2_epoch));
    // Epoch 1 is not in the window
    EXPECT_FALSE(state.epoch_in_window(term, 1_epoch));
    // Nor is 3
    EXPECT_FALSE(state.epoch_in_window(term, 3_epoch));

    // Now the batch that was replicated with offset 0
    apply_replicated(2_epoch);

    // We can GC anything below our initial epoch, we enforce nothing is before
    // it
    EXPECT_EQ(estimate_inactive_epoch(), 1_epoch);

    // Let's now add another batch at epoch 2
    EXPECT_TRUE(state.epoch_in_window(term, 2_epoch));
    apply_replicated(2_epoch);

    // Reconciler now runs
    reconcile(hwm);

    // Our epoch window hasn't moved
    EXPECT_EQ(estimate_inactive_epoch(), 1_epoch);

    EXPECT_FALSE(state.epoch_in_window(term, 5_epoch));

    // Epoch is bumped, our window should now be [2, 5]
    state.advance_max_seen_epoch(term, 5_epoch);

    // This is our new epoch
    EXPECT_TRUE(state.epoch_in_window(term, 5_epoch));
    // Our previous epoch is good still
    EXPECT_TRUE(state.epoch_in_window(term, 2_epoch));
    // And so is something in between (unlikely in real life, but just to show)
    EXPECT_TRUE(state.epoch_in_window(term, 3_epoch));
    // Something below is still bad
    EXPECT_FALSE(state.epoch_in_window(term, 1_epoch));

    // Still not safe to GC, we accept stuff at epoch 0
    EXPECT_EQ(estimate_inactive_epoch(), 1_epoch);

    apply_replicated(5_epoch);

    // GC window still the same
    EXPECT_EQ(estimate_inactive_epoch(), 1_epoch);

    // We can still replicate an epoch at 2
    apply_replicated(2_epoch);

    EXPECT_EQ(estimate_inactive_epoch(), 1_epoch);

    // Now we start to replicate to the epoch to 10 (write lock grabbed)
    state.advance_max_seen_epoch(term, 10_epoch);
    EXPECT_TRUE(state.epoch_in_window(term, 10_epoch));
    EXPECT_TRUE(state.epoch_in_window(term, 5_epoch));
    EXPECT_TRUE(state.epoch_in_window(term, 8_epoch));
    EXPECT_FALSE(state.epoch_in_window(term, 0_epoch));
    EXPECT_FALSE(state.epoch_in_window(term, 4_epoch));

    apply_replicated(10_epoch);

    EXPECT_EQ(estimate_inactive_epoch(), 1_epoch);

    // Reconcile
    reconcile(hwm);

    // NOW we can GC everything below the window
    EXPECT_EQ(estimate_inactive_epoch(), 4_epoch);

    // Now we bump the window again, but haven't replicated it yet.
    state.advance_max_seen_epoch(term, 15_epoch);
    EXPECT_TRUE(state.epoch_in_window(term, 10_epoch));
    EXPECT_TRUE(state.epoch_in_window(term, 15_epoch));
    EXPECT_TRUE(state.epoch_in_window(term, 12_epoch));
    EXPECT_FALSE(state.epoch_in_window(term, 9_epoch));

    EXPECT_EQ(estimate_inactive_epoch(), 4_epoch);

    apply_replicated(15_epoch);

    // reconciliation has not run, it's not save to bump the epoch yet.
    // So we should keep the epoch at 4, even if the window advances
    EXPECT_EQ(estimate_inactive_epoch(), 4_epoch);

    reconcile(hwm);

    // Now the inactive epoch can be advanced because we're reconciled up to it.
    EXPECT_EQ(estimate_inactive_epoch(), 9_epoch);
}

TEST(ctp_stm_state_test, l0_simulation) {
    struct uploaded_l0_file_batch {
        ct::cluster_epoch epoch;
    };
    struct placeholder_batch {
        ct::cluster_epoch epoch;
        kafka::offset offset;
    };
    struct l0_simulation_state {
        // The state for the STM
        ct::ctp_stm_state stm;
        // Batches that are uploaded, but have not yet been fenced nor
        // replicated, so this is basically just a queue of epochs that will
        // be applied to the log. The epochs may not be in order because they
        // may come from different shards/brokers.
        ss::chunked_fifo<uploaded_l0_file_batch> uploaded_batches;
        // Placeholders that are replicated, but not yet applied to the
        // to the STM.
        ss::chunked_fifo<placeholder_batch> unapplied_placeholders;
        // All the placeholders in the log
        ss::chunked_fifo<placeholder_batch> log_placeholders;
        // hwm for the partition
        kafka::offset hwm = 0_offset;

        testing::AssertionResult validate() {
            // The main thing we want to validate is that there are no batches
            // in the log that are GC eligable but have not been reconciled.
            auto lro = stm.get_last_reconciled_offset();
            // Remove reconciled batches
            auto non_reconciled_batches = std::views::filter(
              log_placeholders, [lro](const placeholder_batch& batch) {
                  return batch.offset > lro.value_or(kafka::offset::min());
              });
            // Find the min active epoch in the log
            auto min_active_epoch = std::ranges::min_element(
              non_reconciled_batches,
              std::less<>(),
              [](const placeholder_batch& batch) { return batch.epoch; });
            // What do we say we can GC?
            auto inactive_epoch = stm.estimate_inactive_epoch();
            if (min_active_epoch == non_reconciled_batches.end()) {
                // There is nothing in the log unreconcilded, we should not yet
                // have an inactive epoch above what is yet to be replicated.
                if (!uploaded_batches.empty()) {
                    auto it = std::ranges::min_element(
                      uploaded_batches, std::less<>(), [](const auto& batch) {
                          return batch.epoch;
                      });
                    if (inactive_epoch > it->epoch) {
                        return testing::AssertionFailure() << fmt::format(
                                 "expected inactive epoch ({}) to not be above "
                                 "active epochs {}",
                                 inactive_epoch.value(),
                                 it->epoch);
                    }
                }
            } else if (inactive_epoch >= min_active_epoch->epoch) {
                return testing::AssertionFailure() << fmt::format(
                         "expected the min log epoch ({}) to be all be "
                         "above inactive_epoch: {}",
                         min_active_epoch->epoch,
                         inactive_epoch.value());
            }
            return testing::AssertionSuccess();
        }
    };
    // We simulate the operations for a single partition in l0
    l0_simulation_state universe;
    model::term_id term(1);

    {
        std::vector<uploaded_l0_file_batch> batches{
          // clang-format off
          {.epoch = 5_epoch},
          {.epoch = 5_epoch},
          {.epoch = 6_epoch},
          {.epoch = 8_epoch},
          {.epoch = 6_epoch},
          {.epoch = 8_epoch},
          {.epoch = 8_epoch},
          {.epoch = 10_epoch},
          {.epoch = 8_epoch},
          {.epoch = 9_epoch},
          {.epoch = 10_epoch},
          {.epoch = 8_epoch},
          {.epoch = 10_epoch},
          {.epoch = 10_epoch},
          {.epoch = 10_epoch},
          {.epoch = 8_epoch},
          {.epoch = 8_epoch},
          {.epoch = 15_epoch},
          {.epoch = 15_epoch},
          {.epoch = 10_epoch},
          {.epoch = 12_epoch},
          {.epoch = 10_epoch},
          {.epoch = 15_epoch},
          // clang-format on
        };
        std::ranges::copy(
          batches, std::back_inserter(universe.uploaded_batches));
    }

    std::vector<std::string> oplog;

    while (true) {
        // Always check our invariants
        ASSERT_TRUE(universe.validate())
          << fmt::format("operations:\n{}", fmt::join(oplog, "\n"));
        // Possible operations we can perform in the universe.
        std::vector<std::function<void()>> possible_operations;
        // If there are batches to upload, let's do it.
        if (!universe.uploaded_batches.empty()) {
            possible_operations.emplace_back([&universe, &oplog, term] {
                auto batch = universe.uploaded_batches.front();
                universe.uploaded_batches.pop_front();
                if (!universe.stm.epoch_in_window(term, batch.epoch)) {
                    universe.stm.advance_max_seen_epoch(term, batch.epoch);
                    ASSERT_TRUE(
                      universe.stm.epoch_in_window(term, batch.epoch));
                }
                placeholder_batch placeholder{
                  .epoch = batch.epoch, .offset = universe.hwm++};
                universe.unapplied_placeholders.push_back(placeholder);
                universe.log_placeholders.push_back(placeholder);
                oplog.push_back(
                  fmt::format(
                    "replicated batch {} with epoch {}",
                    placeholder.offset,
                    placeholder.epoch));
            });
        }
        // Apply to STM if we haven't yet
        if (!universe.unapplied_placeholders.empty()) {
            possible_operations.emplace_back([&universe, &oplog] {
                auto batch = universe.unapplied_placeholders.front();
                universe.unapplied_placeholders.pop_front();
                // Apply to the STM
                universe.stm.advance_epoch(
                  batch.epoch, kafka::offset_cast(batch.offset));
                oplog.push_back(
                  fmt::format(
                    "applied batch {} with epoch {}",
                    batch.offset,
                    batch.epoch));
            });
        }
        // Run reconciliation
        auto last_offset = kafka::prev_offset(universe.hwm);
        auto lro = universe.stm.get_last_reconciled_offset().value_or(
          kafka::offset::min());
        if (lro < last_offset) {
            possible_operations.emplace_back(
              [&universe, &oplog, lro, last_offset] {
                  auto new_lro = random_generators::get_int(
                    kafka::next_offset(lro)(), last_offset());
                  universe.stm.advance_last_reconciled_offset(
                    kafka::offset(new_lro), model::offset(new_lro));
                  oplog.push_back(fmt::format("reconciled to {}", new_lro));
              });
        }
        if (possible_operations.empty()) {
            // No more possible operations
            fmt::print(
              std::cerr,
              "min_epoch: {}\n",
              universe.stm.estimate_inactive_epoch().value_or(
                ct::cluster_epoch::min()));
            fmt::print(std::cerr, "operations:\n{}\n", fmt::join(oplog, "\n"));
            return;
        }
        // Run a random operation
        auto op = random_generators::random_choice(possible_operations);
        op();
    }
}

} // anonymous namespace
