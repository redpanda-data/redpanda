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
#include "model/fundamental.h"
#include "random/generators.h"
#include "serde/rw/envelope.h"
#include "serde/rw/optional.h"
#include "serde/rw/rw.h"
#include "serde/rw/vector.h"

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
    // Something in between is not a window boundary: it can only be admitted
    // by moving the window min (write lock in fence_epoch).
    EXPECT_FALSE(state.epoch_in_window(term, 3_epoch));
    EXPECT_TRUE(state.epoch_moves_window(term, 3_epoch));
    // Something below is still bad
    EXPECT_FALSE(state.epoch_in_window(term, 1_epoch));
    EXPECT_FALSE(state.epoch_moves_window(term, 1_epoch));

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
    // Interior epochs need a window move.
    EXPECT_FALSE(state.epoch_in_window(term, 8_epoch));
    EXPECT_TRUE(state.epoch_moves_window(term, 8_epoch));
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
    EXPECT_FALSE(state.epoch_in_window(term, 12_epoch));
    EXPECT_TRUE(state.epoch_moves_window(term, 12_epoch));
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

TEST(ctp_stm_state_test, below_max_fence_requires_applied_evidence) {
    // A fenced epoch bump whose batch never lands leaves a phantom lower
    // bound in the seen window. Admitting a below-max epoch is only sound if
    // some epoch batch is known to precede the max-seen epoch's first batch
    // in the log; otherwise the log's epoch window collapses to [max, max]
    // when the max applies and the below-max batch violates it.
    ct::ctp_stm_state state;
    model::term_id term(1);

    // Two fence-time bumps, no batch lands for either.
    state.advance_max_seen_epoch(term, 132_epoch);
    state.advance_max_seen_epoch(term, 141_epoch);

    // At-max admission is always sound.
    EXPECT_TRUE(state.epoch_in_window(term, 141_epoch));
    // Below-max admission has no applied evidence: reject.
    EXPECT_FALSE(state.epoch_in_window(term, 132_epoch));

    // The max epoch lands as the first batch in the log: the log window is
    // [141, 141], so 132 must still be rejected.
    state.advance_epoch(141_epoch, model::offset{0});
    EXPECT_FALSE(state.epoch_in_window(term, 132_epoch));
    EXPECT_TRUE(state.epoch_in_window(term, 141_epoch));
}

TEST(ctp_stm_state_test, below_max_fence_allowed_with_applied_evidence) {
    // The legitimate in-flight case: the previous epoch's batch landed and
    // applied before the bump, so a straggler at that epoch stays admissible
    // both before and after the max epoch's batch applies.
    ct::ctp_stm_state state;
    model::term_id term(1);

    state.advance_max_seen_epoch(term, 132_epoch);
    state.advance_epoch(132_epoch, model::offset{0});

    state.advance_max_seen_epoch(term, 141_epoch);
    // An applied 132 batch precedes any (future) 141 batch in the log.
    EXPECT_TRUE(state.epoch_in_window(term, 132_epoch));

    state.advance_epoch(141_epoch, model::offset{1});
    // The log window is [132, 141]: 132 remains admissible.
    EXPECT_TRUE(state.epoch_in_window(term, 132_epoch));
    EXPECT_TRUE(state.epoch_in_window(term, 141_epoch));
    EXPECT_FALSE(state.epoch_in_window(term, 131_epoch));
}

TEST(ctp_stm_state_test, seen_window_resets_on_term_change_despite_stale_max) {
    // A fenced bump whose batch never lands can leave a very large
    // _max_seen_epoch behind. A bump in a newer term must reset the window
    // even when the new epoch is below the stale max, otherwise the stale
    // window blocks the reset and the new term keeps fencing against it.
    ct::ctp_stm_state state;
    model::term_id term1(1);
    model::term_id term2(2);

    state.advance_max_seen_epoch(term1, 100_epoch);
    EXPECT_EQ(state.get_max_seen_epoch(term1), 100_epoch);

    // Epochs 7 and 8 land and apply (e.g. replicated by another leader).
    state.advance_epoch(7_epoch, model::offset{0});
    state.advance_epoch(8_epoch, model::offset{1});

    // In the new term the stale window is invisible...
    EXPECT_EQ(state.get_max_seen_epoch(term2), std::nullopt);
    // ...and a bump below the stale max resets it.
    state.advance_max_seen_epoch(term2, 9_epoch);
    EXPECT_EQ(state.get_max_seen_epoch(term2), 9_epoch);
    EXPECT_TRUE(state.epoch_in_window(term2, 9_epoch));
    // Pre-bump epochs are fenced off until the bump batch applies.
    EXPECT_FALSE(state.epoch_in_window(term2, 8_epoch));
}

TEST(ctp_stm_state_test, stale_epoch_rejected_after_seen_applied_divergence) {
    // A fence-time bump whose batch never lands (failed replicate) diverges
    // the seen window from the log content. Interior epochs that land
    // afterwards ratchet the log's epoch window upwards ([10, 12], then
    // [12, 13]); an epoch below it must not be admitted: replicated, it
    // would land above the batches that moved the log window past it and
    // trip the epoch_window_checker vassert in do_apply on every replica
    // (and break the GC epoch lower bound).
    ct::ctp_stm_state state;
    model::term_id term(1);

    state.advance_max_seen_epoch(term, 10_epoch);
    state.advance_epoch(10_epoch, model::offset{0});
    // Bump to 14; the bump batch is discarded.
    state.advance_max_seen_epoch(term, 14_epoch);
    // Interior epochs 12 and 13 land.
    state.advance_epoch(12_epoch, model::offset{1});
    state.advance_epoch(13_epoch, model::offset{2});

    // The log window is [12, 13]: epochs below it are not admissible.
    EXPECT_FALSE(state.epoch_in_window(term, 11_epoch));
    EXPECT_FALSE(state.epoch_in_window(term, 12_epoch));
    // The pending max-seen epoch remains admissible.
    EXPECT_TRUE(state.epoch_in_window(term, 14_epoch));
}

TEST(ctp_stm_state_test, interior_epoch_moves_window_min) {
    ct::ctp_stm_state state;
    model::term_id term(1);

    // Epoch 10 lands and applies, then a bump to 14 whose batch is still in
    // flight.
    state.advance_max_seen_epoch(term, 10_epoch);
    state.advance_epoch(10_epoch, model::offset{0});
    state.advance_max_seen_epoch(term, 14_epoch);

    // Window boundaries are admissible under a read fence.
    EXPECT_TRUE(state.epoch_in_window(term, 10_epoch));
    EXPECT_TRUE(state.epoch_in_window(term, 14_epoch));
    // Interior epochs are not: they require a window move.
    EXPECT_FALSE(state.epoch_in_window(term, 12_epoch));
    EXPECT_TRUE(state.epoch_moves_window(term, 12_epoch));

    state.move_seen_window(term, 12_epoch);
    // 12 became the window min...
    EXPECT_TRUE(state.epoch_in_window(term, 12_epoch));
    // ...and fenced off everything below it.
    EXPECT_FALSE(state.epoch_in_window(term, 10_epoch));
    EXPECT_FALSE(state.epoch_in_window(term, 11_epoch));
    EXPECT_FALSE(state.epoch_moves_window(term, 11_epoch));
}

TEST(ctp_stm_state_test, stale_epoch_rejected_after_interior_window_moves) {
    // Regression: a bump whose batch never lands, followed by interior
    // epochs that land one after another, ratchets the log's epoch window
    // upwards ([10, 12], then [12, 13]). An epoch below the last interior
    // admission must be rejected: replicated, it would land above the
    // batches that moved the log window past it, tripping the
    // epoch_window_checker vassert in do_apply on every replica and breaking
    // the GC epoch lower bound.
    ct::ctp_stm_state state;
    model::term_id term(1);

    state.advance_max_seen_epoch(term, 10_epoch);
    state.advance_epoch(10_epoch, model::offset{0});
    // Bump to 14; the bump batch is discarded (replicate failed).
    state.advance_max_seen_epoch(term, 14_epoch);

    // Interior epochs 12 and 13 are admitted by moving the window min and
    // their batches land: the log window becomes [12, 13].
    ASSERT_TRUE(state.epoch_moves_window(term, 12_epoch));
    state.move_seen_window(term, 12_epoch);
    state.advance_epoch(12_epoch, model::offset{1});
    ASSERT_TRUE(state.epoch_moves_window(term, 13_epoch));
    state.move_seen_window(term, 13_epoch);
    state.advance_epoch(13_epoch, model::offset{2});

    // Epoch 11 is below the log window: neither admissible nor able to move
    // the window.
    EXPECT_FALSE(state.epoch_in_window(term, 11_epoch));
    EXPECT_FALSE(state.epoch_moves_window(term, 11_epoch));
    // Epoch 12 is below the log window max as well: only the window
    // boundaries remain admissible.
    EXPECT_FALSE(state.epoch_in_window(term, 12_epoch));
    EXPECT_TRUE(state.epoch_in_window(term, 13_epoch));
    EXPECT_TRUE(state.epoch_in_window(term, 14_epoch));
}

TEST(ctp_stm_state_test, frozen_window_admits_applied_range) {
    // Once the max-seen epoch's batch has applied, nothing can move the
    // log's epoch window until the next bump, so everything within the
    // applied window is admissible without moving the seen window.
    ct::ctp_stm_state state;
    model::term_id term(1);

    state.advance_max_seen_epoch(term, 10_epoch);
    state.advance_epoch(10_epoch, model::offset{0});
    state.advance_max_seen_epoch(term, 14_epoch);
    state.advance_epoch(14_epoch, model::offset{1});

    // The log window is frozen at [10, 14].
    for (int64_t e = 10; e <= 14; ++e) {
        EXPECT_TRUE(state.epoch_in_window(term, ct::cluster_epoch{e})) << e;
        // No window move is needed (or allowed).
        EXPECT_FALSE(state.epoch_moves_window(term, ct::cluster_epoch{e})) << e;
    }
    EXPECT_FALSE(state.epoch_in_window(term, 9_epoch));
    EXPECT_FALSE(state.epoch_moves_window(term, 9_epoch));
    EXPECT_TRUE(state.epoch_moves_window(term, 15_epoch));
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
        // Batches that are fenced (admitted for replication) but have not
        // reached the log yet. The real system replicates them concurrently,
        // so they may land in any order; a seen-window move waits for all of
        // them to land first (the fence write lock drains in-flight
        // replications).
        std::vector<ct::cluster_epoch> fenced_batches;
        // Placeholders that are replicated, but not yet applied to the
        // to the STM.
        ss::chunked_fifo<placeholder_batch> unapplied_placeholders;
        // All the placeholders in the log
        ss::chunked_fifo<placeholder_batch> log_placeholders;
        // hwm for the partition
        kafka::offset hwm = 0_offset;
        // Mirrors epoch_window_checker: the sliding epoch window of the log
        // content that every appended batch has to satisfy (or every replica
        // dies applying it).
        ct::cluster_epoch checker_min = ct::cluster_epoch::min();
        ct::cluster_epoch checker_max = ct::cluster_epoch::min();

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
                // There is nothing in the log unreconciled, we should not yet
                // have an inactive epoch above what is yet to be replicated
                // (uploaded or fenced and in flight).
                std::optional<ct::cluster_epoch> min_pending;
                for (const auto& batch : uploaded_batches) {
                    min_pending = std::min(
                      min_pending.value_or(batch.epoch), batch.epoch);
                }
                for (auto epoch : fenced_batches) {
                    min_pending = std::min(min_pending.value_or(epoch), epoch);
                }
                if (min_pending.has_value() && inactive_epoch > *min_pending) {
                    return testing::AssertionFailure() << fmt::format(
                             "expected inactive epoch ({}) to not be above "
                             "active epochs {}",
                             inactive_epoch.value(),
                             *min_pending);
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
        // If there are batches to upload, fence the next one. Mirror the
        // fence_epoch branches: epochs admissible under the current window
        // take a read fence; epochs that require moving a window boundary
        // (bumping the max or raising the min) take the write lock, which
        // drains in-flight replications first - so such an admission is only
        // possible while nothing is in flight; everything else is rejected
        // (the producer would retry with a fresh epoch).
        if (!universe.uploaded_batches.empty()) {
            auto epoch = universe.uploaded_batches.front().epoch;
            bool in_window = universe.stm.epoch_in_window(term, epoch);
            bool moves = universe.stm.epoch_moves_window(term, epoch);
            if (in_window || (moves && universe.fenced_batches.empty())) {
                possible_operations.emplace_back(
                  [&universe, &oplog, term, epoch, in_window] {
                      universe.uploaded_batches.pop_front();
                      if (!in_window) {
                          universe.stm.move_seen_window(term, epoch);
                          ASSERT_TRUE(
                            universe.stm.epoch_in_window(term, epoch));
                      }
                      universe.fenced_batches.push_back(epoch);
                      oplog.push_back(
                        fmt::format("fenced batch with epoch {}", epoch));
                  });
            } else if (!moves) {
                possible_operations.emplace_back([&universe, &oplog, epoch] {
                    universe.uploaded_batches.pop_front();
                    oplog.push_back(
                      fmt::format("rejected batch with epoch {}", epoch));
                });
            }
            // Otherwise the admission is waiting for the write lock: the
            // replicate operations below drain the in-flight batches first.
        }
        // Replicate a fenced batch. In-flight batches can reach the log in
        // any order, so pick a random one.
        if (!universe.fenced_batches.empty()) {
            possible_operations.emplace_back([&universe, &oplog] {
                auto idx = random_generators::get_int<size_t>(
                  universe.fenced_batches.size() - 1);
                auto epoch = universe.fenced_batches[idx];
                universe.fenced_batches.erase(
                  universe.fenced_batches.begin()
                  + static_cast<std::ptrdiff_t>(idx));
                placeholder_batch placeholder{
                  .epoch = epoch, .offset = universe.hwm++};
                // Mirror epoch_window_checker::check_epoch: every batch
                // reaching the log must be within the log's sliding epoch
                // window.
                if (epoch > universe.checker_max) {
                    universe.checker_min = universe.checker_max
                                               == ct::cluster_epoch::min()
                                             ? epoch
                                             : universe.checker_max;
                    universe.checker_max = epoch;
                }
                ASSERT_GE(epoch, universe.checker_min) << fmt::format(
                  "epoch {} landed below the log window [{}, {}], "
                  "operations:\n{}",
                  epoch,
                  universe.checker_min,
                  universe.checker_max,
                  fmt::join(oplog, "\n"));
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

TEST(ctp_stm_state_test, min_allowed_local_threshold_defaults_to_min) {
    ct::ctp_stm_state s;
    EXPECT_EQ(s.get_min_allowed_local_threshold(), kafka::offset::min());
}

TEST(ctp_stm_state_test, set_then_get_min_allowed_local_threshold) {
    ct::ctp_stm_state s;
    s.set_min_allowed_local_threshold(kafka::offset{42});
    EXPECT_EQ(s.get_min_allowed_local_threshold(), kafka::offset{42});
}

TEST(ctp_stm_state_test, min_allowed_local_threshold_monotonic) {
    ct::ctp_stm_state s;
    s.set_min_allowed_local_threshold(kafka::offset{100});
    EXPECT_EQ(s.get_min_allowed_local_threshold(), kafka::offset{100});

    // Smaller value is ignored.
    s.set_min_allowed_local_threshold(kafka::offset{50});
    EXPECT_EQ(s.get_min_allowed_local_threshold(), kafka::offset{100});

    // Equal value is a no-op.
    s.set_min_allowed_local_threshold(kafka::offset{100});
    EXPECT_EQ(s.get_min_allowed_local_threshold(), kafka::offset{100});

    // Larger value advances.
    s.set_min_allowed_local_threshold(kafka::offset{150});
    EXPECT_EQ(s.get_min_allowed_local_threshold(), kafka::offset{150});
}

TEST(
  ctp_stm_state_test, min_allowed_local_threshold_round_trips_through_serde) {
    ct::ctp_stm_state s;
    s.set_min_allowed_local_threshold(kafka::offset{1234});
    auto buf = serde::to_iobuf(s);
    auto s2 = serde::from_iobuf<ct::ctp_stm_state>(std::move(buf));
    EXPECT_EQ(s2.get_min_allowed_local_threshold(), kafka::offset{1234});
}

} // anonymous namespace
