// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "raft/heartbeats.h"
#include "raft/tests/raft_fixture.h"
#include "raft/tests/raft_fixture_retry_policy.h"
#include "random/generators.h"
#include "serde/rw/rw.h"
#include "storage/record_batch_builder.h"
#include "test_utils/async.h"
#include "test_utils/random_bytes.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>

#include <algorithm>
#include <ranges>

using namespace raft;

static ss::logger test_log("compaction-recovery-test");

/**
 * The goal of this test is to reproduce the offset translation inconsistency
 * scenario possible with ghost batch overshoots. i.e, if there is a ghost batch
 * spanning the dirty index the follower performs a truncation and recursively
 * applies an incorrect append_entries request with wrong prev_log_index
 * potentially causing data loss && log divergence.
 */
TEST_F_CORO(raft_fixture, test_recovery_after_compaction_with_gaps) {
    enable_offset_translation();

    // Enable compaction globally
    config::shard_local_cfg().log_cleanup_policy.set_value(
      model::cleanup_policy_bitflags::compaction);
    config::shard_local_cfg().tombstone_retention_ms.set_value(0ms);

    // Step 1: Create a 3-node raft group and replicate initial data to
    // establish a committed baseline across the quorum. All replicas are in
    // sync until this index.
    co_await create_simple_group(3);
    auto leader_id = co_await wait_for_leader(10s);

    const ss::sstring shared_key = "compaction-test-key";

    // Build batches with a given batch type, all sharing the same key
    // for compactability.
    auto make_batches_of_type = [&](model::record_batch_type batch_type) {
        return make_batches(random_generators::get_int(20, 50), [&](size_t) {
            storage::record_batch_builder builder(batch_type, model::offset(0));
            auto r_size = random_generators::get_int<size_t>(128, 1_KiB);
            builder.add_raw_kv(
              serde::to_iobuf(shared_key),
              bytes_to_iobuf(tests::random_bytes(r_size)));
            return std::move(builder).build();
        });
    };

    // Helper to replicate batches with a given consistency level.
    auto replicate = [&](
                       model::record_batch_type batch_type,
                       consistency_level cl = consistency_level::leader_ack) {
        return retry_with_leader(
          model::timeout_clock::now() + 30s,
          [&make_batches_of_type, batch_type, cl](
            raft_node_instance& leader_node) mutable {
              return leader_node.raft()
                ->replicate(
                  make_batches_of_type(batch_type), replicate_options(cl))
                .then([](::result<replicate_result> r) {
                    if (!r) {
                        return ::result<model::offset>(r.error());
                    }
                    return ::result<model::offset>(r.value().last_offset);
                });
          });
    };

    // Replicate initial raft_data with quorum_ack so it gets committed and
    // flushed on all nodes. This establishes a known match_index baseline.
    // among all followers
    ASSERT_TRUE_CORO(
      (co_await replicate(
         model::record_batch_type::raft_data, consistency_level::quorum_ack))
        .has_value());

    auto base_committed_offset = node(leader_id).raft()->committed_offset();
    vlog(
      test_log.info,
      "Base committed offset established: {}",
      base_committed_offset);

    // Now enable write caching and disable flushing so subsequent data
    // remains dirty (truncatable) on followers.
    // write caching has nothing to do with this test, it only guarantees that
    // the follower doesn't flush the data immediately keeping it truncatable
    // which is needed to exercise this code path.
    co_await set_write_caching(true);
    co_await disable_background_flushing();

    // Step 2: Replicate interleaved archival_metadata and raft_data batches.
    // The archival_metadata batches generate offset translation entries on
    // the follower. Data remains dirty (unflushed) on followers.

    // Here we use archival_metadata batches to demonstrate but it could be
    // any offset translation batches.
    for (int round = 0; round < 5; ++round) {
        ASSERT_TRUE_CORO(
          (co_await replicate(model::record_batch_type::archival_metadata))
            .has_value());
        ASSERT_TRUE_CORO(
          (co_await replicate(model::record_batch_type::raft_data))
            .has_value());
    }
    // Append a bunch of data so it translates to a huge ghost batch
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE_CORO(
          (co_await replicate(model::record_batch_type::raft_data))
            .has_value());
    }
    // Step 3: Block all messages to one follower (including heartbeats)
    // so the leader has no information about the follower's actual state
    auto blocked_follower_id = *random_follower_id();
    vlog(
      test_log.info,
      "Blocking all messages to follower {}",
      blocked_follower_id);

    node(leader_id).on_dispatch(
      [blocked_follower_id](model::node_id dest_id, raft::msg_type) {
          if (dest_id == blocked_follower_id) {
              throw std::runtime_error("blocked");
          }
          return ss::now();
      });

    // Now reset flushing so the leader (and non-blocked follower) can flush
    // This moves the commit index on the leader and enables compaction.
    co_await reset_background_flushing();

    // Replicate only raft_data after blocking so no new offset translation
    // batches are created. This way compaction produces one large ghost batch
    // that spans across the follower's dirty_offset boundary.
    for (int round = 0; round < 5; ++round) {
        ASSERT_TRUE_CORO(
          (co_await replicate(model::record_batch_type::raft_data))
            .has_value());
    }

    // Wait for the leader to have the data
    leader_id = co_await wait_for_leader(10s);

    // Step 4: Run compaction on the leader to create gaps
    vlog(test_log.info, "Running compaction on leader {}", leader_id);
    co_await node(leader_id).raft()->log()->force_roll();
    ss::abort_source as;
    auto hookset = node(leader_id).raft()->log()->stm_hookset();
    storage::housekeeping_config hk_cfg(
      model::timestamp::max(),
      std::nullopt,
      model::offset::max(),
      hookset->max_tombstone_remove_offset(),
      hookset->max_tx_end_remove_offset(),
      0ms,
      0ms,
      0ms,
      as);
    co_await node(leader_id).raft()->log()->housekeeping(std::move(hk_cfg));
    vlog(
      test_log.info,
      "Leader dirty_offset after compaction: {}, start_offset: {}",
      node(leader_id).raft()->dirty_offset(),
      node(leader_id).raft()->start_offset());

    // Step 5: Now retransmit already sent append_entries. This is needed to
    // simulate the bug. We do this by adding an interceptor for heartbeats and
    // updating the match_index so subsequent append_entries retransmit already
    // sent data. In typical cluster scenarios this could be from a leadership
    // change or some failure scenarios etc.
    auto grp = node(leader_id).raft()->group();
    auto interceptor_active = ss::make_lw_shared<bool>(true);

    node(leader_id).set_reply_interceptor(
      [blocked_follower_id, grp, interceptor_active, base_committed_offset](
        reply_variant reply, model::node_id source_node) {
          if (source_node != blocked_follower_id || !*interceptor_active) {
              return ss::make_ready_future<reply_variant>(std::move(reply));
          }
          auto* hb_reply = std::get_if<heartbeat_reply_v2>(&reply);
          if (!hb_reply) {
              return ss::make_ready_future<reply_variant>(std::move(reply));
          }

          // Build a new heartbeat_reply_v2 with modified offsets
          heartbeat_reply_v2 modified(hb_reply->source(), hb_reply->target());

          // Copy lightweight replies as-is
          hb_reply->for_each_lw_reply(
            [&](raft::group_id g, reply_result r) { modified.add(g, r); });

          // Copy full replies, reporting the base committed offset so the
          // leader starts recovery from just after the committed baseline.
          for (const auto& fr : hb_reply->full_replies()) {
              if (fr.group == grp) {
                  heartbeat_reply_data modified_data = fr.data;
                  modified_data.last_dirty_log_index = base_committed_offset;
                  modified_data.last_flushed_log_index = base_committed_offset;
                  modified_data.last_term_base_offset = model::offset{};
                  modified_data.may_recover = true;
                  vlog(
                    test_log.info,
                    "Intercepted heartbeat reply from {}: reporting "
                    "offset {} (actual dirty={}, flushed={})",
                    blocked_follower_id,
                    base_committed_offset,
                    fr.data.last_dirty_log_index,
                    fr.data.last_flushed_log_index);
                  modified.add(fr.group, fr.result, modified_data);
              } else {
                  modified.add(fr.group, fr.result, fr.data);
              }
          }

          // Stop intercepting after the first modification so recovery
          // can proceed with real replies
          *interceptor_active = false;

          return ss::make_ready_future<reply_variant>(
            reply_variant{std::move(modified)});
      });

    // Step 6: Unblock all messages and let recovery proceed
    // This will retransmit append entries and performs a truncation
    // of the log.
    vlog(test_log.info, "Unblocking all messages to followers");
    node(leader_id).reset_dispatch_handlers();

    auto target_offset = node(leader_id).raft()->dirty_offset();
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      60s, [this, blocked_follower_id, target_offset] {
          auto follower_dirty
            = node(blocked_follower_id).raft()->dirty_offset();
          vlog(
            test_log.info,
            "Waiting for follower {} to sync: dirty_offset={}, target={}",
            blocked_follower_id,
            follower_dirty,
            target_offset);
          return follower_dirty >= target_offset;
      });

    // Verify all nodes converged on committed offset
    RPTEST_REQUIRE_EVENTUALLY_CORO(30s, [this, target_offset] {
        return std::ranges::all_of(nodes(), [target_offset](const auto& pair) {
            return pair.second->raft()->committed_offset() >= target_offset;
        });
    });

    // Verify offset translation state is consistent across all replicas
    // up to the committed offset.
    auto committed = node(leader_id).raft()->committed_offset();
    auto start = model::offset{};
    for (const auto& [_, n] : nodes()) {
        start = std::max(start, n->raft()->start_offset());
    }

    vlog(
      test_log.info,
      "Validating offset translation state across replicas [{}, {}]",
      start,
      committed);

    auto leader_log = node(leader_id).raft()->log();
    for (const auto& [nid, n] : nodes()) {
        if (nid == leader_id) {
            continue;
        }
        auto follower_log = n->raft()->log();
        for (auto o = start; o <= committed; o = model::next_offset(o)) {
            auto leader_delta = leader_log->offset_delta(o);
            auto follower_delta = follower_log->offset_delta(o);
            ASSERT_EQ_CORO(leader_delta, follower_delta)
              << "Offset translation mismatch at offset " << o << " for node "
              << nid << ": leader delta=" << leader_delta
              << " follower delta=" << follower_delta;
        }
    }
}
