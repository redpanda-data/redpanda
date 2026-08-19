// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Raft-level stress for offset translator delta consistency.
//
// Runs a 3-node group under simultaneous election churn (step-downs), produce
// traffic, per-replica prefix eviction via snapshot_and_truncate_log (the
// log_eviction_stm / ctp_stm entry point), and rolling node restarts, while
// continuously checking that all replicas agree on the offset translator delta
// at committed offsets, and that each replica's delta is self-consistent with
// its own log content.

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "raft/consensus.h"
#include "raft/state_machine_manager.h"
#include "raft/tests/raft_fixture.h"
#include "raft/types.h"
#include "random/generators.h"
#include "storage/disk_log_impl.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/parallel_for_each.hh>

#include <fmt/format.h>

using namespace raft;

namespace {
ss::future<> ignore_exceptions(ss::future<> f) {
    try {
        co_await std::move(f);
    } catch (...) {
    }
}
} // namespace

TEST_F_CORO(raft_fixture, offset_translator_consistency_under_churn) {
    static constexpr auto node_count = 3;
    enable_offset_translation();
    for (auto i = 0; i < node_count; ++i) {
        add_node(model::node_id(i), model::revision_id(0));
    }
    co_await ss::coroutine::parallel_for_each(nodes(), [this](auto& pair) {
        // an (empty) stm manager is required for snapshot_and_truncate_log
        return pair.second->init_and_start(
          all_vnodes(), raft::state_machine_manager_builder{});
    });
    co_await wait_for_leader(10s);

    bool stop = false;
    std::optional<model::node_id> restarting;
    std::optional<ss::sstring> divergence;
    // fibers take read locks per iteration; the restarter takes the write
    // lock so that no fiber is mid-operation while a node stops
    ss::rwlock quiesce;

    auto now_plus = [](auto d) { return model::timeout_clock::now() + d; };

    // continuous produce traffic; leader_ack leaves uncommitted tails on
    // abrupt leadership changes -> conflict truncations on followers
    auto producer = [&](this auto) -> ss::future<> {
        size_t i = 0;
        while (!stop) {
            auto held = co_await quiesce.hold_read_lock();
            auto l = get_leader();
            if (!l || l == restarting) {
                held.return_all();
                co_await ss::sleep(5ms);
                continue;
            }
            auto batches = make_batches(
              {{fmt::format("k{}", i), fmt::format("v{}", i)}});
            ++i;
            auto lvl = random_generators::get_int(0, 1) == 0
                         ? consistency_level::quorum_ack
                         : consistency_level::leader_ack;
            co_await ignore_exceptions(
              node(*l)
                .raft()
                ->replicate(std::move(batches), replicate_options(lvl))
                .discard_result());
        }
    };

    // election churn: repeatedly force the leader to step down; every new
    // term replicates a raft_configuration batch (a filtered batch) and can
    // truncate uncommitted config/data batches on other replicas
    auto churner = [&](this auto) -> ss::future<> {
        while (!stop) {
            co_await ss::sleep(
              std::chrono::milliseconds(random_generators::get_int(30, 120)));
            auto held = co_await quiesce.hold_read_lock();
            auto l = get_leader();
            if (!l || l == restarting) {
                continue;
            }
            co_await ignore_exceptions(node(*l).raft()->step_down("churn"));
        }
    };

    // per-replica prefix eviction, like log_eviction_stm / ctp_stm
    auto evictor = [&](this auto) -> ss::future<> {
        while (!stop) {
            co_await ss::sleep(
              std::chrono::milliseconds(random_generators::get_int(20, 80)));
            auto held = co_await quiesce.hold_read_lock();
            for (auto i = 0; i < node_count; ++i) {
                auto id = model::node_id(i);
                if (id == restarting || stop) {
                    continue;
                }
                auto raft = node(id).raft();
                auto committed = raft->committed_offset();
                if (committed < model::offset(0)) {
                    continue;
                }
                auto target = model::offset(
                  committed() - random_generators::get_int(0, 3));
                if (
                  target <= raft->last_snapshot_index()
                  || target < raft->start_offset()) {
                    continue;
                }
                co_await ignore_exceptions(
                  raft->snapshot_and_truncate_log(target).discard_result());
            }
        }
    };

    // rolling graceful restarts (mimics the scale tests' restart phases)
    auto restarter = [&](this auto) -> ss::future<> {
        while (!stop) {
            co_await ss::sleep(
              std::chrono::milliseconds(
                random_generators::get_int(1500, 2500)));
            if (stop) {
                break;
            }
            auto id = model::node_id(
              random_generators::get_int(node_count - 1));
            restarting = id;
            {
                auto held = co_await quiesce.hold_write_lock();
                co_await stop_node(id);
            }
            auto& n = add_node(id, model::revision_id(0));
            co_await n.init_and_start(
              all_vnodes(), raft::state_machine_manager_builder{});
            restarting = std::nullopt;
        }
    };

    // continuous cross-replica delta comparison at committed offsets: all
    // replicas that contain offset o must agree on delta(o)
    auto checker = [&](this auto) -> ss::future<> {
        while (!stop && !divergence) {
            co_await ss::sleep(10ms);
            auto held = co_await quiesce.hold_read_lock();
            model::offset lo{0};
            model::offset hi = model::offset::max();
            for (auto i = 0; i < node_count; ++i) {
                auto id = model::node_id(i);
                if (id == restarting) {
                    hi = model::offset::min();
                    break;
                }
                auto raft = node(id).raft();
                lo = std::max(lo, raft->start_offset());
                hi = std::min(hi, raft->committed_offset());
            }
            if (hi < lo) {
                continue;
            }
            // sample a few offsets in [lo, hi]
            for (int s = 0; s < 4 && !divergence; ++s) {
                auto o = s == 0 ? hi
                                : model::offset(
                                    random_generators::get_int(lo(), hi()));
                std::optional<model::offset_delta> expected;
                for (auto i = 0; i < node_count; ++i) {
                    auto id = model::node_id(i);
                    if (id == restarting) {
                        expected.reset();
                        break;
                    }
                    auto raft = node(id).raft();
                    if (
                      o < raft->start_offset()
                      || o > raft->committed_offset()) {
                        continue;
                    }
                    model::offset_delta d;
                    try {
                        d = raft->log()->offset_delta(o);
                    } catch (...) {
                        continue;
                    }
                    if (expected && d != *expected) {
                        divergence = fmt::format(
                          "delta divergence at offset {}: node {} has {}, "
                          "another node has {}",
                          o,
                          id,
                          d,
                          *expected);
                    }
                    expected = d;
                }
            }
        }
    };

    auto producer_f = producer();
    auto churner_f = churner();
    auto evictor_f = evictor();
    auto restarter_f = restarter();
    auto checker_f = checker();

    static constexpr auto test_duration = 25s;
    auto deadline = now_plus(test_duration);
    while (model::timeout_clock::now() < deadline && !divergence) {
        co_await ss::sleep(100ms);
    }
    stop = true;
    co_await ss::when_all_succeed(
      std::move(producer_f),
      std::move(churner_f),
      std::move(evictor_f),
      std::move(restarter_f),
      std::move(checker_f));

    ASSERT_FALSE_CORO(divergence.has_value()) << *divergence;

    // final absolute self-check: each replica's delta span across its log
    // must equal the number of filtered batch records it stores
    co_await wait_for_leader(10s);
    for (auto i = 0; i < node_count; ++i) {
        auto raft = node(model::node_id(i)).raft();
        auto log = raft->log();
        auto lstats = log->offsets();
        if (lstats.dirty_offset < lstats.start_offset) {
            continue;
        }
        auto reader = co_await log->make_reader(
          storage::local_log_reader_config(
            lstats.start_offset, lstats.dirty_offset, std::nullopt));
        struct gap_counter {
            int64_t filtered_records = 0;
            ss::future<ss::stop_iteration> operator()(model::record_batch b) {
                if (b.header().type != model::record_batch_type::raft_data) {
                    filtered_records += b.record_count();
                }
                co_return ss::stop_iteration::no;
            }
            int64_t end_of_stream() const { return filtered_records; }
        };
        int64_t counted = co_await std::move(reader).consume(
          gap_counter{}, model::no_timeout);
        auto span = log->offset_delta(model::next_offset(lstats.dirty_offset))
                    - log->offset_delta(lstats.start_offset);
        ASSERT_EQ_CORO(span(), counted) << fmt::format(
          "node {} self-inconsistent: delta span {} vs {} filtered records "
          "in log [{}, {}]",
          i,
          span(),
          counted,
          lstats.start_offset,
          lstats.dirty_offset);
    }
}

// End-to-end reproduction of translator divergence from an append that
// fails on a follower after the batch became visible in its log.
//
// A leadership change makes the new leader replicate a raft_configuration
// batch. With the append failure injected on one follower, that batch lands
// in the follower's log but the append reports failure; the leader's retry
// skip-matches the already-present batch without re-appending it. Without
// the storage-side fix the follower's offset translator permanently misses
// the gap and its deltas diverge from its peers' (the production symptom:
// "Offset translator state inconsistency detected").
TEST_F_CORO(raft_fixture, follower_append_failure_keeps_translation) {
    static constexpr auto node_count = 3;
    enable_offset_translation();
    co_await create_simple_group(node_count);
    auto leader = co_await wait_for_leader(10s);

    auto res = co_await node(leader).raft()->replicate(
      make_batches({{"k0", "v0"}, {"k1", "v1"}}),
      replicate_options(consistency_level::quorum_ack));
    ASSERT_TRUE_CORO(res.has_value());
    co_await wait_for_committed_offset(res.value().last_offset, 10s);

    // inject append failures on one follower; batches keep becoming visible
    // in its log, but every append reports failure
    auto follower = model::node_id((leader() + 1) % node_count);
    auto& follower_log = dynamic_cast<storage::disk_log_impl&>(
      *node(follower).underlying_log());
    follower_log.get_failure_probes().set_exception("append");

    // force a leadership change to the healthy follower (if the armed
    // follower won the election instead, its own failed config append would
    // later be conflict-truncated and re-appended, healing the translator
    // as a side effect and masking the hazard): the new leader replicates
    // its configuration batch (a filtered batch) to the armed follower,
    // where the append fails after the batch became visible
    auto dirty_before = node(follower).raft()->dirty_offset();
    auto target = model::node_id((leader() + 2) % node_count);
    auto transfer_reply = co_await node(leader).raft()->transfer_leadership(
      transfer_leadership_request{
        .group = node(leader).raft()->group(),
        .target = target,
      });
    ASSERT_TRUE_CORO(transfer_reply.success);
    co_await wait_for_leader(10s);

    // wait until the new leader's configuration batch became visible in the
    // follower's log through a failed append, then stop injecting so the
    // group can converge
    RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [&] {
        return node(follower).raft()->dirty_offset() > dirty_before;
    });
    follower_log.get_failure_probes().unset("append");

    // the leadership may still be settling; retry until a functional leader
    // accepts writes (a plain loop: a capturing coroutine lambda passed to a
    // retry helper is a use-after-free hazard)
    model::offset committed{};
    for (int attempt = 0; attempt < 100 && committed == model::offset{};
         ++attempt) {
        if (auto l = get_leader(); l.has_value()) {
            auto r = co_await node(*l).raft()->replicate(
              make_batches({{"k2", "v2"}}),
              replicate_options(consistency_level::quorum_ack));
            if (r.has_value()) {
                committed = r.value().last_offset;
                break;
            }
        }
        co_await ss::sleep(100ms);
    }
    ASSERT_GT_CORO(committed, model::offset{});
    // wait until every replica has the full log (wait_for_committed_offset
    // is satisfied by a quorum, which may exclude the injected follower)
    RPTEST_REQUIRE_EVENTUALLY_CORO(30s, [&] {
        for (auto i = 0; i < node_count; ++i) {
            if (
              node(model::node_id(i)).raft()->committed_offset() < committed) {
                return false;
            }
        }
        return true;
    });

    // one more append past the injected batch: this is what makes raft's
    // validate_offset_translator_delta compare deltas at the divergent
    // offset on the follower (and emit "Offset translator state
    // inconsistency detected" when the translator state was corrupted)
    if (auto l = get_leader(); l.has_value()) {
        auto r = co_await node(*l).raft()->replicate(
          make_batches({{"k3", "v3"}}),
          replicate_options(consistency_level::quorum_ack));
        if (r.has_value()) {
            committed = r.value().last_offset;
            RPTEST_REQUIRE_EVENTUALLY_CORO(30s, [&] {
                for (auto i = 0; i < node_count; ++i) {
                    if (
                      node(model::node_id(i)).raft()->committed_offset()
                      < committed) {
                        return false;
                    }
                }
                return true;
            });
        }
    }

    // every replica must agree on the offset translator delta at every
    // committed offset
    for (auto o = model::offset(0); o <= committed; o = model::next_offset(o)) {
        std::optional<model::offset_delta> expected;
        for (auto i = 0; i < node_count; ++i) {
            auto d = node(model::node_id(i)).raft()->log()->offset_delta(o);
            if (expected) {
                ASSERT_EQ_CORO(d, *expected) << fmt::format(
                  "delta divergence at offset {} on node {}", o, i);
            }
            expected = d;
        }
    }
}
