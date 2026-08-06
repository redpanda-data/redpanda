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
