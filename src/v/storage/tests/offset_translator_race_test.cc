// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Stress reproducer for offset translator delta divergence.
//
// Drives a translator-enabled disk_log_impl the way raft does:
//  - fiber M: serialized appends + conflict (suffix) truncations followed by
//    re-appends at the same offsets (mimics do_append_entries under op_lock)
//  - fiber E: prefix truncations without force delta (mimics
//    consensus::write_snapshot -> logp->truncate_prefix, which runs on the
//    log_eviction_stm/ctp_stm background fiber, NOT under the raft op_lock)
//
// Invariant checked between rounds: for every offset o in
// [start_offset, dirty+1], logp->offset_delta(o) must equal the number of
// filtered (raft_configuration) records at offsets < o according to an
// independently maintained ground-truth history. Also periodically simulates
// restart reconstruction: a fresh offset_translator over the same kvstore
// state + sync_with_log must agree with ground truth.

#include "finjector/hbadger.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "random/generators.h"
#include "storage/disk_log_impl.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/offset_translator.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/storage_test_fixture.h"
#include "storage/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/defer.hh>

#include <fmt/format.h>
#include <gtest/gtest.h>

using namespace storage; // NOLINT

namespace {

struct batch_spec {
    model::offset base;
    model::offset last;
    bool filtered;
};

model::record_batch
make_batch(model::record_batch_type type, size_t records, model::term_id term) {
    storage::record_batch_builder b(type, model::offset(0));
    for (size_t i = 0; i < records; ++i) {
        iobuf value;
        value.append("payload-data-payload", 20);
        b.add_raw_kv(iobuf{}, std::move(value));
    }
    auto batch = std::move(b).build();
    batch.set_term(term);
    return batch;
}

struct ground_truth {
    // one entry per offset in [0, dirty]; true = offset belongs to a filtered
    // batch. Never prefix-truncated (delta is cumulative from offset 0).
    std::vector<bool> offsets;
    // batches currently in the log (suffix truncations shrink this, prefix
    // truncations do not affect correctness of delta computations).
    std::vector<batch_spec> batches;

    int64_t expected_delta(model::offset o) const {
        int64_t d = 0;
        for (int64_t i = 0; i < o() && i < int64_t(offsets.size()); ++i) {
            d += offsets[i] ? 1 : 0;
        }
        return d;
    }

    void suffix_truncate(model::offset at) {
        offsets.resize(size_t(at()));
        while (!batches.empty() && batches.back().base >= at) {
            batches.pop_back();
        }
    }
};

} // namespace

class ot_race_fixture : public storage_test_fixture {};

// Regression test for translator state loss on an append that fails after
// the batch became visible in the log.
//
// segment::do_append advances the segment offset tracker (making the batch
// visible to readers and to raft) before the index and batch cache updates;
// an exception from those escapes disk_log_appender::operator() before the
// offset translator processed the batch. Raft replies failure and the leader
// retries, but the retry skip-matches the already-present batch without
// re-appending it, so the translator would permanently miss the gap: this
// replica's deltas diverge from its peers' with nothing but a (possibly
// debug-level) log line ever emitted.
//
// The appender must therefore account for any batch that became visible even
// when failing the append. Uses the storage::log::failure_probes append
// injection point, which fires exactly between batch visibility and the
// translator update.
TEST_F(ot_race_fixture, append_failure_after_visibility_keeps_translation) {
    if (!finjector::honey_badger::is_enabled()) {
        GTEST_SKIP() << "Failure injection is only enabled in debug mode.";
        return;
    }
    auto cfg = default_log_config(test_dir);
    storage::log_manager mgr(std::move(cfg), kvstore, resources, feature_table);
    auto ntp = model::ntp("redpanda", "ot-append-failure", 0);
    auto group = raft::group_id(44);
    std::optional<log_holder> log_h;
    log_h.emplace(manage_log(
      mgr,
      {ntp, mgr.config().base_dir},
      group,
      {model::record_batch_type::raft_configuration}));
    ss::abort_source as;
    (*log_h)->start(std::nullopt, as).get();
    storage::log* logp = log_h->get();
    auto deferred = ss::defer([&]() mutable {
        log_h.reset();
        mgr.stop().get();
    });

    auto append = [&](std::vector<model::record_batch_type> types) {
        auto appender = logp->make_appender(
          storage::log_append_config{storage::log_append_config::fsync::no});
        for (auto t : types) {
            auto b = make_batch(t, 1, model::term_id(1));
            appender(b).get();
        }
        return appender.end_of_stream().get();
    };

    constexpr auto data = model::record_batch_type::raft_data;
    constexpr auto conf = model::record_batch_type::raft_configuration;

    // offsets 0..2: config at 2
    append({data, data, conf});
    ASSERT_EQ(logp->offset_delta(model::offset(3))(), 1);

    // fail the append of the next config batch after it becomes visible
    auto& dl = *dynamic_cast<storage::disk_log_impl*>(logp);
    dl.get_failure_probes().set_exception("append");
    EXPECT_THROW(append({conf}), std::runtime_error);
    dl.get_failure_probes().unset("append");

    // the batch is in the log regardless of the failed append...
    auto lstats = logp->offsets();
    ASSERT_EQ(lstats.dirty_offset, model::offset(3));
    // ...so the translator must account for it
    EXPECT_EQ(logp->offset_delta(model::offset(4))(), 2);

    // a raft-style retry skip-matches the visible batch and continues with
    // subsequent entries; translation must stay consistent...
    append({data});
    EXPECT_EQ(logp->offset_delta(model::offset(5))(), 2);

    // ...including across a restart
    storage::offset_translator fresh(
      {model::record_batch_type::raft_configuration},
      group,
      ntp,
      kvstore,
      resources);
    fresh.start(storage::offset_translator::must_reset::no).get();
    fresh.sync_with_log(*logp, std::nullopt).get();
    EXPECT_EQ(fresh.state()->delta(model::offset(5)), 2);
}

// Regression test for the stale-HKO reconstruction hazard.
//
// Before the two-phase truncation fix, a suffix truncation whose translator
// checkpoint was lost (I/O error / shutdown abort, swallowed by raft with a
// WARN) left the kvstore claiming coverage (highest_known_offset) of an
// offset range the log had since rewritten. On restart, sync_with_log
// trusted that coverage and silently skipped the re-appended config batches
// below it, permanently losing their gaps.
//
// With the fix, prepare_truncate durably lowers the persisted coverage
// before the log is touched, so losing every subsequent checkpoint leaves
// the kvstore under-claiming (old superset map + low hko), which restart
// reconciliation heals by trimming the map to the persisted coverage and
// re-reading the rest from the log. This test manufactures exactly that
// worst-case surviving kvstore state and asserts reconstruction is correct.
TEST_F(ot_race_fixture, truncation_checkpoint_loss_is_recoverable) {
    auto cfg = default_log_config(test_dir);
    storage::log_manager mgr(std::move(cfg), kvstore, resources, feature_table);
    auto ntp = model::ntp("redpanda", "ot-stale-hko", 0);
    auto group = raft::group_id(43);
    std::optional<log_holder> log_h;
    log_h.emplace(manage_log(
      mgr,
      {ntp, mgr.config().base_dir},
      group,
      {model::record_batch_type::raft_configuration}));
    ss::abort_source as;
    (*log_h)->start(std::nullopt, as).get();
    storage::log* logp = log_h->get();
    auto deferred = ss::defer([&]() mutable {
        log_h.reset();
        mgr.stop().get();
    });

    auto append = [&](std::vector<model::record_batch_type> types) {
        auto appender = logp->make_appender(
          storage::log_append_config{storage::log_append_config::fsync::no});
        for (auto t : types) {
            auto b = make_batch(t, 1, model::term_id(1));
            appender(b).get();
        }
        return appender.end_of_stream().get();
    };

    constexpr auto data = model::record_batch_type::raft_data;
    constexpr auto conf = model::record_batch_type::raft_configuration;
    constexpr auto ks = storage::kvstore::key_space::offset_translator;
    auto hko_key = storage::offset_translator::kvstore_highest_known_offset_key(
      group);
    auto map_key = storage::offset_translator::kvstore_offsetmap_key(group);

    // offsets 0..7: configs at 2 and 5
    append({data, data, conf, data, data, conf, data, data});
    ASSERT_EQ(logp->offset_delta(model::offset(8))(), 2);

    // checkpoint so the kvstore covers the whole log (map {2,5}, hko = 7)
    auto& dl = *dynamic_cast<storage::disk_log_impl*>(logp);
    dl.offset_translator().maybe_checkpoint(0).get();
    auto pre_truncation_map = kvstore.get(ks, map_key)->copy();

    // raft conflict truncation at T=4, then the leader re-replicates offsets
    // 4..8 with different content: configs at 4 and 6
    logp->truncate(storage::truncate_config(model::offset(4))).get();
    append({conf, data, conf, data, data});
    // live state is correct: configs at 2, 4, 6
    ASSERT_EQ(logp->offset_delta(model::offset(9))(), 3);

    // simulate losing every checkpoint from complete_truncate onwards: the
    // kvstore retains the pre-truncation map, and the coverage that
    // prepare_truncate persisted before the log was touched (hko = T-1 = 3).
    // This is the worst surviving state the two-phase truncation permits;
    // before the fix the surviving state could instead pair a high hko with
    // a map that no longer covers it, which was unrecoverable.
    kvstore.put(ks, map_key, std::move(pre_truncation_map)).get();
    kvstore.put(ks, hko_key, reflection::to_iobuf(model::offset(3))).get();

    // restart reconstruction must trim the stale map entries above the
    // persisted coverage (the pre-truncation config at 5) and re-derive
    // offsets 4..8 from the log
    storage::offset_translator fresh(
      {model::record_batch_type::raft_configuration},
      group,
      ntp,
      kvstore,
      resources);
    fresh.start(storage::offset_translator::must_reset::no).get();
    fresh.sync_with_log(*logp, std::nullopt).get();
    // compare the full delta profile (a stale gap from the truncated-away
    // content could numerically compensate for a missed re-appended gap at
    // the tail, so checking a single offset is not sufficient)
    for (auto o = model::offset(0); o <= model::offset(9);
         o = model::next_offset(o)) {
        EXPECT_EQ(fresh.state()->delta(o), logp->offset_delta(o)())
          << "reconstructed delta diverges from live state at offset " << o;
    }
}

TEST_F(ot_race_fixture, offset_translator_delta_stress) {
    auto seed = random_generators::get_int<uint64_t>(0, 1UL << 40);
    if (auto* env = std::getenv("OT_STRESS_SEED"); env != nullptr) {
        seed = std::stoull(env);
    }
    fmt::print("offset_translator_delta_stress seed: {}\n", seed);
    std::mt19937_64 rng(seed);
    auto rand_int = [&rng](int lo, int hi) {
        return int(rng() % uint64_t(hi - lo + 1)) + lo;
    };

    auto make_cfg = [&] {
        auto cfg = default_log_config(test_dir);
        cfg.max_segment_size = config::mock_binding<size_t>(16_KiB);
        return cfg;
    };
    auto ntp = model::ntp("redpanda", "ot-race", 0);
    auto group = raft::group_id(42);

    std::optional<storage::log_manager> mgr;
    mgr.emplace(
      make_cfg(),
      std::ref(kvstore),
      std::ref(resources),
      std::ref(feature_table));
    std::optional<log_holder> log_h;
    log_h.emplace(manage_log(
      *mgr,
      {ntp, mgr->config().base_dir},
      group,
      {model::record_batch_type::raft_configuration}));
    ss::abort_source as;
    (*log_h)->start(std::nullopt, as).get();
    storage::log* logp = log_h->get();
    auto deferred = ss::defer([&]() mutable {
        log_h.reset();
        mgr->stop().get();
    });

    ground_truth truth;
    model::term_id term{1};
    model::offset committed{-1};

    auto append_round = [&](this auto, int nbatches) -> ss::future<> {
        auto appender = logp->make_appender(
          storage::log_append_config{storage::log_append_config::fsync::no});
        std::vector<bool> pending_flags;
        std::vector<size_t> pending_sizes;
        for (int i = 0; i < nbatches; ++i) {
            bool filtered = rand_int(0, 99) < 40;
            size_t records = filtered ? 1 : size_t(rand_int(1, 3));
            auto b = make_batch(
              filtered ? model::record_batch_type::raft_configuration
                       : model::record_batch_type::raft_data,
              records,
              term);
            pending_flags.push_back(filtered);
            pending_sizes.push_back(records);
            co_await appender(b);
        }
        auto res = co_await appender.end_of_stream();
        // reconcile ground truth with the offsets the appender assigned
        ASSERT_EQ_CORO(res.base_offset(), int64_t(truth.offsets.size()));
        for (size_t i = 0; i < pending_flags.size(); ++i) {
            auto base = model::offset(int64_t(truth.offsets.size()));
            for (size_t r = 0; r < pending_sizes[i]; ++r) {
                truth.offsets.push_back(pending_flags[i]);
            }
            truth.batches.push_back(
              batch_spec{
                .base = base,
                .last = model::offset(int64_t(truth.offsets.size()) - 1),
                .filtered = pending_flags[i]});
        }
        ASSERT_EQ_CORO(res.last_offset(), int64_t(truth.offsets.size()) - 1);
    };

    auto conflict_round = [&](this auto) -> ss::future<> {
        // pick a batch-aligned truncation point above committed
        std::vector<model::offset> candidates;
        for (const auto& b : truth.batches) {
            if (b.base > committed && b.base > logp->offsets().start_offset) {
                candidates.push_back(b.base);
            }
        }
        if (candidates.empty()) {
            co_return;
        }
        auto at = candidates[size_t(rand_int(0, int(candidates.size()) - 1))];
        co_await logp->truncate(storage::truncate_config(at));
        truth.suffix_truncate(at);
        term = model::term_id(term() + 1);
        // raft always follows a conflict truncation with replacement appends
        co_await append_round(rand_int(1, 3));
    };

    auto evict = [&](this auto) -> ss::future<> {
        // batch-aligned prefix truncation: start offset = some batch's
        // last + 1, bounded by committed (like snapshot_and_truncate_log)
        std::vector<model::offset> candidates;
        auto cur_start = logp->offsets().start_offset;
        for (const auto& b : truth.batches) {
            if (b.last <= committed && model::next_offset(b.last) > cur_start) {
                candidates.push_back(model::next_offset(b.last));
            }
        }
        if (candidates.empty()) {
            co_return;
        }
        auto start
          = candidates[size_t(rand_int(0, int(candidates.size()) - 1))];
        co_await logp->truncate_prefix(storage::truncate_prefix_config(start));
    };

    auto verify = [&](std::string_view when) {
        auto lstats = logp->offsets();
        if (lstats.dirty_offset < lstats.start_offset) {
            return;
        }
        auto from = std::max(lstats.start_offset, model::offset(0));
        for (auto o = from; o <= model::next_offset(lstats.dirty_offset);
             o = model::next_offset(o)) {
            auto expected = truth.expected_delta(o);
            auto actual = logp->offset_delta(o)();
            ASSERT_EQ(actual, expected) << fmt::format(
              "[{}] delta mismatch at offset {} (start {}, dirty {}, "
              "committed {}, seed {})",
              when,
              o,
              lstats.start_offset,
              lstats.dirty_offset,
              committed,
              seed);
        }
    };

    auto verify_reconstruction = [&](this auto) -> ss::future<> {
        // simulate restart: fresh translator over the same kvstore state
        storage::offset_translator fresh(
          {model::record_batch_type::raft_configuration},
          group,
          ntp,
          kvstore,
          resources);
        co_await fresh.start(storage::offset_translator::must_reset::no);
        co_await fresh.sync_with_log(*logp, std::nullopt);
        auto lstats = logp->offsets();
        if (lstats.dirty_offset < lstats.start_offset) {
            co_return;
        }
        auto from = std::max(lstats.start_offset, model::offset(0));
        for (auto o = from; o <= model::next_offset(lstats.dirty_offset);
             o = model::next_offset(o)) {
            auto expected = truth.expected_delta(o);
            auto actual = fresh.state()->delta(o);
            ASSERT_EQ_CORO(actual, expected) << fmt::format(
              "[reconstruction] delta mismatch at offset {} (start {}, dirty "
              "{}, seed {})",
              o,
              lstats.start_offset,
              lstats.dirty_offset,
              seed);
        }
    };

    // reader fiber: readers hold segment read locks, which gates the
    // front-segment write-lock window inside truncate_prefix (like fetches)
    auto read_some = [&](this auto) -> ss::future<> {
        auto lstats = logp->offsets();
        if (lstats.dirty_offset < lstats.start_offset) {
            co_return;
        }
        auto reader = co_await logp->make_reader(
          storage::local_log_reader_config(
            lstats.start_offset, lstats.dirty_offset, std::nullopt));
        struct counter {
            ss::future<ss::stop_iteration> operator()(model::record_batch&) {
                co_return ss::stop_iteration::no;
            }
            void end_of_stream() {}
        };
        co_await std::move(reader).for_each_ref(counter{}, model::no_timeout);
    };

    // checkpoint churn: memory-pressure hints make production checkpoint
    // far more often than the 64MiB threshold
    auto checkpoint_churn = [&](this auto) -> ss::future<> {
        auto& dl = *dynamic_cast<storage::disk_log_impl*>(logp);
        co_await dl.offset_translator().maybe_checkpoint(0);
    };

    // full restart: stop the log manager, recreate it (segment recovery +
    // translator kvstore load), and start the log with the raft-startup
    // truncate cfg (snapshot start offset + force delta), like
    // consensus::start -> truncation_cfg_for_snapshot -> log->start
    auto restart = [&]() {
        log_h.reset();
        mgr->stop().get();
        mgr.emplace(
          make_cfg(),
          std::ref(kvstore),
          std::ref(resources),
          std::ref(feature_table));
        log_h.emplace(manage_log(
          *mgr,
          {ntp, mgr->config().base_dir},
          group,
          {model::record_batch_type::raft_configuration}));
        logp = log_h->get();
        std::optional<storage::truncate_prefix_config> tcfg;
        auto start = logp->offsets().start_offset;
        if (start > model::offset(0)) {
            // raft always passes the snapshot-derived cfg at startup; the
            // snapshot stores delta(next(last_included)) == delta(start)
            tcfg.emplace(
              start, model::offset_delta(truth.expected_delta(start)));
        }
        logp->start(tcfg, as).get();
    };

    const int rounds = 3000;
    for (int round = 0; round < rounds; ++round) {
        // work fiber: appends + occasional conflict truncation (serialized,
        // like raft under op_lock)
        auto work = [&](this auto) -> ss::future<> {
            co_await append_round(rand_int(1, 4));
            if (rand_int(0, 99) < 25) {
                co_await conflict_round();
            }
            if (rand_int(0, 99) < 30) {
                co_await logp->flush();
            }
        };
        // concurrent fibers mirroring production concurrency: eviction
        // (log_eviction_stm), readers (fetch), checkpoint hints
        std::vector<ss::future<>> fibers;
        fibers.push_back(work());
        if (rand_int(0, 99) < 35) {
            fibers.push_back(evict());
        }
        if (rand_int(0, 99) < 40) {
            fibers.push_back(read_some());
        }
        if (rand_int(0, 99) < 30) {
            fibers.push_back(checkpoint_churn());
        }
        ss::when_all_succeed(fibers.begin(), fibers.end()).get();
        // advance commit index sometimes (enables future evictions and
        // bounds conflict truncations)
        if (rand_int(0, 99) < 50) {
            committed = logp->offsets().dirty_offset;
        }
        verify(fmt::format("round {}", round));
        if (HasFatalFailure()) {
            break;
        }
        if (round % 200 == 199) {
            verify_reconstruction().get();
            if (HasFatalFailure()) {
                break;
            }
        }
        if (round % 250 == 249) {
            restart();
            verify(fmt::format("post-restart round {}", round));
            if (HasFatalFailure()) {
                break;
            }
        }
    }
}
