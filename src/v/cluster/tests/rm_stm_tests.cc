// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/errc.h"
#include "cluster/rm_stm_types.h"
#include "cluster/tests/randoms.h"
#include "cluster/tests/rm_stm_test_fixture.h"
#include "finjector/hbadger.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/timestamp.h"
#include "raft/consensus_utils.h"
#include "raft/tests/raft_group_fixture.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/async.h"
#include "test_utils/randoms.h"
#include "utils/directory_walker.h"

#include <seastar/util/defer.hh>

#include <system_error>

using namespace std::chrono_literals;

static const failure_type<cluster::errc>
  invalid_producer_epoch(cluster::errc::invalid_producer_epoch);

struct rich_reader {
    model::batch_identity id;
    model::record_batch_reader reader;
};

static rich_reader make_rreader(
  model::producer_identity pid,
  int first_seq,
  int count,
  bool is_transactional) {
    return rich_reader{
      .id = model::
        batch_identity{.pid = pid, .first_seq = first_seq, .last_seq = first_seq + count - 1, .record_count = count, .is_transactional = is_transactional},
      .reader = random_batch_reader(model::test::record_batch_spec{
        .offset = model::offset(0),
        .allow_compression = true,
        .count = count,
        .producer_id = pid.id,
        .producer_epoch = pid.epoch,
        .base_sequence = first_seq,
        .is_transactional = is_transactional})};
}

void check_snapshot_sizes(cluster::rm_stm& stm, raft::consensus* c) {
    stm.write_local_snapshot().get();
    const auto work_dir = c->log_config().work_directory();
    std::vector<ss::sstring> snapshot_files;
    directory_walker::walk(
      work_dir,
      [&snapshot_files](ss::directory_entry ent) {
          if (!ent.type || *ent.type != ss::directory_entry_type::regular) {
              return ss::now();
          }

          if (
            ent.name.find("abort.idx.") != ss::sstring::npos
            || ent.name.find("tx.snapshot") != ss::sstring::npos) {
              snapshot_files.push_back(ent.name);
          }
          return ss::now();
      })
      .get();

    uint64_t snapshots_size = 0;
    for (const auto& file : snapshot_files) {
        auto file_path = std::filesystem::path(work_dir) / file.c_str();
        snapshots_size += ss::file_size(file_path.string()).get();
    }

    BOOST_REQUIRE_EQUAL(stm.get_local_snapshot_size(), snapshots_size);
}

// tests:
//   - a simple tx execution succeeds
//   - last_stable_offset doesn't advance past an ongoing transaction
FIXTURE_TEST(test_tx_happy_tx, rm_stm_test_fixture) {
    create_stm_and_start_raft();
    auto& stm = *_stm;
    stm.testing_only_disable_auto_abort();

    auto tx_seq = model::tx_seq(0);

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto min_offset = model::offset(0);
    auto max_offset = model::offset(std::numeric_limits<int64_t>::max());

    auto pid1 = model::producer_identity{1, 0};
    auto rreader = make_rreader(pid1, 0, 5, false);
    auto offset_r = stm
                      .replicate(
                        rreader.id,
                        std::move(rreader.reader),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack))
                      .get();
    RPTEST_REQUIRE_EVENTUALLY(
      1s, [&] { return stm.highest_producer_id() == pid1.get_id(); });
    BOOST_REQUIRE((bool)offset_r);
    auto aborted_txs = stm.aborted_transactions(min_offset, max_offset).get();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);
    auto first_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get();

    auto pid2 = model::producer_identity{2, 0};
    auto term_op = stm
                     .begin_tx(
                       pid2,
                       tx_seq,
                       std::chrono::milliseconds(
                         std::numeric_limits<int32_t>::max()),
                       model::partition_id(0))
                     .get();
    BOOST_REQUIRE((bool)term_op);
    BOOST_REQUIRE_EQUAL(stm.highest_producer_id(), pid2.get_id());

    rreader = make_rreader(pid2, 0, 5, true);
    offset_r = stm
                 .replicate(
                   rreader.id,
                   std::move(rreader.reader),
                   raft::replicate_options(raft::consistency_level::quorum_ack))
                 .get();
    BOOST_REQUIRE((bool)offset_r);
    auto tx_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get();
    BOOST_REQUIRE_LE(stm.last_stable_offset(), tx_offset);

    aborted_txs = stm.aborted_transactions(min_offset, max_offset).get();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);

    auto op = stm.commit_tx(pid2, tx_seq, 2'000ms).get();
    BOOST_REQUIRE_EQUAL(op, cluster::tx::errc::none);
    aborted_txs = stm.aborted_transactions(min_offset, max_offset).get();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, tx_offset]() {
        return tx_offset < stm.last_stable_offset();
    }).get();

    BOOST_REQUIRE_EQUAL(stm.highest_producer_id(), pid2.get_id());
    check_snapshot_sizes(stm, _raft.get());
}

// tests:
//   - a simple tx aborting before prepare succeeds
//   - an aborted tx is reflected in aborted_transactions
FIXTURE_TEST(test_tx_aborted_tx_1, rm_stm_test_fixture) {
    create_stm_and_start_raft();
    auto& stm = *_stm;
    stm.testing_only_disable_auto_abort();

    stm.start().get();
    auto tx_seq = model::tx_seq(0);

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto min_offset = model::offset(0);
    auto max_offset = model::offset(std::numeric_limits<int64_t>::max());

    auto pid1 = model::producer_identity{1, 0};
    auto rreader = make_rreader(pid1, 0, 5, false);
    auto offset_r = stm
                      .replicate(
                        rreader.id,
                        std::move(rreader.reader),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack))
                      .get();
    RPTEST_REQUIRE_EVENTUALLY(
      1s, [&] { return stm.highest_producer_id() == pid1.get_id(); });
    BOOST_REQUIRE((bool)offset_r);
    auto aborted_txs = stm.aborted_transactions(min_offset, max_offset).get();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);
    auto first_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get();

    auto pid2 = model::producer_identity{2, 0};
    auto term_op = stm
                     .begin_tx(
                       pid2,
                       tx_seq,
                       std::chrono::milliseconds(
                         std::numeric_limits<int32_t>::max()),
                       model::partition_id(0))
                     .get();
    BOOST_REQUIRE((bool)term_op);
    BOOST_REQUIRE_EQUAL(stm.highest_producer_id(), pid2.get_id());

    rreader = make_rreader(pid2, 0, 5, true);
    offset_r = stm
                 .replicate(
                   rreader.id,
                   std::move(rreader.reader),
                   raft::replicate_options(raft::consistency_level::quorum_ack))
                 .get();
    BOOST_REQUIRE((bool)offset_r);
    auto tx_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get();
    BOOST_REQUIRE_LE(stm.last_stable_offset(), tx_offset);
    aborted_txs = stm.aborted_transactions(min_offset, max_offset).get();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);

    auto op = stm.abort_tx(pid2, tx_seq, 2'000ms).get();
    BOOST_REQUIRE_EQUAL(op, cluster::tx::errc::none);
    BOOST_REQUIRE(stm
                    .wait_no_throw(
                      _raft.get()->committed_offset(),
                      model::timeout_clock::now() + 2'000ms)
                    .get());
    aborted_txs = stm.aborted_transactions(min_offset, max_offset).get();

    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 1);
    BOOST_REQUIRE(
      std::any_of(aborted_txs.begin(), aborted_txs.end(), [pid2](auto x) {
          return x.pid == pid2;
      }));
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, tx_offset]() {
        return tx_offset < stm.last_stable_offset();
    }).get();

    BOOST_REQUIRE_EQUAL(stm.highest_producer_id(), pid2.get_id());
    check_snapshot_sizes(stm, _raft.get());
}

// tests:
//   - a simple tx aborting after prepare succeeds
//   - an aborted tx is reflected in aborted_transactions
FIXTURE_TEST(test_tx_aborted_tx_2, rm_stm_test_fixture) {
    create_stm_and_start_raft();
    auto& stm = *_stm;
    stm.testing_only_disable_auto_abort();

    stm.start().get();

    auto tx_seq = model::tx_seq(0);

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto min_offset = model::offset(0);
    auto max_offset = model::offset(std::numeric_limits<int64_t>::max());

    auto pid1 = model::producer_identity{1, 0};
    auto rreader = make_rreader(pid1, 0, 5, false);
    auto offset_r = stm
                      .replicate(
                        rreader.id,
                        std::move(rreader.reader),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack))
                      .get();
    RPTEST_REQUIRE_EVENTUALLY(
      1s, [&] { return stm.highest_producer_id() == pid1.get_id(); });
    BOOST_REQUIRE((bool)offset_r);
    auto aborted_txs = stm.aborted_transactions(min_offset, max_offset).get();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);
    auto first_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get();

    auto pid2 = model::producer_identity{2, 0};
    auto term_op = stm
                     .begin_tx(
                       pid2,
                       tx_seq,
                       std::chrono::milliseconds(
                         std::numeric_limits<int32_t>::max()),
                       model::partition_id(0))
                     .get();
    BOOST_REQUIRE_EQUAL(stm.highest_producer_id(), pid2.get_id());
    BOOST_REQUIRE((bool)term_op);

    rreader = make_rreader(pid2, 0, 5, true);
    offset_r = stm
                 .replicate(
                   rreader.id,
                   std::move(rreader.reader),
                   raft::replicate_options(raft::consistency_level::quorum_ack))
                 .get();
    BOOST_REQUIRE_EQUAL(stm.highest_producer_id(), pid2.get_id());
    BOOST_REQUIRE((bool)offset_r);
    auto tx_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get();
    BOOST_REQUIRE_LE(stm.last_stable_offset(), tx_offset);
    aborted_txs = stm.aborted_transactions(min_offset, max_offset).get();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);

    auto op = stm.abort_tx(pid2, tx_seq, 2'000ms).get();
    BOOST_REQUIRE_EQUAL(op, cluster::tx::errc::none);
    BOOST_REQUIRE(stm
                    .wait_no_throw(
                      _raft.get()->committed_offset(),
                      model::timeout_clock::now() + 2'000ms)
                    .get());
    aborted_txs = stm.aborted_transactions(min_offset, max_offset).get();

    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 1);
    BOOST_REQUIRE(
      std::any_of(aborted_txs.begin(), aborted_txs.end(), [pid2](auto x) {
          return x.pid == pid2;
      }));

    tests::cooperative_spin_wait_with_timeout(10s, [&stm, tx_offset]() {
        return tx_offset < stm.last_stable_offset();
    }).get();

    BOOST_REQUIRE_EQUAL(stm.highest_producer_id(), pid2.get_id());
    check_snapshot_sizes(stm, _raft.get());
}

// transactional writes of an unknown tx are rejected
FIXTURE_TEST(test_tx_unknown_produce, rm_stm_test_fixture) {
    create_stm_and_start_raft();
    auto& stm = *_stm;
    stm.testing_only_disable_auto_abort();

    stm.start().get();

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto pid1 = model::producer_identity{1, 0};
    auto rreader = make_rreader(pid1, 0, 5, false);
    auto offset_r = stm
                      .replicate(
                        rreader.id,
                        std::move(rreader.reader),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack))
                      .get();
    RPTEST_REQUIRE_EVENTUALLY(
      1s, [&] { return stm.highest_producer_id() == pid1.get_id(); });
    BOOST_REQUIRE((bool)offset_r);

    auto pid2 = model::producer_identity{2, 0};
    rreader = make_rreader(pid2, 0, 5, true);
    offset_r = stm
                 .replicate(
                   rreader.id,
                   std::move(rreader.reader),
                   raft::replicate_options(raft::consistency_level::quorum_ack))
                 .get();
    BOOST_REQUIRE(offset_r == invalid_producer_epoch);
    RPTEST_REQUIRE_EVENTUALLY(
      1s, [&] { return stm.highest_producer_id() == pid1.get_id(); });
}

FIXTURE_TEST(test_stale_begin_tx_fenced, rm_stm_test_fixture) {
    create_stm_and_start_raft();
    auto& stm = *_stm;
    stm.testing_only_disable_auto_abort();

    stm.start().get();

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto tx_seq = model::tx_seq(10);
    auto tx_seq_old = model::tx_seq(9);
    auto tx_seq_new = model::tx_seq(11);
    auto pid1 = model::producer_identity{1, 0};
    auto timeout = std::chrono::milliseconds(
      std::numeric_limits<int32_t>::max());

    auto begin_tx = [&](model::tx_seq seq) {
        return stm.begin_tx(pid1, seq, timeout, model::partition_id(0)).get();
    };

    auto commit_tx = [&](model::tx_seq seq) {
        return stm.commit_tx(pid1, tx_seq, timeout).get();
    };

    // begin should succeed.
    BOOST_REQUIRE(begin_tx(tx_seq));

    // retry should succeed as it is idempotent
    BOOST_REQUIRE(begin_tx(tx_seq));

    // transaction already in progress, old sequence numbers are fenced
    BOOST_REQUIRE_EQUAL(
      begin_tx(tx_seq_old).error(), cluster::tx::errc::request_rejected);
    // newer sequence numbers are rejected.
    BOOST_REQUIRE_EQUAL(
      begin_tx(tx_seq_new).error(), cluster::tx::errc::request_rejected);

    // seal the transaction.
    BOOST_REQUIRE_EQUAL(commit_tx(tx_seq_new), cluster::tx::errc::none);

    // older sequence numbers are fenced
    BOOST_REQUIRE_EQUAL(
      begin_tx(tx_seq_old).error(), cluster::tx::errc::request_rejected);
}

// begin fences off old transactions
FIXTURE_TEST(test_tx_begin_fences_produce, rm_stm_test_fixture) {
    create_stm_and_start_raft();
    auto& stm = *_stm;
    stm.testing_only_disable_auto_abort();

    stm.start().get();

    auto tx_seq = model::tx_seq(0);

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto pid1 = model::producer_identity{1, 0};
    auto rreader = make_rreader(pid1, 0, 5, false);
    auto offset_r = stm
                      .replicate(
                        rreader.id,
                        std::move(rreader.reader),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack))
                      .get();
    BOOST_REQUIRE((bool)offset_r);

    auto pid20 = model::producer_identity{2, 0};
    auto term_op = stm
                     .begin_tx(
                       pid20,
                       tx_seq,
                       std::chrono::milliseconds(
                         std::numeric_limits<int32_t>::max()),
                       model::partition_id(0))
                     .get();
    BOOST_REQUIRE((bool)term_op);

    auto pid21 = model::producer_identity{2, 1};
    term_op = stm
                .begin_tx(
                  pid21,
                  tx_seq,
                  std::chrono::milliseconds(
                    std::numeric_limits<int32_t>::max()),
                  model::partition_id(0))
                .get();
    BOOST_REQUIRE((bool)term_op);

    rreader = make_rreader(pid20, 0, 5, true);
    offset_r = stm
                 .replicate(
                   rreader.id,
                   std::move(rreader.reader),
                   raft::replicate_options(raft::consistency_level::quorum_ack))
                 .get();
    BOOST_REQUIRE(!(bool)offset_r);

    check_snapshot_sizes(stm, _raft.get());
}

// transactional writes of an aborted tx are rejected
FIXTURE_TEST(test_tx_post_aborted_produce, rm_stm_test_fixture) {
    create_stm_and_start_raft();
    auto& stm = *_stm;
    stm.testing_only_disable_auto_abort();

    stm.start().get();

    auto tx_seq = model::tx_seq(0);

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto pid1 = model::producer_identity{1, 0};
    auto rreader = make_rreader(pid1, 0, 5, false);
    auto offset_r = stm
                      .replicate(
                        rreader.id,
                        std::move(rreader.reader),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack))
                      .get();
    BOOST_REQUIRE((bool)offset_r);

    auto pid20 = model::producer_identity{2, 0};
    auto term_op = stm
                     .begin_tx(
                       pid20,
                       tx_seq,
                       std::chrono::milliseconds(
                         std::numeric_limits<int32_t>::max()),
                       model::partition_id(0))
                     .get();
    BOOST_REQUIRE((bool)term_op);

    rreader = make_rreader(pid20, 0, 5, true);
    offset_r = stm
                 .replicate(
                   rreader.id,
                   std::move(rreader.reader),
                   raft::replicate_options(raft::consistency_level::quorum_ack))
                 .get();
    BOOST_REQUIRE((bool)offset_r);

    auto op = stm.abort_tx(pid20, tx_seq, 2'000ms).get();
    BOOST_REQUIRE_EQUAL(op, cluster::tx::errc::none);

    rreader = make_rreader(pid20, 0, 5, true);
    offset_r = stm
                 .replicate(
                   rreader.id,
                   std::move(rreader.reader),
                   raft::replicate_options(raft::consistency_level::quorum_ack))
                 .get();
    BOOST_REQUIRE(offset_r == invalid_producer_epoch);

    check_snapshot_sizes(stm, _raft.get());
}

// Tests aborted transaction semantics with single and multi segment
// transactions. Multiple subsystems that interact with transactions rely on
// aborted transactions for correctness. These serve as regression tests so that
// we do not break the semantics.
FIXTURE_TEST(test_aborted_transactions, rm_stm_test_fixture) {
    create_stm_and_start_raft();
    auto& stm = *_stm;
    stm.testing_only_disable_auto_abort();

    stm.start().get();

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto log = _storage.local().log_mgr().get(_raft->ntp());
    BOOST_REQUIRE(log);
    auto disk_log = log;

    static int64_t pid_counter = 0;
    const auto tx_seq = model::tx_seq(0);
    const auto timeout = std::chrono::milliseconds(
      std::numeric_limits<int32_t>::max());
    const auto opts = raft::replicate_options(
      raft::consistency_level::quorum_ack);
    size_t segment_count = 1;

    auto& segments = disk_log->segments();

    // Few helpers to avoid repeated boiler plate code.

    auto aborted_txs = [&](auto begin, auto end) {
        return stm.aborted_transactions(begin, end).get();
    };

    // Aborted transactions in a given segment index.
    auto aborted_txes_seg = [&](auto segment_index) {
        BOOST_REQUIRE_GE(segment_index, 0);
        BOOST_REQUIRE_LT(segment_index, segments.size());
        auto offsets = segments[segment_index]->offsets();
        vlog(
          logger.info,
          "Seg index {}, begin {}, end {}",
          segment_index,
          offsets.get_base_offset(),
          offsets.get_dirty_offset());
        return aborted_txs(
          offsets.get_base_offset(), offsets.get_dirty_offset());
    };

    BOOST_REQUIRE_EQUAL(
      aborted_txs(model::offset::min(), model::offset::max()).size(), 0);

    // Begins a tx with random pid and writes a data batch.
    // Returns the associated pid.
    auto start_tx = [&]() {
        auto pid = model::producer_identity{pid_counter++, 0};
        BOOST_REQUIRE(
          stm.begin_tx(pid, tx_seq, timeout, model::partition_id(0)).get());
        auto rreader = make_rreader(pid, 0, 5, true);
        BOOST_REQUIRE(
          stm.replicate(rreader.id, std::move(rreader.reader), opts).get());
        return pid;
    };

    auto commit_tx = [&](auto pid) {
        BOOST_REQUIRE_EQUAL(
          stm.commit_tx(pid, tx_seq, timeout).get(), cluster::tx::errc::none);
    };

    auto abort_tx = [&](auto pid) {
        auto rreader = make_rreader(pid, 5, 5, true);
        BOOST_REQUIRE(
          stm.replicate(rreader.id, std::move(rreader.reader), opts).get());
        BOOST_REQUIRE_EQUAL(
          stm.abort_tx(pid, tx_seq, timeout).get(), cluster::tx::errc::none);
    };

    auto roll_log = [&]() {
        disk_log->force_roll(ss::default_priority_class()).get();
        segment_count++;
        BOOST_REQUIRE_EQUAL(disk_log->segment_count(), segment_count);
    };

    // Single segment transactions
    {
        // case 1: begin commit in the same segment
        auto pid = start_tx();
        auto idx = segment_count - 1;
        commit_tx(pid);
        BOOST_REQUIRE_EQUAL(aborted_txes_seg(idx).size(), 0);
        roll_log();
    }

    {
        // case 2: begin abort in the same segment
        auto pid = start_tx();
        auto idx = segment_count - 1;
        abort_tx(pid);
        BOOST_REQUIRE_EQUAL(aborted_txes_seg(idx).size(), 1);
        roll_log();
    }

    {
        // case 3: interleaved commit abort in the same segment
        // begin pid
        //   begin pid2
        //   abort pid2
        // commit pid
        auto pid = start_tx();
        auto pid2 = start_tx();
        auto idx = segment_count - 1;
        abort_tx(pid2);
        commit_tx(pid);

        auto txes = aborted_txes_seg(idx);
        BOOST_REQUIRE_EQUAL(txes.size(), 1);
        BOOST_REQUIRE_EQUAL(txes[0].pid, pid2);
        roll_log();
    }

    {
        // case 4: interleaved in a different way.
        // begin pid
        //   begin pid2
        // commit pid
        //   abort pid2
        auto pid = start_tx();
        auto pid2 = start_tx();
        auto idx = segment_count - 1;
        commit_tx(pid);
        abort_tx(pid2);

        auto txes = aborted_txes_seg(idx);
        BOOST_REQUIRE_EQUAL(txes.size(), 1);
        BOOST_REQUIRE_EQUAL(txes[0].pid, pid2);
        roll_log();
    }

    // Multi segment transactions

    {
        // case 1: begin in one segment and abort in next.
        // begin
        //  roll
        // abort
        auto pid = start_tx();
        auto idx = segment_count - 1;
        roll_log();
        abort_tx(pid);

        // Aborted tx should show in both the segment ranges.
        for (auto s_idx : {idx, idx + 1}) {
            auto txes = aborted_txes_seg(s_idx);
            BOOST_REQUIRE_EQUAL(txes.size(), 1);
            BOOST_REQUIRE_EQUAL(txes[0].pid, pid);
        }
        roll_log();
    }

    {
        // case 2:
        // begin -- segment 0
        //   roll
        // batches -- segment 1
        //   roll
        // abort -- segment 2
        //
        // We have a segment in the middle without control/txn batches but
        // should still report aborted transaction in it's range.
        auto idx = segment_count - 1;
        auto pid = start_tx();
        roll_log();
        // replicate some non transactional data batches.
        auto rreader = make_rreader(
          model::producer_identity{-1, -1}, 0, 5, false);
        BOOST_REQUIRE(
          stm.replicate(rreader.id, std::move(rreader.reader), opts).get());

        // roll and abort.
        roll_log();
        abort_tx(pid);

        for (auto s_idx : {idx, idx + 1, idx + 2}) {
            auto txes = aborted_txes_seg(s_idx);
            BOOST_REQUIRE_EQUAL(txes.size(), 1);
            BOOST_REQUIRE_EQUAL(txes[0].pid, pid);
        }
        roll_log();
    }

    {
        // case 3:
        // begin pid -- segment 0
        // begin pid2 -- segment 0
        // roll
        // commit pid -- segment 1
        // commit pid2 -- segment 1
        auto idx = segment_count - 1;
        auto pid = start_tx();
        auto pid2 = start_tx();

        roll_log();

        commit_tx(pid);

        // At this point, there are no aborted txs
        for (auto s_idx : {idx, idx + 1}) {
            auto txes = aborted_txes_seg(s_idx);
            BOOST_REQUIRE_EQUAL(txes.size(), 0);
        }

        abort_tx(pid2);

        // Now the aborted tx should show up in both segment ranges.
        for (auto s_idx : {idx, idx + 1}) {
            auto txes = aborted_txes_seg(s_idx);
            BOOST_REQUIRE_EQUAL(txes.size(), 1);
            BOOST_REQUIRE_EQUAL(txes[0].pid, pid2);
        }
    }

    check_snapshot_sizes(stm, _raft.get());
}

template<class T>
void sync_ser_verify(T type) {
    // Serialize synchronously
    iobuf buf;
    reflection::adl<T>{}.to(buf, std::move(type));
    iobuf copy = buf.copy();

    // Deserialize sync/async and compare
    iobuf_parser sync_in(std::move(buf));
    iobuf_parser async_in(std::move(copy));

    auto sync_deser_type = reflection::adl<T>{}.from(sync_in);
    auto async_deser_type = reflection::async_adl<T>{}.from(async_in).get();
    BOOST_REQUIRE(sync_deser_type == async_deser_type);
}

template<class T>
void async_ser_verify(T type) {
    // Serialize asynchronously
    iobuf buf;
    reflection::async_adl<T>{}.to(buf, std::move(type)).get();
    iobuf copy = buf.copy();

    // Deserialize sync/async and compare
    iobuf_parser sync_in(std::move(buf));
    iobuf_parser async_in(std::move(copy));

    auto sync_deser_type = reflection::adl<T>{}.from(sync_in);
    auto async_deser_type = reflection::async_adl<T>{}.from(async_in).get();
    BOOST_REQUIRE(sync_deser_type == async_deser_type);
}

cluster::tx::tx_snapshot_v4 make_tx_snapshot_v4() {
    return {
      .fenced = tests::random_frag_vector(model::random_producer_identity),
      .ongoing = tests::random_frag_vector(model::random_tx_range),
      .prepared = tests::random_frag_vector(cluster::random_prepare_marker),
      .aborted = tests::random_frag_vector(model::random_tx_range),
      .abort_indexes = tests::random_frag_vector(cluster::random_abort_index),
      .offset = model::random_offset(),
      .seqs = tests::random_frag_vector(cluster::random_seq_entry),
      .tx_data = tests::random_frag_vector(cluster::random_tx_data_snapshot),
      .expiration = tests::random_frag_vector(
        cluster::random_expiration_snapshot)};
}

cluster::tx::tx_snapshot_v5 make_tx_snapshot_v5() {
    auto producers = tests::random_frag_vector(
      tests::random_producer_state, 50, ctx_logger);
    fragmented_vector<cluster::tx::producer_state_snapshot_deprecated>
      snapshots;
    for (const auto& producer : producers) {
        auto snapshot = producer->snapshot(kafka::offset{0});
        cluster::tx::producer_state_snapshot_deprecated old_snapshot;
        for (auto& req : snapshot.finished_requests) {
            old_snapshot.finished_requests.push_back(
              {.first_sequence = req.first_sequence,
               .last_sequence = req.last_sequence,
               .last_offset = req.last_offset});
        }
        old_snapshot.id = snapshot.id;
        old_snapshot.group = snapshot.group;
        old_snapshot.ms_since_last_update = snapshot.ms_since_last_update;
        snapshots.push_back(std::move(old_snapshot));
    }
    cluster::tx::tx_snapshot_v5 snap;
    snap.offset = model::random_offset();
    snap.producers = std::move(snapshots),
    snap.fenced = tests::random_frag_vector(model::random_producer_identity),
    snap.ongoing = tests::random_frag_vector(model::random_tx_range),
    snap.prepared = tests::random_frag_vector(cluster::random_prepare_marker),
    snap.aborted = tests::random_frag_vector(model::random_tx_range),
    snap.abort_indexes = tests::random_frag_vector(cluster::random_abort_index),
    snap.tx_data = tests::random_frag_vector(cluster::random_tx_data_snapshot),
    snap.expiration = tests::random_frag_vector(
      cluster::random_expiration_snapshot);
    snap.highest_producer_id = model::random_producer_identity().get_id();
    return snap;
}

SEASTAR_THREAD_TEST_CASE(async_adl_snapshot_validation) {
    // Checks equivalence of async and sync adl serialized snapshots.
    // Serialization of snapshots is switched to async with this commit,
    // makes sure the snapshots are compatible pre/post upgrade.
    sync_ser_verify(make_tx_snapshot_v4());
    async_ser_verify(make_tx_snapshot_v4());
}

FIXTURE_TEST(test_snapshot_v4_v5_equivalence, rm_stm_test_fixture) {
    create_stm_and_start_raft();
    auto& stm = *_stm;
    stm.testing_only_disable_auto_abort();

    stm.start().get();
    wait_for_confirmed_leader();

    // Check the stm can apply v4/v5 snapshots
    {
        auto snap_v4 = make_tx_snapshot_v4();
        snap_v4.offset = stm.last_applied_offset();

        iobuf buf;
        reflection::adl<reflection::tx_snapshot_v4>{}.to(
          buf, std::move(snap_v4));
        raft::stm_snapshot_header hdr{
          .version = reflection::tx_snapshot_v4::version,
          .snapshot_size = static_cast<int32_t>(buf.size_bytes()),
          .offset = stm.last_stable_offset(),
        };
        apply_snapshot(hdr, std::move(buf)).get();

        // validate producer stat after snapshot
        // todo (bharathv): fix this check
        // BOOST_REQUIRE_EQUAL(num_producers_from_snapshot, producers().size());
    }

    {
        auto snap_v5 = make_tx_snapshot_v5();
        snap_v5.offset = stm.last_applied_offset();
        auto highest_pid_from_snapshot = snap_v5.highest_producer_id;

        iobuf buf;
        reflection::async_adl<reflection::tx_snapshot_v5>{}
          .to(buf, std::move(snap_v5))
          .get();
        raft::stm_snapshot_header hdr{
          .version = reflection::tx_snapshot_v5::version,
          .snapshot_size = static_cast<int32_t>(buf.size_bytes()),
          .offset = stm.last_stable_offset(),
        };
        apply_snapshot(hdr, std::move(buf)).get();

        // validate producer stat after snapshot
        // todo (bharathv): fix this check
        // BOOST_REQUIRE_EQUAL(num_producers_from_snapshot, producers().size());
        BOOST_REQUIRE_EQUAL(
          highest_pid_from_snapshot, _stm->highest_producer_id());
    }
}

FIXTURE_TEST(test_tx_expiration_without_data_batches, rm_stm_test_fixture) {
    create_stm_and_start_raft();
    auto& stm = *_stm;
    stm.start().get();
    stm.testing_only_disable_auto_abort();

    wait_for_confirmed_leader();
    wait_for_meta_initialized();
    // Add a fence batch
    auto pid = model::producer_identity{0, 0};
    auto term_op = stm
                     .begin_tx(
                       pid,
                       model::tx_seq{0},
                       std::chrono::milliseconds(10),
                       model::partition_id(0))
                     .get();
    BOOST_REQUIRE(term_op.has_value());
    BOOST_REQUIRE_EQUAL(term_op.value(), _raft->confirmed_term());
    tests::cooperative_spin_wait_with_timeout(5s, [this, pid]() {
        auto [expired, _] = get_expired_producers();
        return std::find_if(
                 expired.begin(),
                 expired.end(),
                 [pid](auto producer) { return producer->id() == pid; })
               != expired.end();
    }).get();
}
