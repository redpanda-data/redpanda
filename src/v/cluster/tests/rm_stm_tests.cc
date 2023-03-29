// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/errc.h"
#include "cluster/rm_stm.h"
#include "features/feature_table.h"
#include "finjector/hbadger.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"
#include "raft/consensus_utils.h"
#include "raft/tests/mux_state_machine_fixture.h"
#include "raft/tests/raft_group_fixture.h"
#include "raft/types.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/async.h"
#include "utils/directory_walker.h"

#include <seastar/util/defer.hh>

#include <system_error>

using namespace std::chrono_literals;

static const failure_type<cluster::errc>
  invalid_producer_epoch(cluster::errc::invalid_producer_epoch);

static ss::logger logger{"rm_stm-test"};

struct rich_reader {
    model::batch_identity id;
    model::record_batch_reader reader;
};

static config::binding<uint64_t> get_config_bound() {
    static config::config_store store;
    static config::bounded_property<uint64_t> max_saved_pids_count(
      store,
      "max_saved_pids_count",
      "Max pids count inside rm_stm states",
      {.needs_restart = config::needs_restart::no,
       .visibility = config::visibility::user},
      std::numeric_limits<uint64_t>::max(),
      {.min = 1});

    return max_saved_pids_count.bind();
}

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
    stm.make_snapshot().get();
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
      .get0();

    uint64_t snapshots_size = 0;
    for (const auto& file : snapshot_files) {
        auto file_path = std::filesystem::path(work_dir) / file.c_str();
        snapshots_size += ss::file_size(file_path.string()).get();
    }

    BOOST_REQUIRE_EQUAL(stm.get_snapshot_size(), snapshots_size);
}

// tests:
//   - a simple tx execution succeeds
//   - last_stable_offset doesn't advance past an ongoing transaction
FIXTURE_TEST(test_tx_happy_tx, mux_state_machine_fixture) {
    start_raft();

    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;
    ss::sharded<features::feature_table> feature_table;
    feature_table.start().get0();
    cluster::rm_stm stm(
      logger,
      _raft.get(),
      tx_gateway_frontend,
      feature_table,
      get_config_bound());
    stm.testing_only_disable_auto_abort();

    stm.start().get0();
    auto stop = ss::defer([&stm, &feature_table] {
        stm.stop().get0();
        feature_table.stop().get0();
    });
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
                      .get0();
    BOOST_REQUIRE((bool)offset_r);
    auto aborted_txs = stm.aborted_transactions(min_offset, max_offset).get0();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);
    auto first_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get0();

    auto pid2 = model::producer_identity{2, 0};
    auto term_op = stm
                     .begin_tx(
                       pid2,
                       tx_seq,
                       std::chrono::milliseconds(
                         std::numeric_limits<int32_t>::max()),
                       model::partition_id(0))
                     .get0();
    BOOST_REQUIRE((bool)term_op);

    rreader = make_rreader(pid2, 0, 5, true);
    offset_r = stm
                 .replicate(
                   rreader.id,
                   std::move(rreader.reader),
                   raft::replicate_options(raft::consistency_level::quorum_ack))
                 .get0();
    BOOST_REQUIRE((bool)offset_r);
    auto tx_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get0();
    BOOST_REQUIRE_LE(stm.last_stable_offset(), tx_offset);

    aborted_txs = stm.aborted_transactions(min_offset, max_offset).get0();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);

    auto term = term_op.value();
    auto op = stm
                .prepare_tx(term, model::partition_id(0), pid2, tx_seq, 2'000ms)
                .get0();
    BOOST_REQUIRE_EQUAL(op, cluster::tx_errc::none);
    op = stm.commit_tx(pid2, tx_seq, 2'000ms).get0();
    BOOST_REQUIRE_EQUAL(op, cluster::tx_errc::none);
    aborted_txs = stm.aborted_transactions(min_offset, max_offset).get0();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, tx_offset]() {
        return tx_offset < stm.last_stable_offset();
    }).get0();

    check_snapshot_sizes(stm, _raft.get());
}

// tests:
//   - a simple tx aborting before prepare succeeds
//   - an aborted tx is reflected in aborted_transactions
FIXTURE_TEST(test_tx_aborted_tx_1, mux_state_machine_fixture) {
    start_raft();

    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;
    ss::sharded<features::feature_table> feature_table;
    feature_table.start().get0();
    cluster::rm_stm stm(
      logger,
      _raft.get(),
      tx_gateway_frontend,
      feature_table,
      get_config_bound());
    stm.testing_only_disable_auto_abort();

    stm.start().get0();
    auto stop = ss::defer([&stm, &feature_table] {
        stm.stop().get0();
        feature_table.stop().get0();
    });
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
                      .get0();
    BOOST_REQUIRE((bool)offset_r);
    auto aborted_txs = stm.aborted_transactions(min_offset, max_offset).get0();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);
    auto first_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get0();

    auto pid2 = model::producer_identity{2, 0};
    auto term_op = stm
                     .begin_tx(
                       pid2,
                       tx_seq,
                       std::chrono::milliseconds(
                         std::numeric_limits<int32_t>::max()),
                       model::partition_id(0))
                     .get0();
    BOOST_REQUIRE((bool)term_op);

    rreader = make_rreader(pid2, 0, 5, true);
    offset_r = stm
                 .replicate(
                   rreader.id,
                   std::move(rreader.reader),
                   raft::replicate_options(raft::consistency_level::quorum_ack))
                 .get0();
    BOOST_REQUIRE((bool)offset_r);
    auto tx_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get0();
    BOOST_REQUIRE_LE(stm.last_stable_offset(), tx_offset);
    aborted_txs = stm.aborted_transactions(min_offset, max_offset).get0();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);

    auto op = stm.abort_tx(pid2, tx_seq, 2'000ms).get0();
    BOOST_REQUIRE_EQUAL(op, cluster::tx_errc::none);
    BOOST_REQUIRE(stm
                    .wait_no_throw(
                      _raft.get()->committed_offset(),
                      model::timeout_clock::now() + 2'000ms)
                    .get0());
    aborted_txs = stm.aborted_transactions(min_offset, max_offset).get0();

    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 1);
    BOOST_REQUIRE(
      std::any_of(aborted_txs.begin(), aborted_txs.end(), [pid2](auto x) {
          return x.pid == pid2;
      }));
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, tx_offset]() {
        return tx_offset < stm.last_stable_offset();
    }).get0();

    check_snapshot_sizes(stm, _raft.get());
}

// tests:
//   - a simple tx aborting after prepare succeeds
//   - an aborted tx is reflected in aborted_transactions
FIXTURE_TEST(test_tx_aborted_tx_2, mux_state_machine_fixture) {
    start_raft();

    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;
    ss::sharded<features::feature_table> feature_table;
    feature_table.start().get0();
    cluster::rm_stm stm(
      logger,
      _raft.get(),
      tx_gateway_frontend,
      feature_table,
      get_config_bound());
    stm.testing_only_disable_auto_abort();

    stm.start().get0();
    auto stop = ss::defer([&stm, &feature_table] {
        stm.stop().get0();
        feature_table.stop().get0();
    });
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
                      .get0();
    BOOST_REQUIRE((bool)offset_r);
    auto aborted_txs = stm.aborted_transactions(min_offset, max_offset).get0();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);
    auto first_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get0();

    auto pid2 = model::producer_identity{2, 0};
    auto term_op = stm
                     .begin_tx(
                       pid2,
                       tx_seq,
                       std::chrono::milliseconds(
                         std::numeric_limits<int32_t>::max()),
                       model::partition_id(0))
                     .get0();
    BOOST_REQUIRE((bool)term_op);

    rreader = make_rreader(pid2, 0, 5, true);
    offset_r = stm
                 .replicate(
                   rreader.id,
                   std::move(rreader.reader),
                   raft::replicate_options(raft::consistency_level::quorum_ack))
                 .get0();
    BOOST_REQUIRE((bool)offset_r);
    auto tx_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get0();
    BOOST_REQUIRE_LE(stm.last_stable_offset(), tx_offset);
    aborted_txs = stm.aborted_transactions(min_offset, max_offset).get0();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);

    auto term = term_op.value();
    auto op = stm
                .prepare_tx(term, model::partition_id(0), pid2, tx_seq, 2'000ms)
                .get0();
    BOOST_REQUIRE_EQUAL(op, cluster::tx_errc::none);

    op = stm.abort_tx(pid2, tx_seq, 2'000ms).get0();
    BOOST_REQUIRE_EQUAL(op, cluster::tx_errc::none);
    BOOST_REQUIRE(stm
                    .wait_no_throw(
                      _raft.get()->committed_offset(),
                      model::timeout_clock::now() + 2'000ms)
                    .get0());
    aborted_txs = stm.aborted_transactions(min_offset, max_offset).get0();

    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 1);
    BOOST_REQUIRE(
      std::any_of(aborted_txs.begin(), aborted_txs.end(), [pid2](auto x) {
          return x.pid == pid2;
      }));

    tests::cooperative_spin_wait_with_timeout(10s, [&stm, tx_offset]() {
        return tx_offset < stm.last_stable_offset();
    }).get0();

    check_snapshot_sizes(stm, _raft.get());
}

// transactional writes of an unknown tx are rejected
FIXTURE_TEST(test_tx_unknown_produce, mux_state_machine_fixture) {
    start_raft();

    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;
    ss::sharded<features::feature_table> feature_table;
    feature_table.start().get0();
    cluster::rm_stm stm(
      logger,
      _raft.get(),
      tx_gateway_frontend,
      feature_table,
      get_config_bound());
    stm.testing_only_disable_auto_abort();

    stm.start().get0();
    auto stop = ss::defer([&stm, &feature_table] {
        stm.stop().get0();
        feature_table.stop().get0();
    });

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
                      .get0();
    BOOST_REQUIRE((bool)offset_r);

    auto pid2 = model::producer_identity{2, 0};
    rreader = make_rreader(pid2, 0, 5, true);
    offset_r = stm
                 .replicate(
                   rreader.id,
                   std::move(rreader.reader),
                   raft::replicate_options(raft::consistency_level::quorum_ack))
                 .get0();
    BOOST_REQUIRE(offset_r == invalid_producer_epoch);
}

// begin fences off old transactions
FIXTURE_TEST(test_tx_begin_fences_produce, mux_state_machine_fixture) {
    start_raft();

    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;
    ss::sharded<features::feature_table> feature_table;
    feature_table.start().get0();
    cluster::rm_stm stm(
      logger,
      _raft.get(),
      tx_gateway_frontend,
      feature_table,
      get_config_bound());
    stm.testing_only_disable_auto_abort();

    stm.start().get0();
    auto stop = ss::defer([&stm, &feature_table] {
        stm.stop().get0();
        feature_table.stop().get0();
    });
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
                      .get0();
    BOOST_REQUIRE((bool)offset_r);

    auto pid20 = model::producer_identity{2, 0};
    auto term_op = stm
                     .begin_tx(
                       pid20,
                       tx_seq,
                       std::chrono::milliseconds(
                         std::numeric_limits<int32_t>::max()),
                       model::partition_id(0))
                     .get0();
    BOOST_REQUIRE((bool)term_op);

    auto pid21 = model::producer_identity{2, 1};
    term_op = stm
                .begin_tx(
                  pid21,
                  tx_seq,
                  std::chrono::milliseconds(
                    std::numeric_limits<int32_t>::max()),
                  model::partition_id(0))
                .get0();
    BOOST_REQUIRE((bool)term_op);

    rreader = make_rreader(pid20, 0, 5, true);
    offset_r = stm
                 .replicate(
                   rreader.id,
                   std::move(rreader.reader),
                   raft::replicate_options(raft::consistency_level::quorum_ack))
                 .get0();
    BOOST_REQUIRE(!(bool)offset_r);

    check_snapshot_sizes(stm, _raft.get());
}

// transactional writes of an aborted tx are rejected
FIXTURE_TEST(test_tx_post_aborted_produce, mux_state_machine_fixture) {
    start_raft();

    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;
    ss::sharded<features::feature_table> feature_table;
    feature_table.start().get0();
    cluster::rm_stm stm(
      logger,
      _raft.get(),
      tx_gateway_frontend,
      feature_table,
      get_config_bound());
    stm.testing_only_disable_auto_abort();

    stm.start().get0();
    auto stop = ss::defer([&stm, &feature_table] {
        stm.stop().get0();
        feature_table.stop().get0();
    });
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
                      .get0();
    BOOST_REQUIRE((bool)offset_r);

    auto pid20 = model::producer_identity{2, 0};
    auto term_op = stm
                     .begin_tx(
                       pid20,
                       tx_seq,
                       std::chrono::milliseconds(
                         std::numeric_limits<int32_t>::max()),
                       model::partition_id(0))
                     .get0();
    BOOST_REQUIRE((bool)term_op);

    rreader = make_rreader(pid20, 0, 5, true);
    offset_r = stm
                 .replicate(
                   rreader.id,
                   std::move(rreader.reader),
                   raft::replicate_options(raft::consistency_level::quorum_ack))
                 .get0();
    BOOST_REQUIRE((bool)offset_r);

    auto op = stm.abort_tx(pid20, tx_seq, 2'000ms).get0();
    BOOST_REQUIRE_EQUAL(op, cluster::tx_errc::none);

    rreader = make_rreader(pid20, 0, 5, true);
    offset_r = stm
                 .replicate(
                   rreader.id,
                   std::move(rreader.reader),
                   raft::replicate_options(raft::consistency_level::quorum_ack))
                 .get0();
    BOOST_REQUIRE(offset_r == invalid_producer_epoch);

    check_snapshot_sizes(stm, _raft.get());
}

// Tests aborted transaction semantics with single and multi segment
// transactions. Multiple subsystems that interact with transactions rely on
// aborted transactions for correctness. These serve as regression tests so that
// we do not break the semantics.
FIXTURE_TEST(test_aborted_transactions, mux_state_machine_fixture) {
    start_raft();

    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;
    ss::sharded<features::feature_table> feature_table;
    feature_table.start().get0();
    cluster::rm_stm stm(
      logger,
      _raft.get(),
      tx_gateway_frontend,
      feature_table,
      get_config_bound());
    stm.testing_only_disable_auto_abort();

    stm.start().get0();

    auto stop = ss::defer([&stm, &feature_table] {
        stm.stop().get0();
        feature_table.stop().get0();
    });
    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto log = _storage.local().log_mgr().get(_raft->ntp());
    BOOST_REQUIRE(log);
    auto* disk_log = dynamic_cast<storage::disk_log_impl*>(
      log.value().get_impl());

    static int64_t pid_counter = 0;
    const auto tx_seq = model::tx_seq(0);
    const auto timeout = std::chrono::milliseconds(
      std::numeric_limits<int32_t>::max());
    const auto opts = raft::replicate_options(
      raft::consistency_level::quorum_ack);
    const auto term = _raft->term();
    const auto partition = model::partition_id(0);
    size_t segment_count = 1;

    auto& segments = disk_log->segments();

    // Few helpers to avoid repeated boiler plate code.

    auto aborted_txs = [&](auto begin, auto end) {
        return stm.aborted_transactions(begin, end).get0();
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
          offsets.base_offset,
          offsets.dirty_offset);
        return aborted_txs(offsets.base_offset, offsets.dirty_offset);
    };

    BOOST_REQUIRE_EQUAL(
      aborted_txs(model::offset::min(), model::offset::max()).size(), 0);

    // Begins a tx with random pid and writes a data batch.
    // Returns the associated pid.
    auto start_tx = [&]() {
        auto pid = model::producer_identity{pid_counter++, 0};
        BOOST_REQUIRE(
          stm.begin_tx(pid, tx_seq, timeout, model::partition_id(0)).get0());
        auto rreader = make_rreader(pid, 0, 5, true);
        BOOST_REQUIRE(
          stm.replicate(rreader.id, std::move(rreader.reader), opts).get0());
        return pid;
    };

    auto commit_tx = [&](auto pid) {
        BOOST_REQUIRE_EQUAL(
          stm.prepare_tx(term, partition, pid, tx_seq, timeout).get0(),
          cluster::tx_errc::none);
        BOOST_REQUIRE_EQUAL(
          stm.commit_tx(pid, tx_seq, timeout).get0(), cluster::tx_errc::none);
    };

    auto abort_tx = [&](auto pid) {
        auto rreader = make_rreader(pid, 5, 5, true);
        BOOST_REQUIRE(
          stm.replicate(rreader.id, std::move(rreader.reader), opts).get0());
        BOOST_REQUIRE_EQUAL(
          stm.abort_tx(pid, tx_seq, timeout).get0(), cluster::tx_errc::none);
    };

    auto roll_log = [&]() {
        disk_log->force_roll(ss::default_priority_class()).get0();
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
          stm.replicate(rreader.id, std::move(rreader.reader), opts).get0());

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
