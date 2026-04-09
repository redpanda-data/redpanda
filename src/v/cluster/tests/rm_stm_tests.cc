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
#include "finjector/stress_fiber.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/timestamp.h"
#include "raft/consensus_utils.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/batch_generators.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/types.h"
#include "test_utils/async.h"
#include "test_utils/boost_fixture.h"
#include "test_utils/randoms.h"
#include "utils/directory_walker.h"

#include <seastar/util/defer.hh>

#include <system_error>

using namespace std::chrono_literals;

static const failure_type<cluster::errc>
  invalid_producer_epoch(cluster::errc::invalid_producer_epoch);

static constexpr auto timeout = 30min;

struct batches_with_identity {
    model::batch_identity id;
    chunked_vector<model::record_batch> batches;
};

static batches_with_identity make_batches(
  model::producer_identity pid,
  int first_seq,
  int count,
  bool is_transactional,
  bool is_idempotent = false) {
    batches_with_identity result;
    result.id = {
      .pid = pid,
      .first_seq = first_seq,
      .last_seq = first_seq + count - 1,
      .record_count = count,
      .is_transactional = is_transactional};
    auto gen = linear_int_kv_batch_generator();
    auto spec = model::test::record_batch_spec{
      .offset = model::offset(0),
      .allow_compression = true,
      .count = count,
      .enable_idempotence = is_idempotent,
      .producer_id = pid.id,
      .producer_epoch = pid.epoch,
      .base_sequence = first_seq,
      .is_transactional = is_transactional};
    for (auto& batch : gen(spec, 1)) {
        result.batches.push_back(std::move(batch));
    }

    return result;
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

ss::future<result<cluster::kafka_result>>
replicate_all(cluster::rm_stm& stm, batches_with_identity batches) {
    result<cluster::kafka_result> result(cluster::kafka_result{});
    for (auto& batch : batches.batches) {
        result = co_await stm.replicate(
          batches.id,
          std::move(batch),
          raft::replicate_options(raft::consistency_level::quorum_ack));
        if (result.has_error()) {
            co_return result;
        }
    }
    co_return result;
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

    auto pid1 = model::producer_identity{1, 0};
    auto rreader = make_batches(pid1, 0, 5, false);
    auto offset_r = replicate_all(stm, std::move(rreader)).get();

    RPTEST_REQUIRE_EVENTUALLY(
      1s, [&] { return stm.highest_producer_id() == pid1.get_id(); });
    BOOST_REQUIRE((bool)offset_r);
    auto aborted_txs = get_aborted_txs().get();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);
    auto first_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get();

    auto pid2 = model::producer_identity{2, 0};
    auto term_op
      = stm.begin_tx(pid2, tx_seq, timeout, model::partition_id(0)).get();
    BOOST_REQUIRE((bool)term_op);
    BOOST_REQUIRE_EQUAL(stm.highest_producer_id(), pid2.get_id());

    rreader = make_batches(pid2, 0, 5, true);

    offset_r = replicate_all(stm, std::move(rreader)).get();
    BOOST_REQUIRE((bool)offset_r);
    auto tx_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get();
    BOOST_REQUIRE_LE(stm.last_stable_offset(), tx_offset);

    aborted_txs = get_aborted_txs().get();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);

    auto op = stm.commit_tx(pid2, tx_seq, 2'000ms).get();
    BOOST_REQUIRE_EQUAL(op, cluster::tx::errc::none);
    aborted_txs = get_aborted_txs().get();
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
    auto& stm = start_and_disable_auto_abort();

    auto tx_seq = model::tx_seq(0);

    auto pid1 = model::producer_identity{1, 0};
    auto rreader = make_batches(pid1, 0, 5, false);
    auto offset_r = replicate_all(stm, std::move(rreader)).get();
    RPTEST_REQUIRE_EVENTUALLY(
      1s, [&] { return stm.highest_producer_id() == pid1.get_id(); });
    BOOST_REQUIRE((bool)offset_r);
    auto aborted_txs = get_aborted_txs().get();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);
    auto first_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get();

    auto pid2 = model::producer_identity{2, 0};
    auto term_op
      = stm.begin_tx(pid2, tx_seq, timeout, model::partition_id(0)).get();
    BOOST_REQUIRE((bool)term_op);
    BOOST_REQUIRE_EQUAL(stm.highest_producer_id(), pid2.get_id());

    rreader = make_batches(pid2, 0, 5, true);
    offset_r = replicate_all(stm, std::move(rreader)).get();
    BOOST_REQUIRE((bool)offset_r);
    auto tx_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get();
    BOOST_REQUIRE_LE(stm.last_stable_offset(), tx_offset);
    aborted_txs = get_aborted_txs().get();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);

    auto op = stm.abort_tx(pid2, tx_seq, 2'000ms).get();
    BOOST_REQUIRE_EQUAL(op, cluster::tx::errc::none);
    BOOST_REQUIRE(stm
                    .wait_no_throw(
                      _raft.get()->committed_offset(),
                      model::timeout_clock::now() + 2'000ms)
                    .get());
    aborted_txs = get_aborted_txs().get();

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
    auto& stm = start_and_disable_auto_abort();
    auto tx_seq = model::tx_seq(0);

    auto pid1 = model::producer_identity{1, 0};
    auto rreader = make_batches(pid1, 0, 5, false);
    auto offset_r = replicate_all(stm, std::move(rreader)).get();
    RPTEST_REQUIRE_EVENTUALLY(
      1s, [&] { return stm.highest_producer_id() == pid1.get_id(); });
    BOOST_REQUIRE((bool)offset_r);
    auto aborted_txs = get_aborted_txs().get();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);
    auto first_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get();

    auto pid2 = model::producer_identity{2, 0};
    auto term_op
      = stm.begin_tx(pid2, tx_seq, timeout, model::partition_id(0)).get();
    BOOST_REQUIRE_EQUAL(stm.highest_producer_id(), pid2.get_id());
    BOOST_REQUIRE((bool)term_op);

    rreader = make_batches(pid2, 0, 5, true);
    offset_r = replicate_all(stm, std::move(rreader)).get();
    BOOST_REQUIRE_EQUAL(stm.highest_producer_id(), pid2.get_id());
    BOOST_REQUIRE((bool)offset_r);
    auto tx_offset = offset_r.value().last_offset();
    tests::cooperative_spin_wait_with_timeout(10s, [&stm, first_offset]() {
        return first_offset < stm.last_stable_offset();
    }).get();
    BOOST_REQUIRE_LE(stm.last_stable_offset(), tx_offset);
    aborted_txs = get_aborted_txs().get();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);

    auto op = stm.abort_tx(pid2, tx_seq, 2'000ms).get();
    BOOST_REQUIRE_EQUAL(op, cluster::tx::errc::none);
    BOOST_REQUIRE(stm
                    .wait_no_throw(
                      _raft.get()->committed_offset(),
                      model::timeout_clock::now() + 2'000ms)
                    .get());
    aborted_txs = get_aborted_txs().get();

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
    auto& stm = start_and_disable_auto_abort();

    auto pid1 = model::producer_identity{1, 0};
    auto rreader = make_batches(pid1, 0, 5, false);
    auto offset_r = replicate_all(stm, std::move(rreader)).get();
    RPTEST_REQUIRE_EVENTUALLY(
      1s, [&] { return stm.highest_producer_id() == pid1.get_id(); });
    BOOST_REQUIRE((bool)offset_r);

    auto pid2 = model::producer_identity{2, 0};
    rreader = make_batches(pid2, 0, 5, true);
    offset_r = replicate_all(stm, std::move(rreader)).get();
    BOOST_REQUIRE(offset_r == invalid_producer_epoch);
    RPTEST_REQUIRE_EVENTUALLY(
      1s, [&] { return stm.highest_producer_id() == pid1.get_id(); });
}

FIXTURE_TEST(test_stale_begin_tx_fenced, rm_stm_test_fixture) {
    auto& stm = start_and_disable_auto_abort();

    auto tx_seq = model::tx_seq(10);
    auto tx_seq_old = model::tx_seq(9);
    auto tx_seq_new = model::tx_seq(11);
    auto pid1 = model::producer_identity{1, 0};

    auto begin_tx = [&stm, &pid1](model::tx_seq seq) {
        return stm.begin_tx(pid1, seq, timeout, model::partition_id(0)).get();
    };

    auto commit_tx = [&stm, &pid1](model::tx_seq seq) {
        return stm.commit_tx(pid1, seq, timeout).get();
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
    BOOST_REQUIRE_EQUAL(commit_tx(tx_seq), cluster::tx::errc::none);

    // older sequence numbers are fenced
    BOOST_REQUIRE_EQUAL(
      begin_tx(tx_seq_old).error(), cluster::tx::errc::request_rejected);
}

// begin fences off old transactions
FIXTURE_TEST(test_tx_begin_fences_produce, rm_stm_test_fixture) {
    auto& stm = start_and_disable_auto_abort();

    auto tx_seq = model::tx_seq(0);
    auto pid1 = model::producer_identity{1, 0};
    auto rreader = make_batches(pid1, 0, 5, false);
    auto offset_r = replicate_all(stm, std::move(rreader)).get();
    BOOST_REQUIRE((bool)offset_r);

    auto pid20 = model::producer_identity{2, 0};
    auto term_op
      = stm.begin_tx(pid20, tx_seq, timeout, model::partition_id(0)).get();
    BOOST_REQUIRE((bool)term_op);

    auto pid21 = model::producer_identity{2, 1};
    term_op
      = stm.begin_tx(pid21, tx_seq, timeout, model::partition_id(0)).get();
    BOOST_REQUIRE((bool)term_op);

    rreader = make_batches(pid20, 0, 5, true);
    offset_r = replicate_all(stm, std::move(rreader)).get();
    BOOST_REQUIRE(!(bool)offset_r);

    check_snapshot_sizes(stm, _raft.get());
}

// transactional writes of an aborted tx are rejected
FIXTURE_TEST(test_tx_post_aborted_produce, rm_stm_test_fixture) {
    auto& stm = start_and_disable_auto_abort();

    auto tx_seq = model::tx_seq(0);
    auto pid1 = model::producer_identity{1, 0};
    auto rreader = make_batches(pid1, 0, 5, false);
    auto offset_r = replicate_all(stm, std::move(rreader)).get();
    BOOST_REQUIRE((bool)offset_r);

    auto pid20 = model::producer_identity{2, 0};
    auto term_op
      = stm.begin_tx(pid20, tx_seq, timeout, model::partition_id(0)).get();
    BOOST_REQUIRE((bool)term_op);

    rreader = make_batches(pid20, 0, 5, true);
    offset_r = replicate_all(stm, std::move(rreader)).get();
    BOOST_REQUIRE((bool)offset_r);

    auto op = stm.abort_tx(pid20, tx_seq, 2'000ms).get();
    BOOST_REQUIRE_EQUAL(op, cluster::tx::errc::none);

    rreader = make_batches(pid20, 0, 5, true);
    offset_r = replicate_all(stm, std::move(rreader)).get();
    BOOST_REQUIRE(offset_r == invalid_producer_epoch);

    check_snapshot_sizes(stm, _raft.get());
}

// Tests aborted transaction semantics with single and multi segment
// transactions. Multiple subsystems that interact with transactions rely on
// aborted transactions for correctness. These serve as regression tests so that
// we do not break the semantics.
FIXTURE_TEST(test_aborted_transactions, rm_stm_test_fixture) {
    auto& stm = start_and_disable_auto_abort();

    auto log = _storage.local().log_mgr().get(_raft->ntp());
    BOOST_REQUIRE(log);
    auto disk_log = log;

    static int64_t pid_counter = 0;
    const auto tx_seq = model::tx_seq(0);
    size_t segment_count = 1;

    auto& segments = disk_log->segments();

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
        return stm
          .aborted_transactions(
            offsets.get_base_offset(), offsets.get_dirty_offset())
          .get();
    };

    BOOST_REQUIRE_EQUAL(get_aborted_txs().get().size(), 0);

    // Begins a tx with random pid and writes a data batch.
    // Returns the associated pid.
    auto start_tx = [&]() {
        auto pid = model::producer_identity{pid_counter++, 0};
        BOOST_REQUIRE(
          stm.begin_tx(pid, tx_seq, timeout, model::partition_id(0)).get());

        auto result = replicate_all(stm, make_batches(pid, 0, 5, true)).get();
        BOOST_REQUIRE(result.has_value());
        return pid;
    };

    auto commit_tx = [&](auto pid) {
        BOOST_REQUIRE_EQUAL(
          stm.commit_tx(pid, tx_seq, timeout).get(), cluster::tx::errc::none);
    };

    auto abort_tx = [&](auto pid) {
        auto result = replicate_all(stm, make_batches(pid, 5, 5, true)).get();
        BOOST_REQUIRE(result.has_value());
        BOOST_REQUIRE_EQUAL(
          stm.abort_tx(pid, tx_seq, timeout).get(), cluster::tx::errc::none);
    };

    auto roll_log = [&]() {
        disk_log->force_roll().get();
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
        auto result
          = replicate_all(
              stm, make_batches(model::producer_identity{-1, -1}, 0, 5, false))
              .get();
        BOOST_REQUIRE(result.has_value());

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
    auto offset = model::random_offset_above(model::offset(1));
    return {
      .fenced = tests::random_frag_vector(model::random_producer_identity),
      .ongoing = tests::random_frag_vector(
        model::random_tx_range_below, 20, offset),
      .prepared = tests::random_frag_vector(cluster::random_prepare_marker),
      .aborted = tests::random_frag_vector(model::random_tx_range),
      .abort_indexes = tests::random_frag_vector(cluster::random_abort_index),
      .offset = offset,
      .seqs = tests::random_frag_vector(cluster::random_seq_entry),
      .tx_data = tests::random_frag_vector(cluster::random_tx_data_snapshot),
      .expiration = tests::random_frag_vector(
        cluster::random_expiration_snapshot)};
}

cluster::tx::tx_snapshot_v5 make_tx_snapshot_v5() {
    auto producers = tests::random_frag_vector(
      tests::random_producer_state, 50, ctx_logger);
    chunked_vector<cluster::tx::producer_state_snapshot_deprecated> snapshots;
    for (const auto& producer : producers) {
        auto snapshot = producer->snapshot();
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
    snap.offset = model::random_offset_above(model::offset(1));
    snap.producers = std::move(snapshots);
    snap.fenced = tests::random_frag_vector(model::random_producer_identity);
    snap.ongoing = tests::random_frag_vector(
      model::random_tx_range_below, 20, snap.offset);
    snap.prepared = tests::random_frag_vector(cluster::random_prepare_marker);
    snap.aborted = tests::random_frag_vector(model::random_tx_range);
    snap.abort_indexes = tests::random_frag_vector(cluster::random_abort_index);
    snap.tx_data = tests::random_frag_vector(cluster::random_tx_data_snapshot);
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
    auto& stm = start_and_disable_auto_abort();

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
    auto& stm = start_and_disable_auto_abort();

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

/*
 * This test ensures concurrent evictions can happen in the presence of
 * replication operations and operations that reset the state (snapshots,
 * partition stop).
 */
FIXTURE_TEST(test_concurrent_producer_evictions, rm_stm_test_fixture) {
    start_and_disable_auto_abort();

    // Ensure eviction runs with higher frequency
    // and evicts everything possible.
    update_producer_expiration(0ms);
    rearm_eviction_timer(1ms);

    stress_fiber_manager stress_mgr;
    stress_mgr.start(
      {.min_spins_per_scheduling_point = random_generators::get_int(50, 100),
       .max_spins_per_scheduling_point = random_generators::get_int(500, 1000),
       .num_fibers = random_generators::get_int<size_t>(5, 10)});
    auto stop = ss::defer([&stress_mgr] { stress_mgr.stop().get(); });

    int64_t counter = 0;
    ss::abort_source as;
    ss::gate gate;
    size_t max_replication_fibers = 1000;

    // simulates replication.
    // In each iteration of the loop, we create some producers and randomly
    // hold the producer state lock on some of them(thus preventing eviction).
    // This is roughly the lifecycle of replicate requests using a producer
    // state. This creates stream of producer states in a tight loop, some
    // evictable and some non evictable while eviction constantly runs in the
    // background.
    auto replicate_f = ss::do_until(
      [&as] { return as.abort_requested(); },
      [&, this] {
          std::vector<ss::future<>> spawn_replicate_futures;
          for (int i = 0; i < 5; i++) {
              auto maybe_replicate_f
                = maybe_create_producer(model::producer_identity{counter++, 0})
                    .then([&, this](auto result) {
                        BOOST_REQUIRE(result.has_value());
                        auto producer = result.value().first;
                        if (
                          gate.get_count() < max_replication_fibers
                          && tests::random_bool()) {
                            // simulates replication.
                            ssx::spawn_with_gate(gate, [this, producer] {
                                return stm_read_lock().then([producer](
                                                              auto stm_units) {
                                    return producer
                                      ->run_with_lock([](auto units) {
                                          auto sleep_ms
                                            = std::chrono::milliseconds{
                                              random_generators::get_int(3)};
                                          return ss::sleep(sleep_ms).finally(
                                            [units = std::move(units)] {});
                                      })
                                      .handle_exception_type(
                                        [producer](
                                          const ss::gate_closed_exception&) {
                                            vlog(
                                              logger.info,
                                              "producer {} already evicted, "
                                              "ignoring",
                                              producer->id());
                                        })
                                      .finally(
                                        [producer,
                                         stm_units = std::move(stm_units)] {});
                                });
                            });
                        }
                    });
              spawn_replicate_futures.push_back(std::move(maybe_replicate_f));
          }

          return ss::when_all_succeed(std::move(spawn_replicate_futures))
            .then([]() { return ss::sleep(1ms); });
      });

    // simulates raft snapshot application / partition shutdown
    // applying a snapshot is stop the world operation that resets
    // all the producers.
    auto reset_f = ss::do_until(
      [&as] { return as.abort_requested(); },
      [&, this] {
          return reset_producers().then([] { return ss::sleep(3ms); });
      });

    ss::sleep(20s).finally([&as] { as.request_abort(); }).get();
    ss::when_all_succeed(std::move(replicate_f), std::move(reset_f)).get();
    gate.close().get();
}

FIXTURE_TEST(test_lso_bound_by_open_tx, rm_stm_test_fixture) {
    auto& stm = start_and_disable_auto_abort();
    auto raft = _raft;

    auto pid = model::producer_identity{0, 0};

    auto random_sleep = []() {
        auto sleep_ms = std::chrono::milliseconds{
          random_generators::get_int(0, 5)};
        return ss::sleep(sleep_ms);
    };

    auto snapshot_and_apply = [&] {
        return local_snapshot(cluster::tx::tx_snapshot::version)
          .then([&](auto snapshot) {
              return random_sleep().then(
                [this, snapshot = std::move(snapshot)]() mutable {
                    return apply_snapshot(
                             snapshot.header, std::move(snapshot.data))
                      .discard_result();
                });
          });
    };

    std::optional<model::offset> open_tx_start_offset;
    auto tx_and_snapshot = [&](model::tx_seq tx_seq) {
        // begin tx
        BOOST_REQUIRE(
          stm.begin_tx(pid, tx_seq, timeout, model::partition_id(0)).get());

        open_tx_start_offset = raft->committed_offset();

        if (tests::random_bool()) {
            // snapshot
            snapshot_and_apply().get();
        }
        // replicate some batches
        random_sleep().get();
        auto result = replicate_all(stm, make_batches(pid, 0, 5, true)).get();
        BOOST_REQUIRE(result.has_value());

        if (tests::random_bool()) {
            // snapshot
            snapshot_and_apply().get();
        }
        random_sleep().get();

        open_tx_start_offset.reset();
        // commit
        BOOST_REQUIRE_EQUAL(
          stm.commit_tx(pid, tx_seq, timeout).get(), cluster::tx::errc::none);
    };

    ss::abort_source as;
    auto lso_bound_checker_f = ss::do_until(
      [&as] { return as.abort_requested(); },
      [&] {
          auto current_lso = stm.last_stable_offset();
          bool lso_bound = !open_tx_start_offset
                           || current_lso == open_tx_start_offset;
          if (!lso_bound) {
              vlog(
                logger.error,
                "LSO {} exceeded earliest open tx offset {}",
                current_lso,
                open_tx_start_offset);
          }
          BOOST_REQUIRE(lso_bound);
          return ss::now();
      });

    auto deadline = ss::lowres_clock::now() + 10s;
    model::tx_seq tx_seq{0};
    while (ss::lowres_clock::now() < deadline) {
        tx_and_snapshot(tx_seq++);
    }
    as.request_abort();
    std::move(lso_bound_checker_f).get();
}

FIXTURE_TEST(test_tx_compaction_last_producer_batch, rm_stm_test_fixture) {
    auto& stm = start_and_disable_auto_abort();

    auto log = _storage.local().log_mgr().get(_raft->ntp());
    BOOST_REQUIRE(log);
    log->stm_hookset()->add_stm(_stm);

    auto tx_seq_zero = model::tx_seq{0};
    auto produce = [&](auto pid, auto& seq, int count = 5) {
        BOOST_REQUIRE(
          stm
            .begin_tx(
              pid, seq, std::chrono::milliseconds(10), model::partition_id(0))
            .get());
        BOOST_REQUIRE(
          replicate_all(stm, make_batches(pid, seq, count, true, true)).get());
        BOOST_REQUIRE_EQUAL(
          stm.commit_tx(pid, seq, timeout).get(), cluster::tx::errc::none);
        seq += count;
        log->flush().get();
    };

    auto pid_zero = model::producer_identity{0, 0};
    produce(pid_zero, tx_seq_zero);

    {
        auto rdr = log
                     ->make_reader(
                       storage::local_log_reader_config(
                         model::offset(0), model::offset::max()))
                     .get();
        auto batches = model::consume_reader_to_memory(
                         std::move(rdr), model::no_timeout)
                         .get();

        // Expect the only non-control raft data batch to be the last batch for
        // a producer.
        for (const auto& b : batches) {
            if (
              b.header().type == model::record_batch_type::raft_data
              && !b.header().attrs.is_control()) {
                BOOST_REQUIRE(stm.is_batch_in_idempotent_window(b.header()));
            }
        }
    }

    // Produce more so we have multiple non-control raft data batches.
    produce(pid_zero, tx_seq_zero);

    {
        auto rdr = log
                     ->make_reader(
                       storage::local_log_reader_config(
                         model::offset(0), model::offset::max()))
                     .get();
        auto batches = model::consume_reader_to_memory(
                         std::move(rdr), model::no_timeout)
                         .get();

        // Expect the last non-control raft data batch to be in the idempotent
        // window. The first batch is no longer in the window because
        // begin_tx resets the request state.
        bool seen_first_batch = false;
        bool seen_last_batch = false;
        for (const auto& b : batches) {
            if (
              b.header().type == model::record_batch_type::raft_data
              && !b.header().attrs.is_control()) {
                if (seen_first_batch) {
                    BOOST_REQUIRE(
                      stm.is_batch_in_idempotent_window(b.header()));
                    seen_last_batch = true;
                } else {
                    BOOST_REQUIRE(
                      !stm.is_batch_in_idempotent_window(b.header()));
                    seen_first_batch = true;
                }
            }
        }
        BOOST_REQUIRE(seen_first_batch);
        BOOST_REQUIRE(seen_last_batch);
    }

    // Force roll and compact the log.
    log->force_roll().get();
    auto& segments = log->segments();
    BOOST_REQUIRE_EQUAL(segments.size(), 2);

    auto& disk_log = dynamic_cast<storage::disk_log_impl&>(*log);

    ss::abort_source as;
    compaction::compaction_config cfg(
      model::offset::max(),
      model::offset::max(),
      model::offset::max(),
      std::nullopt,
      std::nullopt,
      as);
    disk_log.sliding_window_compact(cfg).get();

    {
        auto rdr = log
                     ->make_reader(
                       storage::local_log_reader_config(
                         model::offset(0), model::offset::max()))
                     .get();
        auto batches = model::consume_reader_to_memory(
                         std::move(rdr), model::no_timeout)
                         .get();

        // Expect the _only_ non-control raft data batch to be the last batch
        // for a producer after compaction.
        int num_seen_raft_batches = 0;
        for (const auto& b : batches) {
            if (
              b.header().type == model::record_batch_type::raft_data
              && !b.header().attrs.is_control()) {
                BOOST_REQUIRE(stm.is_batch_in_idempotent_window(b.header()));
                ++num_seen_raft_batches;
            }
        }
        BOOST_REQUIRE_EQUAL(num_seen_raft_batches, 1);
    }

    // Append more batches for a _different_ idempotent producer that will
    // compact pid_zero's records.
    auto pid_one = model::producer_identity{1, 0};
    auto tx_seq_one = model::tx_seq{0};
    produce(pid_one, tx_seq_one);

    log->force_roll().get();

    // Compact again to remove all the records from the idempotent producer's
    // batch.
    disk_log.sliding_window_compact(cfg).get();

    {
        auto rdr = log
                     ->make_reader(
                       storage::local_log_reader_config(
                         model::offset(0), model::offset::max()))
                     .get();
        auto batches = model::consume_reader_to_memory(
                         std::move(rdr), model::no_timeout)
                         .get();

        // Expect a placeholder batch with the relevant important
        // idempotent information.
        bool seen_placeholder_batch_pid_zero = false;
        for (const auto& b : batches) {
            if (
              b.header().type
              == model::record_batch_type::compaction_placeholder) {
                BOOST_REQUIRE(stm.is_batch_in_idempotent_window(b.header()));
                if (b.header().producer_id == pid_zero.get_id()()) {
                    seen_placeholder_batch_pid_zero = true;
                }
            }
        }
        BOOST_REQUIRE(seen_placeholder_batch_pid_zero);
    }

    // Produce with pid zero and one one last time (with non-overlapping
    // records)
    produce(pid_zero, tx_seq_zero);

    log->force_roll().get();

    // Compact one last time with adjacent merging. We should see the
    // placeholder batch for pid_zero disappear since it is no longer the last
    // batch for the producer.
    disk_log.sliding_window_compact(cfg).get();
    disk_log.adjacent_merge_compact(segments.copy(), cfg).get();

    {
        auto rdr = log
                     ->make_reader(
                       storage::local_log_reader_config(
                         model::offset(0), model::offset::max()))
                     .get();
        auto batches = model::consume_reader_to_memory(
                         std::move(rdr), model::no_timeout)
                         .get();

        bool seen_placeholder_batch_pid_zero = false;
        for (const auto& b : batches) {
            if (
              b.header().type
              == model::record_batch_type::compaction_placeholder) {
                if (b.header().producer_id == pid_zero.get_id()()) {
                    seen_placeholder_batch_pid_zero = true;
                }
            }
        }
        BOOST_REQUIRE(!seen_placeholder_batch_pid_zero);
    }
}

// Verifies that idempotent producer state survives a raft snapshot
// roundtrip (take + apply). After restoring, the producers should be
// present and the highest_producer_id should be derived from the
// snapshot entries. Additionally, the restored producers should be able
// to continue producing without sequence errors.
FIXTURE_TEST(test_raft_snapshot_roundtrip, rm_stm_test_fixture) {
    auto& stm = start_and_disable_auto_abort();

    auto pid1 = model::producer_identity{1, 0};
    auto pid2 = model::producer_identity{5, 0};
    auto pid3 = model::producer_identity{10, 0};

    for (auto pid : {pid1, pid2, pid3}) {
        auto rreader = make_batches(pid, 0, 5, false);
        auto offset_r = replicate_all(stm, std::move(rreader)).get();
        BOOST_REQUIRE((bool)offset_r);
    }

    RPTEST_REQUIRE_EVENTUALLY(
      1s, [&] { return stm.highest_producer_id() == pid3.get_id(); });
    BOOST_REQUIRE_EQUAL(producers().size(), 3);

    auto committed_offset = _raft->committed_offset();

    // force a snapshot and restore from it.
    _raft->snapshot_and_truncate_log(committed_offset).get();
    auto snapshot = take_raft_snapshot(committed_offset).get();
    apply_raft_snapshot(snapshot).get();

    // All producers should be restored
    BOOST_REQUIRE_EQUAL(producers().size(), 3);
    BOOST_REQUIRE(producers().contains(pid1.get_id()));
    BOOST_REQUIRE(producers().contains(pid2.get_id()));
    BOOST_REQUIRE(producers().contains(pid3.get_id()));

    // highest_producer_id should be derived from snapshot entries
    BOOST_REQUIRE_EQUAL(stm.highest_producer_id(), pid3.get_id());

    // Verify producers can continue producing after restore.
    // Sequence numbers pick up where they left off (5), so producing
    // with first_seq=5 should succeed.
    for (auto pid : {pid1, pid2, pid3}) {
        auto rreader = make_batches(pid, 5, 5, false);
        auto offset_r = replicate_all(stm, std::move(rreader)).get();
        BOOST_REQUIRE((bool)offset_r);
    }
}

// Ensures that a transaction whose begin marker is in a local snapshot, but
// whose data batches aren't is still able to be committed and aborted after a
// restart.
FIXTURE_TEST(
  test_local_snapshot_preserves_open_tx_producer, rm_stm_test_fixture) {
    start_and_disable_auto_abort();
    auto* stm = _stm.get();

    auto pid1 = model::producer_identity{1, 0};
    auto pid2 = model::producer_identity{2, 0};
    auto tx_seq = model::tx_seq{0};

    BOOST_REQUIRE(stm->begin_tx(pid1, tx_seq, timeout, model::partition_id(0))
                    .get()
                    .has_value());
    BOOST_REQUIRE(stm->begin_tx(pid2, tx_seq, timeout, model::partition_id(0))
                    .get()
                    .has_value());

    // Take a local snapshot after the fences but before data batches.
    // Both producers have open transactions (status=initialized) but no
    // finished_requests.
    stm->write_local_snapshot().get();

    // Replicate data batches for both producers.
    auto r1 = replicate_all(*stm, make_batches(pid1, 0, 5, true)).get();
    BOOST_REQUIRE(r1.has_value());
    auto r2 = replicate_all(*stm, make_batches(pid2, 0, 5, true)).get();
    BOOST_REQUIRE(r2.has_value());
    auto last_data_offset = r2.value().last_offset;

    // Restart the entire raft group. On start, the STM loads the local
    // snapshot from disk and replays the log from the snapshot offset.
    restart_stm_and_raft();
    stm = _stm.get();

    // Ensure the producers weren't lost from restarting.
    BOOST_REQUIRE(producers().contains(pid1.get_id()));
    BOOST_REQUIRE(producers().contains(pid2.get_id()));

    // Ensure the LSO is held back by the open transactions.
    auto lso = stm->last_stable_offset();
    BOOST_REQUIRE_NE(lso, model::invalid_lso);
    BOOST_REQUIRE_LE(lso, model::offset(last_data_offset()));

    // Ensure pid1's transaction can be committed
    auto commit_result = stm->commit_tx(pid1, tx_seq, 2'000ms).get();
    BOOST_REQUIRE_EQUAL(commit_result, cluster::tx::errc::none);

    // Ensure pid2's transaction can be aborted
    auto abort_result
      = stm->abort_tx(pid2, tx_seq, model::timeout_clock::duration{2'000ms})
          .get();
    BOOST_REQUIRE_EQUAL(abort_result, cluster::tx::errc::none);

    RPTEST_REQUIRE_EVENTUALLY(10s, [stm, last_data_offset]() {
        return model::offset(last_data_offset()) < stm->last_stable_offset();
    });
}
