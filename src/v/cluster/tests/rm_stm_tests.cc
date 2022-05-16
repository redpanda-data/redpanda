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
#include "finjector/hbadger.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "raft/consensus_utils.h"
#include "raft/tests/mux_state_machine_fixture.h"
#include "raft/tests/raft_group_fixture.h"
#include "raft/types.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/async.h"

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

static rich_reader make_rreader(
  model::producer_identity pid,
  int first_seq,
  int count,
  bool is_transactional) {
    return rich_reader{
      .id = model::
        batch_identity{.pid = pid, .first_seq = first_seq, .last_seq = first_seq + count - 1, .record_count = count, .is_transactional = is_transactional},
      .reader = random_batch_reader(storage::test::record_batch_spec{
        .offset = model::offset(0),
        .allow_compression = true,
        .count = count,
        .producer_id = pid.id,
        .producer_epoch = pid.epoch,
        .base_sequence = first_seq,
        .is_transactional = is_transactional})};
}

// tests:
//   - a simple tx execution succeeds
//   - last_stable_offset doesn't advance past an ongoing transaction
FIXTURE_TEST(test_tx_happy_tx, mux_state_machine_fixture) {
    start_raft();

    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;
    cluster::rm_stm stm(logger, _raft.get(), tx_gateway_frontend);
    stm.testing_only_disable_auto_abort();
    stm.testing_only_enable_transactions();

    stm.start().get0();
    auto stop = ss::defer([&stm] { stm.stop().get0(); });
    auto tx_seq = model::tx_seq(0);

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto min_offset = model::offset(0);
    auto max_offset = model::offset(std::numeric_limits<int64_t>::max());

    auto pid1 = model::producer_identity{.id = 1, .epoch = 0};
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
    BOOST_REQUIRE_LT(first_offset, stm.last_stable_offset());

    auto pid2 = model::producer_identity{.id = 2, .epoch = 0};
    auto term_op = stm
                     .begin_tx(
                       pid2,
                       tx_seq,
                       std::chrono::milliseconds(
                         std::numeric_limits<int32_t>::max()))
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
    BOOST_REQUIRE_LT(first_offset, stm.last_stable_offset());
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

    BOOST_REQUIRE_LT(tx_offset, stm.last_stable_offset());
}

// tests:
//   - a simple tx aborting before prepare succeeds
//   - an aborted tx is reflected in aborted_transactions
FIXTURE_TEST(test_tx_aborted_tx_1, mux_state_machine_fixture) {
    start_raft();

    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;
    cluster::rm_stm stm(logger, _raft.get(), tx_gateway_frontend);
    stm.testing_only_disable_auto_abort();
    stm.testing_only_enable_transactions();

    stm.start().get0();
    auto stop = ss::defer([&stm] { stm.stop().get0(); });
    auto tx_seq = model::tx_seq(0);

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto min_offset = model::offset(0);
    auto max_offset = model::offset(std::numeric_limits<int64_t>::max());

    auto pid1 = model::producer_identity{.id = 1, .epoch = 0};
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
    BOOST_REQUIRE_LT(first_offset, stm.last_stable_offset());

    auto pid2 = model::producer_identity{.id = 2, .epoch = 0};
    auto term_op = stm
                     .begin_tx(
                       pid2,
                       tx_seq,
                       std::chrono::milliseconds(
                         std::numeric_limits<int32_t>::max()))
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
    BOOST_REQUIRE_LT(first_offset, stm.last_stable_offset());
    BOOST_REQUIRE_LE(stm.last_stable_offset(), tx_offset);
    aborted_txs = stm.aborted_transactions(min_offset, max_offset).get0();
    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 0);

    auto op = stm.abort_tx(pid2, tx_seq, 2'000ms).get0();
    BOOST_REQUIRE_EQUAL(op, cluster::tx_errc::none);
    BOOST_REQUIRE(
      stm.wait_no_throw(_raft.get()->committed_offset(), 2'000ms).get0());
    aborted_txs = stm.aborted_transactions(min_offset, max_offset).get0();

    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 1);
    BOOST_REQUIRE(
      std::any_of(aborted_txs.begin(), aborted_txs.end(), [pid2](auto x) {
          return x.pid == pid2;
      }));

    BOOST_REQUIRE_LT(tx_offset, stm.last_stable_offset());
}

// tests:
//   - a simple tx aborting after prepare succeeds
//   - an aborted tx is reflected in aborted_transactions
FIXTURE_TEST(test_tx_aborted_tx_2, mux_state_machine_fixture) {
    start_raft();

    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;
    cluster::rm_stm stm(logger, _raft.get(), tx_gateway_frontend);
    stm.testing_only_disable_auto_abort();
    stm.testing_only_enable_transactions();

    stm.start().get0();
    auto stop = ss::defer([&stm] { stm.stop().get0(); });
    auto tx_seq = model::tx_seq(0);

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto min_offset = model::offset(0);
    auto max_offset = model::offset(std::numeric_limits<int64_t>::max());

    auto pid1 = model::producer_identity{.id = 1, .epoch = 0};
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
    BOOST_REQUIRE_LT(first_offset, stm.last_stable_offset());

    auto pid2 = model::producer_identity{.id = 2, .epoch = 0};
    auto term_op = stm
                     .begin_tx(
                       pid2,
                       tx_seq,
                       std::chrono::milliseconds(
                         std::numeric_limits<int32_t>::max()))
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
    BOOST_REQUIRE_LT(first_offset, stm.last_stable_offset());
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
    BOOST_REQUIRE(
      stm.wait_no_throw(_raft.get()->committed_offset(), 2'000ms).get0());
    aborted_txs = stm.aborted_transactions(min_offset, max_offset).get0();

    BOOST_REQUIRE_EQUAL(aborted_txs.size(), 1);
    BOOST_REQUIRE(
      std::any_of(aborted_txs.begin(), aborted_txs.end(), [pid2](auto x) {
          return x.pid == pid2;
      }));

    BOOST_REQUIRE_LT(tx_offset, stm.last_stable_offset());
}

// transactional writes of an unknown tx are rejected
FIXTURE_TEST(test_tx_unknown_produce, mux_state_machine_fixture) {
    start_raft();

    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;
    cluster::rm_stm stm(logger, _raft.get(), tx_gateway_frontend);
    stm.testing_only_disable_auto_abort();
    stm.testing_only_enable_transactions();

    stm.start().get0();
    auto stop = ss::defer([&stm] { stm.stop().get0(); });

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto pid1 = model::producer_identity{.id = 1, .epoch = 0};
    auto rreader = make_rreader(pid1, 0, 5, false);
    auto offset_r = stm
                      .replicate(
                        rreader.id,
                        std::move(rreader.reader),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack))
                      .get0();
    BOOST_REQUIRE((bool)offset_r);

    auto pid2 = model::producer_identity{.id = 2, .epoch = 0};
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
    cluster::rm_stm stm(logger, _raft.get(), tx_gateway_frontend);
    stm.testing_only_disable_auto_abort();
    stm.testing_only_enable_transactions();

    stm.start().get0();
    auto stop = ss::defer([&stm] { stm.stop().get0(); });
    auto tx_seq = model::tx_seq(0);

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto pid1 = model::producer_identity{.id = 1, .epoch = 0};
    auto rreader = make_rreader(pid1, 0, 5, false);
    auto offset_r = stm
                      .replicate(
                        rreader.id,
                        std::move(rreader.reader),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack))
                      .get0();
    BOOST_REQUIRE((bool)offset_r);

    auto pid20 = model::producer_identity{.id = 2, .epoch = 0};
    auto term_op = stm
                     .begin_tx(
                       pid20,
                       tx_seq,
                       std::chrono::milliseconds(
                         std::numeric_limits<int32_t>::max()))
                     .get0();
    BOOST_REQUIRE((bool)term_op);

    auto pid21 = model::producer_identity{.id = 2, .epoch = 1};
    term_op = stm
                .begin_tx(
                  pid21,
                  tx_seq,
                  std::chrono::milliseconds(
                    std::numeric_limits<int32_t>::max()))
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
}

// transactional writes of an aborted tx are rejected
FIXTURE_TEST(test_tx_post_aborted_produce, mux_state_machine_fixture) {
    start_raft();

    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;
    cluster::rm_stm stm(logger, _raft.get(), tx_gateway_frontend);
    stm.testing_only_disable_auto_abort();
    stm.testing_only_enable_transactions();

    stm.start().get0();
    auto stop = ss::defer([&stm] { stm.stop().get0(); });
    auto tx_seq = model::tx_seq(0);

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto pid1 = model::producer_identity{.id = 1, .epoch = 0};
    auto rreader = make_rreader(pid1, 0, 5, false);
    auto offset_r = stm
                      .replicate(
                        rreader.id,
                        std::move(rreader.reader),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack))
                      .get0();
    BOOST_REQUIRE((bool)offset_r);

    auto pid20 = model::producer_identity{.id = 2, .epoch = 0};
    auto term_op = stm
                     .begin_tx(
                       pid20,
                       tx_seq,
                       std::chrono::milliseconds(
                         std::numeric_limits<int32_t>::max()))
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
}
