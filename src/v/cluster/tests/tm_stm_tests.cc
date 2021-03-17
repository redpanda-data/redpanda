// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tm_stm.h"
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

static ss::logger tm_logger{"tm_stm-test"};

using op_status = cluster::tm_stm::op_status;
using tm_transaction = cluster::tm_transaction;
using tx_status = cluster::tm_transaction::tx_status;

static tm_transaction expect_tx(std::optional<tm_transaction> maybe_tx) {
    BOOST_REQUIRE((bool)maybe_tx);
    return maybe_tx.value();
}

static tm_transaction expect_tx(checked<tm_transaction, op_status> maybe_tx) {
    BOOST_REQUIRE(maybe_tx.has_value());
    return maybe_tx.value();
}

FIXTURE_TEST(test_tm_stm_new_tx, mux_state_machine_fixture) {
    start_raft();

    cluster::tm_stm stm(tm_logger, _raft.get());

    stm.start().get0();
    auto stop = ss::defer([&stm] { stm.stop().get0(); });

    wait_for_leader();
    wait_for_meta_initialized();

    auto tx_id = kafka::transactional_id("app-id-1");
    auto pid = model::producer_identity{.id = 1, .epoch = 0};

    auto op_code = stm.register_new_producer(tx_id, pid).get0();
    BOOST_REQUIRE_EQUAL(op_code, op_status::success);
    auto tx1 = expect_tx(stm.get_tx(tx_id));
    BOOST_REQUIRE_EQUAL(tx1.id, tx_id);
    BOOST_REQUIRE_EQUAL(tx1.pid, pid);
    BOOST_REQUIRE_EQUAL(tx1.status, tx_status::ongoing);
    BOOST_REQUIRE_EQUAL(tx1.partitions.size(), 0);
    std::vector<tm_transaction::tx_partition> partitions = {
      tm_transaction::tx_partition{
        .ntp = model::ntp("kafka", "topic", 0), .etag = model::term_id(0)},
      tm_transaction::tx_partition{
        .ntp = model::ntp("kafka", "topic", 1), .etag = model::term_id(0)}};
    BOOST_REQUIRE(stm.add_partitions(tx_id, tx1.etag, partitions));
    BOOST_REQUIRE_EQUAL(tx1.partitions.size(), 0);
    auto tx2 = expect_tx(stm.get_tx(tx_id));
    BOOST_REQUIRE_EQUAL(tx2.id, tx_id);
    BOOST_REQUIRE_EQUAL(tx2.pid, pid);
    BOOST_REQUIRE_EQUAL(tx2.status, tx_status::ongoing);
    BOOST_REQUIRE_EQUAL(tx2.tx_seq, tx1.tx_seq);
    BOOST_REQUIRE_EQUAL(tx2.partitions.size(), 2);
    BOOST_REQUIRE_EQUAL(tx1.etag.log_etag, tx2.etag.log_etag);
    BOOST_REQUIRE(tx1.etag.mem_etag < tx2.etag.mem_etag);
    auto tx3 = expect_tx(
      stm.try_change_status(tx_id, tx2.etag, tx_status::preparing).get());
    BOOST_REQUIRE_EQUAL(tx3.id, tx_id);
    BOOST_REQUIRE_EQUAL(tx3.pid, pid);
    BOOST_REQUIRE_EQUAL(tx3.status, tx_status::preparing);
    BOOST_REQUIRE_EQUAL(tx3.tx_seq, tx1.tx_seq);
    BOOST_REQUIRE_EQUAL(tx3.partitions.size(), 2);
    BOOST_REQUIRE(tx2.etag.log_etag < tx3.etag.log_etag);
    auto tx4 = expect_tx(
      stm.try_change_status(tx_id, tx3.etag, tx_status::prepared).get());
    BOOST_REQUIRE_EQUAL(tx4.id, tx_id);
    BOOST_REQUIRE_EQUAL(tx4.pid, pid);
    BOOST_REQUIRE_EQUAL(tx4.status, tx_status::prepared);
    BOOST_REQUIRE_EQUAL(tx4.tx_seq, tx1.tx_seq);
    BOOST_REQUIRE_EQUAL(tx4.partitions.size(), 2);
    BOOST_REQUIRE(tx3.etag.log_etag < tx4.etag.log_etag);
    auto tx5 = expect_tx(stm.mark_tx_finished(tx_id, tx4.etag));
    BOOST_REQUIRE_EQUAL(tx5.id, tx_id);
    BOOST_REQUIRE_EQUAL(tx5.pid, pid);
    BOOST_REQUIRE_EQUAL(tx5.status, tx_status::finished);
    BOOST_REQUIRE_EQUAL(tx5.tx_seq, tx1.tx_seq);
    BOOST_REQUIRE_EQUAL(tx5.partitions.size(), 0);
    BOOST_REQUIRE_EQUAL(tx4.etag.log_etag, tx5.etag.log_etag);
    BOOST_REQUIRE(tx4.etag.mem_etag < tx5.etag.mem_etag);
}

FIXTURE_TEST(test_tm_stm_seq_tx, mux_state_machine_fixture) {
    start_raft();

    cluster::tm_stm stm(tm_logger, _raft.get());

    stm.start().get0();
    auto stop = ss::defer([&stm] { stm.stop().get0(); });

    wait_for_leader();
    wait_for_meta_initialized();

    auto tx_id = kafka::transactional_id("app-id-1");
    auto pid = model::producer_identity{.id = 1, .epoch = 0};

    auto op_code = stm.register_new_producer(tx_id, pid).get0();
    BOOST_REQUIRE_EQUAL(op_code, op_status::success);
    auto tx1 = expect_tx(stm.get_tx(tx_id));
    std::vector<tm_transaction::tx_partition> partitions = {
      tm_transaction::tx_partition{
        .ntp = model::ntp("kafka", "topic", 0), .etag = model::term_id(0)},
      tm_transaction::tx_partition{
        .ntp = model::ntp("kafka", "topic", 1), .etag = model::term_id(0)}};
    BOOST_REQUIRE(stm.add_partitions(tx_id, tx1.etag, partitions));
    auto tx2 = expect_tx(stm.get_tx(tx_id));
    auto tx3 = expect_tx(
      stm.try_change_status(tx_id, tx2.etag, tx_status::preparing).get());
    auto tx4 = expect_tx(
      stm.try_change_status(tx_id, tx3.etag, tx_status::prepared).get());
    auto tx5 = expect_tx(stm.mark_tx_finished(tx_id, tx4.etag));

    auto tx6 = expect_tx(stm.mark_tx_ongoing(tx_id, tx5.etag));
    BOOST_REQUIRE_EQUAL(tx6.id, tx_id);
    BOOST_REQUIRE_EQUAL(tx6.pid, pid);
    BOOST_REQUIRE_EQUAL(tx6.status, tx_status::ongoing);
    BOOST_REQUIRE_EQUAL(tx6.partitions.size(), 0);
    BOOST_REQUIRE_EQUAL(tx5.etag.log_etag, tx6.etag.log_etag);
    BOOST_REQUIRE_LT(tx5.etag.mem_etag, tx6.etag.mem_etag);
    BOOST_REQUIRE_NE(tx6.tx_seq, tx1.tx_seq);
}

FIXTURE_TEST(test_tm_stm_re_tx, mux_state_machine_fixture) {
    start_raft();

    cluster::tm_stm stm(tm_logger, _raft.get());

    stm.start().get0();
    auto stop = ss::defer([&stm] { stm.stop().get0(); });

    wait_for_leader();
    wait_for_meta_initialized();

    auto tx_id = kafka::transactional_id("app-id-1");
    auto pid1 = model::producer_identity{.id = 1, .epoch = 0};

    auto op_code = stm.register_new_producer(tx_id, pid1).get0();
    BOOST_REQUIRE(op_code == op_status::success);
    auto tx1 = expect_tx(stm.get_tx(tx_id));
    std::vector<tm_transaction::tx_partition> partitions = {
      tm_transaction::tx_partition{
        .ntp = model::ntp("kafka", "topic", 0), .etag = model::term_id(0)},
      tm_transaction::tx_partition{
        .ntp = model::ntp("kafka", "topic", 1), .etag = model::term_id(0)}};
    BOOST_REQUIRE(stm.add_partitions(tx_id, tx1.etag, partitions));
    auto tx2 = expect_tx(stm.get_tx(tx_id));
    auto tx3 = expect_tx(
      stm.try_change_status(tx_id, tx2.etag, tx_status::preparing).get());
    auto tx4 = expect_tx(
      stm.try_change_status(tx_id, tx3.etag, tx_status::prepared).get());
    auto tx5 = expect_tx(stm.mark_tx_finished(tx_id, tx4.etag));

    auto pid2 = model::producer_identity{.id = 1, .epoch = 1};
    op_code = stm.re_register_producer(tx_id, tx5.etag, pid2).get0();
    BOOST_REQUIRE_EQUAL(op_code, op_status::success);
    auto tx6 = expect_tx(stm.get_tx(tx_id));
    BOOST_REQUIRE_EQUAL(tx6.id, tx_id);
    BOOST_REQUIRE_EQUAL(tx6.pid, pid2);
    BOOST_REQUIRE_EQUAL(tx6.status, tx_status::ongoing);
    BOOST_REQUIRE_EQUAL(tx6.partitions.size(), 0);
    BOOST_REQUIRE_LT(tx5.etag.log_etag, tx6.etag.log_etag);
}
