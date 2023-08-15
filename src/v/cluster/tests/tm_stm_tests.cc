// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tm_stm.h"
#include "cluster/tm_stm_cache.h"
#include "cluster/tx_coordinator_mapper.h"
#include "features/feature_table.h"
#include "finjector/hbadger.h"
#include "kafka/protocol/types.h"
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
#include "test_utils/async.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/defer.hh>

#include <cstdint>
#include <system_error>

static ss::logger tm_logger{"tm_stm-test"};

struct tm_cache_struct {
    tm_cache_struct() { cache = ss::make_lw_shared<cluster::tm_stm_cache>(); }

    ss::lw_shared_ptr<cluster::tm_stm_cache> cache;
};

using op_status = cluster::tm_stm::op_status;
using tm_transaction = cluster::tm_transaction;
using tx_status = cluster::tm_transaction::tx_status;

static tm_transaction expect_tx(checked<tm_transaction, op_status> maybe_tx) {
    BOOST_REQUIRE(maybe_tx.has_value());
    return maybe_tx.value();
}

FIXTURE_TEST(test_tm_stm_new_tx, mux_state_machine_fixture) {
    start_raft();
    tm_cache_struct tm_cache;

    cluster::tm_stm stm(
      tm_logger,
      _raft.get(),
      std::ref(_feature_table),
      std::ref(tm_cache.cache));
    auto c = _raft.get();

    stm.start().get0();
    auto stop = ss::defer([&stm] { stm.stop().get0(); });

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto tx_id = kafka::transactional_id("app-id-1");
    auto pid = model::producer_identity{1, 0};

    auto op_code = stm
                     .register_new_producer(
                       c->term(), tx_id, std::chrono::milliseconds(0), pid)
                     .get0();
    BOOST_REQUIRE_EQUAL(op_code, op_status::success);
    auto tx1 = expect_tx(stm.get_tx(tx_id).get0());
    BOOST_REQUIRE_EQUAL(tx1.id, tx_id);
    BOOST_REQUIRE_EQUAL(tx1.pid, pid);
    BOOST_REQUIRE_EQUAL(tx1.status, tx_status::ready);
    BOOST_REQUIRE_EQUAL(tx1.partitions.size(), 0);
    expect_tx(stm.mark_tx_ongoing(c->term(), tx_id).get0());
    std::vector<tm_transaction::tx_partition> partitions = {
      tm_transaction::tx_partition{
        .ntp = model::ntp("kafka", "topic", 0), .etag = model::term_id(0)},
      tm_transaction::tx_partition{
        .ntp = model::ntp("kafka", "topic", 1), .etag = model::term_id(0)}};
    BOOST_REQUIRE_EQUAL(
      stm.add_partitions(c->term(), tx_id, partitions).get0(),
      cluster::tm_stm::op_status::success);
    BOOST_REQUIRE_EQUAL(tx1.partitions.size(), 0);
    auto tx2 = expect_tx(stm.get_tx(tx_id).get0());
    BOOST_REQUIRE_EQUAL(tx2.id, tx_id);
    BOOST_REQUIRE_EQUAL(tx2.pid, pid);
    BOOST_REQUIRE_EQUAL(tx2.status, tx_status::ongoing);
    BOOST_REQUIRE_GT(tx2.tx_seq, tx1.tx_seq);
    BOOST_REQUIRE_EQUAL(tx2.partitions.size(), 2);
    auto tx4 = expect_tx(stm.mark_tx_prepared(c->term(), tx_id).get());
    BOOST_REQUIRE_EQUAL(tx4.id, tx_id);
    BOOST_REQUIRE_EQUAL(tx4.pid, pid);
    BOOST_REQUIRE_EQUAL(tx4.status, tx_status::prepared);
    BOOST_REQUIRE_EQUAL(tx4.tx_seq, tx2.tx_seq);
    BOOST_REQUIRE_EQUAL(tx4.partitions.size(), 2);
    auto tx5 = expect_tx(stm.mark_tx_ongoing(c->term(), tx_id).get0());
    BOOST_REQUIRE_EQUAL(tx5.id, tx_id);
    BOOST_REQUIRE_EQUAL(tx5.pid, pid);
    BOOST_REQUIRE_EQUAL(tx5.status, tx_status::ongoing);
    BOOST_REQUIRE_GT(tx5.tx_seq, tx2.tx_seq);
    BOOST_REQUIRE_EQUAL(tx5.partitions.size(), 0);
}

FIXTURE_TEST(test_tm_stm_seq_tx, mux_state_machine_fixture) {
    start_raft();
    tm_cache_struct tm_cache;

    cluster::tm_stm stm(
      tm_logger,
      _raft.get(),
      std::ref(_feature_table),
      std::ref(tm_cache.cache));
    auto c = _raft.get();

    stm.start().get0();
    auto stop = ss::defer([&stm] { stm.stop().get0(); });

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto tx_id = kafka::transactional_id("app-id-1");
    auto pid = model::producer_identity{1, 0};

    auto op_code = stm
                     .register_new_producer(
                       c->term(), tx_id, std::chrono::milliseconds(0), pid)
                     .get0();
    BOOST_REQUIRE_EQUAL(op_code, op_status::success);
    auto tx1 = expect_tx(stm.get_tx(tx_id).get0());
    auto tx2 = stm.mark_tx_ongoing(c->term(), tx_id).get0();
    std::vector<tm_transaction::tx_partition> partitions = {
      tm_transaction::tx_partition{
        .ntp = model::ntp("kafka", "topic", 0), .etag = model::term_id(0)},
      tm_transaction::tx_partition{
        .ntp = model::ntp("kafka", "topic", 1), .etag = model::term_id(0)}};
    BOOST_REQUIRE_EQUAL(
      stm.add_partitions(c->term(), tx_id, partitions).get0(),
      cluster::tm_stm::op_status::success);
    auto tx3 = expect_tx(stm.get_tx(tx_id).get0());
    auto tx5 = expect_tx(stm.mark_tx_prepared(c->term(), tx_id).get());
    auto tx6 = expect_tx(stm.mark_tx_ongoing(c->term(), tx_id).get0());
    BOOST_REQUIRE_EQUAL(tx6.id, tx_id);
    BOOST_REQUIRE_EQUAL(tx6.pid, pid);
    BOOST_REQUIRE_EQUAL(tx6.status, tx_status::ongoing);
    BOOST_REQUIRE_EQUAL(tx6.partitions.size(), 0);
    BOOST_REQUIRE_NE(tx6.tx_seq, tx1.tx_seq);
}

FIXTURE_TEST(test_tm_stm_re_tx, mux_state_machine_fixture) {
    start_raft();
    tm_cache_struct tm_cache;

    cluster::tm_stm stm(
      tm_logger,
      _raft.get(),
      std::ref(_feature_table),
      std::ref(tm_cache.cache));
    auto c = _raft.get();

    stm.start().get0();
    auto stop = ss::defer([&stm] { stm.stop().get0(); });

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto tx_id = kafka::transactional_id("app-id-1");
    auto pid1 = model::producer_identity{1, 0};

    auto op_code = stm
                     .register_new_producer(
                       c->term(), tx_id, std::chrono::milliseconds(0), pid1)
                     .get0();
    BOOST_REQUIRE(op_code == op_status::success);
    auto tx1 = expect_tx(stm.get_tx(tx_id).get0());
    std::vector<tm_transaction::tx_partition> partitions = {
      tm_transaction::tx_partition{
        .ntp = model::ntp("kafka", "topic", 0), .etag = model::term_id(0)},
      tm_transaction::tx_partition{
        .ntp = model::ntp("kafka", "topic", 1), .etag = model::term_id(0)}};
    auto tx2 = stm.mark_tx_ongoing(c->term(), tx_id).get0();
    BOOST_REQUIRE_EQUAL(
      stm.add_partitions(c->term(), tx_id, partitions).get0(),
      cluster::tm_stm::op_status::success);
    auto tx3 = expect_tx(stm.get_tx(tx_id).get0());
    auto tx5 = expect_tx(stm.mark_tx_prepared(c->term(), tx_id).get());
    auto tx6 = expect_tx(stm.mark_tx_ongoing(c->term(), tx_id).get0());

    auto pid2 = model::producer_identity{1, 1};
    auto expected_pid = model::producer_identity(3, 5);
    op_code
      = stm
          .re_register_producer(
            c->term(), tx_id, std::chrono::milliseconds(0), pid2, expected_pid)
          .get0();
    BOOST_REQUIRE_EQUAL(op_code, op_status::success);
    auto tx7 = expect_tx(stm.get_tx(tx_id).get0());
    BOOST_REQUIRE_EQUAL(tx7.id, tx_id);
    BOOST_REQUIRE_EQUAL(tx7.pid, pid2);
    BOOST_REQUIRE_EQUAL(tx7.status, tx_status::ready);
    BOOST_REQUIRE_EQUAL(tx7.partitions.size(), 0);
}

constexpr size_t check_size = 1000;

void test_tm_hosts_tx(cluster::tm_stm& stm, int32_t partition_amount) {
    for (int i = 0; i < check_size; ++i) {
        kafka::transactional_id tx_id(std::to_string(i));
        uint32_t tx_id_hash = cluster::get_tx_id_hash(tx_id);
        if (tx_id_hash <= UINT32_MAX / partition_amount) {
            BOOST_REQUIRE(stm.hosts(tx_id));
        } else {
            BOOST_REQUIRE(!stm.hosts(tx_id));
        }
    }
}

void test_tm_hosts_tx_include_exclude(
  cluster::tm_stm& stm, int32_t partition_amount, const raft::consensus* c) {
    for (int i = check_size; i < check_size * 2; ++i) {
        kafka::transactional_id tx_id(std::to_string(i));
        bool is_already_hosted = stm.hosts(tx_id);
        cluster::tm_stm::op_status exclude_hash_res
          = stm.exclude_hosted_transaction(c->term(), tx_id).get0();
        if (is_already_hosted) {
            BOOST_REQUIRE_EQUAL(
              exclude_hash_res, cluster::tm_stm::op_status::success);
        } else {
            BOOST_REQUIRE_EQUAL(
              exclude_hash_res, cluster::tm_stm::op_status::conflict);
        }
        BOOST_REQUIRE(!stm.hosts(tx_id));
    }

    for (int i = check_size * 2; i < check_size * 3; ++i) {
        kafka::transactional_id tx_id(std::to_string(i));
        bool is_already_hosted = stm.hosts(tx_id);
        cluster::tm_stm::op_status include_hash_res
          = stm.include_hosted_transaction(c->term(), tx_id).get0();
        if (!is_already_hosted) {
            BOOST_REQUIRE_EQUAL(
              include_hash_res, cluster::tm_stm::op_status::success);
        } else {
            BOOST_REQUIRE_EQUAL(
              include_hash_res, cluster::tm_stm::op_status::conflict);
        }
        BOOST_REQUIRE(stm.hosts(tx_id));
    }
}

void test_tm_hosts_tx_include_exclude_saved_in_snapshot(
  cluster::tm_stm& stm, int32_t partition_amount, const raft::consensus* c) {
    for (int i = check_size; i < check_size * 2; ++i) {
        kafka::transactional_id tx_id(std::to_string(i));
        BOOST_REQUIRE(!stm.hosts(tx_id));
    }
    for (int i = check_size * 2; i < check_size * 3; ++i) {
        kafka::transactional_id tx_id(std::to_string(i));
        BOOST_REQUIRE(stm.hosts(tx_id));
    }

    for (int i = check_size; i < check_size * 2; ++i) {
        kafka::transactional_id tx_id(std::to_string(i));
        cluster::tm_stm::op_status include_hash_res
          = stm.include_hosted_transaction(c->term(), tx_id).get0();
        BOOST_REQUIRE_EQUAL(
          include_hash_res, cluster::tm_stm::op_status::success);
        BOOST_REQUIRE(stm.hosts(tx_id));
    }

    for (int i = check_size * 2; i < check_size * 3; ++i) {
        kafka::transactional_id tx_id(std::to_string(i));
        cluster::tm_stm::op_status exclude_hash_res
          = stm.exclude_hosted_transaction(c->term(), tx_id).get0();
        BOOST_REQUIRE_EQUAL(
          exclude_hash_res, cluster::tm_stm::op_status::success);
        BOOST_REQUIRE(!stm.hosts(tx_id));
    }
}

FIXTURE_TEST(test_tm_stm_hosted_hash_1_partition, mux_state_machine_fixture) {
    start_raft();
    tm_cache_struct tm_cache;

    cluster::tm_stm stm(
      tm_logger,
      _raft.get(),
      std::ref(_feature_table),
      std::ref(tm_cache.cache));
    auto c = _raft.get();
    stm.start().get0();

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto tx_id = kafka::transactional_id("test_tx_id");

    BOOST_ASSERT(!stm.hosts(tx_id));
    cluster::tm_stm::op_status init_hash_res
      = stm.try_init_hosted_transactions(c->term(), 1).get0();
    BOOST_REQUIRE_EQUAL(init_hash_res, cluster::tm_stm::op_status::success);
    test_tm_hosts_tx(stm, 1);
    test_tm_hosts_tx_include_exclude(stm, 1, c);

    for (size_t i = 0; i < 10; ++i) {
        try {
            stm.make_snapshot().get0();
        } catch (const std::runtime_error err) {
            ss::sleep(10ms).get0();
            if (i == 9) {
                throw;
            }
        }
    }
    stm.stop().get0();
    stop_all();

    // Test load from snapshot
    start_raft();
    tm_cache_struct tm_cache_new;
    cluster::tm_stm new_stm(
      tm_logger,
      _raft.get(),
      std::ref(_feature_table),
      std::ref(tm_cache_new.cache));
    new_stm.start().get0();
    auto stop = ss::defer([&new_stm] { new_stm.stop().get0(); });
    c = _raft.get();
    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    test_tm_hosts_tx(stm, 1);
    test_tm_hosts_tx_include_exclude_saved_in_snapshot(new_stm, 1, c);
}

FIXTURE_TEST(test_tm_stm_hosted_hash_16_partition, mux_state_machine_fixture) {
    start_raft();
    tm_cache_struct tm_cache;

    cluster::tm_stm stm(
      tm_logger,
      _raft.get(),
      std::ref(_feature_table),
      std::ref(tm_cache.cache));
    auto c = _raft.get();
    stm.start().get0();

    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    auto tx_id = kafka::transactional_id("test_tx_id");

    BOOST_ASSERT(!stm.hosts(tx_id));
    cluster::tm_stm::op_status init_hash_res
      = stm.try_init_hosted_transactions(c->term(), 16).get0();
    BOOST_REQUIRE_EQUAL(init_hash_res, cluster::tm_stm::op_status::success);
    test_tm_hosts_tx(stm, 16);
    test_tm_hosts_tx_include_exclude(stm, 16, c);

    for (size_t i = 0; i < 10; ++i) {
        try {
            stm.make_snapshot().get0();
        } catch (const std::runtime_error err) {
            ss::sleep(10ms).get0();
            if (i == 9) {
                throw;
            }
        }
    }
    stm.stop().get0();
    stop_all();

    // Test load from snapshot
    start_raft();
    tm_cache_struct tm_cache_new;
    cluster::tm_stm new_stm(
      tm_logger,
      _raft.get(),
      std::ref(_feature_table),
      std::ref(tm_cache_new.cache));
    new_stm.start().get0();
    auto stop = ss::defer([&new_stm] { new_stm.stop().get0(); });
    c = _raft.get();
    wait_for_confirmed_leader();
    wait_for_meta_initialized();

    test_tm_hosts_tx(stm, 16);
    test_tm_hosts_tx_include_exclude_saved_in_snapshot(new_stm, 16, c);
}
