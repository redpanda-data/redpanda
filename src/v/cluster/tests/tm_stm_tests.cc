// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tests/raft_fixture_retry_policy.h"
#include "cluster/tm_stm.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "test_utils/test.h"
#include "tests/raft_fixture.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <cstdint>
#include <system_error>

namespace {
using namespace raft;

ss::logger tm_logger{"tm_stm-test"};

using stm_t = cluster::tm_stm;
using stm_cssshptrr_t = const ss::shared_ptr<stm_t>&;
using op_status = stm_t::op_status;
using transaction_metadata = cluster::tx_metadata;
using tx_status = cluster::tx_status;
using partitions_t = std::vector<transaction_metadata::tx_partition>;

ss::future<> check_tx(
  const checked<transaction_metadata, op_status>& res,
  kafka::transactional_id tx_id) {
    ASSERT_TRUE_CORO(res);
    ASSERT_EQ_CORO(res.assume_value().id, tx_id);
}

ss::future<> assert_success(op_status status) {
    ASSERT_EQ_CORO(status, op_status::success);
}

ss::future<transaction_metadata> expect_tx(
  checked<transaction_metadata, op_status>&& res,
  kafka::transactional_id tx_id) {
    auto res_mv = std::move(res);
    co_await check_tx(res_mv, tx_id);
    auto ret = std::move(res_mv).value();
    co_return ret;
}

auto expect_tx(kafka::transactional_id tx_id) {
    return [tx_id](checked<transaction_metadata, op_status>&& res)
             -> ss::future<transaction_metadata> {
        return expect_tx(std::move(res), tx_id);
    };
}

struct tm_stm_test_fixture : stm_raft_fixture<stm_t> {
    static constexpr std::chrono::milliseconds TIMEOUT = 30s;

    stm_shptrs_t create_stms(
      state_machine_manager_builder& builder, raft_node_instance& node) {
        return builder.create_stm<stm_t>(
          tm_logger, node.raft().get(), node.get_feature_table());
    }

    template<class Func>
    auto retry_with_term(Func&& func) {
        return retry_with_leader(
          model::timeout_clock::now() + TIMEOUT,
          [this,
           func = std::forward<Func>(func)](raft_node_instance& leader_node) {
              auto stm = get_stm<0>(leader_node);
              auto term = leader_node.raft()->term();
              return func(stm, term);
          });
    }

    ss::future<> register_new_producer(
      kafka::transactional_id tx_id, model::producer_identity pid //
    ) {
        return retry_with_term(
                 [tx_id, pid](stm_cssshptrr_t stm, model::term_id term) {
                     return stm->register_new_producer(term, tx_id, 0s, pid);
                 })
          .then(assert_success);
    }

    ss::future<> add_partitions(
      kafka::transactional_id tx_id,
      const partitions_t& partitions,
      model::tx_seq tx_seq) {
        return retry_with_term([tx_id, &partitions, tx_seq](
                                 stm_cssshptrr_t stm, model::term_id term) {
                   return stm->add_partitions(term, tx_id, tx_seq, partitions);
               })
          .then(assert_success);
    }

    ss::future<transaction_metadata> get_tx(kafka::transactional_id tx_id) {
        return stm_retry_with_leader<0>(
                 TIMEOUT,
                 [tx_id](stm_cssshptrr_t stm) { return stm->get_tx(tx_id); })
          .then(expect_tx(tx_id));
    }

    ss::future<transaction_metadata>
    prepare_commit_tx(kafka::transactional_id tx_id) {
        return retry_with_term(
                 [tx_id](stm_cssshptrr_t stm, model::term_id term)
                   -> ss::future<checked<transaction_metadata, op_status>> {
                     return stm->update_transaction_status(
                       term, tx_id, tx_status::preparing_commit);
                 })
          .then(expect_tx(tx_id));
    }

    ss::future<transaction_metadata>
    finish_tx_commit(kafka::transactional_id tx_id) {
        return retry_with_term(
                 [tx_id](stm_cssshptrr_t stm, model::term_id term)
                   -> ss::future<checked<transaction_metadata, op_status>> {
                     return stm->finish_transaction(
                       term, tx_id, tx_status::completed_commit);
                 })
          .then(expect_tx(tx_id));
    }

    ss::shared_ptr<cluster::tm_stm> _stm;
};

TEST_F_CORO(tm_stm_test_fixture, test_tm_stm_new_tx) {
    co_await initialize_state_machines();

    kafka::transactional_id tx_id("app-id-1");
    model::producer_identity pid{1, 0};

    co_await register_new_producer(tx_id, pid);

    auto tx1 = co_await get_tx(tx_id);
    ASSERT_EQ_CORO(tx1.pid, pid);
    ASSERT_EQ_CORO(tx1.status, tx_status::empty);
    ASSERT_EQ_CORO(tx1.partitions.size(), 0);

    std::vector<transaction_metadata::tx_partition> partitions = {
      transaction_metadata::tx_partition{
        .ntp = model::ntp("kafka", "topic", 0), .etag = model::term_id(0)},
      transaction_metadata::tx_partition{
        .ntp = model::ntp("kafka", "topic", 1), .etag = model::term_id(0)}};
    co_await add_partitions(tx_id, partitions, tx1.tx_seq);

    ASSERT_EQ_CORO(tx1.partitions.size(), 0);

    auto ongoing_tx = co_await get_tx(tx_id);
    ASSERT_EQ_CORO(ongoing_tx.pid, pid);
    ASSERT_EQ_CORO(ongoing_tx.status, tx_status::ongoing);
    ASSERT_EQ_CORO(ongoing_tx.tx_seq, tx1.tx_seq);
    ASSERT_EQ_CORO(ongoing_tx.partitions.size(), 2);

    auto tx4 = co_await prepare_commit_tx(tx_id);
    ASSERT_EQ_CORO(tx4.pid, pid);
    ASSERT_EQ_CORO(tx4.status, tx_status::preparing_commit);
    ASSERT_EQ_CORO(tx4.tx_seq, tx4.tx_seq);
    ASSERT_EQ_CORO(tx4.partitions.size(), 2);

    auto tx5 = co_await finish_tx_commit(tx_id);
    ASSERT_EQ_CORO(tx5.pid, pid);
    ASSERT_EQ_CORO(tx5.status, tx_status::completed_commit);
    ASSERT_EQ_CORO(tx5.tx_seq, tx4.tx_seq);
    ASSERT_EQ_CORO(tx5.partitions.size(), 2);
}

TEST_F_CORO(tm_stm_test_fixture, test_tm_stm_re_tx) {
    co_await initialize_state_machines();

    kafka::transactional_id tx_id("app-id-1");
    model::producer_identity pid1{1, 0};

    co_await register_new_producer(tx_id, pid1);
    co_await get_tx(tx_id);

    partitions_t partitions = {
      transaction_metadata::tx_partition{
        .ntp = model::ntp("kafka", "topic", 0), .etag = model::term_id(0)},
      transaction_metadata::tx_partition{
        .ntp = model::ntp("kafka", "topic", 1), .etag = model::term_id(0)}};

    co_await add_partitions(tx_id, partitions, model::tx_seq{0});
    co_await get_tx(tx_id);
    co_await prepare_commit_tx(tx_id);

    model::producer_identity pid2{1, 1};
    model::producer_identity expected_pid(3, 5);
    co_await retry_with_term([tx_id, pid1, expected_pid, pid2](
                               stm_cssshptrr_t stm, model::term_id term) {
        return stm->update_tx_producer(
          term, tx_id, 0s, pid2, expected_pid, pid1);
    }).then(assert_success);
    auto tx7 = co_await get_tx(tx_id);
    ASSERT_EQ_CORO(tx7.pid, pid2);
    ASSERT_EQ_CORO(tx7.status, tx_status::empty);
    ASSERT_EQ_CORO(tx7.partitions.size(), 0);
}
} // namespace
