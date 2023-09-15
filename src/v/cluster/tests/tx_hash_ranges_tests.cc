// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tm_stm.h"
#include "cluster/tx_hash_ranges.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <cstdint>

SEASTAR_THREAD_TEST_CASE(hash_ranges_addition_union_test) {
    cluster::tx_hash_range range_start(0, 100);
    cluster::tx_hash_range range_middle(101, cluster::tx_tm_hash_max - 100);
    cluster::tx_hash_range range_end(
      cluster::tx_tm_hash_max - 99, cluster::tx_tm_hash_max);
    cluster::tx_hash_ranges_errc add_res;

    cluster::tm_stm::locally_hosted_txs hosted_transactions_1{};
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_1, range_start);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_1, range_middle);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_1, range_end);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);
    BOOST_REQUIRE_EQUAL(hosted_transactions_1.hash_ranges.ranges.size(), 1);
    BOOST_REQUIRE_EQUAL(hosted_transactions_1.hash_ranges.ranges[0].first, 0);
    BOOST_REQUIRE_EQUAL(
      hosted_transactions_1.hash_ranges.ranges[0].last,
      cluster::tx_tm_hash_max);

    cluster::tm_stm::locally_hosted_txs hosted_transactions_2{};
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_2, range_start);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_2, range_end);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_2, range_middle);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);
    BOOST_REQUIRE_EQUAL(hosted_transactions_2.hash_ranges.ranges.size(), 1);
    BOOST_REQUIRE_EQUAL(hosted_transactions_2.hash_ranges.ranges[0].first, 0);
    BOOST_REQUIRE_EQUAL(
      hosted_transactions_2.hash_ranges.ranges[0].last,
      cluster::tx_tm_hash_max);

    cluster::tm_stm::locally_hosted_txs hosted_transactions_3{};
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_3, range_end);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_3, range_middle);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_3, range_start);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);
    BOOST_REQUIRE_EQUAL(hosted_transactions_3.hash_ranges.ranges.size(), 1);
    BOOST_REQUIRE_EQUAL(hosted_transactions_3.hash_ranges.ranges[0].first, 0);
    BOOST_REQUIRE_EQUAL(
      hosted_transactions_3.hash_ranges.ranges[0].last,
      cluster::tx_tm_hash_max);
}

SEASTAR_THREAD_TEST_CASE(hash_ranges_addition_intersects_test) {
    cluster::tx_hash_ranges_errc add_res;
    cluster::tx_hash_range range_100_200(100, 200);
    cluster::tx_hash_range range_100_150(100, 150);
    cluster::tx_hash_range range_150_170(150, 170);
    cluster::tx_hash_range range_170_200(170, 200);

    cluster::tm_stm::locally_hosted_txs hosted_transactions_1{};
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_1, range_100_200);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_1, range_100_150);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::intersection);
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_1, range_150_170);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::intersection);
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_1, range_170_200);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::intersection);

    cluster::tm_stm::locally_hosted_txs hosted_transactions_2{};
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_2, range_100_150);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_2, range_150_170);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::intersection);
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_2, range_170_200);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);

    cluster::tm_stm::locally_hosted_txs hosted_transactions_3{};
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_3, range_150_170);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_3, range_100_150);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::intersection);
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_3, range_170_200);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::intersection);
}

SEASTAR_THREAD_TEST_CASE(hash_ranges_include_exclude_test) {
    cluster::tx_hash_ranges_errc add_res;

    kafka::transactional_id tx_id("tx_1");
    cluster::tm_stm::locally_hosted_txs hosted_transactions_1{};
    add_res = cluster::hosted_transactions::add_range(
      hosted_transactions_1, {0, cluster::tx_tm_hash_max});
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);
    add_res = cluster::hosted_transactions::include_transaction(
      hosted_transactions_1, tx_id);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::intersection);
    add_res = cluster::hosted_transactions::exclude_transaction(
      hosted_transactions_1, tx_id);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);
    BOOST_REQUIRE(
      !cluster::hosted_transactions::contains(hosted_transactions_1, tx_id));
    add_res = cluster::hosted_transactions::include_transaction(
      hosted_transactions_1, tx_id);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);
    BOOST_REQUIRE(
      cluster::hosted_transactions::contains(hosted_transactions_1, tx_id));
    BOOST_REQUIRE_EQUAL(hosted_transactions_1.included_transactions.size(), 0);

    cluster::tm_stm::locally_hosted_txs hosted_transactions_2{};
    add_res = cluster::hosted_transactions::exclude_transaction(
      hosted_transactions_2, tx_id);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::not_hosted);
    add_res = cluster::hosted_transactions::include_transaction(
      hosted_transactions_2, tx_id);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);
    BOOST_REQUIRE(
      cluster::hosted_transactions::contains(hosted_transactions_2, tx_id));
    add_res = cluster::hosted_transactions::exclude_transaction(
      hosted_transactions_2, tx_id);
    BOOST_REQUIRE(add_res == cluster::tx_hash_ranges_errc::success);
    BOOST_REQUIRE_EQUAL(hosted_transactions_2.excluded_transactions.size(), 0);
}
