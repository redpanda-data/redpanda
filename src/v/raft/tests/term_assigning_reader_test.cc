// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/timeout_clock.h"
#include "raft/consensus_utils.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

SEASTAR_THREAD_TEST_CASE(test_assigning_batch_term) {
    auto batches = model::test::make_random_batches(model::offset(0), 10).get();
    auto term = model::term_id(11);
    auto src_reader = model::make_memory_record_batch_reader(
      std::move(batches));
    auto assigning_reader
      = model::make_record_batch_reader<raft::details::term_assigning_reader>(
        std::move(src_reader), term);
    auto batches_with_term = model::consume_reader_to_memory(
                               std::move(assigning_reader), model::no_timeout)
                               .get();

    BOOST_REQUIRE_EQUAL(batches_with_term.size(), 10);
    for (auto& b : batches_with_term) {
        BOOST_REQUIRE_EQUAL(b.term(), term);
    }
};

SEASTAR_THREAD_TEST_CASE(test_assigning_batch_term_release) {
    auto batches = model::test::make_random_batches(model::offset(0), 10).get();
    auto term = model::term_id(11);
    auto src_reader = model::make_memory_record_batch_reader(
      std::move(batches));

    auto assigning_reader
      = model::make_record_batch_reader<raft::details::term_assigning_reader>(
        std::move(src_reader), term);
    auto batches_with_term = model::consume_reader_to_memory(
                               std::move(assigning_reader), model::no_timeout)
                               .get();

    BOOST_REQUIRE_EQUAL(batches_with_term.size(), 10);
    for (auto& b : batches_with_term) {
        BOOST_REQUIRE_EQUAL(b.term(), term);
    }
};
