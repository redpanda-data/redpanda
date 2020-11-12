// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/configuration.h"
#include "raft/consensus_utils.h"
#include "random/generators.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/randoms.h"

#include <seastar/testing/thread_test_case.hh>

std::vector<model::broker> random_brokers() {
    std::vector<model::broker> ret;
    for (auto i = 0; i < random_generators::get_int(5, 10); ++i) {
        ret.push_back(tests::random_broker(i, i));
    }
    return ret;
}

raft::group_configuration random_configuration() {
    auto brokers = random_brokers();
    raft::group_nodes current;
    for (auto& b : brokers) {
        if (random_generators::get_int(0, 100) > 50) {
            current.voters.push_back(b.id());
        } else {
            current.learners.push_back(b.id());
        }
    }

    if (random_generators::get_int(0, 100) > 50) {
        raft::group_nodes old;
        for (auto& b : brokers) {
            if (random_generators::get_int(0, 100) > 50) {
                old.voters.push_back(b.id());
            } else {
                old.learners.push_back(b.id());
            }
        }
        return raft::group_configuration(
          std::move(brokers), std::move(current), std::move(old));
    } else {
        return raft::group_configuration(
          std::move(brokers), std::move(current));
    }
}

SEASTAR_THREAD_TEST_CASE(roundtrip_raft_configuration_entry) {
    auto cfg = random_configuration();
    // serialize to entry
    auto batches = raft::details::serialize_configuration_as_batches(cfg);
    // extract from entry
    auto new_cfg = reflection::from_iobuf<raft::group_configuration>(
      batches.begin()->copy_records().begin()->release_value());

    BOOST_REQUIRE_EQUAL(new_cfg, cfg);
}

struct test_consumer {
    explicit test_consumer(model::offset base_offset)
      : _next_offset(base_offset + model::offset(1)) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch& b) {
        if (
          !b.compressed()
          && b.header().type == raft::configuration_batch_type) {
            _config_offsets.push_back(_next_offset);
        }
        _next_offset += model::offset(b.header().last_offset_delta)
                        + model::offset(1);
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    std::vector<model::offset> end_of_stream() { return _config_offsets; }

    model::offset _next_offset;
    std::vector<model::offset> _config_offsets;
};

SEASTAR_THREAD_TEST_CASE(test_config_extracting_reader) {
    auto cfg_1 = random_configuration();
    auto cfg_2 = random_configuration();
    using batches_t = ss::circular_buffer<model::record_batch>;
    ss::circular_buffer<model::record_batch> all_batches;

    // serialize to batches
    auto cfg_batch_1 = raft::details::serialize_configuration_as_batches(cfg_1);
    auto cfg_batch_2 = raft::details::serialize_configuration_as_batches(cfg_2);
    auto batches = storage::test::make_random_batches(
      model::offset(0), 10, true);

    std::vector<batches_t> ranges;
    ranges.reserve(4);
    // interleave config batches with data batches
    ranges.push_back(std::move(cfg_batch_1));
    ranges.push_back(
      storage::test::make_random_batches(model::offset(0), 10, true));
    ranges.push_back(std::move(cfg_batch_2));
    ranges.push_back(
      storage::test::make_random_batches(model::offset(0), 10, true));

    for (auto& r : ranges) {
        std::move(r.begin(), r.end(), std::back_inserter(all_batches));
    }

    raft::details::for_each_ref_extract_configuration(
      model::offset(100),
      model::make_memory_record_batch_reader(std::move(all_batches)),
      test_consumer(model::offset(100)),
      model::no_timeout)
      .then([&cfg_1, &cfg_2](auto res) {
          auto& [offsets, configurations] = res;
          BOOST_REQUIRE_EQUAL(offsets[0], model::offset(101));
          BOOST_REQUIRE_EQUAL(configurations[0].offset, model::offset(101));
          BOOST_REQUIRE_EQUAL(configurations[0].cfg, cfg_1);

          BOOST_REQUIRE_EQUAL(configurations[1].offset, offsets[1]);
          BOOST_REQUIRE_EQUAL(configurations[1].cfg, cfg_2);
      })
      .get0();
}
