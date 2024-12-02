// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/random.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "storage/index_state.h"

#include <seastar/core/loop.hh>
#include <seastar/core/reactor.hh>
#include <seastar/testing/perf_tests.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

#include <algorithm>
#include <cstdint>

namespace storage {
struct test_data {
    test_data(size_t num_elements) { init(num_elements); }

    void init(
      int num_rows,
      uint32_t max_offset_step = 1000,
      uint32_t max_timestamp_step = 1000,
      uint64_t max_pos_step = 1000) {
        auto offset = random_generators::get_int<uint32_t>(1, 10000);
        auto tx = random_generators::get_int<uint32_t>(1, 10000);
        auto pos = random_generators::get_int<uint64_t>(1, 10000);

        constexpr uint64_t index_step = 64_KiB;
        for (auto i = 0; i < num_rows; ++i) {
            offsets.push_back(offset);
            timestamps.push_back(tx);
            filepos.push_back(pos);
            offset += random_generators::get_int<uint32_t>(0, max_offset_step);
            tx += random_generators::get_int<uint32_t>(0, max_timestamp_step);
            pos += random_generators::get_int<uint64_t>(
              index_step, index_step + max_pos_step);
            if (random_generators::get_int<int>() % 4 == 0) {
                sampling_seq.push_back(i);
            }
        }
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(sampling_seq.begin(), sampling_seq.end(), g);

        // Report memory usage
        storage::index_columns ix;
        storage::compressed_index_columns ixz;
        populate(ix);
        populate(ixz);

        // Non-compressed memory usage
        auto non_compressed_usage
          = ix._position_index.capacity() * sizeof(uint64_t)
            + ix._relative_offset_index.capacity() * sizeof(uint32_t)
            + ix._relative_time_index.capacity() * sizeof(uint32_t);

        // Compressed memory usage
        auto compressed_usage
          = ixz._hints.size()
              * (sizeof(compressed_index_columns::hint_vec_t) + sizeof(uint32_t))
            + ixz._position_index.mem_use()
            + ixz._relative_offset_index.mem_use()
            + ixz._relative_time_index.mem_use();

        std::cout << "Memory usage without compression: "
                  << non_compressed_usage
                  << ", memory usage with compression: " << compressed_usage
                  << std::endl;
    }

    void populate(storage::index_columns_base& ix) {
        for (size_t i = 0; i < offsets.size(); i++) {
            ix.add_entry(offsets.at(i), timestamps.at(i), filepos.at(i));
        }
        ix.shrink_to_fit();
    }

    std::vector<uint32_t> offsets;
    std::vector<uint32_t> timestamps;
    std::vector<uint64_t> filepos;
    // Used to read same values from both indexes
    // during the test
    std::vector<uint64_t> sampling_seq;
};
} // namespace storage

static storage::test_data td(128_MiB / 64_KiB);

PERF_TEST(segment_index_bench, append_compressed) {
    storage::compressed_index_columns ix;

    for (size_t i = 0; i < td.offsets.size(); i++) {
        perf_tests::start_measuring_time();
        ix.add_entry(td.offsets.at(i), td.timestamps.at(i), td.filepos.at(i));
        perf_tests::stop_measuring_time();
    }
}

PERF_TEST(segment_index_bench, append_non_compressed) {
    storage::index_columns ix;

    for (size_t i = 0; i < td.offsets.size(); i++) {
        perf_tests::start_measuring_time();
        ix.add_entry(td.offsets.at(i), td.timestamps.at(i), td.filepos.at(i));
        perf_tests::stop_measuring_time();
    }
}

PERF_TEST(segment_index_bench, materialize_compressed) {
    storage::compressed_index_columns ix;

    td.populate(ix);

    for (auto i : td.sampling_seq) {
        perf_tests::start_measuring_time();
        auto x = ix.get_entry(i);
        perf_tests::stop_measuring_time();
        std::ignore = x;
    }
}

PERF_TEST(segment_index_bench, materialize_non_compressed) {
    storage::index_columns ix;

    td.populate(ix);

    for (auto i : td.sampling_seq) {
        perf_tests::start_measuring_time();
        auto x = ix.get_entry(i);
        perf_tests::stop_measuring_time();
        std::ignore = x;
    }
}

PERF_TEST(segment_index_bench, find_compressed) {
    storage::compressed_index_columns ix;

    td.populate(ix);

    for (auto i : td.sampling_seq) {
        perf_tests::start_measuring_time();
        auto x = ix.offset_lower_bound(td.offsets.at(i));
        perf_tests::stop_measuring_time();
        std::ignore = x;
    }
}

PERF_TEST(segment_index_bench, find_non_compressed) {
    storage::index_columns ix;

    td.populate(ix);

    for (auto i : td.sampling_seq) {
        perf_tests::start_measuring_time();
        auto x = ix.offset_lower_bound(td.offsets.at(i));
        perf_tests::stop_measuring_time();
        std::ignore = x;
    }
}
