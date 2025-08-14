// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/tx_range_manifest.h"
#include "model/record.h"

#include <seastar/core/memory.hh>
#include <seastar/testing/perf_tests.hh>

#include <boost/test/unit_test.hpp>

using namespace cloud_storage;

static remote_segment_path
  segment_path("abcdef01/kafka/topic/0_1/0-1-v1.log.1");

struct dom_deserializer {};
struct sax_deserializer {};

ss::future<> test_body(size_t n) {
    // Test data.
    chunked_vector<model::tx_range> tx_ranges{};
    for (size_t i = 0; i < n; ++i) {
        tx_ranges.push_back(model::tx_range{
          model::producer_identity(i, i + 1),
          model::offset(i + 2),
          model::offset(i + 3)});
    }

    tx_range_manifest m(segment_path, std::move(tx_ranges));
    auto [is, size] = co_await m.serialize();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    co_await ss::copy(is, os);
    auto rstr = make_iobuf_input_stream(std::move(buf));
    tx_range_manifest restored(segment_path);

    auto mem_warn = ss::memory::scoped_large_allocation_warning_threshold(
      128 * 1024 + 1);

    perf_tests::start_measuring_time();
    co_await restored.update(std::move(rstr));
    perf_tests::stop_measuring_time();

    // Sanity check.
    BOOST_REQUIRE(m == restored);
}

PERF_TEST_C(sax_deserializer, 100k) { co_await test_body(100'000); }
