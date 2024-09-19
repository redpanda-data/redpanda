// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "container/fragmented_vector.h"
#include "iceberg/manifest_file_packer.h"
#include "iceberg/manifest_list.h"

#include <fmt/core.h>
#include <gtest/gtest.h>

#include <sstream>
#include <vector>

using namespace iceberg;

namespace {

manifest_file make_file(size_t size_bytes) {
    return manifest_file{
      .manifest_path = "dummy",
      .manifest_length = size_bytes,
    };
}

chunked_vector<manifest_file> make_files(const std::vector<size_t>& sizes) {
    chunked_vector<manifest_file> ret;
    std::ranges::transform(sizes, std::back_inserter(ret), make_file);
    return ret;
}

chunked_vector<manifest_packer::bin_t>
make_bins(const std::vector<std::vector<size_t>>& bin_sizes) {
    chunked_vector<chunked_vector<manifest_file>> ret;
    std::ranges::transform(bin_sizes, std::back_inserter(ret), make_files);
    return ret;
}

ss::sstring to_string(const chunked_vector<manifest_packer::bin_t>& bins) {
    std::ostringstream o;
    o << "{";
    for (const auto& bin : bins) {
        o << "{";
        for (const auto& f : bin) {
            o << fmt::format("{} ", f.manifest_length);
        }
        o << "}";
    }
    o << "}";
    return o.str();
}

} // namespace

TEST(ManifestPackerTest, TestEmptyInputs) {
    for (auto target_sz : {0, 100}) {
        auto input = make_files({});
        auto bins = manifest_packer::pack(target_sz, std::move(input));
        ASSERT_EQ(0, bins.size());
    }
}

// Targeted test demonstrating that bins are packed from the back to front.
TEST(ManifestPackerTest, TestPackBackToFront) {
    auto input = make_files({10, 10, 10, 10, 90, 100, 100, 100});
    auto bins = manifest_packer::pack(100, std::move(input));
    auto expected = make_bins({{10, 10, 10}, {10, 90}, {100}, {100}, {100}});
    ASSERT_EQ(bins, expected)
      << fmt::format("{} vs expected {}", to_string(bins), to_string(expected));
}

// Small files towards the back won't be packed if they don't fit.
TEST(ManifestPackerTest, TestPackSmallBackDoesntFit) {
    auto input = make_files({10, 10, 10, 10, 90, 100, 100, 10});
    auto bins = manifest_packer::pack(100, std::move(input));
    auto expected = make_bins({{10, 10, 10}, {10, 90}, {100}, {100}, {10}});
    ASSERT_EQ(bins, expected)
      << fmt::format("{} vs expected {}", to_string(bins), to_string(expected));
}

// Small files towards the back can still be packed if they fit.
TEST(ManifestPackerTest, TestPackSmallBackFits) {
    auto input = make_files({10, 10, 10, 10, 90, 100, 90, 10});
    auto bins = manifest_packer::pack(100, std::move(input));
    auto expected = make_bins({{10, 10, 10}, {10, 90}, {100}, {90, 10}});
    ASSERT_EQ(bins, expected)
      << fmt::format("{} vs expected {}", to_string(bins), to_string(expected));
}

struct expectation_params {
    size_t target;
    std::vector<std::vector<size_t>> expected;
};

class ManifestPackerParamTest
  : public ::testing::TestWithParam<expectation_params> {};

TEST_P(ManifestPackerParamTest, TestInputs) {
    auto params = GetParam();
    const auto expected = make_bins(params.expected);
    auto input = make_files({1, 2, 3, 4, 5});
    auto bins = manifest_packer::pack(params.target, std::move(input));
    ASSERT_EQ(bins, expected)
      << fmt::format("{} vs expected {}", to_string(bins), to_string(expected));
}

// Test cases taken from:
// https://github.com/apache/iceberg-python/blob/b8b2f66be8c74b6567577b16a70c573fdd31ec56/tests/utils/test_bin_packing.py#L97
INSTANTIATE_TEST_SUITE_P(
  Expectations,
  ManifestPackerParamTest,
  ::testing::Values(
    expectation_params{
      .target = 3,
      .expected = {{1, 2}, {3}, {4}, {5}},
    },
    expectation_params{
      .target = 4,
      .expected = {{1, 2}, {3}, {4}, {5}},
    },
    expectation_params{
      .target = 5,
      .expected = {{1}, {2, 3}, {4}, {5}},
    },
    expectation_params{
      .target = 6,
      .expected = {{1, 2, 3}, {4}, {5}},
    },
    expectation_params{
      .target = 7,
      .expected = {{1, 2}, {3, 4}, {5}},
    },
    expectation_params{
      .target = 8,
      .expected = {{1, 2}, {3, 4}, {5}},
    },
    expectation_params{
      .target = 9,
      .expected = {{1, 2, 3}, {4, 5}},
    },
    expectation_params{
      .target = 10,
      .expected = {{1, 2, 3}, {4, 5}},
    },
    expectation_params{
      .target = 11,
      .expected = {{1, 2, 3}, {4, 5}},
    },
    expectation_params{
      .target = 12,
      .expected = {{1, 2}, {3, 4, 5}},
    },
    expectation_params{
      .target = 13,
      .expected = {{1, 2}, {3, 4, 5}},
    },
    expectation_params{
      .target = 14,
      .expected = {{1}, {2, 3, 4, 5}},
    },
    expectation_params{
      .target = 15,
      .expected = {{1, 2, 3, 4, 5}},
    }),
  [](const testing::TestParamInfo<expectation_params>& info) {
      return fmt::format("{}_target_size_{}", info.index, info.param.target);
  });
