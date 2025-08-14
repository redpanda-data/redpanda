// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/fs_utils.h"
#include "storage/segment.h"
#include "storage/segment_reader.h"
#include "storage/segment_set.h"
#include "storage/storage_resources.h"
#include "test_utils/test_macros.h"
#include "utils/directory_walker.h"

#include <seastar/core/seastar.hh>

#include <gtest/gtest.h>

#include <filesystem>
#include <optional>
#include <ranges>

static ss::logger segment_set_test_log("segment_set_test");

namespace storage {

class SegmentSetFixtureTest : public ::testing::Test {
public:
    struct test_case {
        struct segment_spec {
            segment_spec(model::offset b, model::offset d)
              : base_offset(b)
              , dirty_offset(d) {}
            model::offset base_offset;
            model::offset dirty_offset;
        };
        ss::sstring desc;
        std::vector<segment_spec> segments;
        std::optional<std::vector<segment_spec>> expected_result;
        int expected_num_ignored_files{0};
    };

    static ss::future<int> get_num_ignored_files(size_t test_idx) {
        int num_redundant_files = 0;
        auto dir_path = std::filesystem::current_path()
                        / std::filesystem::path(ss::format("{}", test_idx));
        directory_walker walker;
        co_await walker.walk(
          dir_path.string(),
          [&num_redundant_files](const ss::directory_entry& de) {
              if (de.name.ends_with(".ignore_have_newer")) {
                  ++num_redundant_files;
              }
              return ss::make_ready_future<>();
          });
        co_return num_redundant_files;
    }

    static segment_full_path
    make_segment_path(size_t test_idx, test_case::segment_spec spec) {
        static int cnt = 0;
        return segment_full_path::mock(ss::format(
          "{}/{}_{}_{}", test_idx, spec.base_offset, spec.dirty_offset, cnt++));
    }

    ss::future<segment_set::type>
    make_segment(size_t test_idx, test_case::segment_spec spec) {
        auto ot = make_offset_tracker(spec.base_offset, spec.dirty_offset);
        auto segment_path = make_segment_path(test_idx, spec);
        auto reader = std::make_unique<segment_reader>(
          segment_path, 128_KiB, 10);

        // Touch the file on disk (it may be renamed in
        // maybe_create_contiguous_segment_set).
        co_await ss::open_file_dma(
          segment_path.string(), ss::open_flags::create);
        segment_index idx(
          segment_full_path::mock("mocked"),
          spec.base_offset,
          segment_index::default_data_buffer_step,
          features,
          std::nullopt);

        co_return ss::make_lw_shared<storage::segment>(
          std::move(ot),
          std::move(reader),
          std::move(idx),
          nullptr,
          std::nullopt,
          std::nullopt,
          resources,
          segment::generation_id{0});
    }

    ss::future<> run_test_case(size_t idx, test_case test) {
        vlog(segment_set_test_log.info, "Running test case: {}", test.desc);
        segment_set::underlying_t segs;
        co_await ss::make_directory(ss::format("{}", idx));
        for (const auto& spec : test.segments) {
            segs.push_back(co_await make_segment(idx, spec));
        }
        auto set_opt = co_await maybe_create_contiguous_segment_set(
          std::move(segs));
        if (!set_opt.has_value()) {
            RPTEST_REQUIRE_CORO(!test.expected_result.has_value());
        } else {
            RPTEST_REQUIRE_CORO(test.expected_result.has_value());
            auto& res = set_opt.value();
            auto& expected = test.expected_result.value();
            RPTEST_REQUIRE_EQ_CORO(res.size(), expected.size());
            for (size_t i = 0; i < res.size(); ++i) {
                RPTEST_REQUIRE_EQ_CORO(
                  res[i]->offsets().get_base_offset(), expected[i].base_offset);
                RPTEST_REQUIRE_EQ_CORO(
                  res[i]->offsets().get_dirty_offset(),
                  expected[i].dirty_offset);
            }
        }

        RPTEST_REQUIRE_EQ_CORO(
          test.expected_num_ignored_files, co_await get_num_ignored_files(idx));
    }

    static segment::offset_tracker
    make_offset_tracker(model::offset base, model::offset dirty) {
        auto ot = segment::offset_tracker(model::term_id{0}, base);
        ot.set_offset(segment::offset_tracker::dirty_offset_t{dirty});
        return ot;
    }

protected:
    ss::sharded<features::feature_table> features;
    storage::storage_resources resources{};
};

TEST_F(SegmentSetFixtureTest, recovery) {
    using o = model::offset;
    using er = std::vector<test_case::segment_spec>;
    // clang-format off
    std::vector<test_case> test_cases = {
      test_case{
	.desc = "A number of unsorted segments which make up a contiguous, non-overlapping range.",
	.segments = {
	  test_case::segment_spec(o{2},o{3}),
	  test_case::segment_spec(o{4},o{5}),
	  test_case::segment_spec(o{0},o{1}),
	},
	.expected_result = er{
	  test_case::segment_spec(o{0},o{1}),
	  test_case::segment_spec(o{2},o{3}),
	  test_case::segment_spec(o{4},o{5}),
	}
      },
      test_case{
	.desc = "What looks like a segment produced by adjacent merge compaction with unremoved redundant segments should ignore redundant segments.",
	.segments = {
	  test_case::segment_spec(o{0},o{100}),
	  test_case::segment_spec(o{0},o{50}),
	  test_case::segment_spec(o{50},o{100}),
	},
	.expected_result = er{
	  test_case::segment_spec(o{0},o{100}),
	},
	.expected_num_ignored_files = 2
      },
      test_case{
	.desc = "Greedy search picks segments with the largest offset range.",
	.segments = {
	  test_case::segment_spec(o{0},o{100}),
	  test_case::segment_spec(o{0},o{1000}),
	  test_case::segment_spec(o{1001},o{2000}),
	},
	.expected_result = er{
	  test_case::segment_spec(o{0},o{1000}),
	  test_case::segment_spec(o{1001},o{2000}),
	},
	.expected_num_ignored_files = 1
      },
      test_case{
	.desc = "A set of non-contiguous segments returns std::nullopt.",
	.segments = {
	  test_case::segment_spec(o{0},o{49}),
	  test_case::segment_spec(o{51},o{100}),
	},
	.expected_result = std::nullopt
      },
      test_case{
	.desc = "A set of overlapping segments returns std::nullopt.",
	.segments = {
	  test_case::segment_spec(o{0},o{51}),
	  test_case::segment_spec(o{51},o{100}),
	},
	.expected_result = std::nullopt
      },
      test_case{
	.desc = "Greedy search here leads to an invalid state, and returns std::nullopt.",
	.segments = {
	  test_case::segment_spec(o{0},o{99}),
	  test_case::segment_spec(o{0},o{100}),
	  test_case::segment_spec(o{100},o{199}),
	},
	.expected_result = std::nullopt
      },
      test_case{
	.desc = "Overlapping file is ignored, greedy search does the right thing.",
	.segments = {
	  test_case::segment_spec(o{0},o{49}),
	  test_case::segment_spec(o{48},o{100}),
	  test_case::segment_spec(o{50},o{100}),
	},
	.expected_result = er{
	  test_case::segment_spec(o{0},o{49}),
	  test_case::segment_spec(o{50},o{100}),
	},
	.expected_num_ignored_files = 1
      },
      test_case{
	.desc = "Another potential adjacent segment merging case.",
	.segments = {
	  test_case::segment_spec(o{0},o{1}),
	  test_case::segment_spec(o{2},o{3}),
	  test_case::segment_spec(o{4},o{5}),
	  test_case::segment_spec(o{6},o{10}),
	  test_case::segment_spec(o{6},o{7}),
	  test_case::segment_spec(o{8},o{9}),
	  test_case::segment_spec(o{9},o{10}),
	  test_case::segment_spec(o{11},o{10}),
	  test_case::segment_spec(o{11},o{100}),
	},
	.expected_result = er{
	  test_case::segment_spec(o{0},o{1}),
	  test_case::segment_spec(o{2},o{3}),
	  test_case::segment_spec(o{4},o{5}),
	  test_case::segment_spec(o{6},o{10}),
	  test_case::segment_spec(o{11},o{100}),
	},
	.expected_num_ignored_files = 4
      },
      test_case{
	.desc = "Set of zero segments is already 'contiguous'.",
	.segments = {},
	.expected_result = er{},
      },
      test_case{
	.desc = "Set of one segments is already 'contiguous'.",
	.segments = {
	  test_case::segment_spec(o{0},o{100})
	},
	.expected_result = er{
	  test_case::segment_spec(o{0},o{100})
	},
      },
    };
    // clang-format on

    for (size_t idx = 0; idx < test_cases.size(); ++idx) {
        run_test_case(idx, test_cases[idx]).get();
    }
}

} // namespace storage
