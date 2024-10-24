/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "base/vlog.h"
#include "cloud_storage/segment_meta_cstore.h"
#include "cloud_storage/types.h"
#include "common_def.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "utils/delta_for.h"
#include "utils/human.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <iterator>
#include <limits>
#include <ranges>

using namespace cloud_storage;
static ss::logger test("test-logger-s");

// The performance of these tests depend on compiler optimizations a lot.
// The read codepath only works well when the compiler is able to vectorize
// it. Because of that the runtime of the debug version is very high if the
// parameters are the same. To reduce the runtime of the debug version we
// have to use smaller dataset. It's important to actually run the tests in
// debug to detect potential memory bugs using ASan.
#ifdef NDEBUG
static constexpr size_t short_test_size = 10000;
static constexpr size_t long_test_size = 100000;
#else
static constexpr size_t short_test_size = 1500;
static constexpr size_t long_test_size = 10'000;
#endif

std::vector<segment_meta> generate_metadata(size_t sz) {
    // #include "cloud_storage/tests/7_333.json.h"

    namespace rg = random_generators;
    std::vector<segment_meta> manifest;
    segment_meta curr{
      .is_compacted = false,
      .size_bytes = 812,
      .base_offset = model::offset(0),
      .committed_offset = model::offset(0),
      .base_timestamp = model::timestamp(1646430092103),
      .max_timestamp = model::timestamp(1646430092103),
      .delta_offset = model::offset_delta(0),
      .archiver_term = model::term_id(2),
      .segment_term = model::term_id(0),
      .delta_offset_end = model::offset_delta(0),
      .sname_format = segment_name_format::v3,
      .metadata_size_hint = 0,
    };
    bool short_segment_run = false;
    for (size_t i = 0; i < sz; i++) {
        auto s = curr;
        manifest.push_back(s);
        if (short_segment_run) {
            curr.base_offset = model::next_offset(curr.committed_offset);
            curr.committed_offset = curr.committed_offset
                                    + model::offset(rg::get_int(1, 10));
            curr.size_bytes = rg::get_int(1, 200);
            curr.base_timestamp = curr.max_timestamp;
            curr.max_timestamp = model::timestamp(
              curr.max_timestamp.value() + rg::get_int(0, 1000));
            curr.delta_offset = curr.delta_offset_end;
            curr.delta_offset_end = curr.delta_offset_end
                                    + model::offset_delta(rg::get_int(5));
            if (rg::get_int(50) == 0) {
                curr.segment_term = curr.segment_term
                                    + model::term_id(rg::get_int(1, 20));
                curr.archiver_term = curr.archiver_term
                                     + model::term_id(rg::get_int(1, 20));
            }
            curr.metadata_size_hint = rg::get_int(1, 100);
        } else {
            curr.base_offset = model::next_offset(curr.committed_offset);
            curr.committed_offset = curr.committed_offset
                                    + model::offset(rg::get_int(1, 1000));
            curr.size_bytes = rg::get_int(1, 200000);
            curr.base_timestamp = curr.max_timestamp;
            curr.max_timestamp = model::timestamp(
              curr.max_timestamp.value()
              + rg::get_int(0, (int)short_test_size));
            curr.delta_offset = curr.delta_offset_end;
            curr.delta_offset_end = curr.delta_offset_end
                                    + model::offset_delta(rg::get_int(15));
            if (rg::get_int(50) == 0) {
                curr.segment_term = curr.segment_term
                                    + model::term_id(rg::get_int(1, 20));
                curr.archiver_term = curr.archiver_term
                                     + model::term_id(rg::get_int(1, 20));
            }
            curr.metadata_size_hint = rg::get_int(1, 1000);
        }
        if (rg::get_int(200) == 0) {
            short_segment_run = !short_segment_run;
        }
    }
    return manifest;
}

void test_compression_ratio() {
    segment_meta_cstore store;
    auto manifest = generate_metadata(long_test_size);
    for (const auto& sm : manifest) {
        store.insert(sm);
    }

    auto [inflated_size, actual_size] = store.inflated_actual_size();
    vlog(
      test.info,
      "compression ratio after inserting {} elements",
      manifest.size());
    vlog(
      test.info,
      "ratio: {}",
      (static_cast<double>(actual_size) / inflated_size));
    vlog(test.info, "inflated: {}", human::bytes(inflated_size));
    vlog(test.info, "actual: {}", human::bytes(actual_size));
    BOOST_REQUIRE(inflated_size / 4 > actual_size);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_full_compression_ratio) {
    test_compression_ratio();
}

void test_cstore_iter() {
    segment_meta_cstore store;
    auto manifest = generate_metadata(short_test_size);
    for (const auto& sm : manifest) {
        store.insert(sm);
    }

    BOOST_REQUIRE_EQUAL(store.size(), manifest.size());
    auto it = store.begin();
    for (size_t i = 0; i < store.size(); i++) {
        BOOST_REQUIRE(*it == manifest[i]);
        ++it;
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_full_iter) { test_cstore_iter(); }

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_full_find) {
    segment_meta_cstore store;
    auto manifest = generate_metadata(short_test_size);
    for (const auto& sm : manifest) {
        store.insert(sm);
    }

    BOOST_REQUIRE_EQUAL(store.size(), manifest.size());
    for (size_t i = 0; i < store.size(); i++) {
        auto it = store.find(manifest[i].base_offset);
        BOOST_REQUIRE(it != store.end());
        BOOST_REQUIRE(*it == manifest[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_full_lower_bound) {
    segment_meta_cstore store;
    auto manifest = generate_metadata(short_test_size);
    for (const auto& sm : manifest) {
        store.insert(sm);
    }

    BOOST_REQUIRE_EQUAL(store.size(), manifest.size());
    for (size_t i = 0; i < store.size(); i++) {
        auto it = store.lower_bound(manifest[i].base_offset);
        BOOST_REQUIRE(it != store.end());
        BOOST_REQUIRE(*it == manifest[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_full_upper_bound) {
    segment_meta_cstore store;
    auto manifest = generate_metadata(short_test_size);
    for (const auto& sm : manifest) {
        store.insert(sm);
    }

    BOOST_REQUIRE_EQUAL(store.size(), manifest.size());
    for (size_t i = 0; i < store.size(); i++) {
        auto it = store.upper_bound(manifest[i].base_offset - model::offset(1));
        BOOST_REQUIRE(it != store.end());
        BOOST_REQUIRE(*it == manifest[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_iterators) {
    segment_meta_cstore store;

    static auto constexpr segs = std::array{
      segment_meta{
        .is_compacted = false,
        .size_bytes = 1024,
        .base_offset = model::offset{10},
        .committed_offset = model::offset{19},
      },
      segment_meta{
        .is_compacted = false,
        .size_bytes = 2048,
        .base_offset = model::offset{20},
        .committed_offset = model::offset{29},
      },
      segment_meta{
        .is_compacted = false,
        .size_bytes = 4096,
        .base_offset = model::offset{30},
        .committed_offset = model::offset{39},
      },
      segment_meta{
        .is_compacted = false,
        .size_bytes = 4096,
        .base_offset = model::offset{50},
        .committed_offset = model::offset{59},
      },
    };
    for (auto meta : segs) {
        store.insert(meta);
    }

    BOOST_CHECK_EQUAL(store.size(), segs.size());

    BOOST_CHECK(store.begin() == store.begin());
    BOOST_CHECK(store.end() == store.end());
    BOOST_CHECK(store.begin() == store.at_index(0));
    BOOST_CHECK(++store.begin() == store.at_index(1));
    BOOST_CHECK(store.end() == ++store.at_index(segs.size() - 1));
    BOOST_CHECK(store.upper_bound(segs.back().base_offset) == store.end());
    BOOST_CHECK(++store.begin() == store.upper_bound(segs.front().base_offset));
    static_assert(segs[0].base_offset() > 0);
    BOOST_REQUIRE(
      store.upper_bound(segs.front().base_offset - model::offset{1})
      == store.begin());
    if constexpr (requires(segment_meta_cstore store) {
                      store.begin().index();
                  }) {
        // this if constexpr is to quickly share this test between branches TODO
        // remove later
        BOOST_CHECK_EQUAL(store.at_index(0).index(), store.begin().index());
        BOOST_CHECK(store.end().is_end());
        BOOST_CHECK_EQUAL(
          store.at_index(segs.size() - 1).index(), segs.size() - 1);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_full_contains) {
    segment_meta_cstore store;
    auto manifest = generate_metadata(short_test_size);
    for (const auto& sm : manifest) {
        store.insert(sm);
    }

    BOOST_REQUIRE_EQUAL(store.size(), manifest.size());
    for (size_t i = 0; i < store.size(); i++) {
        BOOST_REQUIRE(store.contains(manifest[i].base_offset));
    }
}

void test_cstore_prefix_truncate(size_t test_size, size_t max_truncate_ix) {
    // failing seed:
    // std::istringstream{"10263162"} >> random_generators::internal::gen;
    BOOST_REQUIRE_GE(test_size, 2);
    BOOST_TEST_INFO(fmt::format(
      "random_generators::internal::gen: [{}]",
      random_generators::internal::gen));

    segment_meta_cstore store;
    auto manifest = generate_metadata(test_size);
    for (const auto& sm : manifest) {
        store.insert(sm);
    }

    // Truncate the generated manifest and the column store
    // and check that all operations can be performed.
    auto ix = random_generators::get_int(
      1, (int)std::min(manifest.size() - 1, max_truncate_ix));
    auto iter = std::next(manifest.begin(), ix);
    auto start_offset = iter->base_offset;

    vlog(
      test.info,
      "going to truncate cstore, test_size={}, max_truncate_ix={}, "
      "start_offset={}, num remaining={}",
      test_size,
      max_truncate_ix,
      start_offset,
      std::distance(iter, manifest.end()));
    manifest.erase(manifest.begin(), iter);
    store.prefix_truncate(start_offset);

    BOOST_REQUIRE_EQUAL(store.begin()->base_offset, start_offset);
    BOOST_REQUIRE_EQUAL(store.size(), manifest.size());

    BOOST_REQUIRE_EQUAL(store.size(), manifest.size());
    for (size_t i = 0; i < store.size(); i++) {
        BOOST_REQUIRE(store.contains(manifest[i].base_offset));
        BOOST_REQUIRE_EQUAL(*store.find(manifest[i].base_offset), manifest[i]);
        BOOST_REQUIRE_EQUAL(*store.at_index(i), manifest[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_prefix_truncate_small) {
    test_cstore_prefix_truncate(short_test_size, 100);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_prefix_truncate_full) {
    test_cstore_prefix_truncate(short_test_size, short_test_size);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_prefix_truncate_complete) {
    segment_meta_cstore store;
    auto manifest = generate_metadata(short_test_size);
    for (const auto& sm : manifest) {
        store.insert(sm);
    }

    store.prefix_truncate(model::offset::max());
    BOOST_REQUIRE(store.empty());

    for (const auto& sm : manifest) {
        store.insert(sm);
    }
    store.prefix_truncate(manifest.back().committed_offset + model::offset{1});
    BOOST_REQUIRE(store.empty());
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_serde_roundtrip) {
    segment_meta_cstore store{};
    auto manifest = generate_metadata(10007);
    for (const auto& sm : manifest) {
        store.insert(sm);
    }
    {
        auto [inflated_sz, actual_sz] = store.inflated_actual_size();
        auto pre_serde_size = store.size();
        auto iobuf = store.to_iobuf();
        auto serialized_sz = iobuf.size_bytes();
        BOOST_REQUIRE_EQUAL(store.size(), pre_serde_size);
        store.from_iobuf(std::move(iobuf));
        vlog(
          test.info,
          "store size inflated:{} in memory:{} serialized:{}",
          human::bytes(inflated_sz),
          human::bytes(actual_sz),
          human::bytes(serialized_sz));
    }

    BOOST_REQUIRE_EQUAL(store.size(), manifest.size());

    // NOTE: store.begin() returns an interator that can't be copied around.
    // can't use std::equal needs to copy the iterators around (a quirk of this
    // implementation) with clang15 we have std::views::ref_view +
    // std::ranges::subranges that take care of this
    auto store_it = store.begin();
    auto store_end = store.end();
    auto manifest_it = manifest.begin();
    for (; store_it != store_end; ++store_it, ++manifest_it) {
        BOOST_REQUIRE_EQUAL(*store_it, *manifest_it);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_insert_single_replacement) {
    // only base/committed offset are interesting for this test
    constexpr static auto base_segment = segment_meta{
      .is_compacted = false,
      .size_bytes = 812,
      .base_offset = model::offset(10),
      .committed_offset = model::offset(20),
      .base_timestamp = model::timestamp(1646430092103),
      .max_timestamp = model::timestamp(1646430092103),
      .delta_offset = model::offset_delta(0),
      .archiver_term = model::term_id(2),
      .segment_term = model::term_id(0),
      .delta_offset_end = model::offset_delta(0),
      .sname_format = segment_name_format::v3,
      .metadata_size_hint = 0,
    };
    // this replacement spans more range and comes before base_segment
    constexpr static auto replacement_segment = [] {
        auto cpy = base_segment;
        cpy.base_offset = model::offset{0};
        return cpy;
    }();
    static_assert(
      replacement_segment.base_offset < base_segment.base_offset
      && replacement_segment.committed_offset == base_segment.committed_offset);

    segment_meta_cstore store{};
    store.insert(base_segment);
    BOOST_CHECK_EQUAL(store.size(), 1);
    BOOST_CHECK(*store.begin() == base_segment);
    store.insert(replacement_segment);
    BOOST_CHECK_EQUAL(store.size(), 1);
    BOOST_CHECK(*store.begin() == replacement_segment);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_insert_whole_range_replacement) {
    segment_meta_cstore store{};
    // replacements either start before or exactly at 0
    constexpr auto seg1 = segment_meta{
      .is_compacted = false,
      .size_bytes = 1024,
      .base_offset = model::offset(0),
      .committed_offset = model::offset(10)};

    store.insert(seg1);

    BOOST_CHECK_EQUAL(store.size(), 1);

    constexpr auto seg2 = segment_meta{
      .is_compacted = false,
      .size_bytes = 1024,
      .base_offset = model::offset(11),
      .committed_offset = model::offset(20)};

    store.insert(seg2);

    BOOST_CHECK_EQUAL(store.size(), 2);

    constexpr auto merged_seg = segment_meta{
      .is_compacted = false,
      .size_bytes = 2000,
      .base_offset = model::offset(0),
      .committed_offset = model::offset(20),
    };

    store.insert(merged_seg);

    BOOST_CHECK_EQUAL(store.size(), 1);
    BOOST_CHECK(*store.begin() == merged_seg);

    constexpr auto compacted_seg = segment_meta{
      .is_compacted = true,
      .size_bytes = 100,
      .base_offset = model::offset(0),
      .committed_offset = model::offset(20),
    };

    store.insert(compacted_seg);

    BOOST_CHECK_EQUAL(store.size(), 1);
    BOOST_CHECK(*store.begin() == compacted_seg);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_insert_replacements) {
    // std::istringstream{"1868201168"} >> random_generators::internal::gen;

    BOOST_TEST_INFO(fmt::format(
      "random_generators::internal::gen: [{}]",
      random_generators::internal::gen));

    segment_meta_cstore store{};
    auto manifest = generate_metadata(9973);
    auto replacements = std::vector<segment_meta>{};
    auto to_be_evicted = std::vector<segment_meta>{};
    auto merged_result = std::vector<segment_meta>{};
    // merge segments at random
    auto still_generating = std::accumulate(
      manifest.begin(),
      manifest.end(),
      false,
      [&](bool generating_replacement, const auto& in) {
          if (generating_replacement) {
              if (random_generators::get_int(1) == 1) {
                  // absorb "in" and keep generating
                  replacements.back().committed_offset = in.committed_offset;
                  to_be_evicted.push_back(in);
                  return true;
              }
              // stop generation
              merged_result.push_back(replacements.back());
              merged_result.push_back(in);
              return false;
          }
          // no running generation
          if (random_generators::get_int(1) == 1) {
              // start generating a new replacement that absorbs "in"
              replacements.push_back(in);
              to_be_evicted.push_back(in);
              return true;
          }

          // no-op
          merged_result.push_back(in);
          return false;
      });

    if (still_generating) {
        // close of last generation
        merged_result.push_back(replacements.back());
    }

    // divide the test in two, in each half: apply a portion of the manifest, a
    // portion of replacements, check the last_segment is correct, rinse and
    // repeat
    auto manifest_partition_point
      = manifest.begin()
        + random_generators::get_int<std::ptrdiff_t>(1, manifest.size());
    // divide the replacements in two such as all the replacements in part 1
    // will not come after manifest part 2 this is to ensure that manifest in
    // part 2 can be applied after applying replacements part 1
    auto replacements_partition_point = std::find_if(
      replacements.begin(),
      replacements.end(),
      [val = *std::next(manifest_partition_point, -1)](auto& repl) {
          return repl.committed_offset > val.committed_offset;
      });

    auto insert_segments = [&](
                             std::span<const segment_meta> manifest_slice,
                             std::span<segment_meta> replacements_slice) {
        // insert original run of segments
        for (auto& e : manifest_slice) {
            store.insert(e);
        }

        std::shuffle(
          replacements_slice.begin(),
          replacements_slice.end(),
          random_generators::internal::gen);
        // insert replacements
        for (auto& r : replacements_slice) {
            store.insert(r);
        }
    };

    auto expected_last_seg = [&] {
        auto manifest_middle = *std::next(manifest_partition_point, -1);
        auto replacement_middle = *std::next(replacements_partition_point, -1);
        // replacement is expected to encompass their counterpart in manifest
        return manifest_middle.committed_offset
                   <= replacement_middle.committed_offset
                 ? replacement_middle
                 : manifest_middle;
    }();

    // insert first part, stop to check that last_segment is accurate,
    // insert rest of segments
    insert_segments(
      {manifest.begin(), manifest_partition_point},
      {replacements.begin(), replacements_partition_point});
    BOOST_REQUIRE(store.last_segment() == expected_last_seg);
    insert_segments(
      {manifest_partition_point, manifest.end()},
      {replacements_partition_point, replacements.end()});

    // transfer store to a vector for easier manipulation
    auto store_it = store.begin();
    auto store_end = store.end();
    auto store_result = std::vector<segment_meta>{};
    for (; store_it != store_end; ++store_it) {
        store_result.push_back(*store_it);
    }

    BOOST_REQUIRE(std::equal(
      store_result.begin(),
      store_result.end(),
      merged_result.begin(),
      merged_result.end()));
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_insert_in_gap) {
    auto make_seg = [](auto base) {
        return segment_meta{
          .is_compacted = false,
          .size_bytes = 812,
          .base_offset = model::offset(base),
          .committed_offset = model::offset(base + 9),
          .base_timestamp = model::timestamp(1646430092103),
          .max_timestamp = model::timestamp(1646430092103),
          .delta_offset = model::offset_delta(0),
          .archiver_term = model::term_id(2),
          .segment_term = model::term_id(0),
          .delta_offset_end = model::offset_delta(0),
          .sname_format = segment_name_format::v3,
          .metadata_size_hint = 0,
        };
    };

    std::vector<size_t> baseline_entries{0, 13, 16, 17, 29, 30, 31, 32};
    for (size_t entries : baseline_entries) {
        auto base_offset = 0;
        std::vector<segment_meta> metas;
        segment_meta_cstore store{};

        // Insert different number of baseline entries to get differrent
        // frame configurations.
        for (size_t i = 0; i < entries; ++i) {
            auto seg = make_seg(base_offset);
            base_offset += 10;

            metas.push_back(seg);
            store.insert(seg);
        }

        auto next_seg = make_seg(base_offset);
        auto next_next_seg = make_seg(base_offset + 10);
        auto next_next_next_seg = make_seg(base_offset + 20);

        metas.push_back(next_seg);
        metas.push_back(next_next_seg);
        metas.push_back(next_next_next_seg);

        // Insert two segments and create a gap between them
        store.insert(next_seg);
        store.insert(next_next_next_seg);

        // Flush the write buffer such that the next insert does
        // not get re-ordered in the right place.
        store.flush_write_buffer();

        // Insert in the gap
        store.insert(next_next_seg);

        BOOST_CHECK_EQUAL(store.size(), metas.size());
        BOOST_CHECK(*store.begin() == metas[0]);
        BOOST_CHECK_EQUAL(
          store.last_segment().value(), metas[metas.size() - 1]);

        auto expected_iter = metas.begin();
        auto cstore_iter = store.begin();

        for (; expected_iter != metas.end(); ++expected_iter, ++cstore_iter) {
            BOOST_CHECK_EQUAL(*expected_iter, *cstore_iter);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_overlap_no_replace) {
    auto make_seg = [](auto base, std::optional<int64_t> last) {
        return segment_meta{
          .is_compacted = false,
          .size_bytes = 812,
          .base_offset = model::offset(base),
          .committed_offset = model::offset(last ? *last : base + 9),
          .base_timestamp = model::timestamp(1646430092103),
          .max_timestamp = model::timestamp(1646430092103),
          .delta_offset = model::offset_delta(0),
          .archiver_term = model::term_id(2),
          .segment_term = model::term_id(0),
          .delta_offset_end = model::offset_delta(0),
          .sname_format = segment_name_format::v3,
          .metadata_size_hint = 0,
        };
    };

    std::vector<size_t> baseline_entries{1, 13, 16, 17, 29, 30, 31, 32};
    for (size_t entries : baseline_entries) {
        auto base_offset = 0;
        std::vector<segment_meta> metas;
        segment_meta_cstore store{};

        // Insert different number of baseline entries to get differrent
        // frame configurations.
        for (size_t i = 0; i < entries; ++i) {
            auto seg = make_seg(base_offset, std::nullopt);
            base_offset += 10;

            metas.push_back(seg);
            store.insert(seg);
        }

        auto last_seg = store.last_segment();
        BOOST_REQUIRE(last_seg.has_value());

        // Select a segment that is fully contained by the last segment.
        auto next_seg = make_seg(
          last_seg->base_offset() - 5, last_seg->base_offset() + 5);
        metas.insert(--metas.end(), next_seg);

        // Flush the write buffer such that the next insert does
        // not get re-ordered in the right place.
        store.flush_write_buffer();

        // Insert the overlapping segment
        store.insert(next_seg);

        BOOST_CHECK_EQUAL(store.size(), metas.size());
        BOOST_CHECK(*store.begin() == metas[0]);
        BOOST_CHECK_EQUAL(
          store.last_segment().value(), metas[metas.size() - 1]);

        auto expected_iter = metas.begin();
        auto cstore_iter = store.begin();

        for (; expected_iter != metas.end(); ++expected_iter, ++cstore_iter) {
            BOOST_CHECK_EQUAL(*expected_iter, *cstore_iter);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_overlap_with_replace) {
    auto make_seg = [](auto base, std::optional<int64_t> last) {
        return segment_meta{
          .is_compacted = false,
          .size_bytes = 812,
          .base_offset = model::offset(base),
          .committed_offset = model::offset(last ? *last : base + 9),
          .base_timestamp = model::timestamp(1646430092103),
          .max_timestamp = model::timestamp(1646430092103),
          .delta_offset = model::offset_delta(0),
          .archiver_term = model::term_id(2),
          .segment_term = model::term_id(0),
          .delta_offset_end = model::offset_delta(0),
          .sname_format = segment_name_format::v3,
          .metadata_size_hint = 0,
        };
    };

    std::vector<size_t> baseline_entries{13, 16, 17, 29, 30, 31, 32};
    for (size_t entries : baseline_entries) {
        auto base_offset = 0;
        std::vector<segment_meta> metas;
        segment_meta_cstore store{};

        // Insert different number of baseline entries to get differrent
        // frame configurations.
        for (size_t i = 0; i < entries; ++i) {
            auto seg = make_seg(base_offset, std::nullopt);
            base_offset += 10;

            metas.push_back(seg);
            store.insert(seg);
        }

        auto replaced_seg = metas[metas.size() - 2];

        // Select a segment that fully includes the penultimate segment.
        auto next_seg = make_seg(
          replaced_seg.base_offset() - 5, replaced_seg.committed_offset() + 5);
        metas.insert(metas.end() - 2, next_seg);
        metas.erase(metas.end() - 2);

        // Flush the write buffer such that the next insert does
        // not get re-ordered in the right place.
        store.flush_write_buffer();

        // Insert the overlapping segment
        store.insert(next_seg);

        BOOST_CHECK_EQUAL(store.size(), metas.size());
        BOOST_CHECK(*store.begin() == metas[0]);
        BOOST_CHECK_EQUAL(
          store.last_segment().value(), metas[metas.size() - 1]);

        auto expected_iter = metas.begin();
        auto cstore_iter = store.begin();

        for (; expected_iter != metas.end(); ++expected_iter, ++cstore_iter) {
            BOOST_CHECK_EQUAL(*expected_iter, *cstore_iter);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_append_retrieve_edge_case) {
    // test to trigger a corner case, where the _hints vector is misaligned with
    // the frames basically the first element of a frame is not tracked by a
    // hint. this can happen with prefix_truncate, that will break the invariant
    // that closed frames are max_frame_size big the content of metadata is not
    // important, only the quantity. this should construct 3 frames, first two
    // complete and the third one open (but with enough data to have
    // compression)
    auto metadata = generate_metadata(
      cloud_storage::cstore_max_frame_size * 2 + details::FOR_buffer_depth);
    segment_meta_cstore store{};
    // step 1: generate two frames
    for (auto& m :
         metadata
           | std::views::take(cloud_storage::cstore_max_frame_size * 2)) {
        store.insert(m);
    }
    // step 1.5 make sure that data is compressed
    BOOST_REQUIRE(store.size() == cloud_storage::cstore_max_frame_size * 2);
    // step 2: prefix truncate to misalign hints vector
    store.prefix_truncate(metadata[1].base_offset);
    BOOST_REQUIRE(store.size() == cloud_storage::cstore_max_frame_size * 2 - 1);

    // step 3: elements to the new frame
    for (auto& m :
         metadata
           | std::views::drop(cloud_storage::cstore_max_frame_size * 2)) {
        store.insert(m);
    }
    BOOST_REQUIRE(store.size() == metadata.size() - 1);

    // retrieving this offset via lower_bound will first returns a _hint that
    // belongs to the previous frame. without the fix, this would cause an
    // out_of_range exception the fix is in
    // segment_meta_cstore.cc::column_store::materialize() `_hint_initial <
    // _hint_threashold -> return nullopt`
    auto last_frame_first_offset
      = metadata[cloud_storage::cstore_max_frame_size * 2].base_offset;
    BOOST_CHECK_NO_THROW(store.lower_bound(last_frame_first_offset));
}
