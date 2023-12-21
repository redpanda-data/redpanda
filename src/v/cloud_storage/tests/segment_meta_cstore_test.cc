/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/segment_meta_cstore.h"
#include "cloud_storage/types.h"
#include "common_def.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "utils/delta_for.h"
#include "utils/human.h"
#include "vlog.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <iterator>
#include <limits>
#include <ranges>
#include <stdexcept>
#include <utility>

using namespace cloud_storage;
static ss::logger test("test-logger-s");

using delta_xor_alg = details::delta_xor;
using delta_xor_frame = segment_meta_column_frame<int64_t, delta_xor_alg{}>;
using delta_delta_alg = details::delta_delta<int64_t>;
using delta_delta_frame = segment_meta_column_frame<int64_t, delta_delta_alg{}>;
using delta_xor_column = segment_meta_column<int64_t, delta_xor_alg>;
using delta_delta_column = segment_meta_column<int64_t, delta_delta_alg>;

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

template<class column_t>
void append_test_case(const int64_t num_elements, column_t& column) {
    size_t total_size = 0;
    int64_t ix = 0;
    for (int64_t i = 0; i < num_elements; i++) {
        ix += random_generators::get_int(1, 100);
        column.append(ix);
        total_size++;
        BOOST_REQUIRE_EQUAL(ix, column.last_value());
    }
    BOOST_REQUIRE_EQUAL(total_size, column.size());
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_append_xor) {
    delta_xor_frame frame{};
    append_test_case(short_test_size, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_append_delta) {
    delta_delta_frame frame{};
    append_test_case(short_test_size, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_append_xor) {
    delta_xor_column col{};
    append_test_case(short_test_size, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_append_delta) {
    delta_delta_column col{};
    append_test_case(short_test_size, col);
}

template<class column_t>
void append_tx_test_case(const int64_t num_elements, column_t& column) {
    size_t total_size = 0;
    int64_t ix = 0;
    for (int64_t i = 0; i < num_elements; i++) {
        ix += random_generators::get_int(1, 100);
        auto tx = column.append_tx(ix);
        if (tx) {
            std::move(*tx).commit();
        }
        total_size++;
        BOOST_REQUIRE_EQUAL(ix, column.last_value());
    }
    BOOST_REQUIRE_EQUAL(total_size, column.size());
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_append_tx_xor) {
    delta_xor_frame frame{};
    append_tx_test_case(short_test_size, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_append_tx_delta) {
    delta_delta_frame frame{};
    append_tx_test_case(short_test_size, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_append_tx_xor) {
    delta_xor_column col{};
    append_tx_test_case(short_test_size, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_append_tx_delta) {
    delta_delta_column col{};
    append_tx_test_case(short_test_size, col);
}

template<class column_t>
void iter_test_case(const int64_t num_elements, column_t& column) {
    size_t total_size = 0;
    std::vector<int64_t> expected;
    int64_t ix = 0;
    for (int64_t i = 0; i < num_elements; i++) {
        ix += random_generators::get_int(1, 100);
        column.append(ix);
        expected.push_back(ix);
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, column.size());

    int i = 0;
    for (auto it = column.begin(); it != column.end(); ++it) {
        BOOST_REQUIRE_EQUAL(it.index(), i);
        BOOST_REQUIRE_EQUAL(*it, expected[i++]);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_iter_xor) {
    delta_xor_frame frame{};
    iter_test_case(short_test_size, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_iter_delta) {
    delta_delta_frame frame{};
    iter_test_case(short_test_size, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_iter_xor) {
    delta_xor_column col{};
    iter_test_case(short_test_size, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_iter_delta) {
    delta_delta_column col{};
    iter_test_case(short_test_size, col);
}

template<class column_t>
void find_test_case(const int64_t num_elements, column_t& column) {
    size_t total_size = 0;
    std::vector<int64_t> samples;
    int64_t ix = 0;
    for (auto i = 0; i < num_elements; i++) {
        ix += random_generators::get_int(1, 100);
        column.append(ix);
        if (samples.empty() || random_generators::get_int(10) == 0) {
            samples.push_back(ix);
        }
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, column.size());
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(samples.begin(), samples.end(), g);

    for (auto expected : samples) {
        auto it = column.find(expected);
        BOOST_REQUIRE(it != column.end());
        auto actual = *it;
        BOOST_REQUIRE_EQUAL(actual, expected);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_find_xor) {
    delta_xor_frame frame{};
    find_test_case(short_test_size, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_find_xor_small) {
    delta_xor_frame frame{};
    find_test_case(random_generators::get_int(1, 16), frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_find_delta) {
    delta_delta_frame frame{};
    find_test_case(short_test_size, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_find_delta_small) {
    delta_delta_frame frame{};
    find_test_case(random_generators::get_int(1, 16), frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_find_xor) {
    delta_xor_column col{};
    find_test_case(short_test_size, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_find_xor_small) {
    delta_xor_column col{};
    find_test_case(random_generators::get_int(1, 16), col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_find_delta) {
    delta_delta_column col{};
    find_test_case(short_test_size, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_find_delta_small) {
    delta_delta_column col{};
    find_test_case(random_generators::get_int(1, 16), col);
}

template<class column_t>
void lower_bound_test_case(const int64_t num_elements, column_t& column) {
    size_t total_size = 0;
    std::vector<int64_t> samples;
    int64_t last = 0;
    int64_t ix = 10000;
    for (auto i = 0; i < num_elements; i++) {
        ix += random_generators::get_int(1, 100);
        column.append(ix);
        last = ix;
        if (samples.empty() || random_generators::get_int(10) == 0) {
            samples.push_back(ix);
        }
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, column.size());
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(samples.begin(), samples.end(), g);

    {
        auto it = column.lower_bound(last);
        BOOST_REQUIRE_EQUAL(last, *it);
        it = column.lower_bound(last + 1);
        BOOST_REQUIRE(it == column.end());
    }

    for (auto expected : samples) {
        auto it = column.lower_bound(expected);
        BOOST_REQUIRE(it != column.end());
        auto actual = *it;
        BOOST_REQUIRE_EQUAL(actual, expected);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_lower_bound_xor) {
    delta_xor_frame frame{};
    lower_bound_test_case(short_test_size, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_lower_bound_xor_small) {
    delta_xor_frame frame{};
    lower_bound_test_case(random_generators::get_int(1, 16), frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_lower_bound_delta) {
    delta_delta_frame frame{};
    lower_bound_test_case(short_test_size, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_lower_bound_delta_small) {
    delta_delta_frame frame{};
    lower_bound_test_case(random_generators::get_int(1, 16), frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_lower_bound_xor) {
    delta_xor_column col{};
    lower_bound_test_case(short_test_size, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_lower_bound_xor_small) {
    delta_xor_column col{};
    lower_bound_test_case(random_generators::get_int(1, 16), col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_lower_bound_delta) {
    delta_delta_column col{};
    lower_bound_test_case(short_test_size, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_lower_bound_delta_small) {
    delta_delta_column col{};
    lower_bound_test_case(random_generators::get_int(1, 16), col);
}

template<class column_t>
void upper_bound_test_case(const int64_t num_elements, column_t& column) {
    size_t total_size = 0;
    std::vector<int64_t> samples;
    int64_t last = 0;
    int64_t ix = 10000;
    for (auto i = 0; i < num_elements; i++) {
        ix += random_generators::get_int(1, 100);
        column.append(ix);
        if (samples.empty() || random_generators::get_int(10) == 0) {
            samples.push_back(ix);
        }
        last = ix;
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, column.size());
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(samples.begin(), samples.end(), g);

    {
        auto it = column.upper_bound(last);
        BOOST_REQUIRE(it == column.end());
    }

    for (auto expected : samples) {
        auto it = column.upper_bound(expected - 1);
        BOOST_REQUIRE(it != column.end());
        auto actual = *it;
        BOOST_REQUIRE_EQUAL(actual, expected);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_upper_bound_xor) {
    delta_xor_frame frame{};
    upper_bound_test_case(short_test_size, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_upper_bound_xor_small) {
    delta_xor_frame frame{};
    upper_bound_test_case(random_generators::get_int(1, 16), frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_upper_bound_delta) {
    delta_delta_frame frame{};
    upper_bound_test_case(short_test_size, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_upper_bound_delta_small) {
    delta_delta_frame frame{};
    upper_bound_test_case(random_generators::get_int(1, 16), frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_upper_bound_xor) {
    delta_xor_column col{};
    upper_bound_test_case(short_test_size, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_upper_bound_xor_small) {
    delta_xor_column col{};
    upper_bound_test_case(random_generators::get_int(1, 16), col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_upper_bound_delta) {
    delta_delta_column col{};
    upper_bound_test_case(short_test_size, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_upper_bound_delta_small) {
    delta_delta_column col{};
    upper_bound_test_case(random_generators::get_int(1, 16), col);
}

template<class column_t>
void at_test_case(const int64_t num_elements, column_t& column) {
    size_t total_size = 0;
    std::vector<std::pair<int64_t, size_t>> samples;
    int64_t value = 0;
    for (int64_t i = 0; i < num_elements; i++) {
        value += random_generators::get_int(1, 100);
        column.append(value);
        if (samples.empty() || random_generators::get_int(10) == 0) {
            samples.emplace_back(value, total_size);
        }
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, column.size());
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(samples.begin(), samples.end(), g);

    for (auto [expected, index] : samples) {
        auto it = column.at_index(index);
        BOOST_REQUIRE(it != column.end());
        auto actual = *it;
        BOOST_REQUIRE_EQUAL(actual, expected);
        BOOST_REQUIRE_EQUAL(it.index(), index);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_at_xor) {
    delta_xor_frame frame{};
    at_test_case(short_test_size, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_at_xor_small) {
    delta_xor_frame frame{};
    at_test_case(random_generators::get_int(16), frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_at_delta) {
    delta_delta_frame frame{};
    at_test_case(short_test_size, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_at_delta_small) {
    delta_delta_frame frame{};
    at_test_case(random_generators::get_int(16), frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_at_xor) {
    delta_xor_column col{};
    at_test_case(short_test_size, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_at_xor_small) {
    delta_xor_column col{};
    at_test_case(random_generators::get_int(16), col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_at_delta) {
    delta_delta_column col{};
    at_test_case(short_test_size, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_at_delta_small) {
    delta_delta_column col{};
    at_test_case(random_generators::get_int(16), col);
}

template<class column_t>
void prefix_truncate_test_case(const int64_t num_elements, column_t& column) {
    size_t total_size = 0;
    struct sample_t {
        int64_t sample;
        int64_t index;
    };
    std::vector<sample_t> samples;
    int64_t value = 0;
    for (int64_t i = 0; i < num_elements; i++) {
        value += random_generators::get_int(1, 100);
        column.append(value);
        if (samples.empty() || random_generators::get_int(10) == 0) {
            vlog(test.info, "Add sample {} at {}", value, i);
            samples.push_back(sample_t{
              .sample = value,
              .index = i,
            });
        }
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, column.size());

    int64_t num_truncated = 0;
    for (auto value : samples) {
        auto delta = value.index - num_truncated;
        vlog(
          test.info,
          "Truncating at {}, sample {}, {}",
          delta,
          value.sample,
          value.index);
        column.prefix_truncate_ix(delta);
        num_truncated += delta;
        auto it = column.begin();
        BOOST_REQUIRE(it != column.end());
        auto actual = *it;
        vlog(test.info, "Found value {}", actual);
        BOOST_REQUIRE_EQUAL(actual, value.sample);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_prefix_truncate_xor) {
    delta_xor_frame frame{};
    prefix_truncate_test_case(10, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_prefix_truncate_delta) {
    delta_delta_frame frame{};
    prefix_truncate_test_case(10, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_prefix_truncate_xor) {
    delta_xor_column col{};
    prefix_truncate_test_case(10, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_prefix_truncate_delta) {
    delta_delta_column col{};
    prefix_truncate_test_case(10, col);
}

template<class column_t>
void at_with_hint_test_case(const int64_t num_elements, column_t& column) {
    struct hint_t {
        std::optional<typename column_t::hint_t> pos;
        uint32_t index;
    };
    size_t total_size = 0;
    std::vector<hint_t> hints;
    std::vector<std::pair<int64_t, size_t>> samples;
    int64_t value = 0;
    for (int64_t i = 0; i < num_elements; i++) {
        value += random_generators::get_int(1, 100);
        column.append(value);
        if (random_generators::get_int(10) == 0) {
            samples.emplace_back(value, total_size);
        }
        if (i % 16 == 0) {
            auto hint = column.get_current_stream_pos();
            hints.push_back(hint_t{
              .pos = hint,
              .index = static_cast<uint32_t>(i),
            });
        }
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, column.size());
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(samples.begin(), samples.end(), g);

    size_t num_skips = 0;
    for (auto [expected, index] : samples) {
        auto h_it = std::lower_bound(
          hints.rbegin(),
          hints.rend(),
          hint_t{.index = static_cast<uint32_t>(index)},
          [](const hint_t& lhs, const hint_t& rhs) {
              return lhs.index > rhs.index;
          });
        if (h_it == hints.rend()) {
            // We won't be able to find the hint for the first row
            num_skips++;
            continue;
        }
        auto it = h_it->pos.has_value()
                    ? column.at_index(index, h_it->pos.value())
                    : column.at_index(index);
        BOOST_REQUIRE(it != column.end());
        auto actual = *it;
        BOOST_REQUIRE_EQUAL(actual, expected);
        BOOST_REQUIRE_EQUAL(it.index(), index);
    }
    BOOST_REQUIRE_LE(num_skips, 16);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_at_with_hint_xor) {
    delta_xor_frame frame{};
    at_with_hint_test_case(short_test_size, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_at_with_hint_delta) {
    delta_delta_frame frame{};
    at_with_hint_test_case(short_test_size, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_at_with_hint_xor) {
    delta_xor_column column{};
    at_with_hint_test_case(short_test_size, column);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_at_with_hint_delta) {
    delta_delta_column column{};
    at_with_hint_test_case(short_test_size, column);
}

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
    for (auto const& sm : manifest) {
        store.insert(sm);
    }

    store.prefix_truncate(model::offset::max());
    BOOST_REQUIRE(store.empty());

    for (auto const& sm : manifest) {
        store.insert(sm);
    }
    store.prefix_truncate(manifest.back().committed_offset + model::offset{1});
    BOOST_REQUIRE(store.empty());
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_serde_roundtrip) {
    segment_meta_cstore store{};
    auto manifest = generate_metadata(10007);
    for (auto const& sm : manifest) {
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
      [&](bool generating_replacement, auto const& in) {
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

class segment_meta_random_walk {
    static constexpr int64_t ts_start = 1700000000000;

public:
    segment_meta_random_walk(
      bool add_reuploads,
      bool add_reorderings,
      bool add_gaps,
      bool add_overlaps,
      bool add_empty)
      : _add_reuploads(add_reuploads)
      , _add_reorderings(add_reorderings)
      , _add_gaps(add_gaps)
      , _add_overlaps(add_overlaps)
      , _add_empty(add_empty) {}

    /// Returns truncate-offset, expected-start-offset
    /// Offsets might be different in case of out of order
    /// truncations.
    std::pair<model::offset, model::offset> maybe_truncate() {
        if (
          !_ooo_write.empty() ||
          // The probability of the truncation has to be relatively low so the
          // size of the cstore would grow to multiple frames
          model::test::with_probability(0.999) || _segments.empty()) {
            return std::make_pair(model::offset{}, model::offset{});
        }
        auto s = _segments.size();
        auto max_index = static_cast<int>(s / 2);
        auto ix = model::test::get_int(0, max_index);
        if (ix == 0) {
            return std::make_pair(model::offset{}, model::offset{});
        }
        auto it = _segments.begin();
        // Generate reordered truncate
        if (
          _add_reorderings && it->first() > 0
          && model::test::with_probability(0.2)) {
            return std::make_pair(model::offset{}, it->first);
        }
        std::advance(it, ix);
        auto new_so = it->first;
        vlog(
          test.info,
          "Truncating first {} segments out of {}, new start offset = {}",
          ix,
          s,
          new_so);
        _segments.erase(_segments.begin(), it);
        if (_segments.empty()) {
            vlog(test.info, "All segments removed");
        } else {
            vlog(test.info, "New start offset = {}", _segments.begin()->first);
        }
        _num_truncations++;
        return std::make_pair(new_so, new_so);
    }

    enum class op_type {
        append,
        prepend,
        replace,
    };

    /// Produce next random segment in the run
    ///
    /// \return generated segment meta and the flag that indicates that the
    ///         segment replaces existing segments or is added to the end.
    std::pair<segment_meta, op_type> next_segment() {
        if (!_ooo_write.empty()) {
            auto m = _ooo_write.back();
            _ooo_write.pop_back();
            vlog(test.info, "Delayed write {}", m);
            _segments.insert(std::make_pair(m.base_offset, m));
            return std::make_pair(m, op_type::replace);
        }
        constexpr int max_rec_per_segment = 200;
        constexpr int max_segment_size = 100000;
        constexpr int max_timestamp_delta = 100000;
        constexpr float small_probability = 0.05;
        constexpr float large_probability = 0.25;
        auto n_rec = model::test::get_int(
          _add_empty ? 0 : 1, max_rec_per_segment);
        auto n_conf = model::test::get_int(0, n_rec);
        auto s_size = model::test::get_int(0, max_segment_size);
        auto ts_delta = model::test::get_int(0, max_timestamp_delta);

        // Generate append
        if (_segments.empty()) {
            // Add first segment
            auto ts_init = model::test::get_int(0, max_timestamp_delta);
            auto append = make_segment(
              0,
              n_rec,
              0,
              n_conf,
              ts_start + ts_init,
              ts_start + ts_init + ts_delta,
              0,
              0,
              s_size);
            _segments.insert(std::make_pair(append.base_offset, append));
            return std::make_pair(append, op_type::append);
        }
        if (
          _add_reuploads && model::test::with_probability(large_probability)) {
            if (
              _num_truncations > 0 && _segments.size() > 0
              && model::test::with_probability(0.1)) {
                // Generate reordering between the truncation and segment
                // append.
                auto prepend = _segments.begin()->second;
                // The segment content doesn't matter in this case.
                // Collapse first segment into one batch and move it one offset
                // back.
                prepend.base_offset = model::prev_offset(prepend.base_offset);
                prepend.committed_offset = model::prev_offset(
                  prepend.base_offset);
                prepend.delta_offset = prepend.delta_offset_end;
                vlog(test.info, "Producing prepend {}", prepend);
                _segments.insert(std::make_pair(prepend.base_offset, prepend));
                return std::make_pair(prepend, op_type::prepend);
            } else {
                // Generate replacement. Pick random segment to replace.
                // Align the end of the reupload to some other segment.
                auto ix = model::test::get_int(0UL, _segments.size() - 1);
                auto it = _segments.begin();
                std::advance(it, ix);
                n_rec = static_cast<int>(
                  it->second.committed_offset() - it->first() + 1);
                auto max_to_replace = _segments.size() - ix;
                auto n_segments_to_replace = model::test::get_int(
                  1, static_cast<int>(max_to_replace));

                auto first = it;
                auto last = it;
                auto replacement = it->second;
                ix++;
                it++;
                for (int i = ix; i < n_segments_to_replace; i++, it++) {
                    replacement.committed_offset = it->second.committed_offset;
                    replacement.max_timestamp = it->second.max_timestamp;
                    replacement.size_bytes += it->second.size_bytes;
                    replacement.delta_offset_end = it->second.delta_offset_end;
                    replacement.archiver_term = it->second.archiver_term;
                    last = it;
                }
                _segments.erase(first, std::next(last));
                _segments.insert(
                  std::make_pair(replacement.base_offset, replacement));
                vlog(test.info, "Producing replacement {}", replacement);
                _segments.insert(
                  std::make_pair(replacement.base_offset, replacement));
                return std::make_pair(replacement, op_type::replace);
            }
        }
        auto p_last = _segments.end();
        auto last_co = std::numeric_limits<int64_t>::min();
        for (auto it = _segments.begin(); it != _segments.end(); it++) {
            if (it->second.committed_offset() > last_co) {
                p_last = it;
                last_co = it->second.committed_offset();
            }
        }
        BOOST_REQUIRE(p_last != _segments.end());

        int gap = 0;
        if (_add_gaps && model::test::with_probability(small_probability)) {
            gap = model::test::get_int(max_rec_per_segment);
        } else if (
          _add_overlaps && model::test::with_probability(small_probability)) {
            gap = model::test::get_int(max_rec_per_segment);
        }

        generated_params props{
          .n_rec = n_rec,
          .n_conf = n_conf,
          .ts_delta = ts_delta,
          .segment_term_delta = model::test::get_int(1),
          .archiver_term_delta = model::test::get_int(1),
          .s_size = s_size,
          .gap_size = gap,
        };
        auto append = make_segment(p_last->second, props);

        if (
          _add_reorderings
          && model::test::with_probability(small_probability)) {
            constexpr int max_reordered = 32;
            auto to_reorder = model::test::get_int(max_reordered);
            for (int i = 0; i < to_reorder; i++) {
                vlog(test.info, "Going to delay append {}", append);
                _ooo_write.push_back(append);
                append = make_segment(_ooo_write.back(), props);
                vlog(test.info, "Producing next append {}", append);
            }
            std::random_device rd;
            std::mt19937 g(rd());
            std::shuffle(_ooo_write.begin(), _ooo_write.end(), g);
        }

        vlog(test.info, "Producing append {}", append);
        _segments.insert(std::make_pair(append.base_offset, append));
        return std::make_pair(append, op_type::append);
    }

    /// Check that the meta is correct (should be in the manifest)
    bool is_valid(const segment_meta& m) {
        auto o = m.base_offset;
        auto it = _segments.find(o);
        if (it == _segments.end()) {
            vlog(test.debug, "Segment {} wasn't generated", m);
        }
        if (it == _segments.end()) {
            return false;
        }
        vlog(test.debug, "Expected value: {}", m);
        vlog(test.debug, "Actual value: {}", it->second);
        return it->second == m;
    }

    const std::map<model::offset, segment_meta>& get_segments() const {
        return _segments;
    }

    /// Get actual last segment (take possible segment overlap into account)
    std::optional<segment_meta> get_last_segment() const {
        if (_segments.empty()) {
            return std::nullopt;
        }
        auto p_last = _segments.end();
        auto last_co = std::numeric_limits<int64_t>::min();
        for (auto it = _segments.begin(); it != _segments.end(); it++) {
            if (it->second.committed_offset() > last_co) {
                p_last = it;
                last_co = it->second.committed_offset();
            }
        }
        if (p_last == _segments.end()) {
            return std::nullopt;
        }
        return p_last->second;
    }

    std::optional<segment_meta> get_first_segment() const {
        if (_segments.empty()) {
            return std::nullopt;
        }
        return _segments.begin()->second;
    }

    std::optional<model::offset> get_start_offset() const {
        auto first = get_first_segment();
        if (!first.has_value()) {
            return std::nullopt;
        }
        return first->base_offset;
    }

    std::optional<model::offset> get_last_offset() const {
        auto last = get_last_segment();
        if (!last.has_value()) {
            return std::nullopt;
        }
        return last->committed_offset;
    }

private:
    struct generated_params {
        int n_rec{0};
        int n_conf{0};
        int ts_delta{0};
        int segment_term_delta{0};
        int archiver_term_delta{0};
        int s_size{0};
        // optional, positive value means gap, negative encodes an overlap
        int gap_size{0};
    };

    segment_meta make_segment(const segment_meta& prev, generated_params p) {
        auto base_offset = prev.committed_offset() + 1;
        auto start_delta = prev.delta_offset_end();
        auto end_delta = start_delta + p.n_conf;
        auto ts_base = prev.max_timestamp.value();
        auto ts_last = ts_base + p.ts_delta;
        auto t_segm = prev.segment_term() + p.segment_term_delta;
        auto t_arch = std::max(
          prev.archiver_term() + p.archiver_term_delta, t_segm);

        if (p.gap_size > 0) {
            base_offset += p.gap_size;
        } else if (p.gap_size < 0) {
            base_offset -= std::min(
              static_cast<int64_t>(-1 * p.gap_size), base_offset);
        }

        return make_segment(
          base_offset,
          p.n_rec,
          start_delta,
          end_delta,
          ts_base,
          ts_last,
          t_arch,
          t_segm,
          p.s_size);
    }

    segment_meta make_segment(
      int64_t base,
      int64_t num_records,
      int64_t start_delta,
      int64_t end_delta,
      int64_t base_ts,
      int64_t last_ts,
      int64_t archiver_term,
      int64_t segment_term,
      uint64_t size) {
        BOOST_REQUIRE(start_delta <= end_delta);
        BOOST_REQUIRE(num_records >= 0);
        BOOST_REQUIRE(size >= 0);
        BOOST_REQUIRE(archiver_term >= segment_term);
        segment_meta m{
          .is_compacted = false,
          .size_bytes = size,
          .base_offset = model::offset(base),
          .committed_offset = model::offset(base + num_records - 1),
          .base_timestamp = model::timestamp(base_ts),
          .max_timestamp = model::timestamp(last_ts),
          .delta_offset = model::offset_delta(start_delta),
          .archiver_term = model::term_id(archiver_term),
          .segment_term = model::term_id(segment_term),
          .delta_offset_end = model::offset_delta(end_delta),
          .sname_format = segment_name_format::v3,
          .metadata_size_hint = 0,
        };
        return m;
    }

    bool _add_reuploads;
    bool _add_reorderings;
    bool _add_gaps;
    bool _add_overlaps;
    bool _add_empty;
    std::map<model::offset, segment_meta> _segments;
    /// Used to generate out of order updates
    std::vector<segment_meta> _ooo_write;
    int _num_truncations{0};
};

void test_segment_meta_cstore_invariants_randomized(
  const int n_iter,
  bool allow_reuploads,
  bool allow_reorderings,
  bool allow_gaps,
  bool allow_overlaps,
  bool allow_empty) {
    segment_meta_random_walk walk(
      allow_reuploads,
      allow_reorderings,
      allow_gaps,
      allow_overlaps,
      allow_empty);

    segment_meta_cstore cstore;
    vlog(test.info, "Running randomized invariants test");
    vlog(
      test.info,
      "n_iter = {}, allow_reuploads = {}, allow_gaps = {}, allow_overlaps = "
      "{}, allow_empty = {}",
      n_iter,
      allow_reuploads,
      allow_gaps,
      allow_overlaps,
      allow_empty);

    auto cond =
      [&](bool condition, const segment_meta& target, bool is_replacement) {
          if (condition) {
              return true;
          }
          if (is_replacement) {
              vlog(test.info, "Check failed for replacement {}", target);
          } else {
              vlog(test.info, "Check failed for append {}", target);
              vlog(
                test.info,
                "Expected last_segment: {}",
                std::prev(walk.get_segments().end())->second);
              vlog(test.info, "Actual last_segment: {}", cstore.last_segment());
          }
          vlog(test.info, "Expected segments:");
          for (const auto& [base, m] : walk.get_segments()) {
              vlog(
                test.info,
                "......{}({})-{}",
                base,
                m.base_offset,
                m.committed_offset);
          }
          vlog(test.info, "\n\n\n\n\n\n\n\n\n");
          vlog(test.info, "Expected segments:");
          for (const auto& m : cstore) {
              vlog(test.info, "......{}-{}", m.base_offset, m.committed_offset);
          }
          return false;
      };

    auto cond1 = [&](bool condition) {
        if (condition) {
            return true;
        }
        vlog(test.info, "Check failed");
        vlog(test.info, "Expected segments:");
        for (const auto& [base, m] : walk.get_segments()) {
            vlog(
              test.info,
              "......{}({})-{}",
              base,
              m.base_offset,
              m.committed_offset);
        }
        vlog(test.info, "\n\n\n\n");
        vlog(test.info, "Expected segments:");
        for (const auto& m : cstore) {
            vlog(test.info, "......{}-{}", m.base_offset, m.committed_offset);
        }
        return false;
    };

    // Last element invariants
    auto check_last_element_invariants = [&](const segment_meta& last_element) {
        auto cs_last = cstore.last_segment();
        vlog(
          test.info,
          "Checking last element {}, cstore last element {}",
          last_element,
          cs_last);
        auto o = last_element.base_offset;
        BOOST_REQUIRE(cond(cs_last == last_element, last_element, false));
        // check that it can be found by its base offset
        {
            auto it = cstore.find(o);
            BOOST_REQUIRE(cond(it != cstore.end(), last_element, false));
            BOOST_REQUIRE(
              cond(*it == cstore.last_segment(), last_element, false));
        }
        // check that prev(end) matches the last_segment
        {
            auto e = cstore.end();
            auto it = cstore.prev(e);
            BOOST_REQUIRE(cond(*it == last_element, last_element, false));
        }
    };

    // Whole log invariants
    auto check_global_invariants = [&](model::offset first) {
        if (cstore.empty()) {
            return;
        }
        auto b_it = cstore.find(first);
        auto e_it = cstore.end();
        BOOST_REQUIRE(cond1(b_it == cstore.begin()));
        BOOST_REQUIRE(cond1(*cstore.prev(e_it) == cstore.last_segment()));
    };

    // Single batch invariants
    auto check_local_invariants = [&](
                                    const segment_meta& meta,
                                    bool is_replacement) {
        vlog(test.info, "Checking {}", meta);
        auto o = meta.base_offset;
        // check that it can be found by its base offset
        {
            auto it = cstore.find(o);
            BOOST_REQUIRE(cond(it != cstore.end(), meta, is_replacement));
            BOOST_REQUIRE(cond(*it == meta, meta, is_replacement));
        }

        // check that it can be found using upper_bound
        {
            auto it = cstore.upper_bound(o);
            if (it != cstore.begin()) {
                auto it2 = cstore.prev(it);
                auto m2 = *it2;
                BOOST_REQUIRE(cond(walk.is_valid(m2), m2, is_replacement));
                BOOST_REQUIRE(cond(m2.base_offset == o, meta, is_replacement));
            } else {
                BOOST_REQUIRE(cond(cstore.size() == 1, meta, is_replacement));
            }
        }
    };
    for (int i = 0; i < n_iter; i++) {
        auto [t_off, exp_so] = walk.maybe_truncate();
        if (t_off != model::offset{}) {
            vlog(
              test.info, "Cstore truncate {}, expected SO: {}", t_off, exp_so);
            cstore.prefix_truncate(t_off);
            check_global_invariants(exp_so);
        }
        auto [meta, op_type] = walk.next_segment();

        try {
            vlog(test.info, "Cstore insert {}", meta);
            auto tx = cstore.append(meta);
            std::ignore = std::move(tx);
        } catch (const std::invalid_argument& e) {
            vlog(
              test.info,
              "Cstore insert {} failed with {}, retrying with flush",
              meta,
              e.what());
            cstore.flush_write_buffer();
            auto tx = cstore.append(meta);
            std::ignore = std::move(tx);
        }

        switch (op_type) {
        case segment_meta_random_walk::op_type::append:
            if (model::test::with_probability(0.1)) {
                check_last_element_invariants(meta);
            }
            break;
        case segment_meta_random_walk::op_type::prepend:
            check_global_invariants(walk.get_start_offset().value());
            break;
        case segment_meta_random_walk::op_type::replace:
            if (model::test::with_probability(0.1)) {
                check_local_invariants(meta, true);
                for (const auto& [key, m] : walk.get_segments()) {
                    std::ignore = key;
                    check_local_invariants(m, true);
                }
            }
            break;
        };
    }
    vlog(test.info, "Completed run of {} iterations", n_iter);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_fuzz_invariants) {
#ifndef _NDEBUG
    const int n_iter = 100000;
    test_segment_meta_cstore_invariants_randomized(
      n_iter, true, true, true, false, false);
#endif
}
