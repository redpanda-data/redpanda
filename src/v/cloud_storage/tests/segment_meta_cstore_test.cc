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
static constexpr size_t long_test_size = 3000;
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
    auto ix = random_generators::get_int(1, (int)max_truncate_ix);
    auto iter = manifest.begin();
    std::advance(iter, ix);
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

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_serde_roundtrip) {
    segment_meta_cstore store{};
    auto manifest = generate_metadata(10007);
    for (auto const& sm : manifest) {
        store.insert(sm);
    }
    {
        auto [inflated_sz, actual_sz] = store.inflated_actual_size();
        auto iobuf = store.to_iobuf();
        auto serialized_sz = iobuf.size_bytes();
        BOOST_REQUIRE(store.empty());
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