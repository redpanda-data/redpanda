/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "container/contiguous_range_map.h"
#include "container/tests/bench_utils.h"
#include "random/generators.h"

#include <seastar/testing/perf_tests.hh>

#include <absl/container/btree_map.h>
#include <boost/range/irange.hpp>

template<typename MapT, size_t KeySetSize, size_t FillPercent>
class MapBenchTest {
    using key_t = typename MapT::key_type;
    static auto make_value() {
        return ::make_value<typename MapT::mapped_type>();
    }

    static std::vector<typename MapT::key_type> make_keys() {
        std::vector<key_t> keys;
        keys.reserve(int(KeySetSize * (FillPercent / 100.0)));
        for (auto i : boost::irange<key_t>(0, KeySetSize)) {
            if (random_generators::get_int<size_t>(0, 100) <= FillPercent) {
                keys.push_back(i);
            }
        }

        return keys;
    }
    static auto make_filled() {
        MapT v;
        auto keys = make_keys();
        for (auto k : keys) {
            v[k] = make_value();
        }
        return v;
    }

public:
    size_t run_sequential_fill_test() {
        MapT map;
        auto val = make_value();
        auto keys = make_keys();
        perf_tests::start_measuring_time();
        for (auto& k : keys) {
            map.emplace(k, val);
        }
        perf_tests::stop_measuring_time();
        return KeySetSize * (FillPercent / 100.0);
    }

    size_t run_random_fill_test() {
        MapT map;
        auto val = make_value();
        auto keys = make_keys();
        std::shuffle(
          keys.begin(), keys.end(), random_generators::internal::gen);
        perf_tests::start_measuring_time();
        for (auto& k : keys) {
            map.emplace(k, val);
        }
        perf_tests::stop_measuring_time();
        return KeySetSize * (FillPercent / 100.0);
    }

    size_t run_look_up_test() {
        MapT map = make_filled();
        std::vector<key_t> to_query;
        std::generate_n(std::back_inserter(to_query), 1000, [] {
            return random_generators::get_int<key_t>(KeySetSize);
        });
        perf_tests::start_measuring_time();
        for (const auto& i : to_query) {
            perf_tests::do_not_optimize(map.find(i));
        }
        perf_tests::stop_measuring_time();
        return 1000;
    }

    size_t run_iterate_test() {
        MapT map = make_filled();

        perf_tests::start_measuring_time();
        for (const auto& p : map) {
            perf_tests::do_not_optimize(p);
        }
        perf_tests::stop_measuring_time();
        return KeySetSize * (FillPercent / 100.0);
    }
};

// NOLINTBEGIN(*-macro-*)
#define INT_KEY_MAP_PERF_TEST(                                                        \
  container, key, value, fill_factor, key_set_size)                                   \
    class                                                                             \
      IntMapBenchTest_##container##_##key##_##value##_##fill_factor##_##key_set_size  \
      : public MapBenchTest<                                                          \
          container<key, value>,                                                      \
          key_set_size,                                                               \
          fill_factor> {};                                                            \
    PERF_TEST_F(                                                                      \
      IntMapBenchTest_##container##_##key##_##value##_##fill_factor##_##key_set_size, \
      SequentialFill) {                                                               \
        return run_sequential_fill_test();                                            \
    }                                                                                 \
    PERF_TEST_F(                                                                      \
      IntMapBenchTest_##container##_##key##_##value##_##fill_factor##_##key_set_size, \
      RandomFill) {                                                                   \
        return run_random_fill_test();                                                \
    }                                                                                 \
    PERF_TEST_F(                                                                      \
      IntMapBenchTest_##container##_##key##_##value##_##fill_factor##_##key_set_size, \
      LookUp) {                                                                       \
        return run_look_up_test();                                                    \
    }                                                                                 \
    PERF_TEST_F(                                                                      \
      IntMapBenchTest_##container##_##key##_##value##_##fill_factor##_##key_set_size, \
      Iterate) {                                                                      \
        return run_iterate_test();                                                    \
    }

// NOLINTEND(*-macro-*)

static constexpr int full = 100;
static constexpr int half_full = 50;

template<typename K, typename V>
using std_map = std::map<K, V>;
template<typename K, typename V>
using absl_btree_map = absl::btree_map<K, V>;

INT_KEY_MAP_PERF_TEST(std_map, uint64_t, large_struct, full, 1000);
INT_KEY_MAP_PERF_TEST(std_map, uint64_t, large_struct, full, 100000);
INT_KEY_MAP_PERF_TEST(std_map, uint64_t, large_struct, half_full, 1000);
INT_KEY_MAP_PERF_TEST(std_map, uint64_t, large_struct, half_full, 100000);

INT_KEY_MAP_PERF_TEST(absl_btree_map, uint64_t, large_struct, full, 1000);
INT_KEY_MAP_PERF_TEST(absl_btree_map, uint64_t, large_struct, full, 100000);
INT_KEY_MAP_PERF_TEST(absl_btree_map, uint64_t, large_struct, half_full, 1000);
INT_KEY_MAP_PERF_TEST(
  absl_btree_map, uint64_t, large_struct, half_full, 100000);

INT_KEY_MAP_PERF_TEST(contiguous_range_map, uint64_t, large_struct, full, 1000);
INT_KEY_MAP_PERF_TEST(
  contiguous_range_map, uint64_t, large_struct, full, 100000);
INT_KEY_MAP_PERF_TEST(
  contiguous_range_map, uint64_t, large_struct, half_full, 1000);
INT_KEY_MAP_PERF_TEST(
  contiguous_range_map, uint64_t, large_struct, half_full, 100000);
