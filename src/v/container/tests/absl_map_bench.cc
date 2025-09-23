/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

// NOTE: This benchmark is not primarily focused on performance measurement,
// but rather on illustrating the variability in behavior of absl map
// containers. absl maps can exhibit significant
// performance variations between runs due to their randomized hashing behavior,
// which is designed to prevent hash collision attacks. This test helps
// demonstrate these variations and compare the consistency of different map
// implementations.
//
// The randomness comes from two sources: `absl::Hash` itself produces different
// value for the same object across different process invocations, and the map
// itself contains a random seed, which has an initial value which varies per
// process and is updated every time a new seed is required (e.g., when a new
// map is created or an existing map is rehashed).
//
// To stop the "per process" randomness, you can disable ASLR, using:
// echo 0 | sudo tee /proc/sys/kernel/randomize_va_space
// as absl uses the address of a global as the initial seed value for the
// Hash and also the map seed.
//
// That will, however, still usually produce random results in benchmarks since
// benchmarks run a variable number of iterations, so create a variable number
// of map objects and so end up with variable seeds. To solve that, you can
// either fix the number of iterations by passing --iterations with a suitably
// large --duration (since the benchmark stops when either limit is reached,
// and we need --iteration limit to be reached first), or you can ensure that
// only a fixed number of map objects are created: this benchmark takes the
// second approach by creating a single map object in the fixture object, which
// is not recreated between benchmark iterations.
//
// The results show that only absl map shows significant run-to-run variability
// in instruction count (and runtime), while the other implementations show
// very low variability.
//
// You can export MAP_BENCH_DEBUG=1 in your env prior to running to see the
// values of the random hashing explicitly.
//
// See absl::container_internal::PerTableSeed for the map seed, and
// kSeed in absl/hash/internal/hash.h for the Hash seed.

#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "container/chunked_hash_map.h"

#include <seastar/testing/perf_tests.hh>

#include <boost/range/irange.hpp>
#include <fmt/format.h>

#include <string_view>
#include <unordered_map>

namespace {
constexpr size_t SIZE = 1000000;

// Initialize debug flag safely
[[gnu::used]] bool get_debug_flag() noexcept {
    auto v = std::getenv("MAP_BENCH_DEBUG");
    return v && std::string_view(v) == "1";
}

template<typename K, typename V>
void debug_map(absl::flat_hash_map<K, V>& map) {
    if (get_debug_flag()) {
        fmt::print(" map.hash_function()(1): {:#X}\n", map.hash_function()(1));
        fmt::print(
          "absl::Hash<uint64_t>{{}}(1): {:#X}\n", absl::Hash<uint64_t>{}(1));
    }
}

template<typename Any>
void debug_map(Any&) {
    // No debug output for non-absl maps
}
} // namespace

template<typename MapT, size_t KeySetSize>
struct MapBenchTest {
    using key_t = typename MapT::key_type;
    using val_t = typename MapT::mapped_type;

    static std::vector<key_t> make_keys() {
        std::vector<key_t> keys;
        keys.reserve(KeySetSize);
        for (auto i : boost::irange<key_t>(0, KeySetSize)) {
            keys.push_back(i);
        }
        return keys;
    }

    static auto make_filled() {
        MapT v;
        auto keys = make_keys();
        for (auto k : keys) {
            v[k] = val_t{0};
        }
        return v;
    }

    MapBenchTest() { debug_map(map); }

    size_t run_iterate_test() {
        // Tests where a particular element is found in the iteration order
        // this is the worst case for variability as the random hashing
        // means that the element could be found after iterating only a
        // few elements, or nearly all of them.
        //
        // Comparing between the runtimes of the map implementations isn't
        // really useful as we are effectively selecting a random number
        // multiplied by the iteration cost: searching for a different element
        // would completely reshuffle the results.
        perf_tests::start_measuring_time();
        for (const auto& p : map) {
            if (p.first == key_t(SIZE / 2 + 1)) {
                break;
            }
            perf_tests::do_not_optimize(p);
        }
        perf_tests::stop_measuring_time();
        return 1;
    }

    MapT map = make_filled();
};

// NOLINTBEGIN(*-macro-*)
#define INT_KEY_MAP_PERF_TEST(container, key, value, key_set_size)             \
    class IntMapBenchTest_##container##_##key##_##value##_##key_set_size       \
      : public MapBenchTest<container<key, value>, key_set_size> {};           \
    PERF_TEST_F(                                                               \
      IntMapBenchTest_##container##_##key##_##value##_##key_set_size,          \
      Iterate) {                                                               \
        return run_iterate_test();                                             \
    }

// NOLINTEND(*-macro-*)

template<typename K, typename V>
using std_hash = std::unordered_map<K, V>;

template<typename K, typename V>
using absl_btree_map = absl::btree_map<K, V>;

template<typename K, typename V>
using absl_flat_map = absl::flat_hash_map<K, V>;

template<typename K, typename V>
using chunked_map = chunked_hash_map<K, V>;

INT_KEY_MAP_PERF_TEST(std_hash, uint64_t, int64_t, SIZE);
INT_KEY_MAP_PERF_TEST(absl_btree_map, uint64_t, int64_t, SIZE);
INT_KEY_MAP_PERF_TEST(absl_flat_map, uint64_t, int64_t, SIZE);
INT_KEY_MAP_PERF_TEST(chunked_map, uint64_t, int64_t, SIZE);
