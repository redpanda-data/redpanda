/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "fuzztest/fuzztest.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <numeric>
#include <unordered_map>
#include <vector>

// ============================================================
// Internal structure validator for chunked_vector.
//
// Redeclared here (matching chunked_vector_test.cc) so we can use it in
// assertions. chunked_vector grants friendship by name, so any class named
// chunked_vector_validator in the global namespace can access private members.
// Must be at global scope to match the friend declaration.
// ============================================================
class chunked_vector_validator {
public:
    template<typename T>
    static testing::AssertionResult validate(const chunked_vector<T>& v) {
        if (v._size > v._capacity) {
            return testing::AssertionFailure() << "size > capacity";
        }
        size_t calc_size = 0, calc_cap = 0;
        for (size_t i = 0; i < v._frags.size(); ++i) {
            const auto& f = v._frags[i];
            calc_size += f.size();
            calc_cap += f.capacity();
            // All fragments except the last must be full.
            if (
              i + 1 < v._frags.size()
              && f.size() < chunked_vector<T>::elements_per_fragment()) {
                return testing::AssertionFailure()
                       << "fragment " << i << " is not full";
            }
        }
        if (calc_size != v._size) {
            return testing::AssertionFailure()
                   << "size mismatch: " << calc_size << " != " << v._size;
        }
        if (calc_cap != v._capacity) {
            return testing::AssertionFailure()
                   << "capacity mismatch: " << calc_cap
                   << " != " << v._capacity;
        }
        return testing::AssertionSuccess();
    }
};

namespace {

// ============================================================
// chunked_vector PBT
// ============================================================

// Encoded operation on a chunked_vector<int32_t>. All arithmetic is done
// inside the test body to keep the input format simple and let FuzzTest
// explore the full space of each field independently.
struct VecOp {
    uint8_t kind;  // taken mod kNumVecOps to select operation
    int32_t value; // Push: value to push
    uint8_t arg;   // PopN / EraseToEnd: applied as (arg % (size+1))
                   // Reserve: used directly
};

static constexpr uint8_t kNumVecOps = 7;
// 0 push_back, 1 pop_back, 2 pop_back_n, 3 erase_to_end,
// 4 reserve, 5 sort, 6 clear

auto VecOpDomain() {
    return fuzztest::StructOf<VecOp>(
      fuzztest::Arbitrary<uint8_t>(),
      fuzztest::Arbitrary<int32_t>(),
      fuzztest::Arbitrary<uint8_t>());
}

// Property: chunked_vector<int32_t> is element-for-element identical to
// std::vector<int32_t> under any sequence of push_back, pop_back,
// pop_back_n, erase_to_end, reserve, sort, and clear. Additionally, the
// internal invariants (size == sum of fragment sizes, capacity == sum of
// fragment capacities, all fragments except the last are completely full)
// must hold after every mutation.
void VectorModelOracle(std::vector<VecOp> ops) {
    chunked_vector<int32_t> impl;
    std::vector<int32_t> oracle;

    for (const auto& op : ops) {
        switch (op.kind % kNumVecOps) {
        case 0: // push_back
            impl.push_back(op.value);
            oracle.push_back(op.value);
            break;
        case 1: // pop_back
            if (!oracle.empty()) {
                impl.pop_back();
                oracle.pop_back();
            }
            break;
        case 2: { // pop_back_n(n), n in [0, size]
            size_t n = oracle.empty() ? 0
                                      : (size_t)op.arg % (oracle.size() + 1);
            impl.pop_back_n(n);
            oracle.erase(oracle.end() - (ptrdiff_t)n, oracle.end());
            break;
        }
        case 3: { // erase_to_end(begin + pos), pos in [0, size]
            size_t pos = oracle.empty() ? 0
                                        : (size_t)op.arg % (oracle.size() + 1);
            impl.erase_to_end(impl.begin() + (ptrdiff_t)pos);
            oracle.erase(oracle.begin() + (ptrdiff_t)pos, oracle.end());
            break;
        }
        case 4: // reserve(arg) — no observable element change
            impl.reserve(op.arg);
            oracle.reserve(op.arg);
            break;
        case 5: // sort
            std::sort(impl.begin(), impl.end());
            std::sort(oracle.begin(), oracle.end());
            break;
        case 6: // clear
            impl.clear();
            oracle.clear();
            break;
        }

        ASSERT_EQ(impl.size(), oracle.size());
        ASSERT_EQ(impl.empty(), oracle.empty());
        ASSERT_TRUE(
          std::equal(impl.begin(), impl.end(), oracle.begin(), oracle.end()))
          << "element mismatch after op=" << (op.kind % kNumVecOps);
        ASSERT_TRUE(chunked_vector_validator::validate(impl));
    }
}
FUZZ_TEST(ChunkedVectorPBT, VectorModelOracle)
  .WithDomains(fuzztest::VectorOf(VecOpDomain()).WithMaxSize(500));

// Property: after sorting a chunked_vector, std::lower_bound and
// std::upper_bound return the same distance-from-begin as on an identically
// sorted std::vector. This exercises the full random-access iterator
// contract (operator+, operator-, operator[], operator<=>); failure here
// would indicate a broken iterator that passes simpler forward-iteration
// tests.
void SortedBinarySearch(std::vector<int32_t> vals) {
    chunked_vector<int32_t> impl(vals.begin(), vals.end());
    std::sort(impl.begin(), impl.end());
    std::sort(vals.begin(), vals.end());

    for (int32_t q : vals) {
        auto exp_lb = (ptrdiff_t)std::distance(
          vals.begin(), std::lower_bound(vals.begin(), vals.end(), q));
        auto got_lb = (ptrdiff_t)std::distance(
          impl.begin(), std::lower_bound(impl.begin(), impl.end(), q));
        ASSERT_EQ(exp_lb, got_lb) << "lower_bound mismatch for q=" << q;

        auto exp_ub = (ptrdiff_t)std::distance(
          vals.begin(), std::upper_bound(vals.begin(), vals.end(), q));
        auto got_ub = (ptrdiff_t)std::distance(
          impl.begin(), std::upper_bound(impl.begin(), impl.end(), q));
        ASSERT_EQ(exp_ub, got_ub) << "upper_bound mismatch for q=" << q;
    }
}
FUZZ_TEST(ChunkedVectorPBT, SortedBinarySearch)
  .WithDomains(
    fuzztest::VectorOf(fuzztest::Arbitrary<int32_t>()).WithMaxSize(500));

// Property: indexed access via operator[](i) and iterator dereference
// *(begin() + i) return the same element for every valid index. These
// code paths compute the fragment index and offset independently, so a
// bug in either calculation shows up here but not in sequential iteration.
void IndexedAccessMatchesIterator(std::vector<int32_t> vals) {
    chunked_vector<int32_t> v(vals.begin(), vals.end());
    for (size_t i = 0; i < v.size(); ++i) {
        ASSERT_EQ(v[i], *(v.begin() + (ptrdiff_t)i))
          << "mismatch at index " << i;
    }
}
FUZZ_TEST(ChunkedVectorPBT, IndexedAccessMatchesIterator)
  .WithDomains(
    fuzztest::VectorOf(fuzztest::Arbitrary<int32_t>()).WithMaxSize(500));

// Property: reverse iteration via rbegin()/rend() produces elements in
// exactly the reverse order of forward iteration. Unlike the operator[]
// test above this exercises the reverse_iterator adapter wrapping the
// random-access iterator.
void ReverseIterationMatchesReverse(std::vector<int32_t> vals) {
    chunked_vector<int32_t> v(vals.begin(), vals.end());
    std::vector<int32_t> fwd(v.begin(), v.end());
    std::vector<int32_t> rev(v.rbegin(), v.rend());
    std::reverse(fwd.begin(), fwd.end());
    ASSERT_EQ(fwd, rev);
}
FUZZ_TEST(ChunkedVectorPBT, ReverseIterationMatchesReverse)
  .WithDomains(
    fuzztest::VectorOf(fuzztest::Arbitrary<int32_t>()).WithMaxSize(500));

// ============================================================
// chunked_hash_map PBT
// ============================================================

// Encoded operation on a chunked_hash_map<int32_t, int32_t>.
struct MapOp {
    uint8_t kind; // taken mod kNumMapOps
    int32_t key;
    int32_t value; // used only for insert
};

static constexpr uint8_t kNumMapOps = 3;
// 0 insert_or_assign, 1 erase, 2 find/contains

auto MapOpDomain() {
    return fuzztest::StructOf<MapOp>(
      fuzztest::Arbitrary<uint8_t>(),
      fuzztest::Arbitrary<int32_t>(),
      fuzztest::Arbitrary<int32_t>());
}

// Property: chunked_hash_map<int32_t,int32_t> matches std::unordered_map
// for all insert_or_assign, erase, and find operations:
//   - size() always agrees
//   - find(k) returns end() iff the oracle does
//   - when a key is present, the stored value matches the oracle
//   - every key present in the oracle is reachable by iterating the map
void MapModelOracle(std::vector<MapOp> ops) {
    chunked_hash_map<int32_t, int32_t> impl;
    std::unordered_map<int32_t, int32_t> oracle;

    for (const auto& op : ops) {
        switch (op.kind % kNumMapOps) {
        case 0: // insert_or_assign
            impl.insert_or_assign(op.key, op.value);
            oracle[op.key] = op.value;
            break;
        case 1: // erase
            impl.erase(op.key);
            oracle.erase(op.key);
            break;
        case 2: { // find
            auto it = impl.find(op.key);
            auto oit = oracle.find(op.key);
            ASSERT_EQ(it == impl.end(), oit == oracle.end())
              << "find disagrees for key=" << op.key;
            if (it != impl.end()) {
                ASSERT_EQ(it->second, oit->second)
                  << "value disagrees for key=" << op.key;
            }
            break;
        }
        }

        ASSERT_EQ(impl.size(), oracle.size());

        // Every key in the oracle must be findable in the map with the same
        // value. This checks that nothing is silently lost across all keys
        // visible so far, not just the one operated on.
        for (const auto& [k, v] : oracle) {
            auto it = impl.find(k);
            ASSERT_NE(it, impl.end()) << "oracle key " << k << " missing";
            ASSERT_EQ(it->second, v) << "value wrong for oracle key " << k;
        }
    }
}
FUZZ_TEST(ChunkedHashMapPBT, MapModelOracle)
  .WithDomains(fuzztest::VectorOf(MapOpDomain()).WithMaxSize(300));

// Property: after inserting a batch of (key, value) pairs with no
// duplicates, iterating over the map visits exactly the same set of keys
// (in any order) as the input. This checks that the segmented-map's bucket
// and value-vector interaction doesn't silently drop or duplicate entries
// during the growth path that adds new chunked_vector fragments.
void InsertBatchThenIterateAll(std::vector<std::pair<int32_t, int32_t>> pairs) {
    // Deduplicate: last write wins, matching insert_or_assign semantics.
    std::unordered_map<int32_t, int32_t> deduped(pairs.begin(), pairs.end());

    chunked_hash_map<int32_t, int32_t> impl(
      deduped.begin(), deduped.end(), deduped.size());

    ASSERT_EQ(impl.size(), deduped.size());

    // Collect keys from impl and oracle, sort both, compare.
    std::vector<int32_t> impl_keys, oracle_keys;
    impl_keys.reserve(impl.size());
    oracle_keys.reserve(deduped.size());
    for (const auto& [k, _] : impl) {
        impl_keys.push_back(k);
    }
    for (const auto& [k, _] : deduped) {
        oracle_keys.push_back(k);
    }
    std::sort(impl_keys.begin(), impl_keys.end());
    std::sort(oracle_keys.begin(), oracle_keys.end());
    ASSERT_EQ(impl_keys, oracle_keys);
}
FUZZ_TEST(ChunkedHashMapPBT, InsertBatchThenIterateAll)
  .WithDomains(
    fuzztest::VectorOf(
      fuzztest::PairOf(
        fuzztest::Arbitrary<int32_t>(), fuzztest::Arbitrary<int32_t>()))
      .WithMaxSize(300));

} // namespace
