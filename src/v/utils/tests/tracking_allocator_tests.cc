// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "json/document.h"
#include "utils/tracking_allocator.h"

#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>
#include <boost/test/unit_test.hpp>

using util::mem_tracker;
template<class T>
using allocator = util::tracking_allocator<T>;

BOOST_AUTO_TEST_CASE(allocator_basic) {
    auto root = ss::make_shared<mem_tracker>("root");
    allocator<int> alloc{root};
    auto* p = alloc.allocate(2);
    BOOST_REQUIRE_EQUAL(root->consumption(), 2 * sizeof(int));

    // create a child
    auto child1 = root->create_child("child1");
    allocator<int64_t> alloc1(child1);
    auto* p1 = alloc1.allocate(3);

    BOOST_REQUIRE_EQUAL(child1->consumption(), 3 * sizeof(int64_t));
    BOOST_REQUIRE_EQUAL(
      root->consumption(), 2 * sizeof(int) + 3 * sizeof(int64_t));

    // deallocate
    alloc1.deallocate(p1, 3);
    BOOST_REQUIRE_EQUAL(child1->consumption(), 0);
    BOOST_REQUIRE_EQUAL(root->consumption(), 2 * sizeof(int));
    alloc.deallocate(p, 2);
    BOOST_REQUIRE_EQUAL(root->consumption(), 0);
}

// Creates a deeply nested mem tracker hierarchy and makes sure
// the json pretty printer outputs a valid json string.
BOOST_AUTO_TEST_CASE(mem_tracker_pretty_printing) {
    using mem_tracker_ptr = ss::shared_ptr<mem_tracker>;
    mem_tracker root{"root"};
    // This method recursively generates nested trackers with deep nesting.
    std::vector<mem_tracker_ptr> extend_lifetimes;
    std::function<void(int, mem_tracker*, mem_tracker*)>
      generate_nested_trackers =
        [&](int depth, mem_tracker* parent, mem_tracker* current) {
            if (depth == 20) {
                return;
            }
            // populate a child at current depth and another
            // at a nested depth.
            mem_tracker_ptr child;
            if (parent) {
                child = parent->create_child("test");
                extend_lifetimes.push_back(child);
            }
            mem_tracker_ptr nested_child;
            if (current) {
                nested_child = current->create_child("test");
                extend_lifetimes.push_back(nested_child);
            }
            generate_nested_trackers(depth + 1, parent, child.get());
            generate_nested_trackers(depth + 1, current, nested_child.get());
        };
    generate_nested_trackers(0, &root, nullptr);

    // Check the json output is well formed.
    auto json = root.pretty_print_json();
    json::Document document;
    document.Parse(json.c_str());
    BOOST_REQUIRE_MESSAGE(
      !document.HasParseError(),
      fmt::format("json: \n {} ascii \n {}", json, root.pretty_print_ascii()));
}

BOOST_AUTO_TEST_CASE(allocator_list_vector) {
    auto tracker = ss::make_shared<mem_tracker>("vec");
    BOOST_REQUIRE_EQUAL(tracker->consumption(), 0);
    {
        allocator<int> alloc{tracker};
        std::vector<int, allocator<int>> v{alloc};
        v.push_back(1);
        BOOST_REQUIRE_EQUAL(tracker->consumption(), sizeof(int));
        v.push_back(2);
        BOOST_REQUIRE_EQUAL(tracker->consumption(), 2 * sizeof(int));
    }
    BOOST_REQUIRE_EQUAL(tracker->consumption(), 0);
    {
        allocator<int> alloc{tracker};
        std::list<int, allocator<int>> v{alloc};
        v.push_back(1);
        BOOST_REQUIRE_GE(tracker->consumption(), 0);
    }
    BOOST_REQUIRE_EQUAL(tracker->consumption(), 0);
}

template<template<class...> class T>
void test_mem_tracked_map() {
    auto tracker = ss::make_shared<mem_tracker>("map");
    BOOST_REQUIRE_EQUAL(tracker->consumption(), 0);
    {
        auto map = util::mem_tracked::map<T, int, int>(tracker);
        map[1] = 2;
        auto before = tracker->consumption();
        BOOST_REQUIRE_GE(before, 0);
        // make copies, allocator should be copied with
        // the same underlying mem_tracker.
        // consumption should be doubled.
        // Copy assignment
        auto map_copy = map;
        auto after_copy = tracker->consumption();
        BOOST_REQUIRE_EQUAL(after_copy, 2 * before);
        // Copy constructor
        auto map_another_copy(map_copy);
        auto after_another_copy = tracker->consumption();
        BOOST_REQUIRE_EQUAL(after_another_copy, 3 * before);

        // moves, deallocates and allocates
        // the same amount of memory, so consumption shouldn't
        // change.
        // move assignment
        auto map_move = std::move(map_another_copy);
        BOOST_REQUIRE_EQUAL(tracker->consumption(), 3 * before);
        // move constructor
        auto map_another_move(std::move(map_move));
        BOOST_REQUIRE_EQUAL(tracker->consumption(), 3 * before);
    }
    BOOST_REQUIRE_EQUAL(tracker->consumption(), 0);
    {
        // Swap
        auto tracker_x = ss::make_shared<mem_tracker>("x");
        auto x = util::mem_tracked::map<T, int, int>(tracker_x);
        x[1] = 2;
        x[2] = 3;
        auto tracker_x_before = tracker_x->consumption();
        BOOST_REQUIRE_GE(tracker_x_before, 0);
        auto tracker_y = ss::make_shared<mem_tracker>("y");
        auto y = util::mem_tracked::map<T, int, int>(tracker_y);
        y[4] = 5;
        auto tracker_y_before = tracker_y->consumption();
        BOOST_REQUIRE_GE(tracker_y_before, 0);
        BOOST_REQUIRE_GE(tracker_x_before, tracker_y_before);
        x.swap(y);
        // Make sure trackers are swapped.
        BOOST_REQUIRE_EQUAL(tracker_x->consumption(), tracker_x_before);
        BOOST_REQUIRE_EQUAL(tracker_y->consumption(), tracker_y_before);
    }
}

template<template<class...> class T>
void test_mem_tracked_set() {
    auto tracker = ss::make_shared<mem_tracker>("set");
    BOOST_REQUIRE_EQUAL(tracker->consumption(), 0);
    {
        auto set = util::mem_tracked::set<T, int>(tracker);
        set.insert(1);
        auto before = tracker->consumption();
        BOOST_REQUIRE_GT(before, 0);
        // make a copy, allocator should be copied with
        // the same underlying mem_tracker.
        // consumption should be doubled.
        // Copy assignment.
        auto set_copy = set;
        BOOST_REQUIRE_EQUAL(tracker->consumption(), 2 * before);
        // Copy constructor.
        auto set_another_copy(set_copy);
        BOOST_REQUIRE_EQUAL(tracker->consumption(), 3 * before);

        // move assignment, deallocates and allocates
        // the same amount of memory.
        // Move assignment
        auto set_move = std::move(set_copy);
        BOOST_REQUIRE_EQUAL(tracker->consumption(), 3 * before);
        // Move constructor
        auto set_move_again(std::move(set_move));
        BOOST_REQUIRE_EQUAL(tracker->consumption(), 3 * before);
    }
    BOOST_REQUIRE_EQUAL(tracker->consumption(), 0);
    {
        // Swap
        auto tracker_x = ss::make_shared<mem_tracker>("x");
        auto x = util::mem_tracked::set<T, int>(tracker_x);
        x.insert(1);
        x.insert(2);
        auto tracker_x_before = tracker_x->consumption();
        BOOST_REQUIRE_GE(tracker_x_before, 0);
        auto tracker_y = ss::make_shared<mem_tracker>("y");
        auto y = util::mem_tracked::set<T, int>(tracker_y);
        y.insert(3);
        auto tracker_y_before = tracker_y->consumption();
        BOOST_REQUIRE_GE(tracker_y_before, 0);
        BOOST_REQUIRE_GE(tracker_x_before, tracker_y_before);
        x.swap(y);
        // Make sure trackers are swapped.
        BOOST_REQUIRE_EQUAL(tracker_x->consumption(), tracker_x_before);
        BOOST_REQUIRE_EQUAL(tracker_y->consumption(), tracker_y_before);
    }
}

BOOST_AUTO_TEST_CASE(allocator_stl) {
    test_mem_tracked_map<std::map>();
    test_mem_tracked_map<std::unordered_map>();
    test_mem_tracked_map<absl::flat_hash_map>();
    test_mem_tracked_map<absl::node_hash_map>();
    test_mem_tracked_map<absl::btree_map>();

    test_mem_tracked_set<std::set>();
    test_mem_tracked_set<std::unordered_set>();
    test_mem_tracked_set<absl::btree_set>();
    test_mem_tracked_set<absl::flat_hash_set>();
    test_mem_tracked_set<absl::node_hash_set>();
}
