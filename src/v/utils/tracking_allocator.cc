/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "utils/tracking_allocator.h"

#include "json/json.h"
#include "utils/human.h"

#include <queue>
#include <sstream>
#include <stack>

namespace util {

ss::shared_ptr<mem_tracker> mem_tracker::create_child(ss::sstring label) {
    auto child = ss::make_shared<mem_tracker>(label);
    _children.push_back(*child);
    return child;
}

// Consumption of self and all children.
// Purposefully written in an iterative way to avoid
// stack overflow with deep mem_tracker hierarchies.
int64_t mem_tracker::consumption() const {
    int64_t total = 0;
    std::queue<std::reference_wrapper<const mem_tracker>> queue;
    queue.push(std::cref(*this));
    while (!queue.empty()) {
        const auto& front = queue.front().get();
        total += front._consumption;
        for (const auto& child : front._children) {
            queue.push(std::cref(child));
        }
        queue.pop();
    }
    return total;
}

// Helper to pretty print a given level.
ss::sstring pretty_print_ascii_level(
  int level, const ss::sstring& label, int64_t consumption) {
    std::stringstream result;
    for (int i = 0; i <= level; i++) {
        result << "| ";
    }
    result << "\n";
    for (int i = 0; i < level; i++) {
        result << "| ";
    }
    result << label << ": " << human::bytes(static_cast<double>(consumption))
           << '\n';
    return result.str();
}

ss::sstring mem_tracker::pretty_print_ascii() const {
    std::stack<std::pair<int, std::reference_wrapper<const mem_tracker>>> stack;
    stack.push(std::pair(0, std::cref(*this)));
    std::stringstream result;
    while (!stack.empty()) {
        const auto& top = stack.top();
        const auto& tracker = top.second.get();
        // We intentionally do not compute cumulative consumption for the tree
        // rooted at the current tracker to avoid an O(n^2) loop.
        result << pretty_print_ascii_level(
          top.first, tracker._label, tracker._consumption);
        stack.pop();
        auto next_level = top.first + 1;
        for (const auto& child : tracker._children) {
            stack.push(std::pair(next_level, std::cref(child)));
        }
    }
    return result.str();
}

ss::sstring mem_tracker::pretty_print_json() const {
    // An iterative version to jsonify the tree to avoid stack overflow
    // issues for trackers with deep trees.
    std::stack<std::optional<std::reference_wrapper<const mem_tracker>>> stack;
    json::StringBuffer result;
    json::Writer<json::StringBuffer> writer(result);
    stack.push(std::cref(*this));
    while (!stack.empty()) {
        const auto& top = stack.top();
        if (!top) {
            writer.EndArray();
            writer.EndObject();
            stack.pop();
            continue;
        }
        const auto& tracker = top.value().get();
        writer.StartObject(); // current tracker
        writer.Key("label");
        writer.String(tracker._label);
        writer.Key("consumption");
        // We intentionally do not compute cumulative consumption for the tree
        // rooted at the current tracker to avoid an O(n^2) loop.
        auto consumption = static_cast<double>(tracker._consumption);
        writer.String(fmt::format("{}", human::bytes(consumption)));
        writer.Key("children");
        writer.StartArray(); // for children

        stack.pop();              // done with the current tracker
        stack.push(std::nullopt); // inject a sentinel for closing braces

        for (const auto& child : tracker._children) {
            stack.push(std::cref(child));
        }
    }
    return result.GetString();
}
}; // namespace util