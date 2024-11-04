// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/dl_overlay.h"
#include "cloud_topics/dl_stm/dl_stm_state.h"
#include "gtest/gtest.h"
#include "random/generators.h"
#include "test_utils/test.h"
#include "utils/uuid.h"

#include <gtest/gtest.h>

#include <algorithm>

namespace ct = experimental::cloud_topics;

class ct::dl_stm_state_accessor {
public:
    static std::deque<ct::dl_overlay_entry>& overlays(ct::dl_stm_state& state) {
        return state._overlays;
    }
};

using q = ct::dl_stm_state_accessor;

namespace {

ct::dl_overlay_object make_overlay_object() {
    auto first_byte_offset = random_generators::get_int(0, 100);
    auto byte_range_size = random_generators::get_int(0, 100);

    return {
      ct::object_id(uuid_t::create()),
      ct::first_byte_offset_t(first_byte_offset),
      ct::byte_range_size_t(byte_range_size),
      ct::dl_stm_object_ownership::shared,
    };
}

ct::dl_overlay
make_overlay(kafka::offset base_offset, kafka::offset last_offset) {
    return {
      base_offset,
      last_offset,
      model::timestamp(0),
      model::timestamp(10),
      absl::btree_map<model::term_id, kafka::offset>{},
      make_overlay_object(),
    };
}
}; // namespace

TEST(dl_stm_state_death, push_overlay) {
    ct::dl_stm_state state;
    state.push_overlay(
      ct::dl_version(1), make_overlay(kafka::offset(0), kafka::offset(10)));
    state.push_overlay(
      ct::dl_version(42), make_overlay(kafka::offset(10), kafka::offset(20)));

    ASSERT_DEATH(
      {
          state.push_overlay(
            ct::dl_version(1),
            make_overlay(kafka::offset(0), kafka::offset(10)));
      },
      "Version can't go backwards. Current version: 42, new version: 1");
}

TEST(dl_stm_state, push_overlay) {
    ct::dl_stm_state state;

    auto overlay1 = make_overlay(kafka::offset(0), kafka::offset(10));
    state.push_overlay(ct::dl_version(1), overlay1);

    ASSERT_EQ(q::overlays(state).size(), 1);

    // Pushing the same overlay again at the same version should be a no-op.
    state.push_overlay(ct::dl_version(1), overlay1);

    ASSERT_EQ(q::overlays(state).size(), 1);
    ASSERT_EQ(q::overlays(state).front().added_at, ct::dl_version(1));
    ASSERT_EQ(q::overlays(state).front().removed_at, ct::dl_version{});

    // Pushing the same overlay again at a different version should throw.
    ASSERT_ANY_THROW(state.push_overlay(ct::dl_version(2), overlay1));
}

TEST(dl_stm_state, lower_bound) {
    auto build_state_with_overlays =
      [](const std::vector<ct::dl_overlay>& overlays) -> ct::dl_stm_state {
        ct::dl_stm_state state;

        auto overlay0 = make_overlay(kafka::offset(0), kafka::offset(10));
        state.push_overlay(ct::dl_version(1), overlay0);

        // Mark the overlay as removed.
        q::overlays(state).front().removed_at = ct::dl_version(2);

        // Add the rest of the overlays.
        for (auto& overlay : overlays) {
            state.push_overlay(ct::dl_version(3), overlay);
        }

        return state;
    };

    auto overlays = std::vector{
      make_overlay(kafka::offset(5), kafka::offset(8)),
      make_overlay(kafka::offset(6), kafka::offset(20)),
      make_overlay(kafka::offset(30), kafka::offset(39)),
      make_overlay(kafka::offset(30), kafka::offset(40)),
    };
    auto base_offset_less_cmp = [](auto& a, auto& b) {
        return a.base_offset < b.base_offset;
    };

    auto push_order = overlays;
    std::sort(push_order.begin(), push_order.end(), base_offset_less_cmp);
    do {
        auto state = build_state_with_overlays(push_order);

        SCOPED_TRACE(fmt::format("Push order: {}", push_order));

        std::cout << "Push order: " << push_order << std::endl;

        // All overlays are added. Including the first one which is marked as
        // removed_at version 2.
        ASSERT_EQ(q::overlays(state).size(), 5);

        ASSERT_EQ(state.lower_bound(kafka::offset(0)), overlays[0])
          << "Searching with an offset lower than any overlay should return "
             "the "
             "first overlay.";

        // Which overlay is returned is an implementation detail and is allowed
        // to change in the future. We might as well return an iterator or all
        // of them.
        {
            ASSERT_EQ(state.lower_bound(kafka::offset(6)), overlays[0])
              << "Searching with an offset that is covered by multiple "
                 "overlays "
                 "should return the overlay with the smallest base offset.";

            ASSERT_EQ(state.lower_bound(kafka::offset(8)), overlays[0])
              << "Searching with an offset that is covered by multiple "
                 "overlays "
                 "should return the overlay with the smallest base offset.";
        }

        ASSERT_EQ(state.lower_bound(kafka::offset(9)), overlays[1]);

        ASSERT_TRUE(
          state.lower_bound(kafka::offset(25)) == overlays[2]
          || state.lower_bound(kafka::offset(25)) == overlays[3])
          << "Searching with an offset that is covered by multiple overlays "
             "with the same base offset should return any of them.";

        ASSERT_EQ(state.lower_bound(kafka::offset(100)), std::nullopt)
          << "Searching with an offset larger than any overlay should return "
             "nullopt.";
    } while (std::next_permutation(
      push_order.begin(), push_order.end(), base_offset_less_cmp));
}
