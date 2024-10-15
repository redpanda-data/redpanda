// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "cloud_topics/dl_stm/dl_stm_state.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/map.h"
#include "serde/rw/named_type.h"
#include "serde/rw/rw.h"
#include "serde/rw/uuid.h"
#include "serde/rw/vector.h"
#include "test_utils/randoms.h"
#include "test_utils/test.h"
#include "utils/uuid.h"

#include <seastar/coroutine/all.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/log.hh>

#include <algorithm>

inline ss::logger test_log("dl_stm_state_test");

namespace ct = experimental::cloud_topics;

struct generated_range {
    std::vector<ct::dl_overlay> batches;
    absl::btree_map<model::term_id, kafka::offset> terms;
};

generated_range generate_monotonic_range(
  kafka::offset base,
  kafka::offset last,
  int granularity,
  ct::dl_stm_object_ownership own) {
    model::term_id current_term(random_generators::get_int(100));
    absl::btree_map<model::term_id, kafka::offset> terms;
    std::vector<ct::dl_overlay> batches;
    for (auto ix = base(); ix <= last(); ix += granularity) {
        absl::btree_map<model::term_id, kafka::offset> terms_delta;
        bool new_term = random_generators::get_int(0, 10) > 9;
        if (new_term) {
            current_term++;
            terms[current_term] = kafka::offset(ix);
            terms_delta[current_term] = kafka::offset(ix);
        }
        ct::dl_overlay batch{
          .base_offset = kafka::offset(ix),
          .last_offset = kafka::offset(ix + granularity - 1),
          .terms = std::move(terms_delta),
          .id = ct::object_id(uuid_t::create()),
          .ownership = own,
          .offset = ct::first_byte_offset_t(0),
          .size_bytes = ct::byte_range_size_t(
            random_generators::get_int(1, 10000)),
        };
        /*TODO: remove*/ vlog(
          test_log.info,
          "NEEDLE: generated batch {}:{}",
          batch.base_offset,
          batch.last_offset);
        batches.push_back(batch);
    }
    return generated_range{
      .batches = std::move(batches),
      .terms = std::move(terms),
    };
}

TEST(dl_stm_state_test, test_append1) {
    // Check overlay handling
    ct::dl_stm_state overlays;
    auto gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(100),
      1,
      ct::dl_stm_object_ownership::exclusive);
    for (const auto& b : gen.batches) {
        overlays.register_overlay_cmd(b);
    }
    for (const auto& b : gen.batches) {
        auto maybe_result = overlays.lower_bound(b.base_offset);
        ASSERT_TRUE(maybe_result.has_value());
        ASSERT_TRUE(maybe_result.value().base_offset == b.base_offset);
        ASSERT_TRUE(maybe_result.value().last_offset == b.last_offset);
    }
}

TEST(dl_stm_state_test, test_append2) {
    // Check overlay handling
    ct::dl_stm_state overlays;
    auto gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(100),
      10,
      ct::dl_stm_object_ownership::exclusive);
    for (const auto& b : gen.batches) {
        overlays.register_overlay_cmd(b);
    }
    for (const auto& b : gen.batches) {
        auto maybe_result = overlays.lower_bound(b.base_offset);
        ASSERT_TRUE(maybe_result.has_value());
        ASSERT_TRUE(maybe_result.value().base_offset == b.base_offset);
        ASSERT_TRUE(maybe_result.value().last_offset == b.last_offset);

        maybe_result = overlays.lower_bound(b.last_offset);
        ASSERT_TRUE(maybe_result.has_value());
        ASSERT_TRUE(maybe_result.value().base_offset == b.base_offset);
        ASSERT_TRUE(maybe_result.value().last_offset == b.last_offset);

        maybe_result = overlays.lower_bound(b.base_offset + model::offset{1});
        ASSERT_TRUE(maybe_result.has_value());
        ASSERT_TRUE(maybe_result.value().base_offset == b.base_offset);
        ASSERT_TRUE(maybe_result.value().last_offset == b.last_offset);
    }
}

TEST(dl_stm_state_test, test_basic_term_lookup) {
    // Check overlay handling
    ct::dl_stm_state overlays;
    auto gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(100),
      1,
      ct::dl_stm_object_ownership::exclusive);
    for (const auto& b : gen.batches) {
        overlays.register_overlay_cmd(b);
    }
    for (auto [term, first_term_offset] : gen.terms) {
        auto maybe_result = overlays.get_term_last_offset(
          term - model::term_id(1));
        ASSERT_TRUE(maybe_result.has_value());
        ASSERT_TRUE(
          maybe_result.value() == kafka::prev_offset(first_term_offset));
    }
}

TEST(dl_stm_state_test, test_leveling1) {
    // Check overlay handling.
    // We have a bunch of small objects replaced by larger objects.
    // Everything should work as if we didn't have small objects.
    ct::dl_stm_state overlays;
    auto gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(100),
      1,
      ct::dl_stm_object_ownership::exclusive);
    for (const auto& b : gen.batches) {
        overlays.register_overlay_cmd(b);
    }
    gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(100),
      10,
      ct::dl_stm_object_ownership::exclusive);
    for (const auto& b : gen.batches) {
        overlays.register_overlay_cmd(b);
    }
    for (const auto& b : gen.batches) {
        auto maybe_result = overlays.lower_bound(b.base_offset);
        ASSERT_TRUE(maybe_result.has_value());
        ASSERT_TRUE(maybe_result.value().base_offset == b.base_offset);
        ASSERT_TRUE(maybe_result.value().last_offset == b.last_offset);

        maybe_result = overlays.lower_bound(b.last_offset);
        ASSERT_TRUE(maybe_result.has_value());
        ASSERT_TRUE(maybe_result.value().base_offset == b.base_offset);
        ASSERT_TRUE(maybe_result.value().last_offset == b.last_offset);

        maybe_result = overlays.lower_bound(b.base_offset + model::offset{1});
        ASSERT_TRUE(maybe_result.has_value());
        ASSERT_TRUE(maybe_result.value().base_offset == b.base_offset);
        ASSERT_TRUE(maybe_result.value().last_offset == b.last_offset);
    }
}

TEST(dl_stm_state_test, test_append_out_of_order) {
    // Check overlay handling.
    // The test adds batches out of order (they're shuffled randomly).
    // The dl_stm_state should be able to handle them just fine.
    // The test case is important because it's supposed to trigger
    // compaction in the dl_stm's state.
    ct::dl_stm_state overlays;
    auto gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(1000),
      10,
      ct::dl_stm_object_ownership::exclusive);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(gen.batches.begin(), gen.batches.end(), g);
    for (const auto& b : gen.batches) {
        overlays.register_overlay_cmd(b);
    }
    //
    for (const auto& b : gen.batches) {
        auto maybe_result = overlays.lower_bound(b.base_offset);
        ASSERT_TRUE(maybe_result.has_value());
        ASSERT_TRUE(maybe_result.value().base_offset == b.base_offset);
        ASSERT_TRUE(maybe_result.value().last_offset == b.last_offset);

        maybe_result = overlays.lower_bound(b.last_offset);
        ASSERT_TRUE(maybe_result.has_value());
        ASSERT_TRUE(maybe_result.value().base_offset == b.base_offset);
        ASSERT_TRUE(maybe_result.value().last_offset == b.last_offset);

        maybe_result = overlays.lower_bound(b.base_offset + model::offset{1});
        ASSERT_TRUE(maybe_result.has_value());
        ASSERT_TRUE(maybe_result.value().base_offset == b.base_offset);
        ASSERT_TRUE(maybe_result.value().last_offset == b.last_offset);
    }
}

TEST(dl_stm_state_test, dl_stm_state_serialization_roundtrip) {
    ct::dl_stm_state original;
    auto gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(1000),
      10,
      ct::dl_stm_object_ownership::exclusive);
    for (const auto& b : gen.batches) {
        original.register_overlay_cmd(b);
    }

    auto buf = serde::to_iobuf(original);
    auto restored = serde::from_iobuf<ct::dl_stm_state>(std::move(buf));

    ASSERT_TRUE(original == restored);
}
