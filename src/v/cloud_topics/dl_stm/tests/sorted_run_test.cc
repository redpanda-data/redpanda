// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/dl_stm/sorted_run.h"
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

#include <seastar/coroutine/all.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/log.hh>

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
        batches.push_back(batch);
    }
    return generated_range{
      .batches = std::move(batches),
      .terms = std::move(terms),
    };
}

TEST(overlay_collection_test, sorted_run_serialization_roundtrip) {
    ct::sorted_run_t original;
    auto gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(1000),
      10,
      ct::dl_stm_object_ownership::exclusive);
    for (const auto& b : gen.batches) {
        original.maybe_append(b);
    }

    auto buf = serde::to_iobuf(original);
    auto restored = serde::from_iobuf<ct::sorted_run_t>(std::move(buf));

    ASSERT_TRUE(original == restored);
}
