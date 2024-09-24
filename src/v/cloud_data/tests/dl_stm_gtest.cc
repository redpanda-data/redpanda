// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "cloud_data/dl_placeholder.h"
#include "cloud_data/dl_stm.h"
#include "cloud_data/types.h"
#include "model/fundamental.h"
#include "raft/tests/raft_fixture.h"
#include "random/generators.h"
#include "serde/rw/rw.h"
#include "test_utils/randoms.h"
#include "test_utils/test.h"
#include "utils/uuid.h"

#include <seastar/coroutine/all.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/log.hh>

#include <algorithm>
#include <exception>
#include <memory>
#include <system_error>

inline ss::logger test_log("dl_stm_gtest");

namespace cd = cloud_data;

struct generated_range {
    std::vector<cd::dl_overlay> batches;
    absl::btree_map<model::term_id, kafka::offset> terms;
};

generated_range generate_monotonic_range(
  kafka::offset base,
  kafka::offset last,
  int granularity,
  cd::dl_stm_object_ownership own) {
    model::term_id current_term(random_generators::get_int(100));
    absl::btree_map<model::term_id, kafka::offset> terms;
    std::vector<cd::dl_overlay> batches;
    for (auto ix = base(); ix <= last(); ix += granularity) {
        absl::btree_map<model::term_id, kafka::offset> terms_delta;
        bool new_term = random_generators::get_int(0, 10) > 9;
        if (new_term) {
            current_term++;
            terms[current_term] = kafka::offset(ix);
            terms_delta[current_term] = kafka::offset(ix);
        }
        cd::dl_overlay batch{
          .base_offset = kafka::offset(ix),
          .last_offset = kafka::offset(ix + granularity - 1),
          .terms = std::move(terms_delta),
          .id = cd::object_id(uuid_t::create()),
          .ownership = own,
          .offset = cd::first_byte_offset_t(0),
          .size_bytes = cd::byte_range_size_t(
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

TEST(overlay_collection_test, test_append1) {
    // Check overlay handling
    cd::detail::dl_stm_state overlays;
    auto gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(100),
      1,
      cd::dl_stm_object_ownership::exclusive);
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

TEST(overlay_collection_test, test_append2) {
    // Check overlay handling
    cd::detail::dl_stm_state overlays;
    auto gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(100),
      10,
      cd::dl_stm_object_ownership::exclusive);
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

TEST(overlay_collection_test, test_basic_term_lookup) {
    // Check overlay handling
    cd::detail::dl_stm_state overlays;
    auto gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(100),
      1,
      cd::dl_stm_object_ownership::exclusive);
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

TEST(overlay_collection_test, test_leveling1) {
    // Check overlay handling.
    // We have a bunch of small objects replaced by larger objects.
    // Everything should work as if we didn't have small objects.
    cd::detail::dl_stm_state overlays;
    auto gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(100),
      1,
      cd::dl_stm_object_ownership::exclusive);
    for (const auto& b : gen.batches) {
        overlays.register_overlay_cmd(b);
    }
    gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(100),
      10,
      cd::dl_stm_object_ownership::exclusive);
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

TEST(overlay_collection_test, test_append_out_of_order) {
    // Check overlay handling.
    // The test adds batches out of order (they're shuffled randomly).
    // The dl_stm_state should be able to handle them just fine.
    // The test case is important because it's supposed to trigger
    // compaction in the dl_stm's state.
    cd::detail::dl_stm_state overlays;
    auto gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(1000),
      10,
      cd::dl_stm_object_ownership::exclusive);
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

TEST(overlay_collection_test, sorted_run_serialization_roundtrip) {
    cd::detail::sorted_run_t original;
    auto gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(1000),
      10,
      cd::dl_stm_object_ownership::exclusive);
    for (const auto& b : gen.batches) {
        original.maybe_append(b);
    }

    auto buf = serde::to_iobuf(original);
    auto restored = serde::from_iobuf<cd::detail::sorted_run_t>(std::move(buf));

    ASSERT_TRUE(original == restored);
}

TEST(overlay_collection_test, overlay_collection_serialization_roundtrip) {
    cd::detail::overlay_collection original;
    auto gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(1000),
      10,
      cd::dl_stm_object_ownership::exclusive);
    for (const auto& b : gen.batches) {
        original.append(b);
    }

    auto buf = serde::to_iobuf(original);
    auto restored = serde::from_iobuf<cd::detail::overlay_collection>(
      std::move(buf));

    ASSERT_TRUE(original == restored);
}

TEST(overlay_collection_test, dl_stm_state_serialization_roundtrip) {
    cd::detail::dl_stm_state original;
    auto gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(1000),
      10,
      cd::dl_stm_object_ownership::exclusive);
    for (const auto& b : gen.batches) {
        original.register_overlay_cmd(b);
    }

    auto buf = serde::to_iobuf(original);
    auto restored = serde::from_iobuf<cd::detail::dl_stm_state>(std::move(buf));

    ASSERT_TRUE(original == restored);
}

struct dl_stm_node {
    dl_stm_node() = default;

    ss::shared_ptr<cloud_data::dl_stm> dl_stm;
};

class dl_stm_fixture : public raft::raft_fixture {
public:
    static constexpr auto node_count = 3;

    cloud_data::dl_stm& get_leader_stm() {
        const auto leader = get_leader();
        if (!leader) {
            throw std::runtime_error{"No leader"};
        }

        auto ptr = _dl_stm_nodes.at(*leader).dl_stm;
        if (!ptr) {
            throw std::runtime_error{
              ssx::sformat("dl_stm for node {} is not initialised", *leader)};
        }

        return *ptr;
    }

    ss::future<> wait_for_apply() {
        auto committed_offset = co_await with_leader(
          10s, [](auto& node) { return node.raft()->committed_offset(); });

        co_await parallel_for_each_node([committed_offset](auto& node) {
            return node.raft()->stm_manager()->wait(
              committed_offset, model::no_timeout);
        });
        co_return;
    }

    ss::future<> start() {
        for (auto i = 0; i < node_count; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }

        for (auto& [id, node] : nodes()) {
            auto& stm_node = _dl_stm_nodes.at(id());

            co_await node->initialise(all_vnodes());

            raft::state_machine_manager_builder builder;
            auto stm = builder.create_stm<cloud_data::dl_stm>(
              test_log, node->raft().get());

            stm_node.dl_stm = std::move(stm);

            vlog(test_log.info, "Starting node {}", id);

            co_await node->start(std::move(builder));
        }
    }

    std::optional<model::term_id> get_current_term() {
        auto node = get_leader();
        if (!node.has_value()) {
            return std::nullopt;
        }
        auto rni = this->node_for(node.value());
        if (!rni.has_value()) {
            return std::nullopt;
        }
        return rni.value().get().raft()->term();
    }

    std::array<dl_stm_node, node_count> _dl_stm_nodes;
};

TEST_F_CORO(dl_stm_fixture, test_stm_apply_commands) {
    const auto long_timeout = std::chrono::seconds(10);
    ss::abort_source never_abort;

    co_await start();
    co_await wait_for_leader(long_timeout);

    // TODO: generate overlay commands
    auto gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(1000),
      10,
      cd::dl_stm_object_ownership::exclusive);

    for (auto b : gen.batches) {
        auto& stm = get_leader_stm();
        auto term = get_current_term();
        if (!term.has_value()) {
            co_await wait_for_leader(long_timeout);
            term = get_current_term();
        }
        auto builder = stm.make_command_builder(term.value());
        builder.add_overlay_batch(
          b.id,
          b.offset,
          b.size_bytes,
          b.ownership,
          b.base_offset,
          b.last_offset,
          b.base_ts,
          b.last_ts);
        auto result = co_await builder.replicate();
        ASSERT_TRUE_CORO(result);
    }

    co_await wait_for_apply();

    for (const auto& node : _dl_stm_nodes) {
        for (const auto& b : gen.batches) {
            auto maybe_result = node.dl_stm->lower_bound(b.base_offset);
            ASSERT_TRUE_CORO(maybe_result.has_value());
            ASSERT_TRUE_CORO(maybe_result.value().base_offset == b.base_offset);
            ASSERT_TRUE_CORO(maybe_result.value().last_offset == b.last_offset);
        }
    }
}
