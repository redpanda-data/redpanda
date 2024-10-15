// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "cloud_topics/dl_stm/dl_stm.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "raft/tests/raft_fixture.h"
#include "random/generators.h"
#include "test_utils/randoms.h"
#include "test_utils/test.h"

#include <seastar/coroutine/all.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/log.hh>

#include <algorithm>
#include <exception>
#include <memory>
#include <system_error>

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

struct dl_stm_node {
    dl_stm_node() = default;

    ss::shared_ptr<ct::dl_stm> dl_stm;
};

class dl_stm_fixture : public raft::raft_fixture {
public:
    static constexpr auto node_count = 3;

    ct::dl_stm& get_leader_stm() {
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
            auto stm = builder.create_stm<ct::dl_stm>(
              ct::cd_log, node->raft().get());

            stm_node.dl_stm = std::move(stm);

            vlog(ct::cd_log.info, "Starting node {}", id);

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

    auto gen = generate_monotonic_range(
      kafka::offset(0),
      kafka::offset(1000),
      10,
      ct::dl_stm_object_ownership::exclusive);

    for (auto b : gen.batches) {
        auto& stm = get_leader_stm();
        auto term = get_current_term();
        if (!term.has_value()) {
            co_await wait_for_leader(long_timeout);
            term = get_current_term();
        }
        ct::command_builder builder;
        builder.add_overlay_batch(
          b.id,
          b.offset,
          b.size_bytes,
          b.ownership,
          b.base_offset,
          b.last_offset,
          b.base_ts,
          b.last_ts);
        auto result = co_await stm.replicate(term.value(), std::move(builder));
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
