// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/outcome.h"
#include "bytes/bytes.h"
#include "gtest/gtest.h"
#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/errc.h"
#include "raft/group_configuration.h"
#include "raft/replicate.h"
#include "raft/tests/raft_fixture.h"
#include "raft/tests/raft_fixture_retry_policy.h"
#include "raft/types.h"
#include "random/generators.h"
#include "serde/rw/rw.h"
#include "ssx/future-util.h"
#include "storage/record_batch_builder.h"
#include "test_utils/async.h"
#include "test_utils/randoms.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/loop.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/bool_class.hh>

#include <absl/container/flat_hash_set.h>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <optional>

using namespace raft;

static ss::logger test_log("reconfiguration-test");

/**
 * Some basic Raft tests validating if Raft test fixture is working correctly
 */

using use_snapshot = ss::bool_class<struct use_snapshot_tag>;
using use_initial_learner_offset
  = ss::bool_class<struct use_initial_learner_offset_tag>;

enum class isolated_t {
    none,
    old_leader,
    old_followers,
    random,
};

std::ostream& operator<<(std::ostream& o, isolated_t pt) {
    switch (pt) {
    case isolated_t::none:
        return o << "isolated::none";
    case isolated_t::old_leader:
        return o << "isolated::old_leader";
    case isolated_t::old_followers:
        return o << "isolated::old_followers";
    case isolated_t::random:
        return o << "isolated::random";
    }
    __builtin_unreachable();
}

struct reconfiguration_test
  : testing::WithParamInterface<std::tuple<
      use_snapshot,
      int,
      int,
      int,
      use_initial_learner_offset,
      consistency_level,
      isolated_t>>
  , raft_fixture {
    ss::future<> wait_for_reconfiguration_to_finish(
      const absl::flat_hash_set<model::node_id>& target_ids,
      std::chrono::milliseconds timeout) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(timeout, [this, target_ids] {
            for (auto id : target_ids) {
                auto& n = node(id);
                if (
                  n.raft()->config().get_state()
                  != raft::configuration_state::simple) {
                    return false;
                }
            }
            return true;
        });
    }

    auto make_random_batches() {
        return make_batches(
          random_generators::get_int(10, 100), [](size_t b_idx) {
              /**
               * Use archival metadata batches to populate offset translator
               */

              auto batch_type = model::record_batch_type::raft_data;
              if (random_generators::get_int(1, 10) >= 7) {
                  // lower probability
                  batch_type = model::record_batch_type::archival_metadata;
              } 
              storage::record_batch_builder builder(
                batch_type, model::offset(0));
              int num_records
                = batch_type == model::record_batch_type::archival_metadata
                    ? 1
                    : random_generators::get_int(5, 10);
              for (int i = 0; i < num_records; ++i) {
                  auto r_size = random_generators::get_int<size_t>(32, 1_KiB);
                  // limited key space
                  builder.add_raw_kv(
                    serde::to_iobuf(
                      fmt::format("{}", random_generators::get_int(0, 5))),
                    bytes_to_iobuf(random_generators::get_bytes(r_size)));
              }
              return std::move(builder).build();
          });
    }
};

ss::future<result<model::offset>>
wait_for_offset(model::offset expected, raft_fixture::raft_nodes_t& nodes) {
    auto start = model::timeout_clock::now();
    // wait for visible offset to propagate
    while (start + 10s > model::timeout_clock::now()) {
        bool aligned = std::all_of(
          nodes.begin(), nodes.end(), [expected](auto& p) {
              vlog(
                test_log.info,
                "node: {}, last_visible_index: {}, expected_offset: {}",
                p.first,
                p.second->raft()->last_visible_index(),
                expected);
              return p.second->raft()->last_visible_index() >= expected;
          });

        if (aligned) {
            co_return expected;
        }
        co_await ss::sleep(1s);
    }
    co_return result<model::offset>(raft::errc::timeout);
}

static void assert_offset_translator_state_is_consistent(
  const std::vector<raft_node_instance*>& nodes) {
    if (nodes.size() <= 1) {
        return;
    }
    model::offset start_offset{};
    auto first_raft = nodes.front()->raft();
    model::offset dirty_offset = first_raft->dirty_offset();
    // get the max start offset
    for (auto* n : nodes) {
        start_offset = std::max(n->raft()->start_offset(), start_offset);
    }
    chunked_vector<int64_t> deltas;
    for (model::offset o :
         boost::irange<model::offset>(start_offset, dirty_offset)) {
        deltas.push_back(first_raft->log()->offset_delta(o));
    }

    for (auto it = std::next(nodes.begin()); it != nodes.end(); ++it) {
        auto idx = 0;
        for (model::offset o :
             boost::irange<model::offset>(start_offset, dirty_offset)) {
            ASSERT_EQ((*it)->raft()->log()->offset_delta(o), deltas[idx++]);
        }
    }
}

TEST_P_CORO(reconfiguration_test, configuration_replace_test) {
    const auto param = GetParam();
    auto snapshot = std::get<use_snapshot>(param);
    int initial_size = std::get<1>(param);
    int nodes_to_add = std::get<2>(param);
    int nodes_to_remove = std::get<3>(param);
    auto use_learner_start_offset = std::get<use_initial_learner_offset>(param);
    auto consistency_lvl = std::get<consistency_level>(param);
    auto isolated = std::get<isolated_t>(param);
    // skip test cases that makes no sense
    if (
      nodes_to_add + initial_size - nodes_to_remove <= 0
      || initial_size < nodes_to_remove) {
        co_return;
    }
    if (initial_size == 1 && isolated == isolated_t::old_followers) {
        co_return;
    }
    fmt::print(
      "test parameters: {{snapshot: {}, initial_size: {}, "
      "nodes_to_add: {}, nodes_to_remove: {}, use_learner_start_offset: {}, "
      "consistency_lvl: {}, isolated: {}}}\n",
      snapshot,
      initial_size,
      nodes_to_add,
      nodes_to_remove,
      use_learner_start_offset,
      consistency_lvl,
      isolated);
    // create group with initial configuration
    co_await create_simple_group(initial_size);

    // replicate batches
    auto replicate_some_batches = [&]() {
        return retry_with_leader(
                 model::timeout_clock::now() + 30s,
                 [this, consistency_lvl](raft_node_instance& leader_node) {
                     return leader_node.raft()
                       ->replicate(
                         make_random_batches(),
                         replicate_options(consistency_lvl))
                       .then([](::result<replicate_result> r) {
                           if (!r) {
                               return ss::make_ready_future<
                                 ::result<model::offset>>(r.error());
                           }
                           return ss::make_ready_future<
                             ::result<model::offset>>(model::offset{1});
                       });
                 })
          .discard_result();
    };

    auto stopped = false;
    auto replicate_f = ss::do_until(
      [&] { return stopped; },
      [&] {
          return replicate_some_batches().then([]() { return ss::sleep(1ms); });
      });

    // wait to seed some data and kick off compaction in bg
    co_await ss::sleep(30s);

    // wait for leader
    auto old_node_ids = all_ids();

    auto current_node_ids = old_node_ids;
    auto current_nodes = all_vnodes();

    for (int i = 0; i < nodes_to_remove; ++i) {
        current_node_ids.erase(current_nodes.back().id());
        current_nodes.pop_back();
    }
    absl::flat_hash_set<raft::vnode> added_nodes;
    co_await ss::coroutine::parallel_for_each(
      boost::irange(nodes_to_add), [&](int i) {
          auto& n = add_node(
            model::node_id(initial_size + i), model::revision_id(0));
          current_nodes.push_back(n.get_vnode());
          current_node_ids.insert(n.get_vnode().id());
          added_nodes.emplace(n.get_vnode());
          return n.init_and_start({});
      });

    ASSERT_EQ_CORO(
      current_nodes.size(), initial_size + nodes_to_add - nodes_to_remove);

    vlog(
      test_log.info,
      "dispatching reconfiguration: {} -> {}",
      old_node_ids,
      current_node_ids);

    // update group configuration
    auto success = co_await retry_with_leader(
      model::timeout_clock::now() + 30s,
      [current_nodes](raft_node_instance& leader_node) {
          return leader_node.raft()
            ->replace_configuration(
              current_nodes, model::revision_id(0), std::nullopt)
            .then([](std::error_code ec) {
                if (ec) {
                    return ::result<bool>(ec);
                }
                return ::result<bool>(true);
            });
      });
    ASSERT_TRUE_CORO(success);

    stopped = true;
    co_await std::move(replicate_f);

    co_await wait_for_reconfiguration_to_finish(current_node_ids, 240s);

    auto offset = co_await with_leader(30s, [](raft_node_instance& leader) {
        return leader.raft()->last_visible_index();
    });

    auto result = co_await wait_for_offset(offset, nodes());
    ASSERT_TRUE_CORO(result.has_value()) << "replicas did not converge";

    absl::flat_hash_set<raft::vnode> current_nodes_set(
      current_nodes.begin(), current_nodes.end());

    // validate configuration
    for (auto id : current_node_ids) {
        auto& n = node(id);
        auto cfg = n.raft()->config();
        auto cfg_vnodes = cfg.all_nodes();
        ASSERT_EQ_CORO(
          current_nodes_set,
          absl::flat_hash_set<raft::vnode>(
            cfg_vnodes.begin(), cfg_vnodes.end()));
        ASSERT_FALSE_CORO(cfg.old_config().has_value());
        ASSERT_TRUE_CORO(cfg.current_config().learners.empty());
    }

    std::vector<raft_node_instance*> current_node_ptrs;
    for (auto id : current_node_ids) {
        current_node_ptrs.push_back(&node(id));
    }
    assert_offset_translator_state_is_consistent(current_node_ptrs);
}

INSTANTIATE_TEST_SUITE_P(
  validate_replacing_raft_configuration,
  reconfiguration_test,
  testing::Combine(
    testing::Values(use_snapshot::no),
    testing::Values(1), // initial size
    testing::Values(5), // to add
    testing::Values(0), // to remove
    testing::Values(use_initial_learner_offset::no),
    testing::Values(consistency_level::quorum_ack),
    testing::Values(isolated_t::none)));
