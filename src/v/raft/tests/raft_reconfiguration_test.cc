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
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/errc.h"
#include "raft/group_configuration.h"
#include "raft/tests/raft_fixture.h"
#include "raft/types.h"
#include "random/generators.h"
#include "serde/rw/rw.h"
#include "serde/serde.h"
#include "storage/record_batch_builder.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/bool_class.hh>

#include <absl/container/flat_hash_set.h>
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <optional>

using namespace raft;

static ss::logger test_log("reconfiguration-test");

/**
 * Some basic Raft tests validating if Raft test fixture is working correctly
 */

using use_snapshot = ss::bool_class<struct use_snapshot_tag>;
using use_initial_learner_offset = ss::bool_class<struct use_snapshot_tag>;
struct test_params {
    use_snapshot snapshot;
    int initial_size;
    int nodes_to_add;
    int nodes_to_remove;
    use_initial_learner_offset learner_start_offset;
    consistency_level consistency_level = raft::consistency_level::quorum_ack;
};

struct reconfiguration_test
  : testing::WithParamInterface<std::tuple<
      use_snapshot,
      int,
      int,
      int,
      use_initial_learner_offset,
      consistency_level>>
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
          random_generators::get_int(100, 500), [](size_t b_idx) {
              /**
               * Use archival metadata batches to populate offset translator
               */
              auto const batch_type = random_generators::random_choice(
                {model::record_batch_type::raft_data,
                 model::record_batch_type::archival_metadata});
              storage::record_batch_builder builder(
                batch_type, model::offset(0));

              for (int i = 0; i < random_generators::get_int(1, 10); ++i) {
                  auto r_size = random_generators::get_int<size_t>(32, 1_KiB);
                  builder.add_raw_kv(
                    serde::to_iobuf(fmt::format("{}-{}", b_idx, i)),
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
    std::vector<int64_t> deltas;
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
    use_snapshot snapshot = std::get<0>(param);
    int initial_size = std::get<1>(param);
    int nodes_to_add = std::get<2>(param);
    int nodes_to_remove = std::get<3>(param);
    use_initial_learner_offset use_learner_start_offset = std::get<4>(param);
    consistency_level consistency_level = std::get<5>(param);
    // skip test cases that makes no sense
    if (
      nodes_to_add + initial_size - nodes_to_remove <= 0
      || initial_size < nodes_to_remove) {
        co_return;
    }
    fmt::print(
      "test parameters: {{snapshot: {}, initial_size: {}, "
      "nodes_to_add: {}, nodes_to_remove: {}, use_learner_start_offset: {}, "
      "consistency_lvl: {}}}\n",
      snapshot,
      initial_size,
      nodes_to_add,
      nodes_to_remove,
      use_learner_start_offset,
      consistency_level);
    // create group with initial configuration
    co_await create_simple_group(initial_size);

    // replicate batches
    auto result = co_await retry_with_leader(
      model::timeout_clock::now() + 30s,
      [this, consistency_level](raft_node_instance& leader_node) {
          return leader_node.raft()
            ->replicate(
              make_random_batches(), replicate_options(consistency_level))
            .then([this](::result<replicate_result> r) {
                if (!r) {
                    return ss::make_ready_future<::result<model::offset>>(
                      r.error());
                }
                return wait_for_offset(r.value().last_offset, nodes());
            });
      });

    // wait for leader
    ASSERT_TRUE_CORO(result.has_value());
    auto leader = co_await wait_for_leader(30s);
    auto& leader_node = node(leader);
    model::offset start_offset = leader_node.raft()->start_offset();
    if (snapshot) {
        if (consistency_level == consistency_level::leader_ack) {
            for (auto& [_, n] : nodes()) {
                co_await n->raft()->refresh_commit_index();
            }
            co_await wait_for_committed_offset(
              leader_node.raft()->flushed_offset(), 10s);
        }

        const auto rand_offset = co_await with_leader(
          30s, [](raft_node_instance& leader_node) {
              auto committed_offset = leader_node.raft()->committed_offset();
              auto start_offset = leader_node.raft()->start_offset();
              /**
               * Take snapshot at offset ranging from start_offset to the middle
               * of the log
               */

              return leader_node.random_batch_base_offset(
                start_offset
                + model::offset((committed_offset - start_offset) / 2));
          });

        const auto last_included_offset = model::prev_offset(rand_offset);
        start_offset = model::next_offset(last_included_offset);
        co_await with_leader(
          30s, [last_included_offset](raft_node_instance& leader_node) {
              return leader_node.raft()->write_snapshot(
                raft::write_snapshot_cfg(last_included_offset, {}));
          });
    }
    std::optional<model::offset> learner_start_offset;
    if (use_learner_start_offset) {
        learner_start_offset = co_await with_leader(
          30s, [](raft_node_instance& leader_node) {
              return leader_node.random_batch_base_offset(
                leader_node.raft()->dirty_offset() - model::offset(1));
          });
        if (learner_start_offset != model::offset{}) {
            start_offset = *learner_start_offset;
        }
    }

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

    // update group configuration
    auto success = co_await retry_with_leader(
      model::timeout_clock::now() + 30s,
      [current_nodes, learner_start_offset](raft_node_instance& leader_node) {
          return leader_node.raft()
            ->replace_configuration(
              current_nodes, model::revision_id(0), learner_start_offset)
            .then([](std::error_code ec) {
                if (ec) {
                    return ::result<bool>(ec);
                }
                return ::result<bool>(true);
            });
      });
    ASSERT_TRUE_CORO(success);
    co_await with_leader(
      30s, [this, consistency_level](raft_node_instance& leader_node) {
          // wait for committed offset to propagate
          if (consistency_level == raft::consistency_level::quorum_ack) {
              return wait_for_committed_offset(
                leader_node.raft()->committed_offset(), 30s);
          } else {
              return wait_for_visible_offset(
                leader_node.raft()->last_visible_index(), 30s);
          }
      });

    co_await wait_for_reconfiguration_to_finish(current_node_ids, 30s);

    co_await assert_logs_equal(start_offset);

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

        if (learner_start_offset && added_nodes.contains(n.get_vnode())) {
            ASSERT_EQ_CORO(n.raft()->start_offset(), learner_start_offset);
        }
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
    testing::Values(use_snapshot::yes, use_snapshot::no),
    testing::Values(1, 3),    // initial size
    testing::Values(0, 1, 3), // to add
    testing::Values(0, 1, 3), // to remove
    testing::Values(use_initial_learner_offset::yes),
    testing::Values(
      consistency_level::quorum_ack, consistency_level::leader_ack)));
