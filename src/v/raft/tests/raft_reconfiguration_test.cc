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
#include "raft/tests/raft_fixture.h"
#include "raft/tests/raft_fixture_retry_policy.h"
#include "raft/types.h"
#include "random/generators.h"
#include "replicate.h"
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
          random_generators::get_int(100, 500), [](size_t b_idx) {
              /**
               * Use archival metadata batches to populate offset translator
               */
              const auto batch_type = random_generators::random_choice(
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
    auto result = co_await retry_with_leader(
      model::timeout_clock::now() + 30s,
      [this, consistency_lvl](raft_node_instance& leader_node) {
          return leader_node.raft()
            ->replicate(
              make_random_batches(), replicate_options(consistency_lvl))
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
        if (consistency_lvl == consistency_level::leader_ack) {
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

    vlog(
      test_log.info,
      "dispatching reconfiguration: {} -> {}",
      old_node_ids,
      current_node_ids);

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

    auto isolated_nodes
      = ss::make_lw_shared<absl::flat_hash_set<model::node_id>>();
    switch (isolated) {
    case isolated_t::none:
        break;
    case isolated_t::old_leader:
        isolated_nodes->insert(leader);
        break;
    case isolated_t::old_followers:
        *isolated_nodes = old_node_ids;
        isolated_nodes->erase(leader);
        break;
    case isolated_t::random:
        for (auto n : all_ids()) {
            if (tests::random_bool()) {
                isolated_nodes->insert(n);
            }
        }
        break;
    }

    if (!isolated_nodes->empty()) {
        vlog(test_log.info, "isolating nodes: {}", *isolated_nodes);

        for (const auto& [source_id, node] : nodes()) {
            node->on_dispatch([=](model::node_id dest_id, raft::msg_type) {
                if (
                  isolated_nodes->contains(source_id)
                  != isolated_nodes->contains(dest_id)) {
                    return ss::sleep(5s);
                }
                return ss::now();
            });
        }

        // heal the partition 5s later
        (void)ss::sleep(5s).then([isolated_nodes] {
            vlog(test_log.info, "healing the network partition");
            isolated_nodes->clear();
        });
    }

    co_await with_leader(
      30s, [this, consistency_lvl](raft_node_instance& leader_node) {
          // wait for committed offset to propagate
          if (consistency_lvl == raft::consistency_level::quorum_ack) {
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
      consistency_level::quorum_ack, consistency_level::leader_ack),
    testing::Values(isolated_t::none)));

INSTANTIATE_TEST_SUITE_P(
  reconfiguration_with_isolated_nodes,
  reconfiguration_test,
  testing::Combine(
    testing::Values(use_snapshot::no),
    testing::Values(3),    // initial size
    testing::Values(2),    // to add
    testing::Values(0, 2), // to remove
    testing::Values(use_initial_learner_offset::yes),
    testing::Values(
      consistency_level::quorum_ack, consistency_level::leader_ack),
    testing::Values(
      isolated_t::old_followers, isolated_t::old_leader, isolated_t::random)));

namespace {
ss::future<std::error_code> wait_for_offset(
  model::offset expected,
  std::vector<model::node_id> ids,
  raft_fixture& fixture) {
    auto start = model::timeout_clock::now();
    // wait for visible offset to propagate
    while (start + 10s > model::timeout_clock::now()) {
        bool aligned = std::all_of(
          ids.begin(), ids.end(), [&](model::node_id id) {
              auto& rni = fixture.node(id);
              vlog(
                test_log.info,
                "node: {}, last_visible_index: {}, expected_offset: {}",
                id,
                rni.raft()->last_visible_index(),
                expected);
              return rni.raft()->last_visible_index() >= expected;
          });

        if (aligned) {
            co_return errc::success;
        }
        co_await ss::sleep(1s);
    }
    co_return raft::errc::timeout;
}
} // namespace

TEST_F_CORO(raft_fixture, test_force_reconfiguration) {
    /**
     * This tests verifies the consistency of logs on all the replicas after a
     * round of force reconfigurations.
     */
    co_await create_simple_group(5);
    co_await wait_for_leader(10s);

    bool stop = false;

    auto replicate_fiber = ss::do_until(
      [&stop] { return stop; },
      [this] {
          ss::lw_shared_ptr<consensus> raft;
          for (auto& n : nodes()) {
              if (n.second->raft()->is_leader()) {
                  raft = n.second->raft();
                  break;
              }
          }

          if (!raft) {
              return ss::sleep(100ms);
          }
          return raft
            ->replicate(
              make_batches(10, 10, 128),
              replicate_options(raft::consistency_level::quorum_ack))
            .then([this](result<replicate_result> result) {
                if (result.has_error()) {
                    vlog(
                      logger().info,
                      "error(replicating): {}",
                      result.error().message());
                }
            });
      });

    std::vector<vnode> base_replica_set = all_vnodes();
    size_t reconfiguration_count = 0;
    model::revision_id next_rev{1};

    auto current_replicas = base_replica_set;

    vlog(logger().info, "initial replicas: {}", current_replicas);
    auto reconfigure_until_success = [&](
                                       model::revision_id rev,
                                       raft::vnode to_skip) {
        auto deadline = model::timeout_clock::now() + 90s;
        return ss::repeat([this, rev, deadline, to_skip, &current_replicas] {
            vassert(
              model::timeout_clock::now() < deadline,
              "Timeout waiting for reconfiguration");
            auto term = node(get_leader().value()).raft()->term();
            return ss::parallel_for_each(
                     nodes().begin(),
                     nodes().end(),
                     [&current_replicas, to_skip, rev](
                       const raft_nodes_t::value_type& pair) {
                         auto raft = pair.second->raft();
                         if (pair.second->get_vnode() == to_skip) {
                             return ss::now();
                         }
                         return raft
                           ->force_replace_configuration_locally(
                             current_replicas, {}, rev)
                           .discard_result();
                     })
              .then([&current_replicas, this, rev, term] {
                  return wait_for_leader_change(
                           model::timeout_clock::now() + 10s, term)
                    .then([this, rev, &current_replicas](
                            model::node_id new_leader_id) {
                        vlog(
                          logger().info,
                          "new leader {} elected in term: {}",
                          new_leader_id,
                          nodes()[new_leader_id]->raft()->term());
                        auto replica_rev
                          = node(new_leader_id).raft()->config().revision_id();
                        if (replica_rev < rev) {
                            vlog(
                              logger().warn,
                              "retrying reconfiguration to {}, requested "
                              "revision: {}, node {} config revision: {}",
                              current_replicas,
                              rev,
                              new_leader_id,
                              replica_rev);
                            return ss::stop_iteration::no;
                        }
                        vlog(
                          logger().info,
                          "successfully reconfigured to {} with revision: {}",
                          current_replicas,
                          rev);
                        return ss::stop_iteration::yes;
                    });
              });
        });
    };
    auto reconfigure_all = [&, this]() {
        /**
         * Switch between all 5 replicas and randomly selected 3 of them
         */
        if (current_replicas.size() == 5) {
            std::shuffle(
              base_replica_set.begin(),
              base_replica_set.end(),
              random_generators::internal::gen);
            current_replicas = {
              base_replica_set.begin(), std::next(base_replica_set.begin(), 3)};
        } else {
            current_replicas = base_replica_set;
        }

        vlog(logger().info, "reconfiguring group to: {}", current_replicas);
        auto to_skip = random_generators::random_choice(base_replica_set);
        auto revision = next_rev++;
        return reconfigure_until_success(revision, to_skip);
    };

    auto reconfigure_fiber = ss::do_until(
      [&] { return stop; },
      [&] {
          return reconfigure_all()
            .then([&]() {
                reconfiguration_count++;

                if (reconfiguration_count >= 50) {
                    stop = true;
                }
                return ss::now();
            })
            .handle_exception([](const std::exception_ptr&) {
                // ignore exception
            });
      });

    auto l_transfer_fiber = ss::do_until(
      [&stop] { return stop; },
      [&, this] {
          std::vector<raft::vnode> not_leaders;
          ss::lw_shared_ptr<consensus> raft;
          for (auto& n : current_replicas) {
              if (node(n.id()).raft()->is_leader()) {
                  raft = node(n.id()).raft();
              } else {
                  not_leaders.push_back(n);
              }
          }

          if (!raft) {
              return ss::sleep(100ms);
          }
          auto target = random_generators::random_choice(not_leaders);
          return raft
            ->transfer_leadership(transfer_leadership_request{
              .group = raft->group(),
              .target = target.id(),
              .timeout = 25ms,
            })
            .then([this](transfer_leadership_reply r) {
                if (r.result != raft::errc::success) {
                    vlog(logger().info, "error(transferring): {}", r);
                }
            })
            .then([] { return ss::sleep(200ms); })
            .handle_exception([](const std::exception_ptr&) {
                // ignore exception
            });
      });

    co_await ss::when_all(
      std::move(replicate_fiber),
      std::move(reconfigure_fiber),
      std::move(l_transfer_fiber));

    logger().info("Validating log consistency");

    auto dirty_offset = co_await retry_with_leader(
      model::timeout_clock::now() + 30s, [](raft_node_instance& leader_node) {
          return ::result<model::offset>(leader_node.raft()->dirty_offset());
      });

    logger().info(
      "Waiting for all nodes to be up to date. Dirty offset: {}",
      dirty_offset.value());

    std::vector<model::node_id> node_ids;
    node_ids.reserve(current_replicas.size());
    for (auto& vn : current_replicas) {
        node_ids.push_back(vn.id());
    }
    auto ec = co_await wait_for_offset(dirty_offset.value(), node_ids, *this);
    ASSERT_EQ_CORO(ec, errc::success);

    using namespace testing;
    for (auto o = model::offset(0); o < dirty_offset.value(); ++o) {
        std::vector<model::offset> kafka_offsets;
        kafka_offsets.reserve(node_ids.size());
        for (auto& id : node_ids) {
            auto& node = this->node(id);
            kafka_offsets.push_back(node.raft()->log()->from_log_offset(o));
        }
        EXPECT_THAT(kafka_offsets, Each(Eq(kafka_offsets[0]))) << fmt::format(
          "Offset translation failure at offset {}, kafka_offets: {}",
          o,
          kafka_offsets);
    }
}
