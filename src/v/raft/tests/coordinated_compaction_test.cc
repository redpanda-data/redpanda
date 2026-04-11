// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "model/fundamental.h"
#include "raft/compaction_coordinator.h"
#include "raft/tests/raft_fixture.h"
#include "raft/tests/raft_fixture_retry_policy.h"
#include "storage/disk_log_impl.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/loop.hh>

#include <fmt/ranges.h>

#include <algorithm>
#include <ranges>
#include <system_error>
#include <utility>

using namespace raft;

struct coco_fixture : raft_fixture {
private:
    ss::abort_source _as;

public:
    coco_fixture() noexcept = default;

    seastar::future<> SetUpAsync() override {
        co_await raft_fixture::SetUpAsync();

        config::shard_local_cfg().replicate_append_timeout_ms.set_value(10s);
        config::shard_local_cfg().log_cleanup_policy.set_value(
          model::cleanup_policy_bitflags::compaction);
        config::shard_local_cfg().tombstone_retention_ms.set_value(1s);
    }

    ss::future<> make_batches_and_replicate(
      int segments_count,
      int batches_per_segment,
      int records_per_batch,
      size_t record_size,
      chunked_vector<model::offset>& last_data_offsets) {
        auto leader_id = co_await wait_for_leader(10s);
        auto leader_node = &node(leader_id);
        for (int _ : std::views::iota(0, segments_count)) {
            vlog(logger().info, "Replicating...");

            model::offset last_data_offset{};
            for (int attempt = 0; attempt < 5; ++attempt) {
                auto r = co_await leader_node->raft()->replicate(
                  make_batches(
                    batches_per_segment, records_per_batch, record_size),
                  replicate_options(consistency_level::quorum_ack, 10s));
                if (r.has_value()) {
                    last_data_offset = r.value().last_offset;
                    break;
                }
                ASSERT_EQ_CORO(r.error(), raft::errc::not_leader);
                vlog(
                  logger().info,
                  "Not leader anymore while replicating, retrying...");
                leader_id = co_await wait_for_leader(10s);
                leader_node = &node(leader_id);
            }
            RPTEST_REQUIRE_NE_CORO(last_data_offset, model::offset{});

            vlog(
              logger().info,
              "last_data_offset: {}, dirty_offset: {}",
              last_data_offset,
              leader_node->raft()->dirty_offset());

            // make sure all fully replicated
            RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this, last_data_offset] {
                return std::ranges::all_of(
                  all_ids(), [this, last_data_offset](model::node_id node_id) {
                      auto committed_offset
                        = node(node_id).raft()->committed_offset();
                      vlog(
                        logger().info,
                        "node {} committed_offset: {}",
                        node_id,
                        committed_offset);
                      return committed_offset >= last_data_offset;
                  });
            });

            // force log segment roll on all nodes
            co_await ss::parallel_for_each(
              all_ids(), [this](model::node_id node_id) {
                  auto& n = node(node_id);
                  return n.raft()->log()->force_roll();
              });

            last_data_offsets.push_back(last_data_offset);
        }
    }

    storage::housekeeping_config make_housekeeping_config(
      raft_node_instance& n,
      model::offset max_collect_offset = model::offset::max()) {
        model::offset max_tombstone_remove_offset
          = n.raft()->log()->stm_hookset()->max_tombstone_remove_offset();
        model::offset max_tx_end_remove_offset
          = n.raft()->log()->stm_hookset()->max_tx_end_remove_offset();
        return {
          model::timestamp::max(),
          std::nullopt,
          max_collect_offset,
          max_tombstone_remove_offset,
          max_tx_end_remove_offset,
          0ms,
          0ms,
          0ms,
          _as};
    }

    ss::future<> run_compaction(
      raft_node_instance& n,
      model::offset max_collect_offset = model::offset::max()) {
        auto cfg = make_housekeeping_config(n, max_collect_offset);

        co_await n.raft()->log()->force_roll();
        for (const auto& seg : n.raft()->log()->segments()) {
            vlog(
              logger().info,
              "[{}] segment {} has appender: {}",
              n.ntp(),
              seg->offsets().get_base_offset(),
              seg->has_appender());
        }

        co_await n.raft()->log()->housekeeping(std::move(cfg));
    }

    template<typename Range, typename MtroPredicate, typename MxroPredicate>
    bool check_group_offsets_on_nodes(
      Range&& ids, MtroPredicate&& mtro_pred, MxroPredicate&& mxro_pred) {
        return std::ranges::all_of(
          std::forward<Range>(ids),
          [this,
           mtro_pred = std::forward<MtroPredicate>(mtro_pred),
           mxro_pred = std::forward<MxroPredicate>(mxro_pred)](
            model::node_id node_id) {
              const auto& coco
                = node(node_id).raft()->get_compaction_coordinator();
              auto mtro = coco.get_max_tombstone_remove_offset();
              auto mxro = coco.get_max_transaction_remove_offset();
              vlog(
                logger().info,
                "on node {} max tombstone remove offset: {}, max transaction "
                "remove offset: {}",
                node_id,
                mtro,
                mxro);
              return mtro_pred(mtro) && mxro_pred(mxro);
          });
    }

    template<typename Predicate>
    bool check_group_offsets_on_all_nodes(Predicate&& pred) {
        return check_group_offsets_on_nodes(all_ids(), pred, pred);
    }

    ss::future<> transfer_leadership_to(model::node_id target) {
        auto start = model::timeout_clock::now();
        while (true) {
            auto current_leader = co_await wait_for_leader(start + 30s);
            if (current_leader == target) {
                co_return;
            }
            vlog(
              logger().info,
              "Transferring leadership from {} to {}",
              current_leader,
              target);
            auto raft = node(current_leader).raft();
            std::ignore = co_await raft->transfer_leadership(
              {.group = raft->group(), .target = target, .timeout = 10s});
        }
    }

    // does not support concurrent isolations of nodes or any other dispatch
    // interceptions
    void isolate_node(model::node_id isolated_id) {
        for (auto& [id, node] : nodes()) {
            node->on_dispatch(
              [isolated_id, id](model::node_id dest_id, raft::msg_type) {
                  if (isolated_id == id || dest_id == isolated_id) {
                      throw std::runtime_error("injected error");
                  }
                  return ss::now();
              });
        }
    }

    void de_isolate_node([[maybe_unused]] model::node_id id) {
        for (auto& [id, node] : nodes()) {
            node->reset_dispatch_handlers();
        }
    }

    ss::future<> drop_node(model::node_id decommissioned_id) {
        std::vector<vnode> new_vnodes{
          std::from_range,
          all_vnodes()
            | std::views::filter([decommissioned_id](const vnode& v) {
                  return v.id() != decommissioned_id;
              })};
        vlog(
          logger().info,
          "dispatching reconfiguration: {} -> {}",
          all_ids(),
          new_vnodes | std::views::transform(&vnode::id));
        auto success = co_await retry_with_leader(
          model::timeout_clock::now() + 30s,
          [&new_vnodes](raft_node_instance& leader_node) {
              return leader_node.raft()
                ->replace_configuration(new_vnodes, model::revision_id(0))
                .then([](std::error_code ec) {
                    if (ec) {
                        return ::result<bool>(ec);
                    }
                    return ::result<bool>(true);
                });
          });
        ASSERT_TRUE_CORO(success);
        co_await stop_node(decommissioned_id, remove_data_dir::no);
    }

    // sleep enough time for all coordinators to exchange MCCOs and MTROs
    auto coordination_delay() {
        auto& node0coco
          = node(model::node_id{0}).raft()->get_compaction_coordinator();
        auto delay
          = compaction_coordinator::test_accessor::local_offsets_getting_delay(
              node0coco)
            + compaction_coordinator::test_accessor::
              group_offsets_distribution_delay();
        // in case leader gets re-elected during the wait
        delay = 2 * delay + 1s;
        vlog(logger().info, "coordination delay is {}", delay);
        return delay;
    }
};

TEST_F_CORO(coco_fixture, test_stalled_recovery) {
    int initial_size = 3;
    co_await create_simple_group(initial_size);

    // replicate some data
    chunked_vector<model::offset> last_data_offsets;
    co_await make_batches_and_replicate(1, 4, 10, 128, last_data_offsets);

    // add node
    auto old_node_ids = all_ids();
    auto& added_node = add_node(
      model::node_id(initial_size + 1), model::revision_id(0));
    co_await added_node.init_and_start({});

    // allow to recover only up to offset 20
    added_node.f_injectable_log()->set_append_delay([this, &added_node] {
        if (added_node.raft().get()->dirty_offset() >= model::offset{20}) {
            throw std::runtime_error("simulated failure");
        }
        vlog(logger().info, "new node offset is 20");
        return 0s;
    });

    // dispatch reconfiguration
    vlog(
      logger().info,
      "dispatching reconfiguration: {} -> {}",
      old_node_ids,
      all_ids());
    auto success = co_await retry_with_leader(
      model::timeout_clock::now() + 30s,
      [this](raft_node_instance& leader_node) {
          return leader_node.raft()
            ->replace_configuration(all_vnodes(), model::revision_id(0))
            .then([](std::error_code ec) {
                if (ec) {
                    return ::result<bool>(ec);
                }
                return ::result<bool>(true);
            });
      });
    ASSERT_TRUE_CORO(success);

    // wait until recovery stalls at 20
    model::offset target_offset{20};
    RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this, &added_node, target_offset] {
        auto recovered_offset = added_node.raft().get()->dirty_offset();
        vlog(
          logger().info,
          "Waiting for recovery, target_offset: {}, recovered_offset: {}",
          target_offset,
          recovered_offset);
        return recovered_offset >= target_offset;
    });

    // cleanly compact the log on original nodes
    co_await ss::parallel_for_each(
      old_node_ids, [this](model::node_id node_id) {
          auto& n = node(node_id);
          return run_compaction(n);
      });

    // make sure local mcco & mxfo on each compaction coordinator are updated
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      coordination_delay(), [this, old_node_ids, &last_data_offsets] {
          return std::ranges::all_of(
            old_node_ids, [this, &last_data_offsets](model::node_id node_id) {
                const auto& raft = node(node_id).raft();
                const auto& coco = raft->get_compaction_coordinator();
                auto mcco = coco.get_local_max_cleanly_compacted_offset();
                auto mxfo = coco.get_local_max_transaction_free_offset();
                vlog(
                  logger().info,
                  "on node {} max cleanly compacted offset: {}, max tx free "
                  "offset: {}, last_data_offsets[0]: {}",
                  node_id,
                  mcco,
                  mxfo,
                  last_data_offsets[0]);
                return mcco > last_data_offsets[0]
                       && mxfo >= last_data_offsets[0];
            });
      });

    // allow to fully recover
    added_node.f_injectable_log()->set_append_delay([] { return 0s; });

    // wait until recovery fully catches up
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      10s, [this, &last_data_offsets, &added_node] {
          auto recovered_offset = added_node.raft().get()->dirty_offset();
          vlog(
            logger().info,
            "Waiting for recovery, last_data_offset: {}, recovered_offset: "
            "{}",
            last_data_offsets.back(),
            recovered_offset);
          return recovered_offset >= last_data_offsets.back();
      });

    // make sure MTRO remains min because the new node is uncompacted and
    // has been inited from uncompacted log
    RPTEST_REQUIRE_EVENTUALLY_CORO(coordination_delay(), [this] {
        return check_group_offsets_on_all_nodes(
          [](model::offset mtro) { return mtro <= model::offset{0}; });
    });

    // run compaction on the new node
    co_await run_compaction(added_node);

    // make sure MTRO advanced to last_data_offset on all nodes
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      coordination_delay(), [this, &last_data_offsets] {
          return check_group_offsets_on_all_nodes(
            [&last_data_offsets](model::offset mtro) {
                return mtro >= last_data_offsets.back();
            });
      });
}

TEST_F_CORO(coco_fixture, test_leadership_change) {
    int initial_size = 3;
    co_await create_simple_group(initial_size);
    co_await transfer_leadership_to(model::node_id{1});

    // replicate some data
    chunked_vector<model::offset> last_data_offsets;
    co_await make_batches_and_replicate(4, 1, 10, 128, last_data_offsets);
    // last_data_offsets are about 10, 20, 30, but may be a bit off due to
    // non-data batches

    co_await run_compaction(node(model::node_id{0}), last_data_offsets[2]);
    co_await run_compaction(node(model::node_id{1}), last_data_offsets[1]);
    co_await run_compaction(node(model::node_id{2}), last_data_offsets[0]);

    // make sure MTRO advanced to last_data_offset on all nodes
    RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this, &last_data_offsets] {
        return check_group_offsets_on_all_nodes(
          [&last_data_offsets](model::offset mtro) {
              return mtro == model::next_offset(last_data_offsets[0]);
          });
    });

    // turn off periodical coordination
    config::shard_local_cfg().tombstone_retention_ms.set_value(86400s);

    co_await run_compaction(node(model::node_id{2}), last_data_offsets[1]);

    // turn on periodical coordination
    config::shard_local_cfg().tombstone_retention_ms.set_value(1s);

    co_await transfer_leadership_to(model::node_id{2});

    // make sure MTRO advanced to last_data_offset on all nodes
    RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this, &last_data_offsets] {
        return check_group_offsets_on_all_nodes(
          [&last_data_offsets](model::offset mtro) {
              return mtro == model::next_offset(last_data_offsets[1]);
          });
    });
}

TEST_F_CORO(coco_fixture, test_leadership_change_during_distribution) {
    int initial_size = 3;
    co_await create_simple_group(initial_size);
    co_await transfer_leadership_to(model::node_id{1});

    // replicate some data
    chunked_vector<model::offset> last_data_offsets;
    co_await make_batches_and_replicate(1, 1, 10, 128, last_data_offsets);

    co_await run_compaction(node(model::node_id{0}), last_data_offsets[0]);
    co_await run_compaction(node(model::node_id{1}), last_data_offsets[0]);
    co_await run_compaction(node(model::node_id{2}), last_data_offsets[0]);

    // make sure MTRO advanced to last_data_offset on the leader only
    RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this, &last_data_offsets] {
        return retry_with_leader(
          model::timeout_clock::now() + 10s,
          [this, &last_data_offsets](raft_node_instance& leader_node) {
              bool coordinated_on_leader = check_group_offsets_on_nodes(
                std::views::single(leader_node.get_vnode().id()),
                [&last_data_offsets](model::offset mtro) {
                    return mtro >= model::next_offset(last_data_offsets[0]);
                },
                [&last_data_offsets](model::offset mxro) {
                    return mxro >= model::next_offset(last_data_offsets[0]);
                });
              // and transfer leadership to another node before the old leader
              // distributes MTRO/MXRO knowledge
              if (coordinated_on_leader) {
                  return leader_node.raft()->step_down("test").then_wrapped(
                    [](auto) { return true; });
              }
              return ssx::now(false);
          });
    });

    // make sure MTRO advanced to last_data_offset on all nodes
    RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this, &last_data_offsets] {
        return check_group_offsets_on_all_nodes(
          [&last_data_offsets](model::offset mtro) {
              return mtro >= model::next_offset(last_data_offsets[0]);
          });
    });
}

TEST_F_CORO(coco_fixture, test_node_isolation) {
    int initial_size = 3;
    co_await create_simple_group(initial_size);

    // replicate some data
    chunked_vector<model::offset> last_data_offsets;
    co_await make_batches_and_replicate(4, 1, 10, 128, last_data_offsets);
    // last_data_offsets are about 10, 20, 30, but may be a bit off due to
    // non-data batches

    isolate_node(model::node_id{2});

    // compact
    for (auto& [id, node] : nodes()) {
        co_await run_compaction(*node, last_data_offsets[2]);
    }

    // make sure MTRO remains min because one of the nodes is isolated
    RPTEST_REQUIRE_EVENTUALLY_CORO(coordination_delay(), [this] {
        return check_group_offsets_on_all_nodes(
          [](model::offset mtro) { return mtro <= model::offset{0}; });
    });

    de_isolate_node(model::node_id{2});

    // make sure MTRO advanced to last_data_offset on all nodes
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      coordination_delay(), [this, &last_data_offsets] {
          return check_group_offsets_on_all_nodes(
            [&last_data_offsets](model::offset mtro) {
                return mtro == model::next_offset(last_data_offsets[2]);
            });
      });
}

TEST_F_CORO(coco_fixture, test_decommission) {
    int initial_size = 4;
    co_await create_simple_group(initial_size);
    co_await transfer_leadership_to(model::node_id{0});

    // replicate some data
    chunked_vector<model::offset> last_data_offsets;
    co_await make_batches_and_replicate(4, 1, 10, 128, last_data_offsets);

    // compact everywhere but the leader
    co_await run_compaction(node(model::node_id{1}), last_data_offsets[2]);
    co_await run_compaction(node(model::node_id{2}), last_data_offsets[2]);
    co_await run_compaction(node(model::node_id{3}), last_data_offsets[2]);

    // dispatch reconfiguration
    co_await drop_node(model::node_id{3});

    // compact the leader
    co_await run_compaction(node(model::node_id{0}), last_data_offsets[2]);

    // make sure MTRO advanced to last_data_offset on all nodes
    RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this, &last_data_offsets] {
        return check_group_offsets_on_all_nodes(
          [&last_data_offsets](model::offset mtro) {
              return mtro >= last_data_offsets[2];
          });
    });
}

TEST_F_CORO(coco_fixture, test_decommission2) {
    int initial_size = 4;
    co_await create_simple_group(initial_size);
    co_await transfer_leadership_to(model::node_id{0});

    // replicate some data
    chunked_vector<model::offset> last_data_offsets;
    co_await make_batches_and_replicate(4, 1, 10, 128, last_data_offsets);

    // compact everywhere but the victim
    co_await run_compaction(node(model::node_id{0}), last_data_offsets[2]);
    co_await run_compaction(node(model::node_id{1}), last_data_offsets[2]);
    co_await run_compaction(node(model::node_id{2}), last_data_offsets[2]);

    // victim node prevents tombstone removal
    RPTEST_REQUIRE_EVENTUALLY_CORO(coordination_delay(), [this] {
        return check_group_offsets_on_all_nodes(
          [](model::offset mtro) { return mtro <= model::offset{0}; });
    });

    // dispatch reconfiguration
    co_await drop_node(model::node_id{3});

    // make sure MTRO advanced to last_data_offset on all nodes
    RPTEST_REQUIRE_EVENTUALLY_CORO(
      coordination_delay(), [this, &last_data_offsets] {
          return check_group_offsets_on_all_nodes(
            [&last_data_offsets](model::offset mtro) {
                return mtro >= last_data_offsets[2];
            });
      });
}

TEST_F_CORO(coco_fixture, test_decommission_during_mtro_distribution) {
    int initial_size = 4;
    co_await create_simple_group(initial_size);
    co_await transfer_leadership_to(model::node_id{0});

    // replicate some data
    chunked_vector<model::offset> last_data_offsets;
    co_await make_batches_and_replicate(4, 1, 10, 128, last_data_offsets);

    // disallow mtro updates from the leader to one of the followers
    node(model::node_id{0})
      .on_dispatch([](model::node_id dest_id, raft::msg_type msg_type) {
          if (
            dest_id == model::node_id{3}
            && msg_type == raft::msg_type::distribute_compaction_mtro) {
              throw std::runtime_error("retriable injected error");
          }
          return ss::now();
      });

    // compact all nodes
    co_await ss::parallel_for_each(
      nodes() | std::views::values,
      [this](std::unique_ptr<raft_node_instance>& n) {
          return run_compaction(*n);
      });

    // dispatch reconfiguration
    co_await drop_node(model::node_id{3});

    // allow mtro updates again
    node(model::node_id{0}).reset_dispatch_handlers();

    // smoke test: make sure no exceptions are thrown and system is stable
    co_await ss::sleep(coordination_delay());
}

TEST_F_CORO(coco_fixture, unclean_compaction) {
    int initial_size = 3;
    co_await create_simple_group(initial_size);
    co_await transfer_leadership_to(model::node_id{0});

    // replicate some data
    chunked_vector<model::offset> last_data_offsets;
    co_await make_batches_and_replicate(2, 5, 10, 128, last_data_offsets);

    // only self-compact, so no clean compaction set
    for (auto& [id, node] : nodes()) {
        auto log = node->underlying_log();
        auto cfg = make_housekeeping_config(*node).compact;
        auto disk_log_ptr = dynamic_cast<storage::disk_log_impl*>(log.get());
        for (auto& seg :
             log->segments() | std::views::reverse | std::views::drop(1)) {
            co_await disk_log_ptr->segment_self_compact(cfg, seg);
        }
    }

    // make sure MTRO remains min because no clean compaction happened
    RPTEST_REQUIRE_EVENTUALLY_CORO(coordination_delay(), [this] {
        return check_group_offsets_on_nodes(
          all_ids(),
          [](model::offset mtro) { return mtro <= model::offset{0}; },
          [](model::offset mxro) { return mxro > model::offset{1}; });
    });
}
