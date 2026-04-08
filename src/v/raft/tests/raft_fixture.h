
/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "raft/tests/raft_fixture_base.h"
#include "test_utils/test.h"

namespace raft {

struct raft_fixture
  : public raft_fixture_base
  , public seastar_test {
    seastar::future<> TearDownAsync() override;
    seastar::future<> SetUpAsync() override;

    ss::future<> wait_for_committed_offset(
      model::offset offset, std::chrono::milliseconds timeout);

    ss::future<> wait_for_visible_offset(
      model::offset offset, std::chrono::milliseconds timeout);
};

template<class... STM>
struct stm_raft_fixture : raft_fixture {
    using stm_shptrs_t = std::tuple<ss::shared_ptr<STM>...>;

    ss::future<> initialize_state_machines() {
        return initialize_state_machines(3);
    }

    ss::future<> initialize_state_machines(int node_cnt) {
        for (auto i = 0; i < node_cnt; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }
        co_await start_nodes();
    }

    ss::future<> start_node(raft_node_instance& node) {
        co_await node.initialise(all_vnodes());
        raft::state_machine_manager_builder builder;
        stm_shptrs_t stm_shptrs = create_stms(builder, node);
        co_await node.start(std::move(builder));
        node_stms.emplace(node.get_vnode(), std::move(stm_shptrs));
    }

    ss::future<> start_nodes() {
        co_await parallel_for_each_node(
          [this](raft_node_instance& node) { return start_node(node); });
    }

    ss::future<> stop_and_recreate_nodes() {
        absl::flat_hash_map<model::node_id, ss::sstring> data_directories;
        for (auto& [id, node] : nodes()) {
            data_directories[id]
              = node->raft()->log()->config().base_directory();
            node_stms.erase(node->get_vnode());
        }

        co_await ss::parallel_for_each(
          std::views::keys(data_directories),
          [this](model::node_id id) { return stop_node(id); });

        for (auto& [id, data_dir] : data_directories) {
            add_node(id, model::revision_id(0), std::move(data_dir));
        }
    }

    // Restarts nodes and deletes their data directories
    // This is useful for tests that need to start with a clean state
    // f.e. for recovery testing.
    ss::future<> restart_node_and_delete_data(model::node_id id) {
        auto dir = node(id).raft()->log()->config().base_directory();
        node_stms.erase(node(id).get_vnode());

        co_await stop_node(id, remove_data_dir::yes);

        add_node(id, model::revision_id(0), std::move(dir));

        co_await start_node(node(id));
    }

    ss::future<> restart_nodes() {
        co_await stop_and_recreate_nodes();
        co_await start_nodes();
    }

    // returns ss::shared_ptr<stm type>
    template<int stm_id>
    auto get_stm(raft_node_instance& node) {
        return std::get<stm_id>(node_stms[node.get_vnode()]);
    }

    template<int stm_id, typename Func>
    auto stm_retry_with_leader(std::chrono::milliseconds timeout, Func&& f) {
        return retry_with_leader(
          model::timeout_clock::now() + timeout,
          [this,
           f = std::forward<Func>(f)](raft_node_instance& leader_node) mutable {
              auto stm = get_stm<stm_id>(leader_node);
              return f(stm);
          });
    }

    absl::flat_hash_map<raft::vnode, stm_shptrs_t> node_stms;

    virtual stm_shptrs_t create_stms(
      state_machine_manager_builder& builder, raft_node_instance& node) = 0;
};

std::ostream& operator<<(std::ostream& o, msg_type type);
} // namespace raft
