// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/tests/raft_fixture_retry_policy.h"
#include "raft/tests/stm_test_fixture.h"

using namespace raft;

inline ss::logger logger("stm-test-logger");
struct other_kv : simple_kv {
    using simple_kv::simple_kv;
    static constexpr std::string_view name = "other_simple_kv";
};
struct throwing_kv : public simple_kv {
    static constexpr std::string_view name = "throwing_kv";
    explicit throwing_kv(raft_node_instance& rn)
      : simple_kv(rn) {}

    ss::future<> apply(
      const model::record_batch& batch,
      const ssx::semaphore_units& units) override {
        if (tests::random_bool()) {
            throw std::runtime_error("runtime error from throwing stm");
        }
        vassert(
          batch.base_offset() == next(),
          "batch {} base offset is not the next to apply, expected base "
          "offset: {}",
          batch.header(),
          next());
        co_await simple_kv::apply(batch, units);
        co_return;
    }

    ss::future<> apply_raft_snapshot(const iobuf& buffer) override {
        if (!_tried_applying) {
            _tried_applying = true;
            throw std::runtime_error("Error from apply_snapshot...");
        }

        return simple_kv::apply_raft_snapshot(buffer);
    };

    bool _tried_applying = false;
};
/**
 * Local snapshot stm manages its own local snapshot.
 */
struct local_snapshot_stm : public simple_kv {
    static constexpr std::string_view name = "local_snapshot_kv";
    explicit local_snapshot_stm(raft_node_instance& rn)
      : simple_kv(rn) {}

    ss::future<> apply(
      const model::record_batch& batch,
      const ssx::semaphore_units& units) override {
        vassert(
          batch.base_offset() == next(),
          "batch {} base offset is not the next to apply, expected base "
          "offset: {}",
          batch.header(),
          next());
        co_await simple_kv::apply(batch, units);
    }

    ss::future<> apply_raft_snapshot(const iobuf& buffer) override {
        vassert(
          buffer.size_bytes() == 0,
          "Only empty buffer is expected to be applied to the local snapshot "
          "managing STM.");
        // reset state
        state = {};
        co_return;
    };
};

// State machine that induces lag from the tip of
// of the log
class slow_kv : public simple_kv {
public:
    static constexpr std::string_view name = "slow_kv";

    explicit slow_kv(raft_node_instance& rn)
      : simple_kv(rn) {}

    ss::future<> apply(
      const model::record_batch& batch,
      const ssx::semaphore_units& apply_units) override {
        co_await ss::sleep(5ms);
        co_return co_await simple_kv::apply(batch, apply_units);
    }

    ss::future<> apply_raft_snapshot(const iobuf&) override {
        return ss::now();
    }
};

// Fails the first apply, starts a background fiber and not lets the
// background apply fiber finish relative to slow_kv
class bg_only_kv : public slow_kv {
public:
    static constexpr std::string_view name = "bg_only_stm";

    explicit bg_only_kv(raft_node_instance& rn)
      : slow_kv(rn) {}

    ss::future<> apply(
      const model::record_batch& batch,
      const ssx::semaphore_units& apply_units) override {
        if (_first_apply) {
            _first_apply = false;
            throw std::runtime_error("induced failure");
        }
        co_await ss::sleep(5ms);
        co_return co_await slow_kv::apply(batch, apply_units);
    }

private:
    bool _first_apply = true;
};

TEST_F_CORO(state_machine_fixture, test_basic_apply) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto other_kv_stm = builder.create_stm<other_kv>(*node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
        stms.push_back(kv_stm);
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(other_kv_stm));
    }

    auto expected = co_await build_random_state(1000);

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
}

TEST_F_CORO(state_machine_fixture, test_snapshot_with_bg_fibers) {
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto slow_kv_stm = builder.create_stm<slow_kv>(*node);
        auto bg_kv_stm = builder.create_stm<bg_only_kv>(*node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(slow_kv_stm));
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(bg_kv_stm));
    }
    auto& leader_node = node(co_await wait_for_leader(10s));
    bool stop = false;
    auto write_sleep_f = ss::do_until(
      [&stop] { return stop; },
      [&] {
          return build_random_state(1000).discard_result().then(
            [] { return ss::sleep(3ms); });
      });

    auto truncate_sleep_f = ss::do_until(
      [&stop] { return stop; },
      [&] {
          return leader_node.raft()
            ->write_snapshot({leader_node.raft()->committed_offset(), iobuf{}})
            .then([] { return ss::sleep(3ms); });
      });

    co_await ss::sleep(10s);
    stop = true;
    co_await ss::when_all(
      std::move(write_sleep_f), std::move(truncate_sleep_f));
}

TEST_F_CORO(state_machine_fixture, test_apply_throwing_exception) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto throwing_kv_stm = builder.create_stm<throwing_kv>(*node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
        stms.push_back(kv_stm);
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(throwing_kv_stm));
    }

    auto expected = co_await build_random_state(1000);

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
}
TEST_F_CORO(
  state_machine_fixture, test_apply_throwing_exception_waiting_for_each_batch) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto throwing_kv_stm = builder.create_stm<throwing_kv>(*node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
        stms.push_back(kv_stm);
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(throwing_kv_stm));
    }

    auto expected = co_await build_random_state(5000, wait_for_each_batch::yes);

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
}

TEST_F_CORO(state_machine_fixture, test_recovery_without_snapshot) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto throwing_kv_stm = builder.create_stm<throwing_kv>(*node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
        stms.push_back(kv_stm);
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(throwing_kv_stm));
    }

    auto expected = co_await build_random_state(1000);

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
    // stop one of the nodes and remove data
    model::node_id stopped_id(1);
    co_await stop_node(stopped_id, remove_data_dir::yes);
    auto& new_node = add_node(stopped_id, model::revision_id{0});

    // start the node back up
    raft::state_machine_manager_builder builder;
    auto kv_stm = builder.create_stm<simple_kv>(new_node);
    auto throwing_kv_stm = builder.create_stm<throwing_kv>(new_node);
    co_await new_node.init_and_start(all_vnodes(), std::move(builder));
    auto committed_offset = co_await with_leader(
      10s,
      [](raft_node_instance& node) { return node.raft()->committed_offset(); });

    co_await new_node.raft()->stm_manager()->wait(
      committed_offset, model::no_timeout);

    ASSERT_EQ_CORO(kv_stm->state, expected);
    ASSERT_EQ_CORO(throwing_kv_stm->state, expected);
}

TEST_F_CORO(state_machine_fixture, test_recovery_from_snapshot) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto throwing_kv_stm = builder.create_stm<throwing_kv>(*node);
        co_await node->init_and_start(all_vnodes(), std::move(builder));
        stms.push_back(kv_stm);
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(throwing_kv_stm));
    }

    auto expected = co_await build_random_state(1000);

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
    // take snapshot at batch boundary
    auto snapshot_offset = co_await with_leader(
      10s, [](raft_node_instance& node) {
          auto committed = node.raft()->committed_offset();
          return node.raft()
            ->make_reader(storage::log_reader_config(
              node.raft()->start_offset(),
              model::offset(random_generators::get_int(
                node.raft()->start_offset()(), committed())),
              ss::default_priority_class()))
            .then([](auto rdr) {
                return model::consume_reader_to_memory(
                  std::move(rdr), model::no_timeout);
            })
            .then([](ss::circular_buffer<model::record_batch> batches) {
                return batches.back().last_offset();
            });
      });
    // take snapshots on all of the nodes
    co_await parallel_for_each_node([snapshot_offset](raft_node_instance& n) {
        return n.raft()
          ->stm_manager()
          ->take_snapshot(snapshot_offset)
          .then([raft = n.raft(), snapshot_offset](
                  state_machine_manager::snapshot_result snapshot_result) {
              return raft->write_snapshot(raft::write_snapshot_cfg(
                snapshot_offset, std::move(snapshot_result.data)));
          });
    });

    auto committed_offset = co_await with_leader(
      10s,
      [](raft_node_instance& node) { return node.raft()->committed_offset(); });

    auto& new_node = add_node(model::node_id(4), model::revision_id{0});
    raft::state_machine_manager_builder builder;
    auto kv_stm = builder.create_stm<simple_kv>(new_node);
    auto throwing_kv_stm = builder.create_stm<throwing_kv>(new_node);
    co_await new_node.init_and_start({}, std::move(builder));

    co_await with_leader(
      10s, [vn = new_node.get_vnode()](raft_node_instance& node) {
          return node.raft()->add_group_member(vn, model::revision_id{0});
      });

    co_await new_node.raft()->stm_manager()->wait(
      committed_offset, model::timeout_clock::now() + 20s);

    ASSERT_EQ_CORO(kv_stm->state, expected);
    ASSERT_EQ_CORO(throwing_kv_stm->state, expected);
}

TEST_F_CORO(
  state_machine_fixture, test_recovery_from_backward_compatible_snapshot) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<local_snapshot_stm>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        stms.push_back(builder.create_stm<local_snapshot_stm>(*node));

        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }

    auto expected = co_await build_random_state(1000);

    co_await wait_for_apply();
    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }

    // take snapshot at batch boundary
    auto snapshot_offset = co_await with_leader(
      10s, [](raft_node_instance& node) {
          auto committed = node.raft()->committed_offset();
          return node.raft()
            ->make_reader(storage::log_reader_config(
              node.raft()->start_offset(),
              model::offset(random_generators::get_int(
                node.raft()->start_offset()(), committed())),
              ss::default_priority_class()))
            .then([](auto rdr) {
                return model::consume_reader_to_memory(
                  std::move(rdr), model::no_timeout);
            })
            .then([](ss::circular_buffer<model::record_batch> batches) {
                return batches.back().last_offset();
            });
      });

    co_await parallel_for_each_node([snapshot_offset](raft_node_instance& n) {
        // create an empty snapshot, the same way Redpanda does in previous
        // versions

        return n.raft()->write_snapshot(
          write_snapshot_cfg(snapshot_offset, iobuf{}));
    });

    auto committed_offset = co_await with_leader(
      10s,
      [](raft_node_instance& node) { return node.raft()->committed_offset(); });

    auto& new_node = add_node(model::node_id(4), model::revision_id{0});
    raft::state_machine_manager_builder builder;
    auto new_stm = builder.create_stm<local_snapshot_stm>(new_node);

    co_await new_node.init_and_start({}, std::move(builder));

    co_await with_leader(
      10s, [vn = new_node.get_vnode()](raft_node_instance& node) {
          return node.raft()->add_group_member(vn, model::revision_id{0});
      });
    // wait for the state to be applied
    co_await new_node.raft()->stm_manager()->wait(
      committed_offset, model::timeout_clock::now() + 20s);

    simple_kv::state_t partial_expected_state;

    auto rdr = co_await new_node.raft()->make_reader(storage::log_reader_config(
      model::next_offset(snapshot_offset),
      committed_offset,
      ss::default_priority_class()));

    auto batches = co_await model::consume_reader_to_memory(
      std::move(rdr), model::no_timeout);

    for (const auto& b : batches) {
        simple_kv::apply_to_state(b, partial_expected_state);
    }

    ASSERT_EQ_CORO(new_stm->state, partial_expected_state);
}

struct controllable_throwing_kv : public simple_kv {
    static constexpr std::string_view name = "controllable_throwing_kv_1";
    explicit controllable_throwing_kv(raft_node_instance& rn)
      : simple_kv(rn) {}

    ss::future<> apply(
      const model::record_batch& batch,
      const ssx::semaphore_units& apply_units) override {
        if (batch.last_offset() > _allow_apply) {
            throw std::runtime_error(fmt::format(
              "not allowed to apply batches with last offset greater than {}. "
              "Current batch last offset: {}",
              _allow_apply,
              batch.last_offset()));
        }
        vassert(
          batch.base_offset() == next(),
          "batch {} base offset is not the next to apply, expected base "
          "offset: {}",
          batch.header(),
          next());
        co_await simple_kv::apply(batch, apply_units);
        co_return;
    }

    void allow_apply_to(model::offset o) { _allow_apply = o; }

    model::offset _allow_apply;
};

struct controllable_throwing_kv_2 : public controllable_throwing_kv {
    using controllable_throwing_kv::controllable_throwing_kv;

    static constexpr std::string_view name = "controllable_throwing_kv_2";
};

struct controllable_throwing_kv_3 : public controllable_throwing_kv {
    using controllable_throwing_kv::controllable_throwing_kv;

    static constexpr std::string_view name = "controllable_throwing_kv_3";
};
TEST_F_CORO(state_machine_fixture, test_all_machines_throw) {
    /**
     * This test covers the scenario in which all state machines thrown an
     * exception during apply, and then one of the state machines makes some
     * progress.
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_1 = builder.create_stm<controllable_throwing_kv>(*node);
        auto kv_2 = builder.create_stm<controllable_throwing_kv_2>(*node);
        auto kv_3 = builder.create_stm<controllable_throwing_kv_3>(*node);

        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(kv_1));
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(kv_2));
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(kv_3));

        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }
    for (auto& [id, node] : nodes()) {
        node->raft()
          ->stm_manager()
          ->get<controllable_throwing_kv>()
          ->allow_apply_to(model::offset(100));
        node->raft()
          ->stm_manager()
          ->get<controllable_throwing_kv_2>()
          ->allow_apply_to(model::offset(100));
        node->raft()
          ->stm_manager()
          ->get<controllable_throwing_kv_3>()
          ->allow_apply_to(model::offset(150));
    }
    vlog(logger().info, "Generating state for test");
    auto expected = co_await build_random_state(
      500, wait_for_each_batch::no, 1);
    vlog(logger().info, "Waiting for state machines");
    RPTEST_REQUIRE_EVENTUALLY_CORO(15s, [&] {
        return std::ranges::all_of(
          nodes() | std::views::values,
          [&](std::unique_ptr<raft_node_instance>& node) {
              auto la = node->raft()
                          ->stm_manager()
                          ->get<controllable_throwing_kv_3>()
                          ->last_applied_offset();
              return la >= model::offset(150);
          });
    });

    for (auto& [id, node] : nodes()) {
        node->raft()
          ->stm_manager()
          ->get<controllable_throwing_kv_2>()
          ->allow_apply_to(model::offset(160));
    }
    RPTEST_REQUIRE_EVENTUALLY_CORO(15s, [&] {
        return std::ranges::all_of(
          nodes() | std::views::values,
          [&](std::unique_ptr<raft_node_instance>& node) {
              auto la = node->raft()
                          ->stm_manager()
                          ->get<controllable_throwing_kv_2>()
                          ->last_applied_offset();
              return la >= model::offset(160);
          });
    });

    for (auto& [id, node] : nodes()) {
        node->raft()
          ->stm_manager()
          ->get<controllable_throwing_kv>()
          ->allow_apply_to(model::offset(1000));
        node->raft()
          ->stm_manager()
          ->get<controllable_throwing_kv_2>()
          ->allow_apply_to(model::offset(1000));
        node->raft()
          ->stm_manager()
          ->get<controllable_throwing_kv_3>()
          ->allow_apply_to(model::offset(1000));
    }

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
}

class non_fast_movable_kv
  : public simple_kv_base<no_at_offset_snapshot_stm_base> {
public:
    static constexpr std::string_view name = "other_persited_kv_stm";
    explicit non_fast_movable_kv(raft_node_instance& rn)
      : simple_kv_base<no_at_offset_snapshot_stm_base>(rn) {}

    ss::future<iobuf> take_snapshot() final {
        co_return serde::to_iobuf(state);
    }
};

TEST_F_CORO(state_machine_fixture, test_opt_out_from_snapshot_at_offset) {
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        builder.create_stm<simple_kv>(*node);
        builder.create_stm<non_fast_movable_kv>(*node);

        co_await node->init_and_start(all_vnodes(), std::move(builder));
    }

    for (auto& [_, node] : nodes()) {
        ASSERT_FALSE_CORO(
          node->raft()->stm_manager()->supports_snapshot_at_offset());
    }

    auto expected = co_await build_random_state(1000);

    // take snapshots on all of the nodes
    absl::flat_hash_map<model::node_id, model::offset> offsets;
    for (auto& [id, node] : nodes()) {
        auto o = co_await node->raft()->stm_manager()->take_snapshot().then(
          [raft = node->raft()](
            state_machine_manager::snapshot_result snapshot_data) {
              return raft
                ->write_snapshot(raft::write_snapshot_cfg(
                  snapshot_data.last_included_offset,
                  std::move(snapshot_data.data)))
                .then([o = snapshot_data.last_included_offset] { return o; });
          });
        offsets[id] = o;
    }

    for (const auto& [id, n] : nodes()) {
        ASSERT_EQ_CORO(
          n->raft()->start_offset(), model::next_offset(offsets[id]));
    }
}
