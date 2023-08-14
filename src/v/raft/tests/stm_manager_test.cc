// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iostream.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/consensus.h"
#include "raft/group_configuration.h"
#include "raft/state_machine_manager.h"
#include "raft/tests/raft_fixture.h"
#include "raft/tests/raft_group_fixture.h"
#include "raft/types.h"
#include "random/generators.h"
#include "serde/envelope.h"
#include "serde/serde.h"
#include "storage/record_batch_builder.h"
#include "storage/types.h"
#include "test_utils/async.h"
#include "test_utils/randoms.h"
#include "test_utils/test.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/log.hh>

using namespace raft;
namespace {
static ss::logger logger("stm-test-logger");
/**
 * We use value entry struct to make kv_store apply operations not
 * idempotent
 **/
struct value_entry
  : serde::envelope<value_entry, serde::version<0>, serde::compat_version<0>> {
    value_entry() = default;

    explicit value_entry(ss::sstring v)
      : value(std::move(v)) {}

    ss::sstring value;
    size_t update_cnt{0};
    friend bool operator==(const value_entry&, const value_entry&) = default;

    auto serde_fields() { return std::tie(value, update_cnt); }
};
} // namespace

/**
 * Simple kv-store state machine
 */
struct simple_kv : public raft::state_machine_base {
    using state_t = absl::flat_hash_map<ss::sstring, value_entry>;
    explicit simple_kv(raft_node_instance& rn)
      : raft_node(rn) {}

    static void apply_to_state(
      const model::record_batch& batch,
      absl::flat_hash_map<ss::sstring, value_entry>& state) {
        if (batch.header().type != model::record_batch_type::raft_data) {
            return;
        }
        batch.for_each_record([&state](model::record r) {
            auto k = serde::from_iobuf<ss::sstring>(r.key().copy());
            auto v = serde::from_iobuf<std::optional<ss::sstring>>(
              r.value().copy());
            if (v) {
                auto [it, success] = state.try_emplace(k, *v);
                if (!success) {
                    it->second.value = std::move(*v);
                    it->second.update_cnt++;
                }
            } else {
                state.erase(k);
            }
        });
    }
    ss::future<> start() override { return ss::now(); }
    ss::future<> stop() override { co_await raft::state_machine_base::stop(); };

    ss::future<> apply(const model::record_batch& batch) override {
        apply_to_state(batch, state);
        co_return;
    }

    ss::future<> apply_raft_snapshot(const iobuf& buffer) override {
        state = serde::from_iobuf<state_t>(buffer.copy());
        co_return;
    };

    std::string_view get_name() const override { return "simple_kv"; };

    ss::future<iobuf>
    take_snapshot(model::offset last_included_offset) override {
        state_t inc_state;
        // build incremental snapshot
        auto snap = co_await raft_node.raft()->open_snapshot();
        auto start_offset = raft_node.raft()->start_offset();
        if (snap) {
            auto data = co_await read_iobuf_exactly(
              snap->reader.input(), co_await snap->reader.get_snapshot_size());
            inc_state = serde::from_iobuf<state_t>(std::move(data));
        }

        auto rdr = co_await raft_node.raft()->make_reader(
          storage::log_reader_config(
            start_offset, last_included_offset, ss::default_priority_class()));

        auto batches = co_await model::consume_reader_to_memory(
          std::move(rdr), model::no_timeout);

        std::for_each(
          batches.begin(),
          batches.end(),
          [&inc_state](const model::record_batch& b) {
              return simple_kv::apply_to_state(b, inc_state);
          });

        co_return serde::to_iobuf(std::move(inc_state));
    };

    state_t state;
    raft_node_instance& raft_node;
};

struct other_simple_kv : public simple_kv {
    explicit other_simple_kv(raft_node_instance& rn)
      : simple_kv(rn) {}
    std::string_view get_name() const override { return "other_simple_kv"; };
};

struct throwing_kv : public simple_kv {
    explicit throwing_kv(raft_node_instance& rn)
      : simple_kv(rn) {}

    std::string_view get_name() const override { return "throwing_kv"; };

    ss::future<> apply(const model::record_batch& batch) override {
        if (tests::random_bool()) {
            throw std::runtime_error("runtime error from throwing stm");
        }
        vassert(
          batch.base_offset() == next(),
          "batch {} base offset is not the next to apply, expected base "
          "offset: {}",
          batch.header(),
          next());
        co_await simple_kv::apply(batch);
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
    explicit local_snapshot_stm(raft_node_instance& rn)
      : simple_kv(rn) {}

    std::string_view get_name() const override { return "local_snapshot_stm"; };

    ss::future<> apply(const model::record_batch& batch) override {
        vassert(
          batch.base_offset() == next(),
          "batch {} base offset is not the next to apply, expected base "
          "offset: {}",
          batch.header(),
          next());
        co_await simple_kv::apply(batch);
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

struct state_machine_manager_fixture : raft_fixture {
    ss::future<result<raft::replicate_result>>
    upsert(ss::sstring k, ss::sstring v) {
        return replicate({std::make_pair(std::move(k), std::move(v))});
    }

    ss::future<result<raft::replicate_result>> remove(ss::sstring k) {
        return replicate({std::make_pair(std::move(k), std::nullopt)});
    }

    ss::future<result<raft::replicate_result>> replicate(
      std::vector<std::pair<ss::sstring, std::optional<ss::sstring>>> values) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));
        for (auto& [k, v] : values) {
            builder.add_raw_kv(serde::to_iobuf(k), serde::to_iobuf(v));
        }

        co_return co_await with_leader(
          5s,
          [b = std::move(builder).build()](
            raft_node_instance& leader_node) mutable {
              return leader_node.raft()->replicate(
                model::make_memory_record_batch_reader(std::move(b)),
                raft::replicate_options(raft::consistency_level::quorum_ack));
          });
    }

    ss::future<absl::flat_hash_map<ss::sstring, value_entry>>
    build_random_state(int op_cnt) {
        absl::flat_hash_map<ss::sstring, value_entry> state;

        for (int i = 0; i < op_cnt;) {
            const auto batch_sz = random_generators::get_int(1, 50);
            std::vector<std::pair<ss::sstring, std::optional<ss::sstring>>> ops;
            for (auto n = 0; n < batch_sz; ++n) {
                auto k = random_generators::gen_alphanum_string(10);
                auto v = random_generators::gen_alphanum_string(10);

                if (state.empty() || tests::random_bool()) {
                    // add
                    auto [it, success] = state.try_emplace(k, v);
                    if (!success) {
                        it->second.update_cnt++;
                    }
                    ops.emplace_back(k, v);
                } else {
                    auto idx = random_generators::get_int<size_t>(
                      0, state.size() - 1);
                    auto it = std::next(state.begin(), idx);

                    if (tests::random_bool()) {
                        // remove
                        ops.emplace_back(it->first, std::nullopt);
                        state.erase(it);
                    } else {
                        // replace
                        ops.emplace_back(it->first, v);
                        it->second.value = v;
                        it->second.update_cnt++;
                    }
                }
            }
            i += batch_sz;

            auto result = co_await replicate(std::move(ops));
            vlog(
              logger.debug,
              "replication result: [last_offset: {}]",
              result.value().last_offset);
        }
        co_return state;
    }

    ss::future<> wait_for_apply() {
        auto committed_offset = co_await with_leader(
          10s, [](raft_node_instance& node) {
              return node.raft()->committed_offset();
          });

        co_await parallel_for_each_node(
          [committed_offset](raft_node_instance& node) {
              return node.raft()->stm_manager()->wait(
                committed_offset, model::no_timeout);
          });
    }

    void create_nodes() {
        for (auto i = 0; i < 3; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }
    }
};

TEST_F_CORO(state_machine_manager_fixture, test_basic_apply) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto other_kv_stm = builder.create_stm<other_simple_kv>(*node);
        co_await node->start(all_vnodes(), std::move(builder));
        stms.push_back(kv_stm);
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(other_kv_stm));
    }

    auto expected = co_await build_random_state(1000);

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
}

TEST_F_CORO(state_machine_manager_fixture, test_apply_throwing_exception) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto throwing_kv_stm = builder.create_stm<throwing_kv>(*node);
        co_await node->start(all_vnodes(), std::move(builder));
        stms.push_back(kv_stm);
        stms.push_back(ss::dynamic_pointer_cast<simple_kv>(throwing_kv_stm));
    }

    auto expected = co_await build_random_state(1000);

    co_await wait_for_apply();

    for (auto& stm : stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
}

TEST_F_CORO(state_machine_manager_fixture, test_recovery_without_snapshot) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto throwing_kv_stm = builder.create_stm<throwing_kv>(*node);
        co_await node->start(all_vnodes(), std::move(builder));
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
    co_await new_node.start(all_vnodes(), std::move(builder));
    auto committed_offset = co_await with_leader(
      10s,
      [](raft_node_instance& node) { return node.raft()->committed_offset(); });

    co_await new_node.raft()->stm_manager()->wait(
      committed_offset, model::no_timeout);

    ASSERT_EQ_CORO(kv_stm->state, expected);
    ASSERT_EQ_CORO(throwing_kv_stm->state, expected);
}

TEST_F_CORO(state_machine_manager_fixture, test_recovery_from_snapshot) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<simple_kv>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        auto kv_stm = builder.create_stm<simple_kv>(*node);
        auto throwing_kv_stm = builder.create_stm<throwing_kv>(*node);
        co_await node->start(all_vnodes(), std::move(builder));
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
          .then([raft = n.raft(), snapshot_offset](iobuf snapshot_data) {
              return raft->write_snapshot(raft::write_snapshot_cfg(
                snapshot_offset, std::move(snapshot_data)));
          });
    });

    auto committed_offset = co_await with_leader(
      10s,
      [](raft_node_instance& node) { return node.raft()->committed_offset(); });

    auto& new_node = add_node(model::node_id(4), model::revision_id{0});
    raft::state_machine_manager_builder builder;
    auto kv_stm = builder.create_stm<simple_kv>(new_node);
    auto throwing_kv_stm = builder.create_stm<throwing_kv>(new_node);
    co_await new_node.start({}, std::move(builder));

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
  state_machine_manager_fixture,
  test_recovery_from_backward_compatible_snapshot) {
    /**
     * Create 3 replicas group with simple_kv STM
     */
    create_nodes();
    std::vector<ss::shared_ptr<local_snapshot_stm>> stms;

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder builder;
        stms.push_back(builder.create_stm<local_snapshot_stm>(*node));

        co_await node->start(all_vnodes(), std::move(builder));
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

    co_await new_node.start({}, std::move(builder));

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

    for (auto const& b : batches) {
        simple_kv::apply_to_state(b, partial_expected_state);
    }

    ASSERT_EQ_CORO(new_stm->state, partial_expected_state);
}
