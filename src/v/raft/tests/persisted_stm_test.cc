// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/errc.h"
#include "raft/group_configuration.h"
#include "raft/persisted_stm.h"
#include "raft/tests/raft_fixture.h"
#include "raft/tests/raft_fixture_retry_policy.h"
#include "raft/tests/stm_test_fixture.h"
#include "raft/types.h"
#include "random/generators.h"
#include "serde/envelope.h"
#include "serde/rw/rw.h"
#include "test_utils/randoms.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

#include <absl/container/flat_hash_map.h>
#include <gtest/gtest.h>

using namespace raft;
inline ss::logger logger("stm-test-logger");

/**
 * Simple struct representing kv-store operation, when value is empty it is a
 * tombstone while when expected value is present it will work as an atomic
 * compare and swap operation
 */
struct kv_operation
  : serde::envelope<kv_operation, serde::version<0>, serde::compat_version<0>> {
    ss::sstring key;
    std::optional<ss::sstring> value;
    std::optional<ss::sstring> expected_value;

    friend bool operator==(const kv_operation&, const kv_operation&) = default;
    auto serde_fields() { return std::tie(key, value, expected_value); }
};

struct kv_state
  : serde::envelope<kv_state, serde::version<0>, serde::compat_version<0>> {
    using state_t = absl::flat_hash_map<ss::sstring, value_entry>;

    bool apply(const kv_operation& op) {
        if (op.expected_value) {
            // CAS
            return compare_and_swap(op.key, *op.expected_value, op.value);
        } else {
            if (op.value) {
                put(op.key, *op.value);
            } else {
                remove(op.key);
            }
            return true;
        }
    }
    bool validate(const kv_operation& op) {
        if (!op.expected_value) {
            return true;
        }
        auto it = kv_map.find(op.key);
        return it != kv_map.end() && it->second.value == op.expected_value;
    }

    void put(ss::sstring key, ss::sstring value) {
        auto [it, success] = kv_map.try_emplace(std::move(key), value);
        if (!success) {
            it->second.update_cnt++;
            it->second.value = value;
        }
    }

    void remove(const ss::sstring& key) { kv_map.erase(key); }

    bool compare_and_swap(
      const ss::sstring& key,
      const ss::sstring& expected_value,
      std::optional<ss::sstring> new_value) {
        auto it = kv_map.find(key);
        if (it == kv_map.end()) {
            return false;
        }

        if (it->second.value == expected_value) {
            if (new_value) {
                it->second.value = std::move(*new_value);
                it->second.update_cnt++;
            } else {
                kv_map.erase(it);
            }
            return true;
        }

        return false;
    }

    state_t kv_map;

    friend bool operator==(const kv_state&, const kv_state&) = default;
    friend std::ostream& operator<<(std::ostream& o, const kv_state& st) {
        for (auto& [k, v] : st.kv_map) {
            fmt::print(o, "{}={}, ", k, v);
        }
        return o;
    }

    auto serde_fields() { return std::tie(kv_map); }
};

class persisted_kv : public persisted_stm<> {
public:
    static constexpr std::string_view name = "persited_kv_stm";
    explicit persisted_kv(raft_node_instance& rn)
      : persisted_stm<>("simple-kv", logger, rn.raft().get())
      , raft_node(rn) {}

    ss::future<> start() override { return persisted_stm<>::start(); }
    ss::future<> stop() override { return persisted_stm<>::stop(); }

    ss::future<result<bool>> execute(kv_operation op) {
        auto synced = co_await sync(10s);
        if (!synced) {
            co_return errc::not_leader;
        }

        auto result = state.validate(op);
        if (!result) {
            co_return false;
        }

        auto r_result = co_await _raft->replicate(
          _insync_term,
          build_batch({std::move(op)}),
          replicate_options(consistency_level::quorum_ack));
        if (!r_result) {
            co_return r_result.error();
        }

        co_return co_await wait_no_throw(
          r_result.value().last_offset, model::timeout_clock::now() + 30s);
    }

    /**
     * Called when local snapshot is applied to the state machine
     */
    ss::future<>
    apply_local_snapshot(stm_snapshot_header header, iobuf&& buffer) final {
        state = serde::from_iobuf<kv_state>(std::move(buffer));
        co_return;
    };

    /**
     * Called when a local snapshot is taken
     */
    ss::future<stm_snapshot> take_local_snapshot(
      [[maybe_unused]] ssx::semaphore_units apply_units) final {
        co_return stm_snapshot::create(
          0, last_applied_offset(), serde::to_iobuf(state));
    };

    static std::optional<kv_operation>
    apply_to_state(const model::record_batch& batch, kv_state& state) {
        if (batch.header().type != model::record_batch_type::raft_data) {
            return std::nullopt;
        }
        kv_operation last_op;
        batch.for_each_record([&state, &last_op](model::record r) {
            auto op = serde::from_iobuf<kv_operation>(r.value().copy());
            /**
             * Here we check if validation pre replication is correct.
             **/
            if (op.expected_value) {
                auto it = state.kv_map.find(op.key);
                vassert(
                  it != state.kv_map.end(),
                  "CAS operation violation no entry with {} key",
                  op.key);
                vassert(
                  it->second.value == op.expected_value,
                  "CAS operation violation, state_value: {}, expected_value: "
                  "{}",
                  it->second.value,
                  op.expected_value);
                if (op.value) {
                    it->second.value = *op.value;
                    it->second.update_cnt++;
                } else {
                    state.kv_map.erase(it);
                }
            } else {
                if (op.value) {
                    state.put(op.key, *op.value);
                } else {
                    state.remove(op.key);
                }
            }
            last_op = op;
        });
        return last_op;
    }

    ss::future<> do_apply(const model::record_batch& batch) override {
        auto last_op = apply_to_state(batch, state);
        if (last_op) {
            last_operation = std::move(*last_op);
        }
        co_return;
    }

    ss::future<> apply_raft_snapshot(const iobuf& buffer) override {
        state = serde::from_iobuf<kv_state>(buffer.copy());
        co_return;
    };

    ss::future<iobuf>
    take_snapshot(model::offset last_included_offset) override {
        kv_state inc_state;
        // build incremental snapshot
        auto snap = co_await raft_node.raft()->open_snapshot();
        auto start_offset = raft_node.raft()->start_offset();
        if (snap) {
            auto data = co_await read_iobuf_exactly(
              snap->reader.input(), co_await snap->reader.get_snapshot_size());
            inc_state = serde::from_iobuf<kv_state>(std::move(data));
        }

        auto rdr = co_await raft_node.raft()->make_reader(
          storage::log_reader_config(
            start_offset, last_included_offset, ss::default_priority_class()));

        auto batches = co_await model::consume_reader_to_memory(
          std::move(rdr), default_timeout());

        std::for_each(
          batches.begin(), batches.end(), [&inc_state](model::record_batch& b) {
              if (b.header().type != model::record_batch_type::raft_data) {
                  return;
              }
              b.for_each_record([&inc_state](model::record r) {
                  inc_state.apply(
                    serde::from_iobuf<kv_operation>(r.release_value()));
                  return ss::stop_iteration::no;
              });
          });

        co_return serde::to_iobuf(std::move(inc_state));
    };

    model::record_batch_reader
    build_batch(std::vector<kv_operation> operations) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));
        for (auto& op : operations) {
            builder.add_raw_kv(iobuf{}, serde::to_iobuf(std::move(op)));
        }
        auto batch = std::move(builder).build();
        return model::make_memory_record_batch_reader({std::move(batch)});
    }

    kv_state state;
    kv_operation last_operation;
    raft_node_instance& raft_node;
};

class other_persisted_kv : public persisted_kv {
public:
    static constexpr std::string_view name = "other_persited_kv_stm";
    explicit other_persisted_kv(raft_node_instance& rn)
      : persisted_kv(rn) {}
    ss::future<> apply_raft_snapshot(const iobuf& buffer) override {
        if (buffer.empty()) {
            co_return;
        }
        state = serde::from_iobuf<kv_state>(buffer.copy());
        co_return;
    };
    /**
     * This STM doesn't execute the full apply logic from the base persisted_kv
     * as it is going to be started without the full data in the snapshot, hence
     * the validation would fail.
     */
    ss::future<> do_apply(const model::record_batch& batch) override {
        if (batch.header().type != model::record_batch_type::raft_data) {
            co_return;
        }
        batch.for_each_record([this](model::record r) {
            last_operation = serde::from_iobuf<kv_operation>(r.value().copy());
        });
        co_return;
    }
};

struct persisted_stm_test_fixture : state_machine_fixture {
    ss::future<> initialize_state_machines() {
        create_nodes();

        for (auto& [_, node] : nodes()) {
            co_await node->initialise(all_vnodes());
            raft::state_machine_manager_builder builder;
            auto stm = builder.create_stm<persisted_kv>(*node);
            co_await node->start(std::move(builder));
            node_stms.emplace(node->get_vnode(), std::move(stm));
        }
    }

    ss::future<>
    apply_operations(kv_state& expected, std::vector<kv_operation> ops) {
        for (auto& op : ops) {
            auto expected_res = expected.apply(op);

            auto result = co_await retry_with_leader(
              model::timeout_clock::now() + 30s,
              [this, op = std::move(op)](raft_node_instance& leader_node) {
                  auto stm = node_stms[leader_node.get_vnode()];
                  return stm->execute(op);
              });
            vassert(
              expected_res == result.value(), "values should be the same");
        }
    }

    std::vector<std::vector<kv_operation>> random_operations(int cnt) {
        absl::flat_hash_map<ss::sstring, ss::sstring> state;
        std::vector<std::vector<kv_operation>> ops;

        for (int i = 0; i < cnt;) {
            auto batch_size = random_generators::get_int(
              1, std::min(20, cnt - i));

            std::vector<kv_operation> batch;
            batch.reserve(batch_size);
            for (int n = 0; n < batch_size; ++n) {
                auto new_v = random_generators::gen_alphanum_string(16);

                if (state.size() > 0 && tests::random_bool()) {
                    auto idx = random_generators::get_int<size_t>(
                      0, state.size() - 1);

                    auto it = state.begin();
                    std::advance(it, idx);

                    // remove
                    if (tests::random_bool()) {
                        batch.push_back(kv_operation{.key = it->first});
                        state.erase(it->first);
                        continue;
                    }
                    // cas
                    if (tests::random_bool()) {
                        // success
                        batch.push_back(kv_operation{
                          .key = it->first,
                          .value = new_v,
                          .expected_value = it->second,
                        });
                        it->second = new_v;
                        continue;
                    }

                    // failure
                    batch.push_back(kv_operation{
                      .key = it->first,
                      .value = new_v,
                      .expected_value = new_v,
                    });

                } else {
                    auto new_k = random_generators::gen_alphanum_string(16);
                    state[new_k] = new_v;

                    batch.push_back(kv_operation{
                      .key = std::move(new_k), .value = std::move(new_v)});
                }
            }
            i += batch_size;
            ops.push_back(std::move(batch));
        }
        return ops;
    }

    ss::future<> restart_cluster() {
        absl::flat_hash_map<model::node_id, ss::sstring> data_directories;
        for (auto& [id, node] : nodes()) {
            data_directories[id]
              = node->raft()->log()->config().base_directory();
        }

        for (auto& [id, data_dir] : data_directories) {
            co_await stop_node(id);
            add_node(id, model::revision_id(0), std::move(data_dir));
        }

        for (auto& [_, node] : nodes()) {
            co_await node->initialise(all_vnodes());
            raft::state_machine_manager_builder builder;
            auto stm = builder.create_stm<persisted_kv>(*node);
            co_await node->start(std::move(builder));
            node_stms.emplace(node->get_vnode(), std::move(stm));
        }
    }

    ss::future<> take_raft_snapshot_all_nodes() {
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
                      std::move(rdr), default_timeout());
                })
                .then([](ss::circular_buffer<model::record_batch> batches) {
                    return batches.back().last_offset();
                });
          });

        // take snapshots on all of the nodes
        co_await parallel_for_each_node([snapshot_offset](
                                          raft_node_instance& n) {
            return n.raft()
              ->stm_manager()
              ->take_snapshot(snapshot_offset)
              .then([raft = n.raft(), snapshot_offset](
                      state_machine_manager::snapshot_result snapshot_result) {
                  return raft->write_snapshot(raft::write_snapshot_cfg(
                    snapshot_offset, std::move(snapshot_result.data)));
              });
        });
    }

    ss::future<> take_local_snapshot_on_every_node() {
        for (auto& stm : node_stms) {
            co_await stm.second->write_local_snapshot();
        }
    }

    absl::flat_hash_map<raft::vnode, ss::shared_ptr<persisted_kv>> node_stms;
};

class slow_persisted_stm : public persisted_stm<> {
public:
    static constexpr std::string_view name = "slow_persisted_stm";

    explicit slow_persisted_stm(raft_node_instance& rn)
      : persisted_stm<>("slow_persisted_stm", logger, rn.raft().get()) {}

    ss::future<> do_apply(const model::record_batch& batch) override {
        _last_stm_applied = batch.last_offset();
        co_return;
    }

    ss::future<> apply_raft_snapshot(const iobuf& buffer) override {
        co_return;
    };

    ss::future<iobuf>
    take_snapshot(model::offset last_included_offset) override {
        co_return iobuf{};
    }

    ss::future<>
    apply_local_snapshot(stm_snapshot_header header, iobuf&& buffer) override {
        _last_stm_applied = serde::from_iobuf<model::offset>(std::move(buffer));
        co_return;
    }

    ss::future<> validate_applied_offsets(model::offset before) {
        ASSERT_EQ_CORO(before, _last_stm_applied);
    }

    ss::future<> validate_snapshot_on_latest_offset() {
        ASSERT_EQ_CORO(_last_stm_applied, last_applied_offset());
    }

    ss::future<stm_snapshot> take_local_snapshot(
      [[maybe_unused]] ssx::semaphore_units apply_units) override {
        co_await validate_snapshot_on_latest_offset();
        auto applied_offset_before = _last_stm_applied;
        co_await validate_applied_offsets(applied_offset_before);
        // sleep a bit to ensure _last_applied_offset doesn't move
        // as we are holding the units.
        co_await ss::sleep(2ms);
        co_await validate_applied_offsets(applied_offset_before);
        co_return stm_snapshot::create(
          0, last_applied_offset(), serde::to_iobuf(_last_stm_applied));
    }

private:
    model::offset _last_stm_applied{};
};

TEST_F_CORO(persisted_stm_test_fixture, test_basic_operations) {
    co_await initialize_state_machines();
    std::vector<kv_operation> ops;
    ops.push_back(kv_operation{.key = "one", .value = "one-v"});
    ops.push_back(kv_operation{.key = "two", .value = "two-v"});
    ops.push_back(kv_operation{.key = "three", .value = "three-v"});
    ops.push_back(kv_operation{.key = "two"});
    /**
     * Should succeed
     */
    ops.push_back(kv_operation{
      .key = "one",
      .value = "one-v-updated",
      .expected_value = "one-v",
    });
    /**
     * Should fail
     */
    ops.push_back(kv_operation{
      .key = "one",
      .value = "one-v-updated-second",
      .expected_value = "one-v",
    });
    kv_state expected;
    co_await apply_operations(expected, std::move(ops));
    co_await wait_for_apply();
    for (const auto& [_, stm] : node_stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
}

TEST_F_CORO(persisted_stm_test_fixture, test_recovery_from_raft_snapshot) {
    co_await initialize_state_machines();
    kv_state expected;
    auto ops = random_operations(2000);
    for (auto batch : ops) {
        co_await apply_operations(expected, std::move(batch));
    }
    co_await wait_for_apply();
    for (const auto& [_, stm] : node_stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }

    // take snapshot at batch boundary
    co_await take_raft_snapshot_all_nodes();
    // restart cluster and wait for the committed offset to be updated and then
    // all the state applied
    auto committed = node(model::node_id(0)).raft()->committed_offset();
    co_await restart_cluster();
    co_await wait_for_committed_offset(committed, 30s);
    co_await wait_for_apply();

    for (const auto& [_, stm] : node_stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
}

TEST_F_CORO(persisted_stm_test_fixture, test_local_snapshot) {
    co_await initialize_state_machines();
    kv_state expected;
    auto ops = random_operations(2000);
    for (auto batch : ops) {
        co_await apply_operations(expected, std::move(batch));
    }
    co_await wait_for_apply();
    for (const auto& [_, stm] : node_stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }

    // take local snapshot on every node
    co_await take_local_snapshot_on_every_node();
    // update state
    auto ops_phase_two = random_operations(50);
    for (auto batch : ops_phase_two) {
        co_await apply_operations(expected, std::move(batch));
    }

    co_await wait_for_apply();
    for (const auto& [_, stm] : node_stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }

    auto committed = node(model::node_id(0)).raft()->committed_offset();
    co_await restart_cluster();
    co_await wait_for_committed_offset(committed, 30s);
    co_await wait_for_apply();

    for (const auto& [_, stm] : node_stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
}

TEST_F_CORO(persisted_stm_test_fixture, test_raft_and_local_snapshot) {
    co_await initialize_state_machines();
    kv_state expected;
    auto ops = random_operations(2000);
    for (auto batch : ops) {
        co_await apply_operations(expected, std::move(batch));
    }
    co_await wait_for_apply();
    for (const auto& [_, stm] : node_stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }

    // take local snapshot on every node
    co_await take_local_snapshot_on_every_node();
    // update state
    auto ops_phase_two = random_operations(50);
    for (auto batch : ops_phase_two) {
        co_await apply_operations(expected, std::move(batch));
    }

    co_await wait_for_apply();
    for (const auto& [_, stm] : node_stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }

    // take Raft snapshot on every node, there are two possibilities here either
    // a snapshot will be taken at offset preceding current local snapshot or
    // the one following local snapshot.
    co_await take_raft_snapshot_all_nodes();

    auto committed = node(model::node_id(0)).raft()->committed_offset();
    co_await restart_cluster();
    co_await wait_for_committed_offset(committed, 30s);
    co_await wait_for_apply();

    for (const auto& [_, stm] : node_stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }
}
/**
 * Tests the scenario in which an STM is added to the partition after it was
 * already alive and Raft snapshot was taken on the partition.
 *
 * The snapshot doesn't contain data for the newly created stm, however the stm
 * next offset should still be updated to make it possible for the STM to catch
 * up.
 */
TEST_F_CORO(persisted_stm_test_fixture, test_adding_state_machine) {
    co_await initialize_state_machines();
    kv_state expected;
    auto ops = random_operations(2000);
    for (auto batch : ops) {
        co_await apply_operations(expected, std::move(batch));
    }
    co_await wait_for_apply();
    for (const auto& [_, stm] : node_stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }

    // take local snapshot on every node
    co_await take_local_snapshot_on_every_node();
    // update state
    auto ops_phase_two = random_operations(50);
    for (auto batch : ops_phase_two) {
        co_await apply_operations(expected, std::move(batch));
    }

    co_await wait_for_apply();
    for (const auto& [_, stm] : node_stms) {
        ASSERT_EQ_CORO(stm->state, expected);
    }

    // take Raft snapshot on every node, there are two possibilities here either
    // a snapshot will be taken at offset preceding current local snapshot or
    // the one following local snapshot.
    co_await take_raft_snapshot_all_nodes();

    auto committed = node(model::node_id(0)).raft()->committed_offset();

    absl::flat_hash_map<model::node_id, ss::sstring> data_directories;
    for (auto& [id, node] : nodes()) {
        data_directories[id] = node->raft()->log()->config().base_directory();
    }

    for (auto& [id, data_dir] : data_directories) {
        co_await stop_node(id);
        add_node(id, model::revision_id(0), std::move(data_dir));
    }
    ss::shared_ptr<other_persisted_kv> other_stm;
    for (auto& [_, node] : nodes()) {
        co_await node->initialise(all_vnodes());
        raft::state_machine_manager_builder builder;
        auto stm = builder.create_stm<persisted_kv>(*node);
        other_stm = builder.create_stm<other_persisted_kv>(*node);
        co_await node->start(std::move(builder));
        node_stms.emplace(node->get_vnode(), std::move(stm));
    }

    co_await wait_for_committed_offset(committed, 30s);
    co_await wait_for_apply();

    for (const auto& [_, stm] : node_stms) {
        ASSERT_EQ_CORO(stm->state, expected);
        ASSERT_EQ_CORO(stm->last_operation, other_stm->last_operation);
    }
}

// Test ensures that the snapshot is not interleaved with apply
TEST_F_CORO(state_machine_fixture, test_concurrent_apply_and_snapshot) {
    // Initialize raft group and stms
    add_node(model::node_id(0), model::revision_id(0));
    auto& raft_nodes = nodes();
    ASSERT_EQ_CORO(raft_nodes.size(), 1);

    auto& node = raft_nodes.begin()->second;
    co_await node->initialise(all_vnodes());
    raft::state_machine_manager_builder builder;
    auto stm = builder.create_stm<slow_persisted_stm>(*node);
    co_await node->start(std::move(builder));
    co_await wait_for_leader(5s);

    bool stop = false;
    auto write_sleep_f = ss::do_until(
      [&stop] { return stop; },
      [&] {
          return build_random_state(100).discard_result().then(
            [] { return ss::sleep(3ms); });
      });

    auto local_snapshot_f = ss::do_until(
      [&stop] { return stop; }, [&] { return stm->write_local_snapshot(); });

    co_await ss::sleep(2s);
    stop = true;
    co_await std::move(write_sleep_f);
    co_await std::move(local_snapshot_f);
}
