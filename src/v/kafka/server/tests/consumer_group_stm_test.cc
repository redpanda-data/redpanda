// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "features/feature_table.h"
#include "kafka/server/consumer_group_stm.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/group_tx_tracker_stm.h"
#include "kafka/server/offset_store.h"
#include "model/fundamental.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/tests/raft_fixture.h"
#include "raft/tests/raft_fixture_retry_policy.h"
#include "raft/tests/stm_test_fixture.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <chrono>

using namespace std::chrono_literals;

namespace {

ss::logger cg_stm_test_logger("cg-stm-test");

const kafka::group_id test_group_id{"grp"};

using serialized_kv = kafka::group_metadata_serializer::key_value;

/// One raft_data batch holding the given records in order, the shape an
/// atomic write of several group records has on the log.
model::record_batch records_batch(std::vector<serialized_kv> kvs) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    for (auto& kv : kvs) {
        builder.add_raw_kv(std::move(kv.key), std::move(kv.value));
    }
    return std::move(builder).build();
}

serialized_kv group_kv(
  const kafka::group_id& group, int32_t epoch, int64_t metadata_hash = 0) {
    return kafka::group_metadata_serializer::to_kv(
      kafka::consumer_group_metadata_kv{
        .key = {.group_id = group},
        .value = kafka::consumer_group_metadata_value{
          .epoch = epoch, .metadata_hash = metadata_hash}});
}

serialized_kv group_tombstone_kv(const kafka::group_id& group) {
    return kafka::group_metadata_serializer::to_kv(
      kafka::consumer_group_metadata_kv{.key = {.group_id = group}});
}

serialized_kv member_kv(
  const kafka::group_id& group,
  const ss::sstring& member,
  const ss::sstring& topic) {
    chunked_vector<model::topic> topics;
    topics.emplace_back(topic);
    return kafka::group_metadata_serializer::to_kv(
      kafka::consumer_group_member_metadata_kv{
        .key = {.group_id = group, .member_id = kafka::member_id(member)},
        .value = kafka::consumer_group_member_metadata_value{
          .client_id = kafka::client_id("client"),
          .client_host = kafka::client_host("host"),
          .subscribed_topic_names = std::move(topics),
          .rebalance_timeout = 30s,
        }});
}

serialized_kv
member_tombstone_kv(const kafka::group_id& group, const ss::sstring& member) {
    return kafka::group_metadata_serializer::to_kv(
      kafka::consumer_group_member_metadata_kv{
        .key = {.group_id = group, .member_id = kafka::member_id(member)}});
}

serialized_kv
target_metadata_kv(const kafka::group_id& group, int32_t assignment_epoch) {
    return kafka::group_metadata_serializer::to_kv(
      kafka::consumer_group_target_assignment_metadata_kv{
        .key = {.group_id = group},
        .value = kafka::consumer_group_target_assignment_metadata_value{
          .assignment_epoch = assignment_epoch}});
}

serialized_kv target_metadata_tombstone_kv(const kafka::group_id& group) {
    return kafka::group_metadata_serializer::to_kv(
      kafka::consumer_group_target_assignment_metadata_kv{
        .key = {.group_id = group}});
}

serialized_kv target_member_kv(
  const kafka::group_id& group,
  const ss::sstring& member,
  model::topic_id topic,
  std::vector<int32_t> partitions) {
    kafka::target_assignment_topic_partitions tp;
    tp.topic_id = topic;
    for (auto p : partitions) {
        tp.partitions.emplace_back(p);
    }
    chunked_vector<kafka::target_assignment_topic_partitions> tps;
    tps.push_back(std::move(tp));
    return kafka::group_metadata_serializer::to_kv(
      kafka::consumer_group_target_assignment_member_kv{
        .key = {.group_id = group, .member_id = kafka::member_id(member)},
        .value = kafka::consumer_group_target_assignment_member_value{
          .topic_partitions = std::move(tps)}});
}

serialized_kv target_member_tombstone_kv(
  const kafka::group_id& group, const ss::sstring& member) {
    return kafka::group_metadata_serializer::to_kv(
      kafka::consumer_group_target_assignment_member_kv{
        .key = {.group_id = group, .member_id = kafka::member_id(member)}});
}

serialized_kv current_assignment_kv(
  const kafka::group_id& group,
  const ss::sstring& member,
  int32_t member_epoch,
  kafka::consumer_group_member_state state,
  model::topic_id topic,
  std::vector<int32_t> partitions) {
    kafka::current_assignment_topic_partitions tp;
    tp.topic_id = topic;
    for (auto p : partitions) {
        tp.partitions.emplace_back(p);
    }
    chunked_vector<kafka::current_assignment_topic_partitions> assigned;
    assigned.push_back(std::move(tp));
    return kafka::group_metadata_serializer::to_kv(
      kafka::consumer_group_current_member_assignment_kv{
        .key = {.group_id = group, .member_id = kafka::member_id(member)},
        .value = kafka::consumer_group_current_member_assignment_value{
          .member_epoch = member_epoch,
          .previous_member_epoch = member_epoch,
          .state = state,
          .assigned_partitions = std::move(assigned)}});
}

serialized_kv current_assignment_tombstone_kv(
  const kafka::group_id& group, const ss::sstring& member) {
    return kafka::group_metadata_serializer::to_kv(
      kafka::consumer_group_current_member_assignment_kv{
        .key = {.group_id = group, .member_id = kafka::member_id(member)}});
}

serialized_kv offset_kv(
  const kafka::group_id& group,
  const ss::sstring& topic,
  int32_t partition,
  int64_t offset) {
    return kafka::group_metadata_serializer::to_kv(
      kafka::offset_metadata_kv{
        .key
        = {.group_id = group, .topic = model::topic(topic), .partition = model::partition_id(partition)},
        .value = kafka::offset_metadata_value{
          .offset = model::offset(offset),
          .commit_timestamp = model::timestamp::now()}});
}

serialized_kv offset_tombstone_kv(
  const kafka::group_id& group, const ss::sstring& topic, int32_t partition) {
    return kafka::group_metadata_serializer::to_kv(
      kafka::offset_metadata_kv{
        .key = {
          .group_id = group,
          .topic = model::topic(topic),
          .partition = model::partition_id(partition)}});
}

serialized_kv classic_group_kv(const kafka::group_id& group) {
    return kafka::group_metadata_serializer::to_kv(
      kafka::group_metadata_kv{
        .key = {.group_id = group}, .value = kafka::group_metadata_value{}});
}

serialized_kv classic_group_tombstone_kv(const kafka::group_id& group) {
    return kafka::group_metadata_serializer::to_kv(
      kafka::group_metadata_kv{.key = {.group_id = group}});
}

template<typename Cmd>
model::record_batch tx_batch(
  model::record_batch_type type,
  int8_t version,
  const model::producer_identity& pid,
  Cmd cmd) {
    iobuf key;
    reflection::serialize(key, type, pid.id);

    iobuf value;
    reflection::serialize(value, version);
    reflection::serialize(value, std::move(cmd));

    storage::record_batch_builder builder(type, model::offset(0));
    builder.set_producer_identity(pid.id, pid.epoch);
    builder.set_control_type();
    builder.add_raw_kv(std::move(key), std::move(value));
    return std::move(builder).build();
}

model::record_batch fence_batch(
  const kafka::group_id& group,
  const model::producer_identity& pid,
  model::tx_seq seq) {
    return tx_batch(
      model::record_batch_type::group_fence_tx,
      kafka::offset_store::fence_control_record_version,
      pid,
      kafka::group_tx::fence_metadata{
        .group_id = group,
        .tx_seq = seq,
        .transaction_timeout_ms = 30s,
        .tm_partition = model::partition_id(0)});
}

model::record_batch prepare_batch(
  const kafka::group_id& group,
  const model::producer_identity& pid,
  model::tx_seq seq,
  const ss::sstring& topic,
  int32_t partition,
  int64_t offset) {
    return tx_batch(
      model::record_batch_type::group_prepare_tx,
      kafka::offset_store::prepared_tx_record_version,
      pid,
      kafka::group_tx::offsets_metadata{
        .group_id = group,
        .pid = pid,
        .tx_seq = seq,
        .offsets = {kafka::group_tx::partition_offset{
          .tp = model::topic_partition(
            model::topic(topic), model::partition_id(partition)),
          .offset = model::offset(offset),
          .leader_epoch = 0}}});
}

model::record_batch
block_batch(const kafka::group_id& group, bool is_blocked, int64_t revision) {
    storage::record_batch_builder builder(
      model::record_batch_type::group_block, model::offset(0));
    kafka::group_block{
      group,
      kafka::group_block_info{
        .is_blocked = is_blocked, .revision_id = model::revision_id(revision)}}
      .add_to_batch_builder(builder);
    return std::move(builder).build();
}

model::record_batch commit_batch(
  const kafka::group_id& group, const model::producer_identity& pid) {
    return tx_batch(
      model::record_batch_type::group_commit_tx,
      kafka::offset_store::commit_tx_record_version,
      pid,
      kafka::group_tx::commit_metadata{.group_id = group});
}

model::record_batch abort_batch(
  const kafka::group_id& group,
  const model::producer_identity& pid,
  model::tx_seq seq) {
    return tx_batch(
      model::record_batch_type::group_abort_tx,
      kafka::offset_store::aborted_tx_record_version,
      pid,
      kafka::group_tx::abort_metadata{.group_id = group, .tx_seq = seq});
}

} // namespace

struct consumer_group_stm_fixture : state_machine_fixture {
    ss::future<> start_cluster() {
        co_await _features.start();
        co_await _features.invoke_on_all(
          [](features::feature_table& f) { f.testing_activate_all(); });
        create_nodes();
        co_await build_state_machines();
    }

    ss::future<> build_state_machines() {
        for (auto& [_, node] : nodes()) {
            co_await node->initialise(all_vnodes());
            raft::state_machine_manager_builder builder;
            auto stm = builder.create_stm<kafka::consumer_group_stm>(
              cg_stm_test_logger, node->raft().get(), _features);
            auto tracker = builder.create_stm<kafka::group_tx_tracker_stm>(
              cg_stm_test_logger, node->raft().get(), _features);
            co_await node->start(std::move(builder));
            _stms[node->get_vnode()] = std::move(stm);
            _trackers[node->get_vnode()] = std::move(tracker);
        }
    }

    /// Stop every node and start it again over the same data directory, so
    /// the state machines rebuild by replaying the log.
    ss::future<> restart_cluster() {
        _stms.clear();
        _trackers.clear();
        absl::flat_hash_map<model::node_id, ss::sstring> data_directories;
        for (auto& [id, node] : nodes()) {
            data_directories[id]
              = node->raft()->log()->config().base_directory();
        }
        for (auto& [id, data_dir] : data_directories) {
            co_await stop_node(id);
            add_node(id, model::revision_id(0), std::move(data_dir));
        }
        co_await build_state_machines();
    }

    ss::future<> TearDownAsync() override {
        co_await raft_fixture::TearDownAsync();
        co_await _features.stop();
    }

    ss::future<model::offset> replicate(model::record_batch batch) {
        auto result = co_await retry_with_leader(
          model::timeout_clock::now() + 30s,
          [b = std::move(batch)](raft_node_instance& leader) mutable {
              return leader.raft()->replicate(
                b.share(),
                raft::replicate_options(raft::consistency_level::quorum_ack));
          });
        vassert(result, "replication failed: {}", result.error().message());
        co_return result.value().last_offset;
    }

    ss::future<model::offset>
    replicate_records(std::vector<serialized_kv> kvs) {
        return replicate(records_batch(std::move(kvs)));
    }

    template<typename... KVs>
    ss::future<model::offset> replicate_records(KVs... kvs) {
        std::vector<serialized_kv> v;
        v.reserve(sizeof...(kvs));
        (v.push_back(std::move(kvs)), ...);
        return replicate(records_batch(std::move(v)));
    }

    ss::future<ss::shared_ptr<kafka::consumer_group_stm>> leader_stm() {
        auto vn = co_await with_leader(
          10s, [](raft_node_instance& n) { return n.get_vnode(); });
        co_return _stms[vn];
    }

    ss::sharded<features::feature_table> _features;
    absl::flat_hash_map<raft::vnode, ss::shared_ptr<kafka::consumer_group_stm>>
      _stms;
    absl::
      flat_hash_map<raft::vnode, ss::shared_ptr<kafka::group_tx_tracker_stm>>
        _trackers;
};

TEST_F_CORO(consumer_group_stm_fixture, records_build_the_group_state) {
    co_await start_cluster();
    const auto topic = model::topic_id(uuid_t::create());

    co_await replicate_records(
      member_kv(test_group_id, "m1", "t"),
      group_kv(test_group_id, 2, 42),
      target_metadata_kv(test_group_id, 2),
      target_member_kv(test_group_id, "m1", topic, {0, 1}),
      current_assignment_kv(
        test_group_id,
        "m1",
        2,
        kafka::consumer_group_member_state::stable,
        topic,
        {0, 1}));
    co_await wait_for_apply();

    // the same state on every replica
    for (auto& [_, stm] : _stms) {
        auto group = stm->get_group(test_group_id);
        ASSERT_TRUE_CORO(group != nullptr);
        ASSERT_EQ_CORO(group->epoch(), kafka::group_epoch(2));
        ASSERT_EQ_CORO(group->metadata_hash(), 42);
        ASSERT_EQ_CORO(group->assignment_epoch(), kafka::assignment_epoch(2));

        const auto& member = group->members().at(kafka::member_id("m1"));
        ASSERT_EQ_CORO(member.subscription.topics.size(), 1);
        ASSERT_EQ_CORO(member.subscription.topics[0], model::topic("t"));
        ASSERT_EQ_CORO(member.assignment.epoch, kafka::member_epoch(2));
        ASSERT_EQ_CORO(member.assignment.assigned.at(topic).size(), 2);

        ASSERT_EQ_CORO(
          group->target_assignment().at(kafka::member_id("m1")).size(), 1);
        ASSERT_EQ_CORO(group->state(), kafka::consumer_group_state::stable);
    }
}

TEST_F_CORO(consumer_group_stm_fixture, a_deletion_batch_drops_the_group) {
    co_await start_cluster();
    const auto topic = model::topic_id(uuid_t::create());

    co_await replicate_records(
      member_kv(test_group_id, "m1", "t"),
      group_kv(test_group_id, 1),
      target_metadata_kv(test_group_id, 1),
      target_member_kv(test_group_id, "m1", topic, {0}),
      current_assignment_kv(
        test_group_id,
        "m1",
        1,
        kafka::consumer_group_member_state::stable,
        topic,
        {0}),
      offset_kv(test_group_id, "t", 0, 42));

    // tombstones in the fixed deletion order: the member's records, the
    // offsets, the target, and the group anchor last
    auto deletion = std::vector<serialized_kv>{};
    deletion.push_back(current_assignment_tombstone_kv(test_group_id, "m1"));
    deletion.push_back(target_member_tombstone_kv(test_group_id, "m1"));
    deletion.push_back(member_tombstone_kv(test_group_id, "m1"));
    deletion.push_back(offset_tombstone_kv(test_group_id, "t", 0));
    deletion.push_back(target_metadata_tombstone_kv(test_group_id));
    deletion.push_back(group_tombstone_kv(test_group_id));
    co_await replicate_records(std::move(deletion));
    co_await wait_for_apply();

    for (auto& [_, stm] : _stms) {
        ASSERT_TRUE_CORO(stm->get_group(test_group_id) == nullptr);
    }

    // a tombstone for state the machine has no record of is a no-op
    auto again = std::vector<serialized_kv>{};
    again.push_back(current_assignment_tombstone_kv(test_group_id, "m1"));
    again.push_back(member_tombstone_kv(test_group_id, "m1"));
    again.push_back(group_tombstone_kv(test_group_id));
    co_await replicate_records(std::move(again));
    co_await wait_for_apply();

    for (auto& [_, stm] : _stms) {
        ASSERT_TRUE_CORO(stm->get_group(test_group_id) == nullptr);
    }
}

TEST_F_CORO(consumer_group_stm_fixture, offsets_apply_latest_wins) {
    co_await start_cluster();
    const auto tp = model::topic_partition(
      model::topic("t"), model::partition_id(0));

    co_await replicate_records(group_kv(test_group_id, 1));
    co_await replicate_records(offset_kv(test_group_id, "t", 0, 42));
    co_await replicate_records(offset_kv(test_group_id, "t", 0, 43));
    co_await wait_for_apply();

    for (auto& [_, stm] : _stms) {
        auto group = stm->get_group(test_group_id);
        ASSERT_TRUE_CORO(group != nullptr);
        auto offset = group->offsets().offset(tp);
        ASSERT_TRUE_CORO(offset.has_value());
        ASSERT_EQ_CORO(offset->offset, model::offset(43));
    }

    co_await replicate_records(offset_tombstone_kv(test_group_id, "t", 0));
    co_await wait_for_apply();

    for (auto& [_, stm] : _stms) {
        auto group = stm->get_group(test_group_id);
        ASSERT_FALSE_CORO(group->offsets().offset(tp).has_value());
    }
}

TEST_F_CORO(consumer_group_stm_fixture, an_offset_does_not_create_a_group) {
    co_await start_cluster();
    const auto tp = model::topic_partition(
      model::topic("t"), model::partition_id(0));

    co_await replicate_records(offset_kv(test_group_id, "t", 0, 42));
    co_await wait_for_apply();

    // the offsets are held whatever protocol owns the id, but they do not
    // make a group of it
    for (auto& [_, stm] : _stms) {
        ASSERT_TRUE_CORO(stm->get_group(test_group_id) == nullptr);
        auto offsets = stm->get_offsets(test_group_id);
        ASSERT_TRUE_CORO(offsets != nullptr);
        ASSERT_EQ_CORO(offsets->offset(tp)->offset, model::offset(42));
    }

    // the last offset of an id no group holds leaves nothing behind
    co_await replicate_records(offset_tombstone_kv(test_group_id, "t", 0));
    co_await wait_for_apply();

    for (auto& [_, stm] : _stms) {
        ASSERT_TRUE_CORO(stm->get_offsets(test_group_id) == nullptr);
    }
}

TEST_F_CORO(
  consumer_group_stm_fixture, a_group_created_after_its_offsets_finds_them) {
    co_await start_cluster();
    const auto tp = model::topic_partition(
      model::topic("t"), model::partition_id(0));

    // The order compaction leaves behind: a group's metadata record is
    // rewritten on every epoch bump, so it survives above offsets committed
    // before the bump, and replay reaches those offsets before the group
    // exists. Discarding them would lose committed offsets on every restart.
    co_await replicate_records(offset_kv(test_group_id, "t", 0, 42));
    co_await replicate_records(group_kv(test_group_id, 7));
    co_await wait_for_apply();

    for (auto& [_, stm] : _stms) {
        auto group = stm->get_group(test_group_id);
        ASSERT_TRUE_CORO(group != nullptr);
        ASSERT_EQ_CORO(group->epoch(), kafka::group_epoch(7));
        auto offset = group->offsets().offset(tp);
        ASSERT_TRUE_CORO(offset.has_value());
        ASSERT_EQ_CORO(offset->offset, model::offset(42));
    }
}

TEST_F_CORO(consumer_group_stm_fixture, a_transaction_commits_its_offsets) {
    co_await start_cluster();
    const auto pid = model::producer_identity{7, 0};
    const auto tp = model::topic_partition(
      model::topic("t"), model::partition_id(0));

    co_await replicate_records(group_kv(test_group_id, 1));
    auto fence_offset = co_await replicate(
      fence_batch(test_group_id, pid, model::tx_seq(1)));
    co_await wait_for_apply();

    // the open transaction pins compaction below its fence
    auto stm = co_await leader_stm();
    ASSERT_EQ_CORO(
      stm->max_removable_local_log_offset(), model::prev_offset(fence_offset));

    co_await replicate(
      prepare_batch(test_group_id, pid, model::tx_seq(1), "t", 0, 42));
    co_await wait_for_apply();

    // staged, not committed
    ASSERT_FALSE_CORO(
      stm->get_group(test_group_id)->offsets().offset(tp).has_value());

    co_await replicate(commit_batch(test_group_id, pid));
    co_await wait_for_apply();

    for (auto& [_, node_stm] : _stms) {
        auto group = node_stm->get_group(test_group_id);
        auto offset = group->offsets().offset(tp);
        ASSERT_TRUE_CORO(offset.has_value());
        ASSERT_EQ_CORO(offset->offset, model::offset(42));
        ASSERT_FALSE_CORO(group->offsets().has_transactions_in_progress());
        // nothing pins compaction anymore
        ASSERT_EQ_CORO(
          node_stm->max_removable_local_log_offset(), node_stm->last_applied());
    }
}

TEST_F_CORO(consumer_group_stm_fixture, an_abort_discards_the_staged_offsets) {
    co_await start_cluster();
    const auto pid = model::producer_identity{7, 0};
    const auto tp = model::topic_partition(
      model::topic("t"), model::partition_id(0));

    co_await replicate_records(group_kv(test_group_id, 1));
    co_await replicate(fence_batch(test_group_id, pid, model::tx_seq(1)));
    co_await replicate(
      prepare_batch(test_group_id, pid, model::tx_seq(1), "t", 0, 42));
    co_await replicate(abort_batch(test_group_id, pid, model::tx_seq(1)));
    co_await wait_for_apply();

    for (auto& [_, stm] : _stms) {
        auto group = stm->get_group(test_group_id);
        ASSERT_FALSE_CORO(group->offsets().offset(tp).has_value());
        ASSERT_FALSE_CORO(group->offsets().has_transactions_in_progress());
        ASSERT_EQ_CORO(
          stm->max_removable_local_log_offset(), stm->last_applied());
    }
}

TEST_F_CORO(
  consumer_group_stm_fixture, a_tombstone_drops_a_group_with_an_open_tx) {
    co_await start_cluster();
    const auto pid = model::producer_identity{7, 0};

    co_await replicate_records(group_kv(test_group_id, 1));
    co_await replicate(fence_batch(test_group_id, pid, model::tx_seq(1)));
    co_await replicate_records(group_tombstone_kv(test_group_id));
    co_await wait_for_apply();

    // the writer is what checks a group is deletable; applied state follows
    // the log, so the group is gone and its transaction pins nothing
    for (auto& [_, stm] : _stms) {
        ASSERT_TRUE_CORO(stm->get_group(test_group_id) == nullptr);
        ASSERT_EQ_CORO(
          stm->max_removable_local_log_offset(), stm->last_applied());
    }
}

TEST_F_CORO(consumer_group_stm_fixture, a_recreated_group_starts_clean) {
    co_await start_cluster();
    const auto tp = model::topic_partition(
      model::topic("t"), model::partition_id(0));

    co_await replicate_records(
      group_kv(test_group_id, 1), member_kv(test_group_id, "m-1", "t"));
    co_await replicate_records(offset_kv(test_group_id, "t", 0, 42));
    co_await replicate_records(group_tombstone_kv(test_group_id));
    co_await replicate_records(group_kv(test_group_id, 5));
    co_await wait_for_apply();

    // The create after the tombstone is a new group, not the dropped one:
    // nothing of the old membership survives it. Its offsets do, because
    // nothing tombstoned them, which is also what makes them survive the
    // conversion of an empty group.
    for (auto& [_, stm] : _stms) {
        auto group = stm->get_group(test_group_id);
        ASSERT_TRUE_CORO(group != nullptr);
        ASSERT_EQ_CORO(group->epoch(), kafka::group_epoch(5));
        ASSERT_TRUE_CORO(group->members().empty());
        ASSERT_EQ_CORO(group->offsets().offset(tp)->offset, model::offset(42));
    }
}

TEST_F_CORO(consumer_group_stm_fixture, a_block_freezes_the_group) {
    co_await start_cluster();

    co_await replicate_records(group_kv(test_group_id, 1));
    co_await replicate(block_batch(test_group_id, false, 5));
    // stale: an older revision than the one applied, so it is not a block
    co_await replicate(block_batch(test_group_id, true, 3));
    // invalid: the same revision with the block flag flipped
    co_await replicate(block_batch(test_group_id, true, 5));
    co_await replicate_records(group_kv(test_group_id, 2));
    co_await wait_for_apply();

    for (auto& [_, stm] : _stms) {
        auto group = stm->get_group(test_group_id);
        ASSERT_TRUE_CORO(group != nullptr);
        ASSERT_EQ_CORO(group->epoch(), kafka::group_epoch(2));
    }

    // a block stops the group's records from applying, and the group itself
    // is kept: unblocking replays nothing, so dropping it would leave a group
    // whose whole record set is on the log unrecoverable
    co_await replicate(block_batch(test_group_id, true, 6));
    co_await replicate_records(group_kv(test_group_id, 3));
    co_await wait_for_apply();

    for (auto& [_, stm] : _stms) {
        auto group = stm->get_group(test_group_id);
        ASSERT_TRUE_CORO(group != nullptr);
        ASSERT_EQ_CORO(group->epoch(), kafka::group_epoch(2));
    }

    co_await replicate(block_batch(test_group_id, false, 7));
    co_await replicate_records(group_kv(test_group_id, 4));
    co_await wait_for_apply();

    for (auto& [_, stm] : _stms) {
        auto group = stm->get_group(test_group_id);
        ASSERT_TRUE_CORO(group != nullptr);
        ASSERT_EQ_CORO(group->epoch(), kafka::group_epoch(4));
    }
}

TEST_F_CORO(consumer_group_stm_fixture, an_undecodable_record_is_skipped) {
    co_await start_cluster();
    const auto tp = model::topic_partition(
      model::topic("t"), model::partition_id(0));

    co_await replicate_records(group_kv(test_group_id, 1));

    // a key version no decoder knows, in the same batch as a record that must
    // still apply: a record this machine cannot read is one it does not own,
    // and a throw would stall apply for every group on the partition
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    iobuf unknown_key;
    unknown_key.append("\x00\x63", 2);
    builder.add_raw_kv(std::move(unknown_key), iobuf());
    auto kv = offset_kv(test_group_id, "t", 0, 42);
    builder.add_raw_kv(std::move(kv.key), std::move(kv.value));
    co_await replicate(std::move(builder).build());
    co_await wait_for_apply();

    for (auto& [_, stm] : _stms) {
        auto offset = stm->get_group(test_group_id)->offsets().offset(tp);
        ASSERT_TRUE_CORO(offset.has_value());
        ASSERT_EQ_CORO(offset->offset, model::offset(42));
    }
}

TEST_F_CORO(consumer_group_stm_fixture, an_applied_offset_is_reclaimable) {
    co_await start_cluster();
    const auto tp = model::topic_partition(
      model::topic("t"), model::partition_id(0));

    co_await replicate_records(group_kv(test_group_id, 1));
    co_await replicate_records(offset_kv(test_group_id, "t", 0, 42));
    co_await wait_for_apply();

    // the record carries no non_reclaimable field, so reading one would
    // exempt every applied offset from retention
    for (auto& [_, stm] : _stms) {
        auto offset = stm->get_group(test_group_id)->offsets().offset(tp);
        ASSERT_TRUE_CORO(offset.has_value());
        ASSERT_FALSE_CORO(offset->non_reclaimable);
    }
}

TEST_F_CORO(consumer_group_stm_fixture, an_upgrade_batch_creates_the_group) {
    co_await start_cluster();

    // the empty-group conversion shape: the classic group's tombstone and the
    // consumer group's create in one atomic batch
    co_await replicate_records(
      classic_group_tombstone_kv(test_group_id), group_kv(test_group_id, 0));
    co_await wait_for_apply();

    for (auto& [_, stm] : _stms) {
        ASSERT_TRUE_CORO(stm->get_group(test_group_id) != nullptr);
    }
}

TEST_F_CORO(consumer_group_stm_fixture, a_downgrade_batch_drops_the_group) {
    co_await start_cluster();
    const auto tp = model::topic_partition(
      model::topic("t"), model::partition_id(0));

    co_await replicate_records(group_kv(test_group_id, 1));
    co_await replicate_records(offset_kv(test_group_id, "t", 0, 42));
    co_await wait_for_apply();

    auto leader = co_await leader_stm();
    ASSERT_TRUE_CORO(
      leader->get_group(test_group_id)->offsets().offset(tp).has_value());

    // committed offsets do not block the conversion; they stay for the
    // classic coordinator to recover, which reads them from the log
    co_await replicate_records(
      group_tombstone_kv(test_group_id), classic_group_kv(test_group_id));
    co_await wait_for_apply();

    for (auto& [_, stm] : _stms) {
        ASSERT_TRUE_CORO(stm->get_group(test_group_id) == nullptr);
        ASSERT_EQ_CORO(
          stm->get_offsets(test_group_id)->offset(tp)->offset,
          model::offset(42));
    }
}

TEST_F_CORO(consumer_group_stm_fixture, a_member_tombstone_clears_its_target) {
    co_await start_cluster();
    const auto topic = model::topic_id(uuid_t::create());

    co_await replicate_records(
      group_kv(test_group_id, 1),
      member_kv(test_group_id, "m-1", "t"),
      target_member_kv(test_group_id, "m-1", topic, {0, 1}));
    co_await replicate_records(member_tombstone_kv(test_group_id, "m-1"));
    co_await wait_for_apply();

    // a target outliving its member would be assigned to nobody
    for (auto& [_, stm] : _stms) {
        auto group = stm->get_group(test_group_id);
        ASSERT_TRUE_CORO(group->members().empty());
        ASSERT_TRUE_CORO(group->target_assignment().empty());
    }
}

TEST_F_CORO(consumer_group_stm_fixture, a_restart_replays_to_the_same_state) {
    co_await start_cluster();
    const auto pid = model::producer_identity{7, 0};
    const auto topic = model::topic_id(uuid_t::create());
    const auto tp = model::topic_partition(
      model::topic("t"), model::partition_id(0));

    co_await replicate_records(
      member_kv(test_group_id, "m1", "t"),
      group_kv(test_group_id, 2, 42),
      target_metadata_kv(test_group_id, 2),
      target_member_kv(test_group_id, "m1", topic, {0}),
      current_assignment_kv(
        test_group_id,
        "m1",
        2,
        kafka::consumer_group_member_state::stable,
        topic,
        {0}));
    co_await replicate_records(offset_kv(test_group_id, "t", 0, 42));
    auto fence_offset = co_await replicate(
      fence_batch(test_group_id, pid, model::tx_seq(1)));
    co_await replicate(
      prepare_batch(test_group_id, pid, model::tx_seq(1), "t", 0, 43));

    co_await restart_cluster();
    co_await wait_for_apply();

    for (auto& [_, stm] : _stms) {
        auto group = stm->get_group(test_group_id);
        ASSERT_TRUE_CORO(group != nullptr);
        ASSERT_EQ_CORO(group->epoch(), kafka::group_epoch(2));
        ASSERT_EQ_CORO(group->metadata_hash(), 42);
        ASSERT_EQ_CORO(group->assignment_epoch(), kafka::assignment_epoch(2));
        ASSERT_EQ_CORO(group->members().size(), 1);
        ASSERT_EQ_CORO(group->target_assignment().size(), 1);
        ASSERT_EQ_CORO(group->state(), kafka::consumer_group_state::stable);

        auto offset = group->offsets().offset(tp);
        ASSERT_TRUE_CORO(offset.has_value());
        ASSERT_EQ_CORO(offset->offset, model::offset(42));

        // the open transaction is re-established, still pinning compaction
        ASSERT_TRUE_CORO(group->offsets().has_transactions_in_progress());
        ASSERT_EQ_CORO(
          stm->max_removable_local_log_offset(),
          model::prev_offset(fence_offset));
    }
}

TEST_F_CORO(
  consumer_group_stm_fixture, a_compacted_log_shape_rebuilds_the_state) {
    co_await start_cluster();
    const kafka::group_id full{"grp-full"};
    const kafka::group_id compacted{"grp-compacted"};

    // the full history: an epoch bump, a subscription change, a superseded
    // offset, and a member that joined and left
    co_await replicate_records(member_kv(full, "m1", "old"), group_kv(full, 1));
    co_await replicate_records(offset_kv(full, "t", 0, 42));
    co_await replicate_records(member_kv(full, "m2", "x"), group_kv(full, 2));
    co_await replicate_records(member_kv(full, "m1", "new"));
    co_await replicate_records(offset_kv(full, "t", 0, 43));
    co_await replicate_records(
      member_tombstone_kv(full, "m2"), group_kv(full, 3));

    // What dedup compaction leaves of it: the latest record per key, each
    // where it was written, with the removed member's create and tombstone
    // both gone. The group's metadata record was rewritten last, so it lands
    // above the offset that was committed before it.
    co_await replicate_records(member_kv(compacted, "m1", "new"));
    co_await replicate_records(offset_kv(compacted, "t", 0, 43));
    co_await replicate_records(group_kv(compacted, 3));
    co_await wait_for_apply();

    for (auto& [_, stm] : _stms) {
        auto a = stm->get_group(full);
        auto b = stm->get_group(compacted);
        ASSERT_TRUE_CORO(a != nullptr);
        ASSERT_TRUE_CORO(b != nullptr);
        ASSERT_EQ_CORO(a->epoch(), b->epoch());
        ASSERT_EQ_CORO(a->members().size(), b->members().size());
        const auto& ma = a->members().at(kafka::member_id("m1"));
        const auto& mb = b->members().at(kafka::member_id("m1"));
        ASSERT_EQ_CORO(ma.subscription.topics[0], mb.subscription.topics[0]);
        const auto tp = model::topic_partition(
          model::topic("t"), model::partition_id(0));
        ASSERT_EQ_CORO(
          a->offsets().offset(tp)->offset, b->offsets().offset(tp)->offset);
    }
}

TEST_F_CORO(
  consumer_group_stm_fixture, the_state_machines_split_the_shared_log) {
    co_await start_cluster();
    const kafka::group_id classic{"grp-classic"};
    const auto classic_pid = model::producer_identity{7, 0};
    const auto consumer_pid = model::producer_identity{8, 0};

    co_await replicate_records(classic_group_kv(classic));
    co_await replicate_records(group_kv(test_group_id, 1));
    co_await replicate(fence_batch(classic, classic_pid, model::tx_seq(1)));
    co_await replicate(
      fence_batch(test_group_id, consumer_pid, model::tx_seq(1)));
    co_await wait_for_apply();

    for (auto& [vn, stm] : _stms) {
        // the consumer machine owns its group's transaction and ignores the
        // classic group entirely
        ASSERT_TRUE_CORO(stm->get_group(classic) == nullptr);
        auto group = stm->get_group(test_group_id);
        ASSERT_TRUE_CORO(group != nullptr);
        ASSERT_TRUE_CORO(group->offsets().has_transactions_in_progress());

        // the transaction tracker owns the classic group's and never sees a
        // consumer group
        const auto& tracked = _trackers[vn]->inflight_transactions();
        ASSERT_TRUE_CORO(tracked.contains(classic));
        ASSERT_FALSE_CORO(tracked.contains(test_group_id));
        ASSERT_FALSE_CORO(tracked.at(classic).producer_states.empty());
    }
}

TEST_F_CORO(consumer_group_stm_fixture, sync_gates_reads_to_the_leader) {
    co_await start_cluster();
    co_await replicate_records(group_kv(test_group_id, 1));

    auto leader_vn = co_await with_leader(
      10s, [](raft_node_instance& n) { return n.get_vnode(); });

    ASSERT_TRUE_CORO(co_await _stms[leader_vn]->sync(10s));
    ASSERT_TRUE_CORO(_stms[leader_vn]->get_group(test_group_id) != nullptr);

    for (auto& [vn, stm] : _stms) {
        if (vn != leader_vn) {
            ASSERT_FALSE_CORO(co_await stm->sync(1s));
        }
    }
}
