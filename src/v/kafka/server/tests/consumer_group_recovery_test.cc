// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "container/chunked_hash_map.h"
#include "kafka/protocol/types.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/group_recovery_consumer.h"
#include "kafka/server/group_stm.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "storage/record_batch_builder.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/node_hash_map.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <tuple>

using namespace std::chrono_literals;
using namespace kafka;
using namespace std::chrono_literals;

struct cg_recovery_test_fixture : seastar_test {
    ss::future<group_recovery_consumer_state>
    recover_from_batches(ss::circular_buffer<model::record_batch> batches) {
        group_recovery_consumer consumer(
          make_consumer_offsets_serializer(), as);

        return model::make_memory_record_batch_reader(std::move(batches))
          .consume(std::move(consumer), model::no_timeout);
    }
    template<typename T>
    model::record_batch serialize(T metadata) {
        storage::record_batch_builder buider(
          model::record_batch_type::raft_data, offset++);

        auto kv = serializer.to_kv(std::move(metadata));
        buider.add_raw_kv(std::move(kv.key), std::move(kv.value));

        return std::move(buider).build();
    }

    template<typename T>
    model::record_batch serialize(std::vector<T> metadata) {
        storage::record_batch_builder buider(
          model::record_batch_type::raft_data, offset++);
        for (auto& m : metadata) {
            auto kv = serializer.to_kv(std::move(m));
            buider.add_raw_kv(std::move(kv.key), std::move(kv.value));
        }

        return std::move(buider).build();
    }

    template<typename... Args>
    ss::circular_buffer<model::record_batch> serialize_metadata(Args... args) {
        ss::circular_buffer<model::record_batch> batches;

        (batches.push_back(serialize(std::move(args))), ...);
        return batches;
    }

    group_metadata_kv make_group_metadata(
      std::string_view g_name,
      generation_id g_id,
      protocol_type p_type,
      std::optional<protocol_name> p_name,
      std::optional<member_id> leader,
      model::timestamp ts,
      std::vector<member_state> members) {
        group_metadata_kv kv;
        kv.key = group_metadata_key{group_id(g_name)};
        kv.value = group_metadata_value{
          .protocol_type = std::move(p_type),
          .generation = g_id,
          .protocol = std::move(p_name),
          .leader = std::move(leader),
          .state_timestamp = ts,
          .members = std::move(members),
        };

        return kv;
    }

    member_state make_member_state(
      member_id id,
      std::optional<group_instance_id> g_inst_id,
      client_id cid,
      client_host host,
      std::chrono::milliseconds rb_tout,
      std::chrono::milliseconds s_tout) {
        return member_state{
          .id = std::move(id),
          .instance_id = std::move(g_inst_id),
          .client_id = std::move(cid),
          .client_host = std::move(host),
          .rebalance_timeout = rb_tout,
          .session_timeout = s_tout,
          .subscription = iobuf{},
          .assignment = iobuf{},
        };
    }

    offset_metadata_kv make_offset_kv(
      std::string_view group,
      std::string_view topic,
      int partition_id,
      int64_t offset,
      kafka::leader_epoch epoch,
      ss::sstring metadata = "") {
        return offset_metadata_kv{
          .key
          = offset_metadata_key{.group_id = group_id(group), .topic = model::topic(topic), .partition = model::partition_id(partition_id),},
          .value = offset_metadata_value{
            .offset = model::offset(offset),
            .leader_epoch = epoch,
            .metadata = std::move(metadata)}};
    }

    group_metadata_serializer serializer = make_consumer_offsets_serializer();
    ss::abort_source as;
    model::offset offset{0};

    std::pair<model::topic_partition, model::offset>
    parse_offset(std::string_view str) const {
        std::vector<std::string_view> splits = absl::StrSplit(str, '/');
        if (splits.size() != 2) {
            throw std::runtime_error(
              ss::format("invalid committed_offset: {}", str));
        }
        std::pair<model::topic_partition, model::offset> ret;
        ret.first.topic = model::topic(splits[0]);

        splits = absl::StrSplit(splits[1], '@');
        if (splits.size() != 2) {
            throw std::runtime_error(
              ss::format("invalid committed_offset: {}", str));
        }
        std::string_view partition = splits[0];
        std::string_view offset = splits[1];
        bool valid = true;
        valid &= absl::SimpleAtoi(partition, &ret.first.partition);
        valid &= absl::SimpleAtoi(offset, &ret.second);

        if (!valid) {
            throw std::runtime_error(
              ss::format("invalid committed_offset: {}", str));
        }
        return ret;
    }
    /**
     * Simple method to validate if committed offset for particular topic and
     * and partition has an expected value, the list of expected commit values
     * is expressed as a strings of format: <topic>/partition@offset
     */
    template<typename... Args>
    void expect_committed_offsets(
      const chunked_hash_map<
        model::topic_partition,
        group_stm::logged_metadata>& metadata,
      Args... offset_desc) {
        absl::node_hash_map<model::topic_partition, model::offset> expected_set;
        (expected_set.emplace(parse_offset(offset_desc)), ...);

        for (auto [tp, offset] : expected_set) {
            auto it = metadata.find(tp);
            ASSERT_TRUE(it != metadata.end());
            EXPECT_EQ(it->second.metadata.offset, offset);
        }
    }

    template<typename T>
    model::record_batch make_tx_batch(
      model::record_batch_type type,
      int8_t version,
      const model::producer_identity& pid,
      T cmd) {
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

    model::record_batch make_tx_fence_batch(
      const model::producer_identity& pid, group_tx::fence_metadata cmd) {
        return make_tx_batch(
          model::record_batch_type::tx_fence,
          group::fence_control_record_version,
          pid,
          std::move(cmd));
    }

    group_tx::partition_offset
    make_tx_offset(std::string_view topic, int partition, int offset) {
        return group_tx::partition_offset{
          .tp = model::topic_partition(
            model::topic(topic), model::partition_id(partition)),
          .offset = model::offset(offset),
        };
    }

    ss::circular_buffer<model::record_batch>
    copy_batches(const ss::circular_buffer<model::record_batch>& orig) {
        ss::circular_buffer<model::record_batch> ret;
        ret.reserve(orig.size());
        std::transform(
          orig.begin(),
          orig.end(),
          std::back_inserter(ret),
          [](const model::record_batch& b) { return b.copy(); });
        return ret;
    }
    /**
     * Returns batches representing metadata of single group with id `g-1` one
     * member and two committed offsets for test-1/0 at 1024 and for test-2/1 at
     * 256.
     */
    std::tuple<group_metadata_kv, ss::circular_buffer<model::record_batch>>
    serialize_single_group() {
        kafka::group_id gr_1("g-1");
        auto g_1_metadata = make_group_metadata(
          gr_1(),
          generation_id(0),
          protocol_type("proto"),
          protocol_name("proto-name"),
          member_id("member-1"),
          model::timestamp::now(),
          std::vector<member_state>{});

        g_1_metadata.value->members.push_back(make_member_state(
          member_id("m-1"),
          std::nullopt,
          client_id("cid-m1"),
          client_host("127.0.0.1"),
          60s,
          30s));
        auto o_md = make_offset_kv(gr_1(), "test-1", 0, 1024, leader_epoch(0));
        auto o_md_2 = make_offset_kv(
          gr_1(), "test-2", 10, 256, leader_epoch(0));

        return std::make_tuple(
          g_1_metadata.copy(),
          serialize_metadata(
            g_1_metadata.copy(),
            std::vector<offset_metadata_kv>{o_md, o_md_2}));
    }
};

TEST_F_CORO(cg_recovery_test_fixture, test_single_group_recovery) {
    auto [meta, batches] = serialize_single_group();
    auto state = co_await recover_from_batches(std::move(batches));
    const auto gr = meta.key.group_id;
    EXPECT_EQ(state.groups.size(), 1);
    EXPECT_EQ(state.groups[gr].get_metadata(), meta.value);
    EXPECT_EQ(state.groups[gr].offsets().size(), 2);
    expect_committed_offsets(
      state.groups[gr].offsets(), "test-1/0@1024", "test-2/10@256");
}

TEST_F_CORO(cg_recovery_test_fixture, test_tombstone_recovery) {
    auto g_1_metadata = make_group_metadata(
      "g-1",
      generation_id(0),
      protocol_type("proto"),
      protocol_name("proto-name"),
      member_id("member-1"),
      model::timestamp::now(),
      std::vector<member_state>{});

    auto g_2_metadata = make_group_metadata(
      "g-2",
      generation_id(10),
      protocol_type("proto-2"),
      protocol_name("proto-name-2"),
      member_id("member-1"),
      model::timestamp::now(),
      std::vector<member_state>{});

    g_2_metadata.value->members.push_back(make_member_state(
      member_id("m-1"),
      std::nullopt,
      client_id("cid-m1"),
      client_host("127.0.0.1"),
      60s,
      30s));
    auto o_md = make_offset_kv("g-1", "test-1", 0, 1024, leader_epoch(0));
    auto o_md_2 = make_offset_kv("g-1", "test-2", 10, 256, leader_epoch(0));
    auto o_md_3 = make_offset_kv("g-2", "test-20", 2, 123, leader_epoch(0));
    auto o_md_4 = make_offset_kv("g-2", "test-123", 10, 45, leader_epoch(0));

    auto batches = serialize_metadata(
      g_1_metadata.copy(),
      std::vector<offset_metadata_kv>{o_md, o_md_2},
      g_2_metadata.copy(),
      std::vector<offset_metadata_kv>{o_md_3},
      std::vector<offset_metadata_kv>{o_md_4});

    auto state = co_await recover_from_batches(std::move(batches));
    const auto gr_1 = g_1_metadata.key.group_id;
    const auto gr_2 = g_2_metadata.key.group_id;
    EXPECT_EQ(state.groups.size(), 2);
    EXPECT_EQ(state.groups[gr_1].get_metadata(), g_1_metadata.value);
    expect_committed_offsets(
      state.groups[gr_1].offsets(), "test-1/0@1024", "test-2/10@256");

    expect_committed_offsets(
      state.groups[gr_2].offsets(), "test-20/2@123", "test-123/10@45");

    // add group 1 tombstone
    auto g_1_ts_kv = group_metadata_kv{
      .key = group_metadata_key(kafka::group_id("g-1"))};
    std::vector<group_metadata_kv> g_1_tombstones;
    g_1_tombstones.push_back(g_1_ts_kv.copy());

    // add offset tombstone for group 2 topic-20 partition 2
    auto g_2_o_ts = offset_metadata_kv{
      .key = offset_metadata_key{
        .group_id = kafka::group_id("g-2"),
        .topic = model::topic("t-20"),
        .partition = model::partition_id(2)}};

    auto batches_with_tombstones = serialize_metadata(
      g_1_metadata.copy(),
      std::vector<offset_metadata_kv>{o_md, o_md_2},
      g_2_metadata.copy(),
      std::vector<offset_metadata_kv>{o_md_3},
      std::vector<offset_metadata_kv>{o_md_4},
      g_1_ts_kv.copy(),
      g_2_o_ts);

    auto state_new = co_await recover_from_batches(
      std::move(batches_with_tombstones));
    EXPECT_EQ(state_new.groups.size(), 1);
    EXPECT_FALSE(state_new.groups.contains(gr_1));
    EXPECT_FALSE(state_new.groups[gr_2].offsets().contains(
      model::topic_partition(model::topic("t-20"), model::partition_id(2))));
}

TEST_F_CORO(cg_recovery_test_fixture, test_tx_happy_path) {
    /**
     * Create a single group
     */
    auto [meta, batches] = serialize_single_group();
    const auto gr_1 = meta.key.group_id;
    model::producer_identity pid(100, 0);
    model::tx_seq tx_seq(3);
    batches.push_back(make_tx_fence_batch(
      pid,
      group_tx::fence_metadata{
        .group_id = gr_1,
        .tx_seq = tx_seq,
        .transaction_timeout_ms = 10s,
        .tm_partition = model::partition_id(10),
      }));

    batches.push_back(make_tx_batch(
      model::record_batch_type::group_prepare_tx,
      0,
      pid,
      group_tx::offsets_metadata{
        .group_id = gr_1,
        .pid = pid,
        .tx_seq = tx_seq,
        .offsets = std::vector<group_tx::partition_offset>{
          make_tx_offset("test-1", 0, 2048),
          make_tx_offset("topic-3", 12, 1)}}));

    auto state = co_await recover_from_batches(copy_batches(batches));
    EXPECT_EQ(state.groups[gr_1].producers().size(), 1);
    // tx is ongoing offsets included in the transaction should not be
    // visible in state machine

    EXPECT_EQ(state.groups.size(), 1);
    expect_committed_offsets(
      state.groups[gr_1].offsets(), "test-1/0@1024", "test-2/10@256");

    batches.push_back(make_tx_batch(
      model::record_batch_type::group_commit_tx,
      0,
      pid,
      group_tx::commit_metadata{.group_id = gr_1}));

    state = co_await recover_from_batches(copy_batches(batches));

    EXPECT_EQ(state.groups[gr_1].producers().size(), 1);

    expect_committed_offsets(
      state.groups[gr_1].offsets(),
      "test-1/0@2048",
      "test-2/10@256",
      "topic-3/12@1");

    /**
     * try aborting already committed tx, this should fail
     */
    batches.push_back(make_tx_batch(
      model::record_batch_type::group_abort_tx,
      0,
      pid,
      group_tx::abort_metadata{.group_id = gr_1, .tx_seq = tx_seq}));

    state = co_await recover_from_batches(copy_batches(batches));
    expect_committed_offsets(
      state.groups[gr_1].offsets(),
      "test-1/0@2048",
      "test-2/10@256",
      "topic-3/12@1");

    EXPECT_EQ(state.groups[gr_1].producers().size(), 1);
    EXPECT_EQ(state.groups[gr_1].producers().begin()->second.tx, nullptr);
}

TEST_F_CORO(cg_recovery_test_fixture, test_tx_abort) {
    /**
     * Create a single group
     */
    auto [meta, batches] = serialize_single_group();
    const auto gr_1 = meta.key.group_id;
    model::producer_identity pid(100, 0);
    model::tx_seq tx_seq(3);
    // set fence
    batches.push_back(make_tx_fence_batch(
      pid,
      group_tx::fence_metadata{
        .group_id = gr_1,
        .tx_seq = tx_seq,
        .transaction_timeout_ms = 10s,
        .tm_partition = model::partition_id(10),
      }));
    // begin tx
    batches.push_back(make_tx_batch(
      model::record_batch_type::group_prepare_tx,
      0,
      pid,
      group_tx::offsets_metadata{
        .group_id = gr_1,
        .pid = pid,
        .tx_seq = tx_seq,
        .offsets = std::vector<group_tx::partition_offset>{
          make_tx_offset("test-1", 0, 2048),
          make_tx_offset("topic-3", 12, 1)}}));

    // abort tx
    batches.push_back(make_tx_batch(
      model::record_batch_type::group_abort_tx,
      0,
      pid,
      group_tx::abort_metadata{.group_id = gr_1, .tx_seq = tx_seq}));
    auto state = co_await recover_from_batches(copy_batches(batches));
    EXPECT_EQ(state.groups[gr_1].producers().size(), 1);
    EXPECT_EQ(state.groups[gr_1].producers().begin()->second.tx, nullptr);

    expect_committed_offsets(
      state.groups[gr_1].offsets(), "test-1/0@1024", "test-2/10@256");

    // commit of aborted tx should be ignored
    batches.push_back(make_tx_batch(
      model::record_batch_type::group_commit_tx,
      0,
      pid,
      group_tx::commit_metadata{.group_id = gr_1}));

    EXPECT_EQ(state.groups[gr_1].producers().size(), 1);
    EXPECT_EQ(state.groups[gr_1].producers().begin()->second.tx, nullptr);

    expect_committed_offsets(
      state.groups[gr_1].offsets(), "test-1/0@1024", "test-2/10@256");
}
