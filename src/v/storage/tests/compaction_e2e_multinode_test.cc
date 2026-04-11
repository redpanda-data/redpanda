// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "absl/container/btree_map.h"
#include "base/vlog.h"
#include "cluster/members_frontend.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "cluster/tests/tx_compaction_utils.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "storage/tests/manual_mixin.h"
#include "test_utils/async.h"
#include "test_utils/boost_fixture.h"
#include "test_utils/scoped_config.h"

using namespace tests;

namespace {
ss::logger testlog("cmp_multi_testlog");
} // namespace

class compaction_multinode_test
  : public storage_manual_mixin
  , public cluster_test_fixture {};

FIXTURE_TEST(replicate_after_compaction, compaction_multinode_test) {
    const model::topic topic{"mocha"};
    model::node_id id{0};
    auto* app = create_node_application(id);
    auto* rp = instance(id);
    wait_for_controller_leadership(id).get();

    cluster::topic_properties props;
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
    // Try to roll on every batch.
    rp->add_topic({model::kafka_namespace, topic}, 1, props).get();
    model::partition_id pid{0};
    auto ntp = model::ntp(model::kafka_namespace, topic, pid);
    RPTEST_REQUIRE_EVENTUALLY(5s, [&] {
        auto [_, prt] = get_leader(ntp);
        return prt != nullptr;
    });

    kafka_produce_transport producer(rp->make_kafka_client().get());
    producer.start().get();
    auto deferred_close = ss::defer([&producer] { producer.stop().get(); });

    // []: batch
    // {}: segment
    // Rough keys, excluding config batches:
    // {[0 1 2 3 4 5] [4 5] [4 5] [4 5] [4 5]}
    producer.produce_to_partition(topic, pid, kv_t::sequence(0, 5)).get();
    producer.produce_to_partition(topic, pid, kv_t::sequence(4, 2)).get();
    producer.produce_to_partition(topic, pid, kv_t::sequence(4, 2)).get();
    producer.produce_to_partition(topic, pid, kv_t::sequence(4, 2)).get();

    // Compact. The resulting segments will look roughly like:
    // {[0 1 2 3] [4 5]}
    auto [_, first_partition] = get_leader(ntp);
    auto first_log = first_partition->log();
    first_log->flush().get();
    first_log->force_roll().get();
    BOOST_REQUIRE_EQUAL(first_log->segment_count(), 2);
    ss::abort_source as;
    auto conf = storage::housekeeping_config::make_config(
      model::timestamp::min(),
      std::nullopt,
      first_log->stm_hookset()->max_removable_local_log_offset(),
      first_log->stm_hookset()->max_removable_local_log_offset(),
      first_log->stm_hookset()->max_removable_local_log_offset(),
      std::nullopt,
      std::nullopt,
      std::chrono::milliseconds{0},
      as);
    first_log->housekeeping(conf).get();

    // Update the segment size to be tiny, forcing our new replica to have a
    // different segment layout.
    auto update = cluster::topic_properties_update{
      {model::kafka_namespace, topic}};
    update.properties.segment_size.op
      = cluster::incremental_update_operation::set;
    update.properties.segment_size.value = 5;
    app->controller->get_topics_frontend()
      .local()
      .update_topic_properties({update}, model::timeout_clock().now() + 5s)
      .get();

    // Decommission the original node, forcing replication to a new replica.
    auto* app2 = create_node_application(model::node_id(1));
    wait_for_all_members(10s).get();
    auto err = app->controller->get_members_frontend()
                 .local()
                 .decommission_node(model::node_id(0))
                 .get();
    BOOST_REQUIRE(!err);
    RPTEST_REQUIRE_EVENTUALLY(60s, [&] {
        auto partition = app->partition_manager.local().get(ntp);
        return partition == nullptr;
    });
    auto [new_rp, new_partition] = get_leader(ntp);
    BOOST_REQUIRE(new_rp);
    BOOST_REQUIRE(app2 == &new_rp->app);

    // With the lower segment size, our segments will each only have one batch.
    // Write new records, to encourage removal of older batches.
    // {[0 1 2 3]} {[4 5]} {[0 1 2 3 4 5]}
    kafka_produce_transport new_producer(new_rp->make_kafka_client().get());
    new_producer.start().get();
    auto new_deferred_close = ss::defer(
      [&new_producer] { new_producer.stop().get(); });
    new_producer.produce_to_partition(topic, pid, kv_t::sequence(0, 5)).get();
    auto new_log = new_partition->log();
    new_log->flush().get();
    new_log->force_roll().get();

    // The segments should maintain their last records.
    // {[3]} {[5]} {[0 1 2 3 4 5]}
    auto conf2 = storage::housekeeping_config::make_config(
      model::timestamp::min(),
      std::nullopt,
      new_log->stm_hookset()->max_removable_local_log_offset(),
      new_log->stm_hookset()->max_removable_local_log_offset(),
      new_log->stm_hookset()->max_removable_local_log_offset(),
      std::nullopt,
      std::nullopt,
      std::chrono::milliseconds{0},
      as);
    new_log->housekeeping(conf2).get();

    // Make sure our segments are contiguous.
    model::offset prev_last{-1};
    for (const auto& seg : new_log->segments()) {
        BOOST_REQUIRE_EQUAL(
          seg->offsets().get_base_offset(), model::next_offset(prev_last));
        prev_last = seg->offsets().get_committed_offset();
    }
}

FIXTURE_TEST(compact_transactions_and_replicate, compaction_multinode_test) {
    scoped_config cfg;
    cfg.get("log_compaction_tx_batch_removal_enabled").set_value(true);
    const model::topic topic{"mocha"};
    model::node_id id{0};
    auto* app = create_node_application(id);
    auto* rp = instance(id);
    wait_for_controller_leadership(id).get();

    cluster::topic_properties props;
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
    rp->add_topic({model::kafka_namespace, topic}, 1, props).get();
    model::partition_id pid{0};
    auto ntp = model::ntp(model::kafka_namespace, topic, pid);
    RPTEST_REQUIRE_EVENTUALLY(5s, [&] {
        auto [_, prt] = get_leader(ntp);
        return prt != nullptr;
    });
    auto [_, first_partition] = get_leader(ntp);
    auto first_log = first_partition->log();
    using cluster::tx_executor;
    tx_executor exec;
    auto make_ctx = [&](int64_t id, model::term_id term) {
        model::producer_identity pid{id, 0};
        return tx_executor::tx_op_ctx{
          exec.data_gen(), first_partition->rm_stm(), first_log, pid, term};
    };
    // Produce transactional records.
    tx_executor::sorted_tx_ops_t ops;
    auto term = first_partition->raft()->term();

    int weight = 1;
    ops.emplace(
      ss::make_shared(tx_executor::begin_op(make_ctx(1, term), weight++)));
    ops.emplace(
      ss::make_shared(tx_executor::roll_op(make_ctx(1, term), weight++)));
    ops.emplace(
      ss::make_shared(tx_executor::data_op(make_ctx(1, term), weight++, 1)));
    ops.emplace(
      ss::make_shared(tx_executor::roll_op(make_ctx(1, term), weight++)));

    ops.emplace(
      ss::make_shared(tx_executor::abort_op(make_ctx(1, term), weight++)));
    ops.emplace(
      ss::make_shared(tx_executor::begin_op(make_ctx(2, term), weight++)));
    ops.emplace(
      ss::make_shared(tx_executor::data_op(make_ctx(2, term), weight++, 1)));
    ops.emplace(
      ss::make_shared(tx_executor::abort_op(make_ctx(2, term), weight++)));

    exec.execute(std::move(ops)).get();
    first_partition->log()->flush().get();
    first_partition->log()->force_roll().get();

    first_log->flush().get();
    first_log->force_roll().get();
    ss::abort_source as;
    auto collect_offset
      = first_log->stm_hookset()->max_removable_local_log_offset();
    auto conf = storage::housekeeping_config::make_config(
      model::timestamp::min(),
      std::nullopt,
      collect_offset,
      collect_offset,
      collect_offset,
      std::nullopt,
      std::nullopt,
      std::chrono::milliseconds{0},
      as);
    first_log->housekeeping(conf).get();

    // Decommission the first node, forcing replication to the new node.
    auto* app2 = create_node_application(model::node_id(1));
    wait_for_all_members(10s).get();
    auto err = app->controller->get_members_frontend()
                 .local()
                 .decommission_node(model::node_id(0))
                 .get();
    BOOST_REQUIRE(!err);
    RPTEST_REQUIRE_EVENTUALLY(60s, [&] {
        auto partition = app->partition_manager.local().get(ntp);
        return partition == nullptr;
    });
    auto [new_rp, new_partition] = get_leader(ntp);
    BOOST_REQUIRE(new_rp);
    BOOST_REQUIRE(app2 == &new_rp->app);

    auto new_log = new_partition->log();
    new_log->flush().get();
    new_log->force_roll().get();

    auto conf2 = storage::housekeeping_config::make_config(
      model::timestamp::min(),
      std::nullopt,
      new_log->stm_hookset()->max_removable_local_log_offset(),
      new_log->stm_hookset()->max_removable_local_log_offset(),
      new_log->stm_hookset()->max_removable_local_log_offset(),
      std::nullopt,
      std::nullopt,
      std::chrono::milliseconds{0},
      as);
    new_log->housekeeping(conf2).get();
    exec.validate(new_log).get();
}

FIXTURE_TEST(segment_tx_flags, compaction_multinode_test) {
    scoped_config cfg;
    cfg.get("log_compaction_tx_batch_removal_enabled").set_value(true);
    const model::topic topic{"tapioca"};
    model::node_id id{0};
    create_node_application(id);
    auto* rp = instance(id);
    wait_for_controller_leadership(id).get();

    cluster::topic_properties props;
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
    rp->add_topic({model::kafka_namespace, topic}, 1, props).get();
    model::partition_id pid{0};
    auto ntp = model::ntp(model::kafka_namespace, topic, pid);
    RPTEST_REQUIRE_EVENTUALLY(5s, [&] {
        auto [_, prt] = get_leader(ntp);
        return prt != nullptr;
    });
    auto [_, partition] = get_leader(ntp);
    auto log = partition->log();
    using cluster::tx_executor;
    tx_executor exec;
    auto make_ctx = [&](int64_t id, model::term_id term) {
        model::producer_identity pid{id, 0};
        return tx_executor::tx_op_ctx{
          exec.data_gen(), partition->rm_stm(), log, pid, term};
    };
    // Produce transactional records.
    tx_executor::sorted_tx_ops_t ops;
    auto term = partition->raft()->term();

    int weight = 1;
    ops.emplace(
      ss::make_shared(tx_executor::begin_op(make_ctx(1, term), weight++)));
    ops.emplace(
      ss::make_shared(tx_executor::roll_op(make_ctx(1, term), weight++)));
    ops.emplace(
      ss::make_shared(tx_executor::data_op(make_ctx(1, term), weight++, 1)));
    ops.emplace(
      ss::make_shared(tx_executor::roll_op(make_ctx(1, term), weight++)));

    ops.emplace(
      ss::make_shared(tx_executor::abort_op(make_ctx(1, term), weight++)));
    ops.emplace(
      ss::make_shared(tx_executor::begin_op(make_ctx(2, term), weight++)));
    ops.emplace(
      ss::make_shared(tx_executor::data_op(make_ctx(2, term), weight++, 1)));
    ops.emplace(
      ss::make_shared(tx_executor::abort_op(make_ctx(2, term), weight++)));

    exec.execute(std::move(ops)).get();
    partition->log()->flush().get();
    partition->log()->force_roll().get();

    // Before compaction, segments should by default have these flags as `true`.
    for (const auto& segment : log->segments()) {
        BOOST_REQUIRE(segment->index().may_have_transaction_control_batches());
        BOOST_REQUIRE(
          segment->index().may_have_transaction_data_or_fence_batches());
    }

    ss::abort_source as;
    auto collect_offset = log->stm_hookset()->max_removable_local_log_offset();
    {
        auto conf = storage::housekeeping_config::make_config(
          model::timestamp::min(),
          std::nullopt,
          collect_offset,
          collect_offset,
          collect_offset,
          std::nullopt,
          std::nullopt,
          std::chrono::milliseconds{0},
          as);
        log->housekeeping(conf).get();
    }

    // After the first compaction, control batches are still present, but data
    // batches have had their bit unset (due to being compacted twice over
    // during self compaction -> sliding window compaction)
    for (const auto& segment : log->segments()) {
        if (segment->has_self_compact_timestamp()) {
            BOOST_REQUIRE(
              segment->index().may_have_transaction_control_batches());
            BOOST_REQUIRE(
              !segment->index().may_have_transaction_data_or_fence_batches());
        }
    }

    {
        auto conf = storage::housekeeping_config::make_config(
          model::timestamp::min(),
          std::nullopt,
          collect_offset,
          collect_offset,
          collect_offset,
          std::nullopt,
          std::chrono::milliseconds{0},
          std::chrono::milliseconds{0},
          as);
        log->housekeeping(conf).get();
    }

    // `may_have_transaction_data_or_fence_batches` will be false after the
    // previous compaction, as transactional bits were unset and fences were
    // removed. `may_have_transaction_control_batches` will also be false due to
    // compacting with `tx_retention_ms` set.
    for (const auto& segment : log->segments()) {
        if (segment->has_self_compact_timestamp()) {
            BOOST_REQUIRE(
              !segment->index().may_have_transaction_control_batches());
            BOOST_REQUIRE(
              !segment->index().may_have_transaction_data_or_fence_batches());
        }
    }
}

FIXTURE_TEST(segment_tx_flags_compaction_disabled, compaction_multinode_test) {
    scoped_config cfg;
    cfg.get("log_compaction_merge_max_segments_per_range")
      .set_value(std::make_optional<uint32_t>(0));
    cfg.get("log_compaction_merge_max_ranges")
      .set_value(std::make_optional<uint32_t>(0));
    cfg.get("log_compaction_tx_batch_removal_enabled").set_value(false);

    const model::topic topic{"tapioca"};
    model::node_id id{0};
    create_node_application(id);
    auto* rp = instance(id);
    wait_for_controller_leadership(id).get();

    cluster::topic_properties props;
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;
    rp->add_topic({model::kafka_namespace, topic}, 1, props).get();
    model::partition_id pid{0};
    auto ntp = model::ntp(model::kafka_namespace, topic, pid);
    RPTEST_REQUIRE_EVENTUALLY(5s, [&] {
        auto [_, prt] = get_leader(ntp);
        return prt != nullptr;
    });
    auto [_, partition] = get_leader(ntp);
    auto log = partition->log();
    using cluster::tx_executor;
    tx_executor exec;
    auto make_ctx = [&](int64_t id, model::term_id term) {
        model::producer_identity pid{id, 0};
        return tx_executor::tx_op_ctx{
          exec.data_gen(), partition->rm_stm(), log, pid, term};
    };
    // Produce transactional records.
    tx_executor::sorted_tx_ops_t ops;
    auto term = partition->raft()->term();

    auto num_segments = 3;
    int weight = 1;
    ops.emplace(
      ss::make_shared(tx_executor::begin_op(make_ctx(1, term), weight++)));
    ops.emplace(
      ss::make_shared(tx_executor::roll_op(make_ctx(1, term), weight++)));
    ops.emplace(
      ss::make_shared(tx_executor::data_op(make_ctx(1, term), weight++, 1)));
    ops.emplace(
      ss::make_shared(tx_executor::roll_op(make_ctx(1, term), weight++)));
    ops.emplace(
      ss::make_shared(tx_executor::commit_op(make_ctx(1, term), weight++)));

    exec.execute(std::move(ops)).get();
    partition->log()->flush().get();
    partition->log()->force_roll().get();

    // num_segments + 1 for active segment
    BOOST_REQUIRE_EQUAL(log->segment_count(), num_segments + 1);
    auto& segments = log->segments();

    // Before compaction, segments should by default have these flags as `true`.
    for (const auto& segment : segments) {
        BOOST_REQUIRE(segment->index().may_have_transaction_control_batches());
        BOOST_REQUIRE(
          segment->index().may_have_transaction_data_or_fence_batches());
    }

    ss::abort_source as;
    auto collect_offset = log->stm_hookset()->max_removable_local_log_offset();
    {
        // Compact while `log_compaction_tx_batch_removal_enabled` is `false`,
        // preventing unsetting of transactional bits or removal of any control
        // batches.
        auto conf = storage::housekeeping_config::make_config(
          model::timestamp::min(),
          std::nullopt,
          collect_offset,
          collect_offset,
          collect_offset,
          std::nullopt,
          std::nullopt,
          std::chrono::milliseconds{0},
          as);
        log->housekeeping(conf).get();
    }

    BOOST_REQUIRE_EQUAL(segments.size(), num_segments + 1);

    // First segment has a `tx_fence` batch.
    BOOST_REQUIRE(
      segments[0]->index().may_have_transaction_data_or_fence_batches());
    BOOST_REQUIRE(segments[0]->index().may_have_transaction_control_batches());

    // Second segment has a transactional raft batch.
    BOOST_REQUIRE(
      segments[1]->index().may_have_transaction_data_or_fence_batches());
    BOOST_REQUIRE(!segments[1]->index().may_have_transaction_control_batches());

    // Third segment has a control (commit) batch.
    BOOST_REQUIRE(
      !segments[2]->index().may_have_transaction_data_or_fence_batches());
    BOOST_REQUIRE(segments[2]->index().may_have_transaction_control_batches());

    {
        cfg.get("log_compaction_tx_batch_removal_enabled").set_value(true);
        // Compact twice to remove transactional batches and unset bits.
        auto conf = storage::housekeeping_config::make_config(
          model::timestamp::min(),
          std::nullopt,
          collect_offset,
          collect_offset,
          collect_offset,
          std::nullopt,
          std::chrono::milliseconds{0},
          std::chrono::milliseconds{0},
          as);
        log->housekeeping(conf).get();
        log->housekeeping(conf).get();
    }

    BOOST_REQUIRE_EQUAL(segments.size(), num_segments + 1);

    // All segments have a self compact timestamp, and no remaining
    // transactional control batches or raft data/fence batches.
    BOOST_REQUIRE(segments[0]->has_self_compact_timestamp());
    BOOST_REQUIRE(!segments[0]->index().may_have_transaction_control_batches());
    BOOST_REQUIRE(
      !segments[0]->index().may_have_transaction_data_or_fence_batches());

    BOOST_REQUIRE(segments[1]->has_self_compact_timestamp());
    BOOST_REQUIRE(!segments[1]->index().may_have_transaction_control_batches());
    BOOST_REQUIRE(
      !segments[1]->index().may_have_transaction_data_or_fence_batches());

    BOOST_REQUIRE(segments[2]->has_self_compact_timestamp());
    BOOST_REQUIRE(!segments[2]->index().may_have_transaction_control_batches());
    BOOST_REQUIRE(
      !segments[2]->index().may_have_transaction_data_or_fence_batches());
}
