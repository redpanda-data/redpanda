// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

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
#include "test_utils/scoped_config.h"

#include <seastar/core/io_priority_class.hh>

#include <absl/container/btree_map.h>

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
    first_log->force_roll(ss::default_priority_class()).get();
    BOOST_REQUIRE_EQUAL(first_log->segment_count(), 2);
    ss::abort_source as;
    storage::housekeeping_config conf(
      model::timestamp::min(),
      std::nullopt,
      first_log->stm_manager()->max_collectible_offset(),
      std::nullopt,
      ss::default_priority_class(),
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
    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
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
    new_producer.produce_to_partition(topic, pid, kv_t::sequence(0, 5)).get();
    auto new_log = new_partition->log();
    new_log->flush().get();
    new_log->force_roll(ss::default_priority_class()).get();

    // The segments should maintain their last records.
    // {[3]} {[5]} {[0 1 2 3 4 5]}
    storage::housekeeping_config conf2(
      model::timestamp::min(),
      std::nullopt,
      new_log->stm_manager()->max_collectible_offset(),
      std::nullopt,
      ss::default_priority_class(),
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
    first_partition->log()->force_roll(ss::default_priority_class()).get();

    first_log->flush().get();
    first_log->force_roll(ss::default_priority_class()).get();
    ss::abort_source as;
    storage::housekeeping_config conf(
      model::timestamp::min(),
      std::nullopt,
      first_log->stm_manager()->max_collectible_offset(),
      std::nullopt,
      ss::default_priority_class(),
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
    RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
        auto partition = app->partition_manager.local().get(ntp);
        return partition == nullptr;
    });
    auto [new_rp, new_partition] = get_leader(ntp);
    BOOST_REQUIRE(new_rp);
    BOOST_REQUIRE(app2 == &new_rp->app);

    auto new_log = new_partition->log();
    new_log->flush().get();
    new_log->force_roll(ss::default_priority_class()).get();

    storage::housekeeping_config conf2(
      model::timestamp::min(),
      std::nullopt,
      new_log->stm_manager()->max_collectible_offset(),
      std::nullopt,
      ss::default_priority_class(),
      as);
    new_log->housekeeping(conf2).get();
    exec.validate(new_log, 2).get();
}
