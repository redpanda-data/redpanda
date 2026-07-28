/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/vlog.h"
#include "cloud_topics/tests/cluster_fixture.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"

#include <seastar/util/log.hh>

#include <fmt/format.h>
#include <gtest/gtest.h>

using tests::kv_t;
using namespace cloud_topics;

namespace {

ss::logger test_log("leader_epoch_test");
const model::topic topic_name{"tapioca"};
const model::ntp ntp{model::kafka_namespace, topic_name, 0};
const model::topic_namespace nt{model::kafka_namespace, topic_name};

} // namespace

class LeaderEpochTest
  : public cluster_fixture
  , public ::testing::Test {
public:
    void SetUp() override {
        cfg.get("enable_leader_balancer").set_value(false);
        cfg.get("cloud_topics_disable_reconciliation_loop").set_value(true);
        cfg.get("raft_heartbeat_interval_ms").set_value(50ms);
        cfg.get("raft_heartbeat_timeout_ms").set_value(500ms);
        for (int i = 0; i < 3; ++i) {
            add_node();
        }
        wait_for_all_members(5s).get();

        cluster::topic_properties props;
        props.storage_mode = model::redpanda_storage_mode::cloud;
        props.shadow_indexing = model::shadow_indexing_mode::disabled;

        create_topic({model::kafka_namespace, topic_name}, 1, 3, props).get();
    }

    ss::future<ss::lw_shared_ptr<cluster::partition>> wait_for_leadership() {
        ss::lw_shared_ptr<cluster::partition> p;
        co_await tests::cooperative_spin_wait_with_timeout(10s, [&] {
            auto [leader_fx, leader_p] = get_leader(ntp);
            if (!leader_fx) {
                return false;
            }
            p = leader_p;
            return true;
        });
        co_return p;
    }

    // Produces the given number of batches and records. Adds the number of
    // records to produce_counter and returns the last Kafka offset produced.
    ss::future<kafka::offset> produce(
      size_t batch_count, int records_per_batch, size_t& produce_counter) {
        auto leader = (co_await wait_for_leadership())->raft();
        auto leader_id = leader->self();
        auto producer = co_await make_producer(leader_id.id());
        kafka::offset last{};
        for (size_t b = 0; b < batch_count; ++b) {
            auto records = kv_t::sequence(produce_counter, records_per_batch);
            auto base_offset = co_await producer->produce_to_partition(
              topic_name, model::partition_id(0), std::move(records));
            last = kafka::offset{base_offset() + records_per_batch - 1};
            produce_counter += records_per_batch;
            vlog(
              test_log.info,
              "Produced up to offset {} in term {}",
              last,
              leader->term());
        }
        co_return last;
    }

    // In a loop, produces data and steps down, recording the last offset
    // produced per term.
    ss::future<std::optional<std::exception_ptr>> produce_step_down_loop(
      ss::gate& gate,
      absl::btree_map<model::term_id, kafka::offset>& term_last_offsets,
      size_t& produce_counter,
      size_t& term_counter) {
        auto gh = gate.hold();
        try {
            while (!gate.is_closed()) {
                auto last_offset = co_await produce(10, 10, produce_counter);
                auto leader = (co_await wait_for_leadership())->raft();

                term_last_offsets.emplace(leader->term(), last_offset);
                co_await leader->step_down("test");
                ++term_counter;
                co_await ss::sleep(250ms);
            }
        } catch (...) {
            co_return std::current_exception();
        }
        co_return std::nullopt;
    }

    // In a loop, checks each replica that the epochs are correct.
    ss::future<std::optional<std::exception_ptr>> validate_loop(
      ss::gate& gate,
      const absl::btree_map<model::term_id, kafka::offset>& term_last_offsets,
      size_t& validate_counter) {
        auto gh = gate.hold();
        try {
            while (!gate.is_closed()) {
                bool lagging_replica = false;
                for (const auto& node_id : instance_ids()) {
                    auto* node = instance(node_id);
                    auto p = node->app.partition_manager.local().get(ntp);
                    EXPECT_FALSE(p == nullptr);
                    auto proxy = kafka::make_partition_proxy(p);

                    auto terms_copy = term_last_offsets;
                    // Validate all the term last offsets we know about so far.
                    for (const auto& [term, expected_last_offset] :
                         terms_copy) {
                        auto end_res
                          = co_await proxy.get_leader_epoch_last_offset(
                            kafka::leader_epoch(term()));
                        if (!end_res.has_value()) {
                            // It's possible the replica is lagging and doesn't
                            // know about this term yet.
                            lagging_replica = true;
                            continue;
                        }
                        // NOTE: Kafka API is asking for the end offset of the
                        // term, which is last + 1.
                        EXPECT_EQ(
                          model::offset_cast(end_res.value()),
                          kafka::next_offset(expected_last_offset));
                    }
                }
                // Don't count the validation success if we didn't validate all
                // the replicas because of a lagging replica.
                if (!lagging_replica) {
                    ++validate_counter;
                }
                co_await ss::sleep(100ms);
            }
        } catch (...) {
            co_return std::current_exception();
        }
        co_return std::nullopt;
    }

    ss::future<std::optional<kafka::offset>> get_last_l1_offset() {
        auto tp_id = instance(model::node_id{0})
                       ->app.controller->get_topics_state()
                       .local()
                       .topics_map()
                       .at(nt)
                       .get_configuration()
                       .tp_id;
        EXPECT_TRUE(tp_id.has_value());

        auto& meta = get_ct_app(model::node_id{0})
                       .get_sharded_replicated_metastore()
                       ->local();
        auto l1_offset = co_await meta.get_offsets(
          model::topic_id_partition(*tp_id, model::partition_id{0}));
        if (!l1_offset.has_value()) {
            co_return std::nullopt;
        }
        co_return l1_offset->next_offset;
    }

protected:
    scoped_config cfg;
};

TEST_F(LeaderEpochTest, TestBasicGetLeaderEpoch) {
    size_t produce_counter = 0;
    absl::btree_map<model::term_id, kafka::offset> term_last_offsets;
    for (int i = 0; i < 5; ++i) {
        auto leader = wait_for_leadership().get()->raft();
        auto term = leader->term();
        ASSERT_FALSE(term_last_offsets.contains(term));

        auto last_produced_o = produce(10, 10, produce_counter).get();
        term_last_offsets.emplace(term, last_produced_o);

        leader->step_down("test").get();
    }

    auto p = wait_for_leadership().get();
    ASSERT_FALSE(p == nullptr);
    auto proxy = kafka::make_partition_proxy(p);
    for (const auto& [term, expected_last_offset] : term_last_offsets) {
        auto end_res
          = proxy.get_leader_epoch_last_offset(kafka::leader_epoch(term()))
              .get();
        ASSERT_TRUE(end_res.has_value());
        EXPECT_EQ(
          model::offset_cast(end_res.value()),
          kafka::next_offset(expected_last_offset));
    }
}

TEST_F(LeaderEpochTest, TestGetLeaderEpochWhileReconciling) {
    size_t produce_counter = 0;
    size_t validate_counter = 0;
    size_t term_counter = 0;
    absl::btree_map<model::term_id, kafka::offset> term_last_offsets;

    ss::gate gate;

    // Start a fiber that produces and forces a leader stepdown in a loop,
    // collecting the term last offsets.
    auto produce_fut = produce_step_down_loop(
      gate, term_last_offsets, produce_counter, term_counter);

    // Start another fiber that validates all the term last offsets match what
    // we expected.
    auto validate_fut = validate_loop(
      gate, term_last_offsets, validate_counter);

    // In the test body, reconcile a few times
    kafka::offset last_l1_offset{};
    for (int i = 0; i < 5; ++i) {
        // First wait for more records to be produced.
        auto initial_produce = produce_counter;
        auto initial_validate = validate_counter;
        auto initial_term_count = term_counter;
        RPTEST_REQUIRE_EVENTUALLY(10s, [&] {
            return produce_counter > initial_produce
                   && validate_counter > initial_validate
                   && term_counter > initial_term_count;
        });

        auto deadline = ss::lowres_clock::now() + 10s;
        while (true) {
            ASSERT_LT(ss::lowres_clock::now(), deadline);

            // Try reconciling everywhere, and only proceed if we are making
            // progress.
            for (auto node_id : instance_ids()) {
                auto& ct_app = get_ct_app(node_id);
                ct_app.get_reconciler()
                  ->invoke_on_all(
                    [](auto& reconciler) { return reconciler.reconcile(); })
                  .get();
            }
            auto l1_o = get_last_l1_offset().get();
            if (l1_o.has_value() && l1_o > last_l1_offset) {
                last_l1_offset = *l1_o;
                break;
            }
            ss::sleep(50ms).get();
        }
    }
    // Stop the fibers and make sure there were no errors.
    auto gate_fut = gate.close();
    auto producer_error = produce_fut.get();
    auto validator_error = validate_fut.get();
    gate_fut.get();

    ASSERT_FALSE(producer_error.has_value())
      << "Producer fiber failed with exception "
      << fmt::format("{}", ss::formattable(*producer_error));
    ASSERT_FALSE(validator_error.has_value())
      << "Validator fiber failed with exception "
      << fmt::format("{}", ss::formattable(*validator_error));

    // One more validation.
    auto p = wait_for_leadership().get();
    ASSERT_FALSE(p == nullptr);
    auto proxy = kafka::make_partition_proxy(p);
    for (const auto& [term, expected_last_offset] : term_last_offsets) {
        auto end_res
          = proxy.get_leader_epoch_last_offset(kafka::leader_epoch(term()))
              .get();
        ASSERT_TRUE(end_res.has_value());
        EXPECT_EQ(
          model::offset_cast(end_res.value()),
          kafka::next_offset(expected_last_offset));
    }
}
