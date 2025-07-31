// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/fwd.h"
#include "cluster/types.h"
#include "kafka/data/replicated_partition.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record_batch_types.h"
#include "raft/replicate.h"
#include "redpanda/tests/fixture.h"
#include "storage/record_batch_builder.h"
#include "test_utils/async.h"

FIXTURE_TEST(test_replicated_partition_end_offset, redpanda_thread_fixture) {
    wait_for_controller_leadership().get();

    model::topic_namespace tp_ns(
      model::kafka_namespace, model::topic("test-topic"));

    add_topic(tp_ns).get();
    model::ntp ntp(tp_ns.ns, tp_ns.tp, model::partition_id(0));
    auto shard = app.shard_table.local().shard_for(ntp);

    tests::cooperative_spin_wait_with_timeout(10s, [this, shard, &ntp] {
        return app.partition_manager.invoke_on(
          *shard, [&ntp](cluster::partition_manager& pm) {
              auto p = pm.get(ntp);
              return p->is_leader();
          });
    }).get();

    app.partition_manager
      .invoke_on(
        *shard,
        [&ntp](cluster::partition_manager& pm) {
            auto p = pm.get(ntp);
            kafka::replicated_partition rp(p);
            auto p_info = rp.get_partition_info();
            /**
             * Since log is empty from Kafka client perspective (no data
             * batches), the end offset which is exclusive must be equal to 0
             */
            BOOST_REQUIRE_EQUAL(rp.log_end_offset(), model::offset{0});
            BOOST_REQUIRE_EQUAL(rp.high_watermark(), model::offset{0});

            storage::record_batch_builder builder(
              model::record_batch_type::version_fence, model::offset(0));
            builder.add_raw_kv(iobuf{}, iobuf{});
            builder.add_raw_kv(iobuf{}, iobuf{});
            builder.add_raw_kv(iobuf{}, iobuf{});

            // replicate a batch that is subjected to offset translation
            return p
              ->replicate(
                chunked_vector<model::record_batch>::single(
                  std::move(builder).build()),
                raft::replicate_options(raft::consistency_level::quorum_ack))
              .then([p, rp](result<cluster::kafka_result> rr) {
                  BOOST_REQUIRE(rr.has_value());
                  BOOST_REQUIRE_GT(p->dirty_offset(), model::offset{0});

                  BOOST_REQUIRE_EQUAL(rp.log_end_offset(), model::offset{0});
                  BOOST_REQUIRE_EQUAL(rp.high_watermark(), model::offset{0});
              });
        })
      .get();
}

FIXTURE_TEST(test_replicated_partition_leader_epoch, redpanda_thread_fixture) {
    wait_for_controller_leadership().get();

    model::topic_namespace tp_ns(
      model::kafka_namespace, model::topic("test-topic"));

    add_topic(tp_ns).get();
    model::ntp ntp(tp_ns.ns, tp_ns.tp, model::partition_id(0));
    auto shard = app.shard_table.local().shard_for(ntp);

    model::term_id first_term;
    tests::cooperative_spin_wait_with_timeout(10s, [&] {
        return app.partition_manager.invoke_on(
          *shard, [&](cluster::partition_manager& pm) {
              auto p = pm.get(ntp);
              first_term = p->raft()->term();
              return p->is_leader();
          });
    }).get();

    const auto num_terms = 3;
    const auto batches_per_term = 10;

    BOOST_TEST_CONTEXT("Seeding partition data") {
        auto producer = tests::kafka_produce_transport(
          make_kafka_client().get());
        producer.start().get();
        auto deferred_close = ss::defer([&producer] { producer.stop().get(); });

        for (auto _ : boost::irange(num_terms)) {
            for (auto _ : boost::irange(batches_per_term)) {
                producer
                  .produce_to_partition(
                    ntp.tp.topic, ntp.tp.partition, {{"key0", "val0"}})
                  .get();
            }

            app.partition_manager
              .invoke_on(
                *shard,
                [&ntp](cluster::partition_manager& pm) {
                    auto p = pm.get(ntp);

                    return p->raft()
                      ->step_down("stepping down to increment term")
                      .then([p] {
                          return tests::cooperative_spin_wait_with_timeout(
                            10s, [p] {
                                return p->raft()->term() != model::term_id{}
                                       && p->raft()->is_leader();
                            });
                      });
                })
              .get();
        }
    }

    auto run_leader_epoch_checks =
      [&](const cluster::partition_manager& pm) -> void {
        auto rp = kafka::replicated_partition(pm.get(ntp)->shared_from_this());

        for (auto term_ix : boost::irange(num_terms)) {
            const auto expected_epoch = kafka::leader_epoch{
              static_cast<int>(first_term() + term_ix)};

            BOOST_TEST_CONTEXT(
              "Checking leader epoch for offset in term " << term_ix) {
                // Query leader epoch for first offset in the
                // segment, middle of the segment, and last offset in
                // the segment and expect the same term.
                const auto first_offset = kafka::offset(
                  static_cast<long>(term_ix * batches_per_term));
                const auto middle_offset = kafka::offset(static_cast<long>(
                  term_ix * batches_per_term + batches_per_term / 2));
                const auto last_offset = kafka::offset(
                  static_cast<long>((term_ix + 1) * batches_per_term - 1));

                BOOST_TEST_MESSAGE(
                  "Checking leader epoch for first offset: " << first_offset);
                BOOST_CHECK_EQUAL(
                  rp.leader_epoch(first_offset).get(), expected_epoch);

                BOOST_TEST_MESSAGE(
                  "Checking leader epoch for middle offset: " << middle_offset);
                BOOST_CHECK_EQUAL(
                  rp.leader_epoch(middle_offset).get(), expected_epoch);

                BOOST_TEST_MESSAGE(
                  "Checking leader epoch for last offset: " << last_offset);
                BOOST_CHECK_EQUAL(
                  rp.leader_epoch(last_offset).get(), expected_epoch);
            }
        }

        BOOST_TEST_MESSAGE(
          "Checking leader epoch for out-of-bounds offsets before and after "
          "the log");
        const auto before_log_start = kafka::offset{-1};
        BOOST_CHECK_EXCEPTION(
          rp.leader_epoch(before_log_start).get(),
          std::runtime_error,
          [](const std::runtime_error& e) {
              BOOST_TEST_MESSAGE("Checking exception message " << e.what());
              return std::string_view(e.what()).contains(
                "offset(k)=-1 is not available, start offset(k)=0, start "
                "offset(r)=0");
          });

        const auto first_local_offset = kafka::offset(
          static_cast<long>(num_terms * batches_per_term));
        BOOST_CHECK_EXCEPTION(
          rp.leader_epoch(first_local_offset).get(),
          std::runtime_error,
          [](const std::runtime_error& e) {
              BOOST_TEST_MESSAGE("Checking exception message " << e.what());
              return std::string_view(e.what()).contains(
                "can not get term for offset(k)=30 >= hwm(k)=30");
          });
    };

    app.partition_manager
      .invoke_on(
        *shard,
        [&](cluster::partition_manager& pm) {
            return ss::async([&] { run_leader_epoch_checks(pm); });
        })
      .get();
}
