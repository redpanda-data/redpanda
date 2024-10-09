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
#include "kafka/server/partition_proxy.h"
#include "kafka/server/replicated_partition.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
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
                model::make_memory_record_batch_reader(
                  {std::move(builder).build()}),
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
