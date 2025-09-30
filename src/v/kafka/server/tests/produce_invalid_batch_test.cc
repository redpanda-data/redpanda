// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "container/chunked_vector.h"
#include "kafka/client/transport.h"
#include "kafka/protocol/produce.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/tests/raw_record_batch_factory.h"
#include "model/timestamp.h"
#include "redpanda/tests/fixture.h"
#include "storage/record_batch_builder.h"
#include "test_utils/async.h"
#include "test_utils/boost_fixture.h"

#include <seastar/core/metrics_types.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/bool_class.hh>

#include <vector>

using namespace std::chrono_literals;

struct test_fixture : public redpanda_thread_fixture {
    const model::topic_namespace test_tp_ns = {
      model::ns("kafka"), model::topic("test-topic")};
    void start() {
        add_topic(test_tp_ns, static_cast<int>(1)).get();
        producer = std::make_unique<kafka::client::transport>(
          make_kafka_client().get());
        producer->connect().get();
        model::ntp ntp(test_tp_ns.ns, test_tp_ns.tp, model::partition_id(0));
        tests::cooperative_spin_wait_with_timeout(2s, [ntp, this] {
            auto shard = app.shard_table.local().shard_for(ntp);
            if (!shard) {
                return ss::make_ready_future<bool>(false);
            }
            return app.partition_manager.invoke_on(
              *shard, [ntp](cluster::partition_manager& pm) {
                  return pm.get(ntp)->is_leader();
              });
        }).get();
    }

    model::offset high_watermark() {
        model::ntp ntp(test_tp_ns.ns, test_tp_ns.tp, model::partition_id(0));
        auto shard = app.shard_table.local().shard_for(ntp);
        return app.partition_manager
          .invoke_on(
            *shard,
            [ntp](cluster::partition_manager& mgr) {
                auto partition = mgr.get(ntp);
                return partition->high_watermark();
            })
          .get();
    }
    model::record_batch make_batch(size_t record_count) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));

        for (size_t i = 0; i < record_count; ++i) {
            iobuf v{};
            v.append("v", 1);
            builder.add_raw_kv(iobuf{}, std::move(v));
        }

        return std::move(builder).build();
    }

    ss::future<kafka::produce_response>
    produce_batch(chunked_vector<kafka::produce_request::partition> batches) {
        kafka::produce_request::topic tp;
        tp.partitions = std::move(batches);
        tp.name = test_topic;
        chunked_vector<kafka::produce_request::topic> topics;
        topics.push_back(std::move(tp));
        kafka::produce_request req(std::nullopt, 1, std::move(topics));
        req.data.timeout_ms = std::chrono::seconds(2);
        req.has_idempotent = false;
        req.has_transactional = false;
        return producer->dispatch(std::move(req), kafka::api_version(7));
    }

    std::vector<model::offset> fetch_offsets;

    std::unique_ptr<kafka::client::transport> producer;
    ss::abort_source as;

    const model::topic& test_topic = test_tp_ns.tp;
};

FIXTURE_TEST(test_handling_message_with_truncated_batch, test_fixture) {
    wait_for_controller_leadership().get();
    start();
    auto deferred_close = ss::defer([this] { producer->stop().get(); });
    kafka::produce_request::partition partition;
    partition.partition_index = model::partition_id(0);
    auto batch_1 = make_batch(128);
    auto batch_2 = make_batch(128);
    chunked_vector<kafka::produce_request::partition> batches;
    model::record_batch truncated_batch_1
      = model::test::raw_record_batch_factory::create_record_batch(
        batch_1.header(),
        batch_1.data().copy().share(0, batch_1.data().size_bytes() / 2),
        true);
    // first batch is truncated
    batches.push_back(
      kafka::produce_request::partition{
        .partition_index = model::partition_id(0),
        .records = kafka::produce_request_record_data{
          std::move(truncated_batch_1)}});

    batches.push_back(
      kafka::produce_request::partition{
        .partition_index = model::partition_id(1),
        .records = kafka::produce_request_record_data{std::move(batch_2)}});

    auto resp_f = produce_batch(std::move(batches));
    BOOST_REQUIRE_THROW(
      resp_f.get(), kafka::client::kafka_request_disconnected_exception);
};
