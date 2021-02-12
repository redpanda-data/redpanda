/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/partition.h"
#include "config/configuration.h"
#include "coproc/script_manager.h"
#include "coproc/tests/utils/coprocessor.h"
#include "coproc/tests/utils/helpers.h"
#include "coproc/tests/utils/supervisor_test_fixture.h"
#include "coproc/types.h"
#include "kafka/client/transport.h"
#include "kafka/protocol/batch_consumer.h"
#include "kafka/protocol/fetch.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "redpanda/tests/fixture.h"
#include "rpc/types.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/fixture.h"

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

using namespace std::literals;

class redpanda_plus_supervisor_fixture
  : public redpanda_thread_fixture
  , public supervisor_test_fixture {
public:
    redpanda_plus_supervisor_fixture()
      : redpanda_thread_fixture()
      , supervisor_test_fixture() {}

    template<typename CoprocessorType>
    bool add_and_register_coprocessor(uint32_t sid, simple_input_set&& sis) {
        auto dataset = make_enable_req(sid, sis);
        // First register it within the engine...
        supervisor_test_fixture::add_copro<CoprocessorType>(sid, std::move(sis))
          .get();
        // Then with redpanda/v/coproc
        auto client = rpc::client<coproc::script_manager_client_protocol>(
          rpc::transport_configuration{
            .server_addr = ss::socket_address(
              ss::net::inet_address("127.0.0.1"), 43118),
            .credentials = nullptr});
        client.connect(model::no_timeout).get();
        const auto resp = client
                            .enable_copros(
                              coproc::enable_copros_request{
                                .inputs = {std::move(dataset)}},
                              rpc::client_opts(rpc::no_timeout))
                            .get0()
                            .value()
                            .data;
        client.stop().get();
        return resp.acks.size() == 1
               && resp.acks[0].second[0]
                    == coproc::enable_response_code::success;
    }

    ss::future<std::optional<iobuf>> materialized_log_read(
      const model::materialized_ntp& mntpv, size_t min_bytes) {
        const auto shard = app.shard_table.local().shard_for(
          mntpv.source_ntp());
        if (!shard) {
            return ss::make_ready_future<std::optional<iobuf>>(std::nullopt);
        }
        return app.storage.invoke_on(
          *shard, [mntpv, min_bytes](storage::api& api) mutable {
              auto olog = api.log_mgr().get(mntpv.input_ntp());
              if (!olog) {
                  return ss::make_ready_future<std::optional<iobuf>>(
                    std::nullopt);
              }
              storage::log_reader_config cfg(
                model::offset(0),
                model::model_limits<model::offset>::max(),
                min_bytes,
                std::numeric_limits<size_t>::max(),
                ss::default_priority_class(),
                raft::data_batch_type,
                std::nullopt,
                std::nullopt);
              return olog->make_reader(cfg).then(
                [](model::record_batch_reader rbr) {
                    return std::move(rbr)
                      .consume(
                        kafka::kafka_batch_serializer{}, model::no_timeout)
                      .then([](kafka::kafka_batch_serializer::result res) {
                          // TODO(Rob) crc checks
                          // auto& [res, kafka_crc_check] = result;
                          // if (!kafka_crc_check) {
                          //     return std::optional<iobuf>(std::nullopt);
                          // }
                          return std::optional<iobuf>(std::move(res.data));
                      });
                });
          });
    }
};

FIXTURE_TEST(
  test_read_from_materialized_topic, redpanda_plus_supervisor_fixture) {
    model::topic topic("foo");
    model::partition_id pid(0);
    model::offset offset(0);
    auto ntp = make_default_ntp(topic, pid);
    auto log_config = make_default_config();
    model::topic materialized_topic = model::to_materialized_topic(
      topic, identity_coprocessor::identity_topic);

    size_t min_expected_bytes = 0;
    {
        using namespace storage;
        storage::disk_log_builder builder(log_config);
        storage::ntp_config ntp_cfg(
          ntp, log_config.base_dir, nullptr, model::revision_id(2));
        builder | start(std::move(ntp_cfg)) | add_segment(model::offset(0))
          | add_random_batch(
            model::offset(0),
            10,
            maybe_compress_batches::no,
            model::record_batch_type(1))
          | stop();
        min_expected_bytes = builder.bytes_written();
        info("Bytes written into source topic: {}", min_expected_bytes);
    }

    wait_for_controller_leadership().get0();
    add_topic(model::topic_namespace_view(ntp)).get();

    // Register the coprocessor after the source topic is in place
    BOOST_REQUIRE(add_and_register_coprocessor<identity_coprocessor>(
      1121, {{"foo", coproc::topic_ingestion_policy::latest}}));

    /// Wait until the materialized topic has entered existance
    const auto mntpv = model::materialized_ntp(
      model::ntp(model::kafka_namespace, materialized_topic, pid));
    auto origin_shard = app.shard_table.local().shard_for(mntpv.source_ntp());
    tests::cooperative_spin_wait_with_timeout(10s, [this, origin_shard, mntpv] {
        return app.partition_manager.invoke_on(
          *origin_shard, [mntpv](cluster::partition_manager& mgr) {
              auto partition = mgr.get(mntpv.source_ntp());
              if (!partition) {
                  return false;
              }
              if (auto log = mgr.log(mntpv.input_ntp())) {
                  return log->offsets().committed_offset >= model::offset(0);
              }
              return false;
          });
    }).get();

    // Connect a kafka client to the expected output topic
    kafka::fetch_request req;
    req.max_bytes = std::numeric_limits<int32_t>::max();
    req.min_bytes = 1; // At LEAST 'bytes_written' in src topic
    req.max_wait_time = 2s;
    req.topics = {
      {.name = materialized_topic,
       .partitions = {{.id = pid, .fetch_offset = model::offset(0)}}}};

    auto client = make_kafka_client().get0();
    client.connect().get();
    auto resp = client.dispatch(req, kafka::api_version(4)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    // Manually read the materialized topic from disk
    const auto data = materialized_log_read(mntpv, min_expected_bytes).get0();
    BOOST_REQUIRE(data);
    BOOST_REQUIRE_EQUAL(resp.partitions.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.partitions[0].name, materialized_topic);
    BOOST_REQUIRE_EQUAL(
      resp.partitions[0].responses[0].error, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.partitions[0].responses[0].id, pid);
    BOOST_REQUIRE(resp.partitions[0].responses[0].record_set);
    BOOST_REQUIRE_EQUAL(
      std::move(*resp.partitions[0].responses[0].record_set).release(), data);
}
