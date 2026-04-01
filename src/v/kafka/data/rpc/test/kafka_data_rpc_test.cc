/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/data/rpc/test/deps.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "net/server.h"
#include "rpc/connection_cache.h"
#include "rpc/rpc_server.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sharded.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string_view>

namespace kafka::data::rpc::test {

namespace {

using namespace std::chrono_literals;

constexpr uint16_t test_server_port = 8080;
constexpr model::node_id self_node = model::node_id(1);
constexpr model::node_id other_node = model::node_id(2);

struct test_parameters {
    model::node_id leader_node;
    model::node_id non_leader_node;

    friend std::ostream&
    operator<<(std::ostream& os, const test_parameters& tp) {
        return os << "{leader_node: " << tp.leader_node
                  << " non_leader_node: " << tp.non_leader_node << "}";
    }
};

class KafkaDataRpcTest : public ::testing::TestWithParam<test_parameters> {
public:
    void SetUp() override {
        _as.start().get();
        _kd = std::make_unique<kafka_data_test_fixture>(
          self_node, &_conn_cache, other_node);
        _kd->wire_up_and_start();

        net::server_configuration scfg("kafka_data_test_rpc_server");
        scfg.addrs.emplace_back(
          ss::socket_address(ss::ipv4_addr("127.0.0.1", test_server_port)));
        scfg.max_service_memory_per_core = 1_GiB;
        scfg.disable_metrics = net::metrics_disabled::yes;
        scfg.disable_public_metrics = net::public_metrics_disabled::yes;
        _server = std::make_unique<::rpc::rpc_server>(scfg);
        std::vector<std::unique_ptr<::rpc::service>> rpc_services;
        _kd->register_services(rpc_services, _server.get());
        _server->add_services(std::move(rpc_services));
        _server->start();

        _conn_cache.start(std::ref(_as), std::nullopt).get();
        ::rpc::transport_configuration tcfg(
          net::unresolved_address("127.0.0.1", test_server_port));
        tcfg.disable_metrics = net::metrics_disabled::yes;
        _conn_cache.local()
          .emplace(
            other_node,
            tcfg,
            ::make_exponential_backoff_policy<ss::lowres_clock>(1s, 3s))
          .get();
    }

    void TearDown() override {
        _conn_cache.stop().get();
        _server->stop().get();
        _server.reset();
        _as.stop().get();
        _kd->reset();
    }

    void set_default_new_topic_leader(model::node_id node_id) {
        _kd->topic_creator()->set_default_new_topic_leader(node_id);
    }

    void
    create_topic(const model::topic_namespace& tp_ns, int partition_count = 1) {
        set_default_new_topic_leader(leader_node());
        _kd->client()
          .local()
          .create_topic(tp_ns, cluster::topic_properties{}, partition_count)
          .get();
    }

    cluster::errc update_topic(cluster::topic_properties_update update) {
        return _kd->client().local().update_topic(std::move(update)).get();
    }

    void elect_leader(const model::ntp& ntp, model::node_id node_id) {
        _kd->elect_leader(ntp, node_id);
    }

    void set_errors_to_inject(int n) {
        _kd->local_partition_manager()->set_errors(n);
        _kd->remote_partition_manager()->set_errors(n);
    }

    cluster::errc produce(const model::ntp& ntp, record_batches batches) {
        return _kd->client()
          .local()
          .produce(ntp.tp, std::move(batches.underlying))
          .get();
    }

    std::optional<cluster::topic_configuration>
    local_find_topic_cfg(model::topic_namespace_view tp_ns) {
        return _kd->local_metadata_cache()->find_topic_cfg(tp_ns);
    }

    std::optional<cluster::topic_configuration>
    remote_find_topic_cfg(model::topic_namespace_view tp_ns) {
        return _kd->remote_metadata_cache()->find_topic_cfg(tp_ns);
    }

    model::node_id leader_node() const { return GetParam().leader_node; }
    model::node_id non_leader_node() const {
        return GetParam().non_leader_node;
    }

    ssx::semaphore& server_memory() { return _server->memory(); }

    record_batches non_leader_batches(const model::ntp& ntp) {
        return batches_for(non_leader_node(), ntp);
    }
    record_batches leader_batches(const model::ntp& ntp) {
        return batches_for(leader_node(), ntp);
    }

    result<partition_offsets_map, cluster::errc>
    get_partition_offsets(model::ktp ktp) {
        chunked_vector<topic_partitions> requested_topics;
        requested_topics.push_back(
          topic_partitions{
            .topic = ktp.get_topic(),
            .partitions = chunked_vector<model::partition_id>::single(
              ktp.get_partition()),
          });
        return _kd->client()
          .local()
          .get_partition_offsets(std::move(requested_topics))
          .get();
    }

    result<consume_reply, cluster::errc> consume(
      model::topic_partition tp,
      kafka::offset start_offset,
      kafka::offset max_offset,
      size_t min_bytes = 0,
      size_t max_bytes = std::numeric_limits<size_t>::max()) {
        return _kd->client()
          .local()
          .consume(tp, start_offset, max_offset, min_bytes, max_bytes, 1s)
          .get();
    }

private:
    record_batches batches_for(model::node_id node, const model::ntp& ntp) {
        auto manager = node == self_node ? _kd->local_partition_manager()
                                         : _kd->remote_partition_manager();
        return manager->partition_records(ntp);
    }
    std::unique_ptr<::rpc::rpc_server> _server;
    ss::sharded<::rpc::connection_cache> _conn_cache;

    std::unique_ptr<kafka_data_test_fixture> _kd;

    ss::sharded<ss::abort_source> _as;
};

model::ntp make_ntp(std::string_view topic) {
    return {
      model::kafka_namespace, model::topic(topic), model::partition_id(0)};
}

} // namespace

using ::testing::IsEmpty;

TEST_P(KafkaDataRpcTest, ClientCanProduce) {
    auto ntp = make_ntp("foo");
    create_topic(model::topic_namespace(ntp.ns, ntp.tp.topic));
    auto batches = record_batches::make();
    set_errors_to_inject(2);
    cluster::errc ec = produce(ntp, batches);
    EXPECT_EQ(ec, cluster::errc::success)
      << cluster::error_category().message(int(ec));
    EXPECT_THAT(non_leader_batches(ntp), IsEmpty());
    EXPECT_EQ(leader_batches(ntp), batches);
}

TEST_P(KafkaDataRpcTest, ClientCanUpdateTopics) {
    auto ntp = make_ntp("foo");
    constexpr int batch_max_bytes = 23;
    create_topic(model::topic_namespace(ntp.ns, ntp.tp.topic));
    cluster::topic_properties_update update;
    update.tp_ns = model::topic_namespace{ntp.ns, ntp.tp.topic};
    update.properties.batch_max_bytes = {
      batch_max_bytes, cluster::incremental_update_operation::set};

    auto ec = update_topic(update);
    EXPECT_EQ(ec, cluster::errc::success);

    {
        // local cfg
        auto cfg = local_find_topic_cfg(
          model::topic_namespace{ntp.ns, ntp.tp.topic});
        ASSERT_TRUE(cfg.has_value());
        EXPECT_EQ(
          cfg.value().properties.batch_max_bytes,
          update.properties.batch_max_bytes.value);
    }
    {
        // remote cfg
        auto cfg = remote_find_topic_cfg(
          model::topic_namespace{ntp.ns, ntp.tp.topic});
        ASSERT_TRUE(cfg.has_value());
        EXPECT_EQ(
          cfg.value().properties.batch_max_bytes,
          update.properties.batch_max_bytes.value);
    }
}

TEST_P(KafkaDataRpcTest, ClientCanRequestPartitionOffsets) {
    auto ntp = make_ntp("foo");

    create_topic(model::topic_namespace(ntp.ns, ntp.tp.topic));

    auto res = get_partition_offsets(
      model::ktp(ntp.tp.topic, ntp.tp.partition));
    ASSERT_TRUE(res.has_value());
    auto offsets = std::move(res.value());
    EXPECT_EQ(offsets.size(), 1);

    auto p_offsets = offsets[ntp.tp.topic][ntp.tp.partition];

    EXPECT_EQ(p_offsets.err, cluster::errc::success);
    // expect the hardcoded values from the in-memory proxy
    EXPECT_EQ(p_offsets.offsets.high_watermark, kafka::offset(102));
    EXPECT_EQ(p_offsets.offsets.last_stable_offset, kafka::offset(101));

    auto not_existing = make_ntp("bar");

    auto ne_res = get_partition_offsets(
      model::ktp(not_existing.tp.topic, model::partition_id(0)));
    ASSERT_TRUE(ne_res.has_value());
    auto offsets_2 = std::move(ne_res.value());
    EXPECT_EQ(offsets_2.size(), 1);
    auto p_offsets_2
      = offsets_2[not_existing.tp.topic][not_existing.tp.partition];
    EXPECT_EQ(p_offsets_2.err, cluster::errc::topic_not_exists);
}

TEST_P(KafkaDataRpcTest, ClientCanConsume) {
    auto ntp = make_ntp("foo");
    create_topic(model::topic_namespace(ntp.ns, ntp.tp.topic));

    // Produce some batches
    auto batches = record_batches::make();
    cluster::errc ec = produce(ntp, batches);
    EXPECT_EQ(ec, cluster::errc::success)
      << cluster::error_category().message(int(ec));

    // Verify batches were produced to the leader
    EXPECT_EQ(leader_batches(ntp), batches);

    // Consume from offset 0 to max
    auto consume_result = consume(
      ntp.tp,
      kafka::offset(0),
      kafka::offset::max(),
      0,
      std::numeric_limits<size_t>::max());

    ASSERT_TRUE(consume_result.has_value());
    auto reply = std::move(consume_result.value());

    EXPECT_EQ(reply.err, cluster::errc::success)
      << cluster::error_category().message(int(reply.err));
    EXPECT_EQ(reply.tp, ntp.tp);
    EXPECT_EQ(reply.batches.size(), batches.size());

    // Verify consumed batches match produced batches
    auto batches_it = batches.underlying.begin();
    for (const auto& consumed_batch : reply.batches) {
        EXPECT_EQ(
          consumed_batch.header().record_count,
          batches_it->header().record_count);
        ++batches_it;
    }

    // Test consuming non-existent topic
    auto not_existing = make_ntp("bar");
    auto ne_result = consume(
      not_existing.tp,
      kafka::offset(0),
      kafka::offset::max(),
      0,
      std::numeric_limits<size_t>::max());

    ASSERT_TRUE(ne_result.has_value());
    auto ne_reply = std::move(ne_result.value());
    EXPECT_EQ(ne_reply.err, cluster::errc::topic_not_exists);
}

TEST_P(KafkaDataRpcTest, ProduceRejectsUnderMemoryPressure) {
    if (leader_node() == self_node) {
        GTEST_SKIP() << "Local produces bypass network_service";
    }

    auto ntp = make_ntp("memory-pressure-test");
    create_topic(model::topic_namespace(ntp.ns, ntp.tp.topic));

    // The test server is configured with 1 GiB of RPC memory. Exhaust
    // it past the 10% low-water mark so the memory check in
    // network_service::produce() rejects.
    auto units = ss::get_units(
                   server_memory(), server_memory().available_units() - 1)
                   .get();

    auto result = produce(ntp, record_batches::make());
    EXPECT_EQ(result, cluster::errc::timeout);
}

INSTANTIATE_TEST_SUITE_P(
  WorksLocallyAndRemotely,
  KafkaDataRpcTest,
  ::testing::Values(
    test_parameters{.leader_node = self_node, .non_leader_node = other_node},
    test_parameters{.leader_node = other_node, .non_leader_node = self_node}));

} // namespace kafka::data::rpc::test
