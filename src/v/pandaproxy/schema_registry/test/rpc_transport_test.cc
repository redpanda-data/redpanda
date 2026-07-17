/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/data/rpc/test/deps.h"
#include "kafka/protocol/exceptions.h"
#include "model/namespace.h"
#include "net/server.h"
#include "pandaproxy/schema_registry/rpc_transport.h"
#include "rpc/connection_cache.h"
#include "rpc/rpc_server.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/sharded.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <vector>

namespace pps = pandaproxy::schema_registry;
namespace kdr = kafka::data::rpc;

namespace {

constexpr uint16_t test_server_port = 8081;
constexpr model::node_id self_node = model::node_id(1);
constexpr model::node_id other_node = model::node_id(2);

struct test_parameters {
    model::node_id leader_node;

    friend std::ostream&
    operator<<(std::ostream& os, const test_parameters& tp) {
        return os << "{leader_node: " << tp.leader_node << "}";
    }
};

class SchemaRegistryRpcTransportTest
  : public ::testing::TestWithParam<test_parameters> {
public:
    void SetUp() override {
        _as.start().get();
        _kd = std::make_unique<kdr::test::kafka_data_test_fixture>(
          self_node, &_conn_cache, other_node);
        _kd->wire_up_and_start();

        net::server_configuration scfg("sr_rpc_transport_test_server");
        scfg.addrs.emplace_back(
          ss::socket_address(ss::ipv4_addr("127.0.0.1", test_server_port)));
        scfg.max_service_memory_per_core = 1_GiB;
        scfg.disable_metrics = net::metrics_disabled::yes;
        scfg.disable_public_metrics = net::public_metrics_disabled::yes;
        _server = std::make_unique<::rpc::rpc_server>(scfg);
        std::vector<std::unique_ptr<::rpc::service>> rpc_services;
        _kd->register_services(rpc_services);
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

        _kd->topic_creator()->set_default_new_topic_leader(leader_node());

        _transport = std::make_unique<pps::rpc_transport>(
          _kd->client().local());

        auto ec = _transport
                    ->create_topic(
                      model::topic_namespace_view{
                        model::kafka_namespace,
                        model::schema_registry_internal_tp.topic},
                      1,
                      cluster::topic_properties{},
                      1)
                    .get();
        ASSERT_EQ(ec, cluster::errc::success);
    }

    void TearDown() override {
        _transport.reset();
        _conn_cache.stop().get();
        _server->stop().get();
        _server.reset();
        _as.stop().get();
        _kd->reset();
    }

    model::node_id leader_node() const { return GetParam().leader_node; }

    pps::rpc_transport& transport() { return *_transport; }

    model::offset hwm() { return transport().get_high_watermark().get(); }

    void set_errors(int n) {
        _kd->local_partition_manager()->set_errors(n);
        _kd->remote_partition_manager()->set_errors(n);
    }

    model::record_batch make_batch(std::string_view key, std::string_view val) {
        storage::record_batch_builder rb{
          model::record_batch_type::raft_data, model::offset{0}};
        iobuf key_buf;
        key_buf.append(key.data(), key.size());
        iobuf val_buf;
        val_buf.append(val.data(), val.size());
        rb.add_raw_kv(std::move(key_buf), std::move(val_buf));
        return std::move(rb).build();
    }

    model::record_batch make_multi_record_batch(
      std::vector<std::pair<std::string, std::string>> kvs) {
        storage::record_batch_builder rb{
          model::record_batch_type::raft_data, model::offset{0}};
        for (auto& [k, v] : kvs) {
            iobuf key_buf;
            key_buf.append(k.data(), k.size());
            iobuf val_buf;
            val_buf.append(v.data(), v.size());
            rb.add_raw_kv(std::move(key_buf), std::move(val_buf));
        }
        return std::move(rb).build();
    }

private:
    std::unique_ptr<::rpc::rpc_server> _server;
    ss::sharded<::rpc::connection_cache> _conn_cache;
    std::unique_ptr<kdr::test::kafka_data_test_fixture> _kd;
    ss::sharded<ss::abort_source> _as;
    std::unique_ptr<pps::rpc_transport> _transport;
};

} // namespace

TEST_P(SchemaRegistryRpcTransportTest, ProduceReturnsBaseOffset) {
    auto batch = make_batch("key1", "val1");
    auto result = transport().produce(std::move(batch)).get();
    EXPECT_GE(result.base_offset, model::offset(0));
}

TEST_P(SchemaRegistryRpcTransportTest, ProduceMultiRecordReturnsBaseOffset) {
    auto batch = make_multi_record_batch({{"k1", "v1"}, {"k2", "v2"}});
    ASSERT_EQ(batch.record_count(), 2);
    auto result = transport().produce(std::move(batch)).get();
    // 2-record batch starting at offset 0: base=0, last=1.
    EXPECT_EQ(result.base_offset, model::offset{0});
}

TEST_P(SchemaRegistryRpcTransportTest, GetHighWatermarkAfterProduce) {
    // No records produced HWM should be 0 (next_offset of -1).
    EXPECT_EQ(hwm(), model::offset(0));
    transport().produce(make_batch("k1", "v1")).get();
    EXPECT_EQ(hwm(), model::offset(1));
}

TEST_P(
  SchemaRegistryRpcTransportTest, GetHighWatermarkRetriesOnTransientError) {
    // Inject 2 transient errors; get_high_watermark should retry and succeed
    transport().produce(make_batch("k1", "v1")).get();
    set_errors(2);
    EXPECT_EQ(hwm(), model::offset(1));
}

TEST_P(SchemaRegistryRpcTransportTest, ConsumeRangeProducedBatches) {
    std::vector<model::record_batch> records_produced;
    records_produced.push_back(make_batch("k1", "v1"));
    records_produced.push_back(make_batch("k2", "v2"));
    records_produced.push_back(make_batch("k3", "v3"));

    model::offset max_produced{0};
    for (auto& b : records_produced) {
        b.header().base_offset
          = transport().produce(b.copy()).get().base_offset;
        max_produced = std::max(max_produced, b.header().base_offset);
    }

    std::vector<model::record_batch> records_consumed;
    model::offset max_consumed{0};
    // Note that the offset range is partially open [beg, end)
    transport()
      .consume_range(
        model::offset(0),
        model::offset(3),
        [&records_consumed, &max_consumed](
          this auto, model::record_batch b) -> ss::future<ss::stop_iteration> {
            max_consumed = std::max(max_consumed, b.base_offset());
            records_consumed.push_back(std::move(b));
            co_return ss::stop_iteration::no;
        })
      .get();

    EXPECT_EQ(records_consumed.size(), records_produced.size());
    for (const auto& [c, p] :
         std::views::zip(records_consumed, records_produced)) {
        EXPECT_EQ(c, p);
    }
    EXPECT_EQ(max_consumed, max_produced);
}

TEST_P(SchemaRegistryRpcTransportTest, ConsumeRangeRetriesOnTransientError) {
    transport().produce(make_batch("k1", "v1")).get();

    set_errors(2);
    int consumed_count = 0;
    transport()
      .consume_range(
        model::offset(0),
        model::offset(1),
        [&consumed_count](
          this auto, model::record_batch) -> ss::future<ss::stop_iteration> {
            ++consumed_count;
            co_return ss::stop_iteration::no;
        })
      .get();
    EXPECT_EQ(consumed_count, 1);
}

TEST_P(SchemaRegistryRpcTransportTest, ConsumeRangeEmpty) {
    // Consuming an empty range should return without hitting the callback but
    // NOT error.
    int consumed_count = 0;
    transport()
      .consume_range(
        model::offset(0),
        model::offset(0),
        [&consumed_count](
          this auto, model::record_batch) -> ss::future<ss::stop_iteration> {
            ++consumed_count;
            co_return ss::stop_iteration::no;
        })
      .get();
    EXPECT_EQ(consumed_count, 0);
}

TEST_P(SchemaRegistryRpcTransportTest, ConsumeRangePastHwmThrows) {
    // This is a schema registry thing. If we can't consume the whole range,
    // then the in-memory store will be out of date. It's always an error.
    transport().produce(make_batch("k1", "v1")).get();

    EXPECT_THROW(
      transport()
        .consume_range(
          model::offset(0),
          hwm() + model::offset(10),
          [](model::record_batch) -> ss::future<ss::stop_iteration> {
              co_return ss::stop_iteration::no;
          })
        .get(),
      kafka::exception);
}

INSTANTIATE_TEST_SUITE_P(
  LocalAndRemoteLeader,
  SchemaRegistryRpcTransportTest,
  ::testing::Values(
    test_parameters{.leader_node = self_node},
    test_parameters{.leader_node = other_node}));
