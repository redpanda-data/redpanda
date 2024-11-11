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
            ::rpc::make_exponential_backoff_policy<ss::lowres_clock>(1s, 3s))
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

    model::node_id leader_node() const { return GetParam().leader_node; }
    model::node_id non_leader_node() const {
        return GetParam().non_leader_node;
    }

    record_batches non_leader_batches(const model::ntp& ntp) {
        return batches_for(non_leader_node(), ntp);
    }
    record_batches leader_batches(const model::ntp& ntp) {
        return batches_for(leader_node(), ntp);
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

INSTANTIATE_TEST_SUITE_P(
  WorksLocallyAndRemotely,
  KafkaDataRpcTest,
  ::testing::Values(
    test_parameters{.leader_node = self_node, .non_leader_node = other_node},
    test_parameters{.leader_node = other_node, .non_leader_node = self_node}));

} // namespace kafka::data::rpc::test
