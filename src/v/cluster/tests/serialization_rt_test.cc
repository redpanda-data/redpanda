// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/health_monitor_types.h"
#include "cluster/metadata_dissemination_types.h"
#include "cluster/tests/utils.h"
#include "cluster/types.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/tests/randoms.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "test_utils/randoms.h"
#include "test_utils/rpc.h"
#include "tristate.h"
#include "units.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <chrono>
#include <cstdint>
#include <optional>

using namespace std::chrono_literals; // NOLINT

SEASTAR_THREAD_TEST_CASE(topic_config_rt_test) {
    cluster::topic_configuration cfg(
      model::ns("test"), model::topic{"a_topic"}, 3, 1);

    cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::deletion
        | model::cleanup_policy_bitflags::compaction;
    cfg.properties.compaction_strategy = model::compaction_strategy::offset;
    cfg.properties.compression = model::compression::snappy;
    cfg.properties.segment_size = std::optional<size_t>(1_GiB);
    cfg.properties.retention_bytes = tristate<size_t>{};
    cfg.properties.retention_duration = tristate<std::chrono::milliseconds>(
      10h);

    auto d = serialize_roundtrip_rpc(std::move(cfg));

    BOOST_REQUIRE_EQUAL(model::ns("test"), d.tp_ns.ns);
    BOOST_REQUIRE_EQUAL(model::topic("a_topic"), d.tp_ns.tp);
    BOOST_REQUIRE_EQUAL(3, d.partition_count);
    BOOST_REQUIRE_EQUAL(1, d.replication_factor);
    BOOST_REQUIRE_EQUAL(model::compression::snappy, d.properties.compression);
    BOOST_REQUIRE_EQUAL(
      model::cleanup_policy_bitflags::deletion
        | model::cleanup_policy_bitflags::compaction,
      d.properties.cleanup_policy_bitflags);

    BOOST_REQUIRE_EQUAL(
      model::compaction_strategy::offset, d.properties.compaction_strategy);
    BOOST_CHECK(10h == d.properties.retention_duration.value());
    BOOST_REQUIRE_EQUAL(tristate<size_t>{}, d.properties.retention_bytes);
}

SEASTAR_THREAD_TEST_CASE(broker_metadata_rt_test) {
    model::broker b(
      model::node_id(0),
      net::unresolved_address("127.0.0.1", 9092),
      net::unresolved_address("172.0.1.2", 9999),
      model::rack_id("test"),
      model::broker_properties{
        .cores = 8,
        .available_memory_gb = 1024,
        .available_disk_gb = static_cast<uint32_t>(10000000000),
        .mount_paths = {"/", "/var/lib"},
        .etc_props = {{"max_segment_size", "1233451"}}});
    auto d = serialize_roundtrip_rpc(std::move(b));

    BOOST_REQUIRE_EQUAL(d.id(), model::node_id(0));
    BOOST_REQUIRE_EQUAL(
      d.kafka_advertised_listeners()[0].address.host(), "127.0.0.1");
    BOOST_REQUIRE_EQUAL(d.kafka_advertised_listeners()[0].address.port(), 9092);
    BOOST_REQUIRE_EQUAL(d.rpc_address().host(), "172.0.1.2");
    BOOST_REQUIRE_EQUAL(d.properties().cores, 8);
    BOOST_REQUIRE_EQUAL(d.properties().available_memory_gb, 1024);
    BOOST_REQUIRE_EQUAL(
      d.properties().available_disk_gb, static_cast<uint32_t>(10000000000));
    BOOST_REQUIRE_EQUAL(
      d.properties().mount_paths, std::vector<ss::sstring>({"/", "/var/lib"}));
    BOOST_REQUIRE_EQUAL(d.properties().etc_props.size(), 1);
    BOOST_REQUIRE_EQUAL(
      d.properties().etc_props.find("max_segment_size")->second, "1233451");
    BOOST_CHECK(d.rack() == std::optional<ss::sstring>("test"));
}

SEASTAR_THREAD_TEST_CASE(partition_assignment_rt_test) {
    cluster::partition_assignment p_as{
      raft::group_id(2),
      model::partition_id(3),
      {{.node_id = model::node_id(0), .shard = 1}}};

    auto d = serialize_roundtrip_rpc(std::move(p_as));

    BOOST_REQUIRE_EQUAL(d.group, raft::group_id(2));
    BOOST_REQUIRE_EQUAL(d.id, model::partition_id(3));
    BOOST_REQUIRE_EQUAL(d.replicas.size(), 1);
    BOOST_REQUIRE_EQUAL(d.replicas[0].node_id(), 0);
    BOOST_REQUIRE_EQUAL(d.replicas[0].shard, 1);
}

SEASTAR_THREAD_TEST_CASE(create_topics_request) {
    // clang-format off
    cluster::create_topics_request req{
      .topics = {cluster::topic_configuration(
                   model::ns("default"), model::topic("tp-1"), 12, 3),
                 cluster::topic_configuration(
                   model::ns("default"), model::topic("tp-2"), 6, 5)},
      .timeout = std::chrono::seconds(1)};
    // clang-format on
    auto res = serialize_roundtrip_rpc(std::move(req));
    BOOST_CHECK(res.timeout == std::chrono::seconds(1));
    BOOST_REQUIRE_EQUAL(res.topics[0].partition_count, 12);
    BOOST_REQUIRE_EQUAL(res.topics[0].replication_factor, 3);
    BOOST_REQUIRE_EQUAL(res.topics[0].tp_ns.ns, model::ns("default"));
    BOOST_REQUIRE_EQUAL(res.topics[0].tp_ns.tp, model::topic("tp-1"));
    BOOST_REQUIRE_EQUAL(res.topics[1].partition_count, 6);
    BOOST_REQUIRE_EQUAL(res.topics[1].replication_factor, 5);
    BOOST_REQUIRE_EQUAL(res.topics[0].tp_ns.ns, model::ns("default"));
    BOOST_REQUIRE_EQUAL(res.topics[1].tp_ns.tp, model::topic("tp-2"));
}

SEASTAR_THREAD_TEST_CASE(create_topics_reply) {
    auto md1 = model::topic_metadata(
      model::topic_namespace(model::ns("test-ns"), model::topic("tp-1")));
    auto pmd1 = model::partition_metadata(model::partition_id(0));
    pmd1.leader_node = model::node_id(10);
    pmd1.replicas.push_back(model::broker_shard{model::node_id(10), 0});
    pmd1.replicas.push_back(model::broker_shard{model::node_id(12), 1});
    pmd1.replicas.push_back(model::broker_shard{model::node_id(13), 2});
    md1.partitions = {pmd1};
    // clang-format off
    cluster::create_topics_reply req{
      .results
      = {cluster::topic_result(
           model::topic_namespace(model::ns("default"), model::topic("tp-1")),
           cluster::errc::success),
         cluster::topic_result(
           model::topic_namespace(model::ns("default"), model::topic("tp-2")),
           cluster::errc::notification_wait_timeout)},
      .metadata = {md1}};
    // clang-format on
    auto res = serialize_roundtrip_rpc(std::move(req));

    BOOST_REQUIRE_EQUAL(res.results[0].tp_ns.tp, model::topic("tp-1"));
    BOOST_REQUIRE_EQUAL(res.results[0].ec, cluster::errc::success);
    BOOST_REQUIRE_EQUAL(res.results[1].tp_ns.tp, model::topic("tp-2"));
    BOOST_REQUIRE_EQUAL(
      res.results[1].ec, cluster::errc::notification_wait_timeout);
    BOOST_REQUIRE_EQUAL(res.metadata[0].tp_ns.tp, md1.tp_ns.tp);
    BOOST_REQUIRE_EQUAL(res.metadata[0].partitions[0].id, pmd1.id);
    BOOST_REQUIRE_EQUAL(
      res.metadata[0].partitions[0].leader_node.value(),
      pmd1.leader_node.value());
    BOOST_REQUIRE_EQUAL(
      res.metadata[0].partitions[0].replicas[0].node_id,
      pmd1.replicas[0].node_id);
    BOOST_REQUIRE_EQUAL(
      res.metadata[0].partitions[0].replicas[0].shard, pmd1.replicas[0].shard);
    BOOST_REQUIRE_EQUAL(
      res.metadata[0].partitions[0].replicas[1].node_id,
      pmd1.replicas[1].node_id);
    BOOST_REQUIRE_EQUAL(
      res.metadata[0].partitions[0].replicas[1].shard, pmd1.replicas[1].shard);
    BOOST_REQUIRE_EQUAL(
      res.metadata[0].partitions[0].replicas[2].node_id,
      pmd1.replicas[2].node_id);
    BOOST_REQUIRE_EQUAL(
      res.metadata[0].partitions[0].replicas[2].shard, pmd1.replicas[2].shard);
}

SEASTAR_THREAD_TEST_CASE(config_invariants_test) {
    auto invariants = cluster::configuration_invariants(model::node_id(12), 64);

    auto res = serialize_roundtrip_rpc(std::move(invariants));
    BOOST_REQUIRE_EQUAL(res.core_count, 64);
    BOOST_REQUIRE_EQUAL(res.node_id, model::node_id(12));
    BOOST_REQUIRE_EQUAL(res.version, 0);
}

SEASTAR_THREAD_TEST_CASE(config_update_req_resp_test) {
    auto req_broker = tests::random_broker(0, 10);
    auto target_node = model::node_id(23);

    cluster::configuration_update_request req(req_broker, target_node);

    auto req_res = serialize_roundtrip_rpc(std::move(req));
    BOOST_REQUIRE_EQUAL(req_broker, req_res.node);
    BOOST_REQUIRE_EQUAL(target_node, req_res.target_node);

    cluster::configuration_update_reply reply{true};
    auto reply_res = serialize_roundtrip_rpc(std::move(reply));
    BOOST_REQUIRE_EQUAL(reply_res.success, true);
}

namespace old {
struct incremental_topic_updates_v1 {
    cluster::property_update<std::optional<model::compression>> compression;
    cluster::property_update<std::optional<model::cleanup_policy_bitflags>>
      cleanup_policy_bitflags;
    cluster::property_update<std::optional<model::compaction_strategy>>
      compaction_strategy;
    cluster::property_update<std::optional<model::timestamp_type>>
      timestamp_type;
    cluster::property_update<std::optional<size_t>> segment_size;
    cluster::property_update<tristate<size_t>> retention_bytes;
    cluster::property_update<tristate<std::chrono::milliseconds>>
      retention_duration;
};

struct incremental_topic_updates_v2 {
    cluster::property_update<std::optional<model::compression>> compression;
    cluster::property_update<std::optional<model::cleanup_policy_bitflags>>
      cleanup_policy_bitflags;
    cluster::property_update<std::optional<model::compaction_strategy>>
      compaction_strategy;
    cluster::property_update<std::optional<model::timestamp_type>>
      timestamp_type;
    cluster::property_update<std::optional<size_t>> segment_size;
    cluster::property_update<tristate<size_t>> retention_bytes;
    cluster::property_update<tristate<std::chrono::milliseconds>>
      retention_duration;
    cluster::property_update<std::optional<v8_engine::data_policy>> data_policy;
};

} // namespace old

bool rand_bool() { return random_generators::get_int(0, 100) > 50; }

cluster::incremental_update_operation random_op() {
    return cluster::incremental_update_operation(
      random_generators::get_int<int8_t>(0, 2));
}

cluster::incremental_topic_updates random_incremental_topic_updates() {
    cluster::incremental_topic_updates ret;
    if (rand_bool()) {
        ret.compression.value = model::compression(
          random_generators::get_int(0, 4));
        ret.compression.op = random_op();
    }

    if (rand_bool()) {
        ret.cleanup_policy_bitflags.value = model::cleanup_policy_bitflags(
          random_generators::get_int(0, 3));
        ret.cleanup_policy_bitflags.op = random_op();
    }

    if (rand_bool()) {
        ret.compaction_strategy.value = model::compaction_strategy(
          random_generators::get_int(0, 2));
        ret.compaction_strategy.op = random_op();
    }

    if (rand_bool()) {
        ret.timestamp_type.value = model::timestamp_type(
          random_generators::get_int(0, 1));
        ret.timestamp_type.op = random_op();
    }

    if (rand_bool()) {
        if (!rand_bool()) {
            ret.retention_bytes.value = tristate<size_t>();
            ret.retention_bytes.op = random_op();
        } else {
            ret.retention_bytes.value = tristate<size_t>(
              random_generators::get_int<size_t>(0, 10_GiB));
            ret.retention_bytes.op = random_op();
        }
    }

    if (rand_bool()) {
        if (!rand_bool()) {
            ret.retention_duration.value
              = tristate<std::chrono::milliseconds>();
            ret.retention_duration.op = random_op();
        } else {
            ret.retention_duration.value = tristate<std::chrono::milliseconds>(
              std::chrono::milliseconds(
                random_generators::get_int(0, 500000000)));
            ret.retention_duration.op = random_op();
        }
    }

    return ret;
}

SEASTAR_THREAD_TEST_CASE(incremental_topic_updates_rt_test) {
    cluster::incremental_topic_updates updates
      = random_incremental_topic_updates();
    auto original = updates;

    auto result = serialize_roundtrip_rpc(std::move(updates));

    BOOST_CHECK(result == original);
}

SEASTAR_THREAD_TEST_CASE(incremental_topic_updates_backward_compatibilty_test) {
    cluster::incremental_topic_updates updates
      = random_incremental_topic_updates();

    old::incremental_topic_updates_v1 old_updates;
    old_updates.cleanup_policy_bitflags = updates.cleanup_policy_bitflags;
    old_updates.compaction_strategy = updates.compaction_strategy;
    old_updates.compression = updates.compression;
    old_updates.timestamp_type = updates.timestamp_type;
    old_updates.retention_bytes = updates.retention_bytes;
    old_updates.retention_duration = updates.retention_duration;
    old_updates.segment_size = updates.segment_size;

    // serialize old version
    iobuf buf = reflection::to_iobuf(old_updates);
    iobuf_parser parser(std::move(buf));
    // deserialize with new type
    auto result = reflection::adl<cluster::incremental_topic_updates>{}.from(
      parser);

    BOOST_CHECK(
      old_updates.cleanup_policy_bitflags == result.cleanup_policy_bitflags);
    BOOST_CHECK(old_updates.compaction_strategy == result.compaction_strategy);
    BOOST_CHECK(old_updates.compression == result.compression);
    BOOST_CHECK(old_updates.timestamp_type == result.timestamp_type);
    BOOST_CHECK(old_updates.retention_bytes == result.retention_bytes);
    BOOST_CHECK(old_updates.retention_duration == result.retention_duration);
    BOOST_CHECK(old_updates.segment_size == result.segment_size);

    old::incremental_topic_updates_v2 old_updates_with_dp;
    old_updates_with_dp.cleanup_policy_bitflags
      = updates.cleanup_policy_bitflags;
    old_updates_with_dp.compaction_strategy = updates.compaction_strategy;
    old_updates_with_dp.compression = updates.compression;
    old_updates_with_dp.timestamp_type = updates.timestamp_type;
    old_updates_with_dp.retention_bytes = updates.retention_bytes;
    old_updates_with_dp.retention_duration = updates.retention_duration;
    old_updates_with_dp.segment_size = updates.segment_size;
    old_updates_with_dp.data_policy.op = random_op();
    old_updates_with_dp.data_policy.value = v8_engine::data_policy(
      random_generators::gen_alphanum_string(6),
      random_generators::gen_alphanum_string(6));

    // serialize old version
    buf = reflection::to_iobuf(old_updates_with_dp);
    iobuf_parser parser_with_dp(std::move(buf));
    // deserialize with new type
    result = reflection::adl<cluster::incremental_topic_updates>{}.from(
      parser_with_dp);

    BOOST_CHECK(
      old_updates.cleanup_policy_bitflags == result.cleanup_policy_bitflags);
    BOOST_CHECK(
      old_updates_with_dp.compaction_strategy == result.compaction_strategy);
    BOOST_CHECK(old_updates_with_dp.compression == result.compression);
    BOOST_CHECK(old_updates_with_dp.timestamp_type == result.timestamp_type);
    BOOST_CHECK(old_updates_with_dp.retention_bytes == result.retention_bytes);
    BOOST_CHECK(
      old_updates_with_dp.retention_duration == result.retention_duration);
    BOOST_CHECK(old_updates_with_dp.segment_size == result.segment_size);
}

SEASTAR_THREAD_TEST_CASE(partition_status_serialiaztion_test) {
    cluster::partition_status status{
      .id = model::partition_id(10),
      .term = model::term_id(256),
      .leader_id = model::node_id(123),
      .revision_id = model::revision_id(1024),
    };
    auto original = status;

    auto result = serialize_roundtrip_rpc(std::move(status));

    BOOST_CHECK(result == original);
}

struct partition_status_v0 {
    int8_t version = 0;
    model::partition_id id;
    model::term_id term;
    std::optional<model::node_id> leader_id;
};

SEASTAR_THREAD_TEST_CASE(partition_status_serialization_backward_compat_test) {
    partition_status_v0 status{
      .id = model::partition_id(10),
      .term = model::term_id(256),
      .leader_id = model::node_id(123),
    };

    auto original = status;
    auto buf = reflection::to_iobuf(std::move(status));
    auto result = reflection::from_iobuf<cluster::partition_status>(
      std::move(buf));

    BOOST_REQUIRE_EQUAL(result.id, original.id);
    BOOST_REQUIRE_EQUAL(result.term, original.term);
    BOOST_REQUIRE_EQUAL(result.leader_id, original.leader_id);
    BOOST_REQUIRE_EQUAL(result.revision_id, model::revision_id{});
}
namespace reflection {
template<>
struct adl<partition_status_v0> {
    void to(iobuf& out, partition_status_v0&& s) {
        serialize(out, int8_t(0), s.id, s.term, s.leader_id);
    }

    partition_status_v0 from(iobuf_parser& p) {
        auto version = adl<int8_t>{}.from(p);
        auto id = adl<model::partition_id>{}.from(p);
        auto term = adl<model::term_id>{}.from(p);
        auto leader = adl<std::optional<model::node_id>>{}.from(p);

        return partition_status_v0{
          .id = id,
          .term = term,
          .leader_id = leader,
        };
    }
};
} // namespace reflection

SEASTAR_THREAD_TEST_CASE(partition_status_serialization_old_version) {
    std::vector<cluster::partition_status> statuses;
    statuses.push_back(cluster::partition_status{
      .id = model::partition_id(0),
      .term = model::term_id(256),
      .leader_id = model::node_id(123),
      .revision_id = model::revision_id{},
    });
    statuses.push_back(cluster::partition_status{
      .id = model::partition_id(1),
      .term = model::term_id(256),
      .leader_id = model::node_id(123),
      .revision_id = model::revision_id{},
    });

    auto original = statuses;
    auto buf = reflection::to_iobuf(std::move(statuses));
    auto result = reflection::from_iobuf<std::vector<partition_status_v0>>(
      std::move(buf));
    for (auto i = 0; i < statuses.size(); ++i) {
        BOOST_CHECK(result[i].id == original[i].id);
        BOOST_CHECK(result[i].term == original[i].term);
        BOOST_CHECK(result[i].leader_id == original[i].leader_id);
    }
}

template<typename T>
void roundtrip_test(const T original) {
    auto serde_in = original;
    auto adl_in = original;

    auto serde_out = serde::to_iobuf(std::move(serde_in));
    auto adl_out = reflection::to_iobuf(std::move(adl_in));

    auto from_serde = serde::from_iobuf<T>(std::move(serde_out));
    auto from_adl = reflection::from_iobuf<T>(std::move(adl_out));

    BOOST_REQUIRE(original == from_serde);
    BOOST_REQUIRE(original == from_adl);
}

template<typename T>
cluster::property_update<T> random_property_update(T value) {
    return {
      value,
      random_generators::random_choice(
        std::vector<cluster::incremental_update_operation>{
          cluster::incremental_update_operation::set,
          cluster::incremental_update_operation::remove}),
    };
}

cluster::topic_properties random_topic_properties() {
    cluster::topic_properties properties;
    properties.cleanup_policy_bitflags = tests::random_optional(
      [] { return model::random_cleanup_policy(); });
    properties.compaction_strategy = tests::random_optional(
      [] { return model::random_compaction_strategy(); });
    properties.compression = tests::random_optional(
      [] { return model::random_compression(); });
    properties.timestamp_type = tests::random_optional(
      [] { return model::random_timestamp_type(); });
    properties.segment_size = tests::random_optional(
      [] { return random_generators::get_int(100_MiB, 1_GiB); });
    properties.retention_bytes = tests::random_tristate(
      [] { return random_generators::get_int(100_MiB, 1_GiB); });
    properties.retention_duration = tests::random_tristate(
      [] { return tests::random_duration_ms(); });
    properties.recovery = tests::random_optional(
      [] { return tests::random_bool(); });
    properties.shadow_indexing = tests::random_optional(
      [] { return model::random_shadow_indexing_mode(); });
    return properties;
}

std::vector<cluster::partition_assignment> random_partition_assignments() {
    std::vector<cluster::partition_assignment> ret;

    for (auto a = 0; a < random_generators::get_int(1, 10); a++) {
        cluster::partition_assignment p_as;
        p_as.group = tests::random_named_int<raft::group_id>();
        p_as.id = tests::random_named_int<model::partition_id>();

        for (int i = 0; i < random_generators::get_int(1, 10); ++i) {
            p_as.replicas.push_back(model::broker_shard{
              .node_id = tests::random_named_int<model::node_id>(),
              .shard = random_generators::get_int<uint16_t>(1, 128),
            });
        }
        ret.push_back(std::move(p_as));
    }
    return ret;
}

cluster::topic_configuration random_topic_configuration() {
    cluster::topic_configuration tp_cfg;
    tp_cfg.tp_ns = model::random_topic_namespace();
    tp_cfg.properties = random_topic_properties();
    tp_cfg.replication_factor = random_generators::get_int<int16_t>(0, 10);
    tp_cfg.partition_count = random_generators::get_int(0, 100);
    return tp_cfg;
}

cluster::create_partitions_configuration
random_create_partitions_configuration() {
    cluster::create_partitions_configuration cpc;
    cpc.tp_ns = model::random_topic_namespace();
    cpc.new_total_partition_count = random_generators::get_int<int16_t>(
      10, 100);

    for (int i = 0; i < 3; ++i) {
        cpc.custom_assignments.push_back(
          {tests::random_named_int<model::node_id>(),
           tests::random_named_int<model::node_id>(),
           tests::random_named_int<model::node_id>()});
    }
    return cpc;
}
security::scram_credential random_credential() {
    return security::scram_credential(
      random_generators::get_bytes(256),
      random_generators::get_bytes(256),
      random_generators::get_bytes(256),
      random_generators::get_int(1, 10));
}

security::resource_type random_resource_type() {
    return random_generators::random_choice<security::resource_type>(
      {security::resource_type::cluster,
       security::resource_type::group,
       security::resource_type::topic,
       security::resource_type::transactional_id});
}

security::pattern_type random_pattern_type() {
    return random_generators::random_choice<security::pattern_type>(
      {security::pattern_type::literal, security::pattern_type::prefixed});
}

security::resource_pattern random_resource_pattern() {
    return {
      random_resource_type(),
      random_generators::gen_alphanum_string(10),
      random_pattern_type()};
}

security::acl_principal random_acl_principal() {
    return {
      security::principal_type::user,
      random_generators::gen_alphanum_string(12)};
}
security::acl_host create_acl_host() {
    return security::acl_host(ss::net::inet_address("127.0.0.1"));
}

SEASTAR_THREAD_TEST_CASE(serde_reflection_roundtrip) {
    roundtrip_test(cluster::ntp_leader(
      model::ntp(
        model::ns("a namespace"),
        model::topic("a topic"),
        model::partition_id(287)),
      model::term_id(1234),
      model::node_id(2)));

    roundtrip_test(cluster::ntp_leader_revision(
      model::ntp(
        model::ns("a namespace"),
        model::topic("a topic"),
        model::partition_id(287)),
      model::term_id(1234),
      model::node_id(2),
      model::revision_id(888)));

    roundtrip_test(cluster::update_leadership_request({
      cluster::ntp_leader(
        model::ntp(
          model::ns("a namespace"),
          model::topic("a topic"),
          model::partition_id(287)),
        model::term_id(1234),
        model::node_id(2)),
    }));

    roundtrip_test(cluster::update_leadership_request_v2({
      cluster::ntp_leader_revision(
        model::ntp(
          model::ns("a namespace"),
          model::topic("a topic"),
          model::partition_id(287)),
        model::term_id(1234),
        model::node_id(2),
        model::revision_id(8888)),
    }));

    roundtrip_test(cluster::update_leadership_reply());

    roundtrip_test(cluster::get_leadership_request());

    roundtrip_test(cluster::get_leadership_reply({
      cluster::ntp_leader(
        model::ntp(
          model::ns("a namespace"),
          model::topic("a topic"),
          model::partition_id(287)),
        model::term_id(1234),
        model::node_id(2)),
    }));

    roundtrip_test(
      cluster::allocate_id_request(model::timeout_clock::duration(234234)));

    roundtrip_test(
      cluster::allocate_id_reply(23433, cluster::errc::invalid_node_operation));
    {
        cluster::partition_assignment p_as;
        p_as.group = tests::random_named_int<raft::group_id>();
        p_as.id = tests::random_named_int<model::partition_id>();
        for (int i = 0; i < 5; ++i) {
            p_as.replicas.push_back(model::broker_shard{
              .node_id = tests::random_named_int<model::node_id>(),
              .shard = random_generators::get_int<uint16_t>(1, 20),
            });
        }

        roundtrip_test(p_as);
    }
    { roundtrip_test(random_topic_properties()); }
    {
        roundtrip_test(
          random_property_update(random_generators::gen_alphanum_string(10)));

        roundtrip_test(random_property_update(tests::random_tristate(
          [] { return random_generators::get_int<size_t>(0, 100000); })));
    }
    {
        cluster::incremental_topic_updates updates{
          .compression = random_property_update(
            tests::random_optional([] { return model::random_compression(); })),
          .cleanup_policy_bitflags = random_property_update(
            tests::random_optional(
              [] { return model::random_cleanup_policy(); })),
          .compaction_strategy = random_property_update(tests::random_optional(
            [] { return model::random_compaction_strategy(); })),
          .timestamp_type = random_property_update(tests::random_optional(
            [] { return model::random_timestamp_type(); })),
          .segment_size = random_property_update(tests::random_optional(
            [] { return random_generators::get_int(100_MiB, 1_GiB); })),
          .retention_bytes = random_property_update(tests::random_tristate(
            [] { return random_generators::get_int(100_MiB, 1_GiB); })),
          .retention_duration = random_property_update(
            tests::random_tristate([] { return tests::random_duration_ms(); })),
          .shadow_indexing = random_property_update(tests::random_optional(
            [] { return model::random_shadow_indexing_mode(); })),
        };
        roundtrip_test(updates);
    }
    { roundtrip_test(random_topic_configuration()); }
    { roundtrip_test(random_create_partitions_configuration()); }
    {
        cluster::topic_configuration_assignment cfg;
        cfg.cfg = random_topic_configuration();
        cfg.assignments = random_partition_assignments();

        roundtrip_test(cfg);
    }
    {
        cluster::create_partitions_configuration_assignment cfg;
        cfg.cfg = random_create_partitions_configuration();
        cfg.assignments = random_partition_assignments();

        roundtrip_test(cfg);
    }
}
