// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/units.h"
#include "cluster/commands.h"
#include "cluster/controller_snapshot.h"
#include "cluster/health_monitor_types.h"
#include "cluster/metadata_dissemination_types.h"
#include "cluster/tests/randoms.h"
#include "cluster/tests/topic_properties_generator.h"
#include "cluster/tests/utils.h"
#include "cluster/tx_protocol_types.h"
#include "cluster/types.h"
#include "compat/check.h"
#include "container/fragmented_vector.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"
#include "model/timestamp.h"
#include "pandaproxy/schema_registry/test/random.h"
#include "raft/fundamental.h"
#include "raft/group_configuration.h"
#include "raft/types.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "reflection/async_adl.h"
#include "security/tests/randoms.h"
#include "storage/types.h"
#include "test_utils/randoms.h"
#include "test_utils/rpc.h"
#include "utils/tristate.h"
#include "v8_engine/data_policy.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <chrono>
#include <cstdint>
#include <optional>
#include <vector>

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
        .etc_props = {{"max_segment_size", "1233451"}},
        .available_memory_bytes = 1024 * 1_GiB,
        .in_fips_mode = model::fips_mode_flag::enabled});
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
    BOOST_REQUIRE_EQUAL(d.properties().available_memory_bytes, 1024 * 1_GiB);
    BOOST_REQUIRE_EQUAL(
      d.properties().in_fips_mode, model::fips_mode_flag::enabled);
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
       {cluster::topic_result(
           model::topic_namespace(model::ns("default"), model::topic("tp-1")),
           cluster::errc::success),
         cluster::topic_result(
           model::topic_namespace(model::ns("default"), model::topic("tp-2")),
           cluster::errc::notification_wait_timeout)},
       {md1}, {}};
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

    auto res = serialize_roundtrip_adl(std::move(invariants));
    BOOST_REQUIRE_EQUAL(res.core_count, 64);
    BOOST_REQUIRE_EQUAL(res.node_id, model::node_id(12));
    BOOST_REQUIRE_EQUAL(res.version, 0);
}

SEASTAR_THREAD_TEST_CASE(config_update_req_resp_test) {
    auto req_broker = model::random_broker(0, 10);
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
      .size_bytes = 4096,
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

struct partition_status_v1 {
    int8_t version = 0;
    model::partition_id id;
    model::term_id term;
    std::optional<model::node_id> leader_id;
    model::revision_id revision_id;
};

namespace reflection {
template<>
struct adl<partition_status_v0> {
    void to(iobuf& out, partition_status_v0&& s) {
        serialize(out, int8_t(0), s.id, s.term, s.leader_id);
    }

    partition_status_v0 from(iobuf_parser& p) {
        adl<int8_t>{}.from(p); // version
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

template<>
struct adl<partition_status_v1> {
    void to(iobuf& out, partition_status_v1&& s) {
        if (s.revision_id == model::revision_id{}) {
            serialize(out, int8_t(0), s.id, s.term, s.leader_id);
        } else {
            serialize(
              out, int8_t(-1), s.id, s.term, s.leader_id, s.revision_id);
        }
    }

    partition_status_v1 from(iobuf_parser& p) {
        auto version = adl<int8_t>{}.from(p);
        auto id = adl<model::partition_id>{}.from(p);
        auto term = adl<model::term_id>{}.from(p);
        auto leader = adl<std::optional<model::node_id>>{}.from(p);
        partition_status_v1 ret{
          .id = id,
          .term = term,
          .leader_id = leader,
        };
        if (version < 0) {
            ret.revision_id = adl<model::revision_id>{}.from(p);
        }
        return ret;
    }
};
} // namespace reflection

template<typename T>
void roundtrip_test(T a) {
    using compat::compat_copy;
    auto [serde_in, original] = compat_copy(std::move(a));
    auto serde_out = serde::to_iobuf(std::move(serde_in));

    T from_serde;
    std::exception_ptr err;
    try {
        from_serde = serde::from_iobuf<T>(serde_out.copy());
    } catch (...) {
        err = std::current_exception();
    }

    if constexpr (std::equality_comparable<T>) {
        // On failures, log the type and the content of the buffer.
        if (err || original != from_serde) {
            std::cerr << "Failed serde roundtrip on " << typeid(T).name()
                      << std::endl;
            std::cerr << "Dump " << serde_out.size_bytes()
                      << " bytes: " << serde_out.hexdump(1024) << std::endl;
        }

        if (err) {
            std::rethrow_exception(err);
        }

        BOOST_REQUIRE(original == from_serde);
    } else {
        auto serde_out_after_roundtrip = serde::to_iobuf(from_serde);
        if (err || serde_out_after_roundtrip != serde_out) {
            std::cerr << "Failed serde roundtrip on " << typeid(T).name()
                      << std::endl;
            std::cerr << "Iobuf before " << serde_out.size_bytes()
                      << " bytes: " << serde_out.hexdump(1024) << std::endl;
            std::cerr << "Iobuf after "
                      << serde_out_after_roundtrip.size_bytes()
                      << " bytes: " << serde_out_after_roundtrip.hexdump(1024)
                      << std::endl;
        }

        if (err) {
            std::rethrow_exception(err);
        }

        BOOST_REQUIRE(serde_out_after_roundtrip == serde_out);
    }
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

ss::chunked_fifo<cluster::partition_assignment> random_partition_assignments() {
    ss::chunked_fifo<cluster::partition_assignment> ret;

    for (auto a = 0, ma = random_generators::get_int(1, 10); a < ma; a++) {
        cluster::partition_assignment p_as;
        p_as.group = tests::random_named_int<raft::group_id>();
        p_as.id = tests::random_named_int<model::partition_id>();

        for (int i = 0, mi = random_generators::get_int(1, 10); i < mi; ++i) {
            p_as.replicas.push_back(model::broker_shard{
              .node_id = tests::random_named_int<model::node_id>(),
              .shard = random_generators::get_int<uint16_t>(1, 128),
            });
        }
        ret.push_back(std::move(p_as));
    }
    return ret;
}

cluster::topic_configuration old_random_topic_configuration() {
    cluster::topic_configuration tp_cfg;
    tp_cfg.tp_ns = model::random_topic_namespace();
    tp_cfg.properties = old_random_topic_properties();
    tp_cfg.replication_factor = random_generators::get_int<int16_t>(0, 10);
    tp_cfg.partition_count = random_generators::get_int(0, 100);
    return tp_cfg;
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
std::vector<ss::sstring> random_strings() {
    auto cnt = random_generators::get_int(0, 20);
    std::vector<ss::sstring> ret;
    ret.reserve(cnt);
    for (int i = 0; i < cnt; ++i) {
        ret.push_back(random_generators::gen_alphanum_string(
          random_generators::get_int(1, 64)));
    }
    return ret;
}
cluster::cluster_property_kv random_property_kv() {
    return {
      random_generators::gen_alphanum_string(random_generators::get_int(1, 64)),
      random_generators::gen_alphanum_string(
        random_generators::get_int(1, 64))};
}
cluster::feature_update_action random_feature_update_action() {
    cluster::feature_update_action action;
    action.action = random_generators::random_choice(
      std::vector<cluster::feature_update_action::action_t>{
        cluster::feature_update_action::action_t::activate,
        cluster::feature_update_action::action_t::deactivate,
        cluster::feature_update_action::action_t::complete_preparing,
      });
    action.feature_name = random_generators::gen_alphanum_string(32);
    return action;
}

model::producer_identity random_producer_identity() {
    return {
      random_generators::get_int(
        std::numeric_limits<int64_t>::min(),
        std::numeric_limits<int64_t>::max()),
      random_generators::get_int(
        std::numeric_limits<int16_t>::min(),
        std::numeric_limits<int16_t>::max())};
}

model::timeout_clock::duration random_timeout_clock_duration() {
    return model::timeout_clock::duration(
      random_generators::get_int<int64_t>(-100000, 100000) * 1000000);
}

cluster::tx::errc random_tx_errc() {
    return random_generators::random_choice(std::vector<cluster::tx::errc>{
      cluster::tx::errc::none,
      cluster::tx::errc::leader_not_found,
      cluster::tx::errc::shard_not_found,
      cluster::tx::errc::partition_not_found,
      cluster::tx::errc::stm_not_found,
      cluster::tx::errc::partition_not_exists,
      cluster::tx::errc::pid_not_found,
      cluster::tx::errc::timeout,
      cluster::tx::errc::conflict,
      cluster::tx::errc::fenced,
      cluster::tx::errc::stale,
      cluster::tx::errc::not_coordinator,
      cluster::tx::errc::coordinator_not_available,
      cluster::tx::errc::preparing_rebalance,
      cluster::tx::errc::rebalance_in_progress,
      cluster::tx::errc::coordinator_load_in_progress,
      cluster::tx::errc::unknown_server_error,
      cluster::tx::errc::request_rejected,
      cluster::tx::errc::invalid_producer_id_mapping,
      cluster::tx::errc::invalid_txn_state});
}

cluster::partitions_filter random_partitions_filter() {
    cluster::partitions_filter::ns_map_t ret;
    for (int i = 0, mi = random_generators::get_int(10); i < mi; i++) {
        cluster::partitions_filter::topic_map_t topics;
        for (int j = 0, mj = random_generators::get_int(10); j < mj; j++) {
            cluster::partitions_filter::partitions_set_t partitions;
            for (int k = 0, mk = random_generators::get_int(10); k < mk; k++) {
                partitions.emplace(
                  tests::random_named_int<model::partition_id>());
            }
            topics.emplace(
              tests::random_named_string<model::topic>(), partitions);
        }
        ret.emplace(tests::random_named_string<model::ns>(), topics);
    }
    return {.namespaces = ret};
}

cluster::drain_manager::drain_status random_drain_status() {
    cluster::drain_manager::drain_status data{
      .finished = tests::random_bool(),
      .errors = tests::random_bool(),
    };
    if (tests::random_bool()) {
        data.partitions = random_generators::get_int<size_t>();
    }
    if (tests::random_bool()) {
        data.eligible = random_generators::get_int<size_t>();
    }
    if (tests::random_bool()) {
        data.transferring = random_generators::get_int<size_t>();
    }
    if (tests::random_bool()) {
        data.failed = random_generators::get_int<size_t>();
    }
    return data;
}

cluster::topic_status random_topic_status() {
    cluster::partition_statuses_t partitions;
    for (int i = 0, mi = random_generators::get_int(10); i < mi; i++) {
        partitions.push_back(cluster::partition_status{
          .id = tests::random_named_int<model::partition_id>(),
          .term = tests::random_named_int<model::term_id>(),
          .leader_id = std::nullopt,
          .revision_id = tests::random_named_int<model::revision_id>(),
          .size_bytes = random_generators::get_int<size_t>(),
        });
    }
    cluster::topic_status data(
      model::random_topic_namespace(), std::move(partitions));
    return data;
}

cluster::node::local_state random_local_state() {
    auto data_disk = storage::disk{
      .path = random_generators::gen_alphanum_string(
        random_generators::get_int(20)),
      .free = random_generators::get_int<uint64_t>(),
      .total = random_generators::get_int<uint64_t>(),
    };

    // Maybe report a separate cache drive
    std::optional<storage::disk> cache_disk;
    if (random_generators::get_int(0, 1) == 0) {
        cache_disk = storage::disk{
          .path = random_generators::gen_alphanum_string(
            random_generators::get_int(20)),
          .free = random_generators::get_int<uint64_t>(),
          .total = random_generators::get_int<uint64_t>(),
        };
    }
    cluster::node::local_state data{
      .redpanda_version
      = tests::random_named_string<cluster::node::application_version>(),
      .logical_version = tests::random_named_int<cluster::cluster_version>(),
      .uptime = tests::random_duration_ms(),
      .data_disk = data_disk,
      .cache_disk = cache_disk};
    return data;
}

cluster::cluster_health_report random_cluster_health_report() {
    std::vector<cluster::node_state> node_states;
    for (auto i = 0, mi = random_generators::get_int(20); i < mi; ++i) {
        node_states.push_back(cluster::random_node_state());
    }
    std::vector<cluster::node_health_report_ptr> node_reports;
    for (auto i = 0, mi = random_generators::get_int(20); i < mi; ++i) {
        chunked_vector<cluster::topic_status> topics;
        for (auto i = 0, mi = random_generators::get_int(20); i < mi; ++i) {
            topics.push_back(random_topic_status());
        }
        auto report = cluster::node_health_report(
          tests::random_named_int<model::node_id>(),
          random_local_state(),
          std::move(topics),
          random_drain_status());

        // Reduce to an ADL-encodable state
        report.local_state.cache_disk = std::nullopt;

        node_reports.emplace_back(
          ss::make_lw_shared<cluster::node_health_report>(std::move(report)));
    }
    cluster::cluster_health_report data{
      .raft0_leader = std::nullopt,
      .node_states = node_states,
      .node_reports = std::move(node_reports),
    };
    if (tests::random_bool()) {
        data.raft0_leader = tests::random_named_int<model::node_id>();
    }
    return data;
}

model::partition_metadata random_partition_metadata() {
    model::partition_metadata data{
      tests::random_named_int<model::partition_id>(),
    };
    for (auto i = 0, mi = random_generators::get_int(20); i < mi; ++i) {
        data.replicas.push_back(model::broker_shard{
          .node_id = tests::random_named_int<model::node_id>(),
          .shard = random_generators::get_int<uint16_t>(128),
        });
    }
    if (tests::random_bool()) {
        data.leader_node = tests::random_named_int<model::node_id>();
    }
    return data;
}

SEASTAR_THREAD_TEST_CASE(serde_reflection_roundtrip) {
    roundtrip_test(cluster::ntp_leader(
      model::random_ntp(),
      tests::random_named_int<model::term_id>(),
      tests::random_named_int<model::node_id>()));

    roundtrip_test(cluster::ntp_leader_revision(
      model::random_ntp(),
      tests::random_named_int<model::term_id>(),
      tests::random_named_int<model::node_id>(),
      tests::random_named_int<model::revision_id>()));
    chunked_vector<cluster::ntp_leader_revision> l_revs;
    l_revs.emplace_back(
      model::random_ntp(),
      tests::random_named_int<model::term_id>(),
      tests::random_named_int<model::node_id>(),
      tests::random_named_int<model::revision_id>());
    roundtrip_test(cluster::update_leadership_request_v2(std::move(l_revs)));

    roundtrip_test(cluster::update_leadership_reply());

    roundtrip_test(cluster::get_leadership_request());

    fragmented_vector<cluster::ntp_leader> leaders;
    leaders.emplace_back(
      model::random_ntp(),
      tests::random_named_int<model::term_id>(),
      tests::random_named_int<model::node_id>());
    roundtrip_test(cluster::get_leadership_reply(
      std::move(leaders), cluster::get_leadership_reply::is_success::yes));

    roundtrip_test(
      cluster::allocate_id_request(random_timeout_clock_duration()));

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
    { roundtrip_test(random_remote_topic_properties()); }
    { roundtrip_test(old_random_topic_properties()); }
    { roundtrip_test(random_topic_properties()); }
    {
        roundtrip_test(
          random_property_update(random_generators::gen_alphanum_string(10)));

        roundtrip_test(random_property_update(tests::random_tristate(
          [] { return random_generators::get_int<size_t>(0, 100000); })));
    }
    {
        cluster::incremental_topic_updates updates;
        updates.compression = random_property_update(
          tests::random_optional([] { return model::random_compression(); }));
        updates.cleanup_policy_bitflags = random_property_update(
          tests::random_optional(
            [] { return model::random_cleanup_policy(); }));
        updates.compaction_strategy = random_property_update(
          tests::random_optional(
            [] { return model::random_compaction_strategy(); }));
        updates.timestamp_type = random_property_update(tests::random_optional(
          [] { return model::random_timestamp_type(); }));
        updates.segment_size = random_property_update(tests::random_optional(
          [] { return random_generators::get_int(100_MiB, 1_GiB); }));
        updates.retention_bytes = random_property_update(tests::random_tristate(
          [] { return random_generators::get_int(100_MiB, 1_GiB); }));
        updates.retention_duration = random_property_update(
          tests::random_tristate([] { return tests::random_duration_ms(); }));
        updates.remote_delete = random_property_update(tests::random_bool());
        updates.get_shadow_indexing() = random_property_update(
          tests::random_optional(
            [] { return model::random_shadow_indexing_mode(); }));
        roundtrip_test(updates);
    }
    { roundtrip_test(old_random_topic_configuration()); }
    { roundtrip_test(random_topic_configuration()); }
    { roundtrip_test(random_create_partitions_configuration()); }
    {
        cluster::topic_configuration_assignment cfg;
        cfg.cfg = old_random_topic_configuration();
        cfg.assignments = random_partition_assignments();

        roundtrip_test(cfg);
    }
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
    {
        cluster::create_acls_cmd_data data;
        for (auto i = 0, mi = random_generators::get_int(5, 25); i < mi; ++i) {
            data.bindings.push_back(tests::random_acl_binding());
        }
        roundtrip_test(data);
    }
    {
        cluster::delete_acls_cmd_data data;
        for (auto i = 0, mi = random_generators::get_int(5, 25); i < mi; ++i) {
            data.filters.push_back(tests::random_acl_binding_filter());
        }
        roundtrip_test(data);
    }
    {
        cluster::config_status status;
        status.node = tests::random_named_int<model::node_id>();
        status.restart = tests::random_bool();
        status.version = tests::random_named_int<cluster::config_version>();
        status.invalid = random_strings();
        status.unknown = random_strings();

        roundtrip_test(status);
    }
    { roundtrip_test(random_property_kv()); }
    {
        cluster::cluster_config_delta_cmd_data data;
        data.upsert = {
          random_property_kv(), random_property_kv(), random_property_kv()};
        data.remove = random_strings();

        roundtrip_test(data);
    }
    {
        cluster::cluster_config_status_cmd_data data;
        data.status.node = tests::random_named_int<model::node_id>();
        data.status.restart = tests::random_bool();
        data.status.version
          = tests::random_named_int<cluster::config_version>();
        data.status.invalid = random_strings();
        data.status.unknown = random_strings();

        roundtrip_test(data);
    }
    { roundtrip_test(random_feature_update_action()); }
    {
        cluster::feature_update_cmd_data data;
        data.actions = {
          random_feature_update_action(),
          random_feature_update_action(),
          random_feature_update_action(),
          random_feature_update_action()};
        data.logical_version
          = tests::random_named_int<cluster::cluster_version>();

        roundtrip_test(data);
    }
    {
        cluster::try_abort_request data{
          tests::random_named_int<model::partition_id>(),
          random_producer_identity(),
          tests::random_named_int<model::tx_seq>(),
          random_timeout_clock_duration()};

        roundtrip_test(data);
    }
    {
        cluster::try_abort_reply data{
          cluster::try_abort_reply::committed_type(tests::random_bool()),
          cluster::try_abort_reply::aborted_type(tests::random_bool()),
          random_tx_errc()};

        roundtrip_test(data);
    }
    {
        cluster::init_tm_tx_request data{
          tests::random_named_string<kafka::transactional_id>(),
          std::chrono::duration_cast<std::chrono::milliseconds>(
            random_timeout_clock_duration()),
          random_timeout_clock_duration()};

        roundtrip_test(data);
    }
    {
        cluster::init_tm_tx_reply data{
          random_producer_identity(), random_tx_errc()};

        roundtrip_test(data);
    }
    {
        cluster::begin_tx_request data{
          model::random_ntp(),
          random_producer_identity(),
          tests::random_named_int<model::tx_seq>(),
          std::chrono::duration_cast<std::chrono::milliseconds>(
            random_timeout_clock_duration()),
          tests::random_named_int<model::partition_id>()};

        roundtrip_test(data);
    }
    {
        cluster::begin_tx_reply data{
          model::random_ntp(),
          tests::random_named_int<model::term_id>(),
          random_tx_errc()};

        roundtrip_test(data);
    }
    {
        cluster::prepare_tx_request data{
          model::random_ntp(),
          tests::random_named_int<model::term_id>(),
          tests::random_named_int<model::partition_id>(),
          random_producer_identity(),
          tests::random_named_int<model::tx_seq>(),
          random_timeout_clock_duration()};

        roundtrip_test(data);
    }
    {
        cluster::prepare_tx_reply data{random_tx_errc()};
        roundtrip_test(data);
    }
    {
        cluster::commit_tx_request data{
          model::random_ntp(),
          random_producer_identity(),
          tests::random_named_int<model::tx_seq>(),
          random_timeout_clock_duration()};

        roundtrip_test(data);
    }
    {
        cluster::commit_tx_reply data{random_tx_errc()};
        roundtrip_test(data);
    }
    {
        cluster::abort_tx_request data{
          model::random_ntp(),
          random_producer_identity(),
          tests::random_named_int<model::tx_seq>(),
          random_timeout_clock_duration()};

        roundtrip_test(data);
    }
    {
        cluster::abort_tx_reply data{random_tx_errc()};
        roundtrip_test(data);
    }
    {
        cluster::begin_group_tx_request data{
          model::random_ntp(),
          tests::random_named_string<kafka::group_id>(),
          random_producer_identity(),
          tests::random_named_int<model::tx_seq>(),
          random_timeout_clock_duration(),
          tests::random_named_int<model::partition_id>()};

        roundtrip_test(data);
    }
    {
        // with default ntp ctor
        cluster::begin_group_tx_request data{
          tests::random_named_string<kafka::group_id>(),
          random_producer_identity(),
          tests::random_named_int<model::tx_seq>(),
          random_timeout_clock_duration(),
          tests::random_named_int<model::partition_id>()};

        roundtrip_test(data);
    }
    {
        cluster::begin_group_tx_reply data{
          tests::random_named_int<model::term_id>(), random_tx_errc()};
        roundtrip_test(data);
    }
    {
        // error only ctor
        cluster::begin_group_tx_reply data{random_tx_errc()};
        roundtrip_test(data);
    }
    {
        cluster::prepare_group_tx_request data{
          model::random_ntp(),
          tests::random_named_string<kafka::group_id>(),
          tests::random_named_int<model::term_id>(),
          random_producer_identity(),
          tests::random_named_int<model::tx_seq>(),
          random_timeout_clock_duration()};

        roundtrip_test(data);
    }
    {
        // with default ntp ctor
        cluster::prepare_group_tx_request data{
          tests::random_named_string<kafka::group_id>(),
          tests::random_named_int<model::term_id>(),
          random_producer_identity(),
          tests::random_named_int<model::tx_seq>(),
          random_timeout_clock_duration()};

        roundtrip_test(data);
    }
    {
        cluster::prepare_group_tx_reply data{random_tx_errc()};
        roundtrip_test(data);
    }
    {
        cluster::commit_group_tx_request data{
          model::random_ntp(),
          random_producer_identity(),
          tests::random_named_int<model::tx_seq>(),
          tests::random_named_string<kafka::group_id>(),
          random_timeout_clock_duration()};

        roundtrip_test(data);
    }
    {
        // with default ntp ctor
        cluster::commit_group_tx_request data{
          random_producer_identity(),
          tests::random_named_int<model::tx_seq>(),
          tests::random_named_string<kafka::group_id>(),
          random_timeout_clock_duration()};

        roundtrip_test(data);
    }
    {
        cluster::commit_group_tx_reply data{random_tx_errc()};
        roundtrip_test(data);
    }
    {
        cluster::abort_group_tx_request data{
          model::random_ntp(),
          tests::random_named_string<kafka::group_id>(),
          random_producer_identity(),
          tests::random_named_int<model::tx_seq>(),
          random_timeout_clock_duration()};

        roundtrip_test(data);
    }
    {
        // with default ntp ctor
        cluster::abort_group_tx_request data{
          tests::random_named_string<kafka::group_id>(),
          random_producer_identity(),
          tests::random_named_int<model::tx_seq>(),
          random_timeout_clock_duration()};

        roundtrip_test(data);
    }
    {
        cluster::abort_group_tx_reply data{random_tx_errc()};
        roundtrip_test(data);
    }
    {
        std::vector<std::optional<ss::net::inet_address::family>> families = {
          std::nullopt,
          ss::net::inet_address::family::INET,
          ss::net::inet_address::family::INET6};
        net::unresolved_address data{
          random_generators::gen_alphanum_string(
            random_generators::get_int(0, 100)),
          random_generators::get_int<uint16_t>(0, 16000),
          random_generators::random_choice(families)};
        roundtrip_test(data);
        // adl roundtrip doesn't work because family is not serialized
    }
    { roundtrip_test(model::random_broker_endpoint()); }
    { roundtrip_test(model::random_broker_properties()); }
    { roundtrip_test(model::random_broker()); }
    {
        cluster::configuration_update_request data{
          model::random_broker(), tests::random_named_int<model::node_id>()};
        roundtrip_test(data);
    }
    {
        cluster::configuration_update_reply data{tests::random_bool()};
        roundtrip_test(data);
    }
    {
        cluster::feature_barrier_request data{
          .tag = tests::random_named_string<cluster::feature_barrier_tag>(),
          .peer = model::node_id(random_generators::get_int(100)),
          .entered = tests::random_bool(),
        };
        roundtrip_test(data);
    }
    {
        cluster::feature_barrier_response data{
          .entered = tests::random_bool(),
          .complete = tests::random_bool(),
        };
        roundtrip_test(data);
    }
    {
        cluster::set_maintenance_mode_request data{
          .id = model::node_id(random_generators::get_int(100)),
          .enabled = tests::random_bool(),
        };
        roundtrip_test(data);
    }
    {
        cluster::set_maintenance_mode_reply data{
          .error = cluster::errc::join_request_dispatch_error,
        };
        roundtrip_test(data);
    }
    {
        std::vector<cluster::cluster_property_kv> upsert;
        for (int i = 0, mi = random_generators::get_int(10); i < mi; i++) {
            upsert.emplace_back(
              random_generators::gen_alphanum_string(
                random_generators::get_int(100)),
              random_generators::gen_alphanum_string(
                random_generators::get_int(100)));
        }
        std::vector<ss::sstring> remove;
        for (int i = 0, mi = random_generators::get_int(10); i < mi; i++) {
            remove.push_back(random_generators::gen_alphanum_string(
              random_generators::get_int(100)));
        }
        cluster::config_update_request data{
          .upsert = upsert,
          .remove = remove,
        };
        roundtrip_test(data);
    }
    {
        cluster::config_update_reply data{
          .error = cluster::errc::allocation_error,
          .latest_version = tests::random_named_int<cluster::config_version>(),
        };
        roundtrip_test(data);
    }
    {
        cluster::hello_request data{
          .peer = model::node_id(random_generators::get_int(100)),
          .start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            random_timeout_clock_duration()),
        };
        roundtrip_test(data);
    };
    {
        cluster::hello_reply data{
          .error = cluster::errc::allocation_error,
        };
        roundtrip_test(data);
    }
    {
        cluster::config_status status;
        status.node = tests::random_named_int<model::node_id>();
        status.restart = tests::random_bool();
        status.version = tests::random_named_int<cluster::config_version>();
        status.invalid = random_strings();
        status.unknown = random_strings();

        cluster::config_status_request data{
          .status = status,
        };

        roundtrip_test(data);
    }
    {
        cluster::config_status_reply data{
          .error = cluster::errc::allocation_error,
        };
        roundtrip_test(data);
    }
    {
        std::vector<uint8_t> node_uuid;
        for (int i = 0, mi = random_generators::get_int(100); i < mi; i++) {
            node_uuid.push_back(random_generators::get_int(255));
        }
        cluster::join_node_request data{
          tests::random_named_int<cluster::cluster_version>(),
          tests::random_named_int<cluster::cluster_version>(),
          node_uuid,
          model::random_broker()};
        roundtrip_test(data);
    }
    {
        cluster::join_node_reply data{
          tests::random_bool() ? cluster::join_node_reply::status_code::success
                               : cluster::join_node_reply::status_code::error,
          tests::random_named_int<model::node_id>(),
        };
        roundtrip_test(data);
    }
    {
        std::vector<model::broker_shard> new_replica_set;
        for (int i = 0, mi = random_generators::get_int(50); i < mi; i++) {
            new_replica_set.push_back(model::broker_shard{
              .node_id = tests::random_named_int<model::node_id>(),
              .shard = random_generators::get_int<uint32_t>(),
            });
        }
        cluster::finish_partition_update_request data{
          .ntp = model::random_ntp(),
          .new_replica_set = new_replica_set,
        };
        roundtrip_test(data);
    }
    {
        cluster::finish_partition_update_reply data{
          .result = cluster::errc::invalid_configuration_update,
        };
        roundtrip_test(data);
    }
    {
        cluster::property_update<std::optional<v8_engine::data_policy>>
          data_policy;
        if (tests::random_bool()) {
            data_policy.value = v8_engine::data_policy(
              random_generators::gen_alphanum_string(20),
              random_generators::gen_alphanum_string(20));
        }
        data_policy.op = random_op();

        cluster::incremental_topic_custom_updates data{
          .data_policy = data_policy,
        };
        roundtrip_test(data);
    }
    {
        cluster::property_update<std::optional<v8_engine::data_policy>>
          data_policy;
        if (tests::random_bool()) {
            data_policy.value = v8_engine::data_policy(
              random_generators::gen_alphanum_string(20),
              random_generators::gen_alphanum_string(20));
        }
        data_policy.op = random_op();
        cluster::incremental_topic_custom_updates custom_properties{
          .data_policy = data_policy,
        };

        cluster::topic_properties_update data{
          model::random_topic_namespace(),
          random_incremental_topic_updates(),
          custom_properties,
        };
        roundtrip_test(data);
    }
    {
        cluster::topic_properties_update_vector updates;
        for (int i = 0, mi = random_generators::get_int(10); i < mi; i++) {
            cluster::property_update<std::optional<v8_engine::data_policy>>
              data_policy;
            if (tests::random_bool()) {
                data_policy.value = v8_engine::data_policy(
                  random_generators::gen_alphanum_string(20),
                  random_generators::gen_alphanum_string(20));
            }
            data_policy.op = random_op();
            cluster::incremental_topic_custom_updates custom_properties{
              .data_policy = data_policy,
            };
            updates.emplace_back(
              model::random_topic_namespace(),
              random_incremental_topic_updates(),
              custom_properties);
        }
        cluster::update_topic_properties_request data{
          .updates = std::move(updates),
        };
        roundtrip_test(std::move(data));
    }
    {
        cluster::topic_result data{
          model::random_topic_namespace(),
          cluster::errc::topic_already_exists,
        };
        roundtrip_test(data);
    }
    {
        std::vector<cluster::topic_result> results;
        for (int i = 0, mi = random_generators::get_int(10); i < mi; i++) {
            results.push_back(cluster::topic_result{
              model::random_topic_namespace(),
              cluster::errc::source_topic_not_exists,
            });
        }
        cluster::update_topic_properties_reply data{
          .results = results,
        };
        roundtrip_test(data);
    }
    {
        std::vector<model::ntp> ntps;
        for (int i = 0, mi = random_generators::get_int(10); i < mi; i++) {
            ntps.push_back(model::random_ntp());
        }
        cluster::reconciliation_state_request data{
          .ntps = ntps,
        };
        roundtrip_test(data);
    }
    {
        cluster::backend_operation data{
          .source_shard = random_generators::get_int<unsigned>(1000),
          .p_as = random_partition_assignments().front(),
          .type = cluster::partition_operation_type::remove,
        };
        roundtrip_test(data);
    }
    {
        ss::chunked_fifo<cluster::backend_operation> backend_operations;
        for (int i = 0, mi = random_generators::get_int(10); i < mi; i++) {
            backend_operations.push_back(cluster::backend_operation{
              .source_shard = random_generators::get_int<unsigned>(1000),
              .p_as = random_partition_assignments().front(),
              .type = cluster::partition_operation_type::remove,
            });
        }

        cluster::ntp_reconciliation_state data{
          model::random_ntp(),
          std::move(backend_operations),
          cluster::reconciliation_status::error,
          cluster::errc::feature_disabled,
        };
        roundtrip_test(std::move(data));
    }
    {
        std::vector<cluster::ntp_reconciliation_state> results;
        for (int i = 0, mi = random_generators::get_int(10); i < mi; i++) {
            ss::chunked_fifo<cluster::backend_operation> backend_operations;
            for (int j = 0, mj = random_generators::get_int(10); j < mj; j++) {
                backend_operations.push_back(cluster::backend_operation{
                  .source_shard = random_generators::get_int<unsigned>(1000),
                  .p_as = random_partition_assignments().front(),
                  .type = cluster::partition_operation_type::remove,
                });
            }
            results.emplace_back(
              model::random_ntp(),
              std::move(backend_operations),
              cluster::reconciliation_status::error,
              cluster::errc::feature_disabled);
        }
        cluster::reconciliation_state_reply data{
          .results = std::move(results),
        };
        roundtrip_test(std::move(data));
    }
    {
        cluster::create_acls_cmd_data create_acls_data{};
        for (auto i = 0, mi = random_generators::get_int(20); i < mi; ++i) {
            create_acls_data.bindings.push_back(tests::random_acl_binding());
        }
        cluster::create_acls_request data{
          create_acls_data, random_timeout_clock_duration()};
        roundtrip_test(data);
    }
    {
        std::vector<cluster::errc> results;
        for (int i = 0, mi = random_generators::get_int(20); i < mi; i++) {
            results.push_back(cluster::errc::invalid_node_operation);
        }
        cluster::create_acls_reply data;
        roundtrip_test(data);
    }
    {
        cluster::delete_acls_cmd_data delete_acls_data{};
        for (auto i = 0, mi = random_generators::get_int(20); i < mi; ++i) {
            delete_acls_data.filters.push_back(
              tests::random_acl_binding_filter());
        }
        cluster::delete_acls_request data{
          delete_acls_data, random_timeout_clock_duration()};
        roundtrip_test(data);
    }
    {
        std::vector<security::acl_binding> bindings;
        for (auto i = 0, mi = random_generators::get_int(20); i < mi; ++i) {
            bindings.push_back(tests::random_acl_binding());
        }
        cluster::delete_acls_result data{
          .error = cluster::errc::join_request_dispatch_error,
          .bindings = bindings,
        };
        roundtrip_test(data);
    }
    {
        std::vector<cluster::delete_acls_result> results;
        for (auto i = 0, mi = random_generators::get_int(20); i < mi; ++i) {
            std::vector<security::acl_binding> bindings;
            for (auto j = 0, mj = random_generators::get_int(20); j < mj; ++j) {
                bindings.push_back(tests::random_acl_binding());
            }
            results.push_back(cluster::delete_acls_result{
              .error = cluster::errc::join_request_dispatch_error,
              .bindings = bindings,
            });
        }
        cluster::delete_acls_reply data{
          .results = results,
        };
        roundtrip_test(data);
    }
    {
        cluster::decommission_node_request data{
          .id = tests::random_named_int<model::node_id>(),
        };
        roundtrip_test(data);
    }
    {
        cluster::decommission_node_reply data{
          .error = cluster::errc::no_eligible_allocation_nodes,
        };
        roundtrip_test(data);
    }
    {
        cluster::recommission_node_request data{
          .id = tests::random_named_int<model::node_id>(),
        };
        roundtrip_test(data);
    }
    {
        cluster::recommission_node_reply data{
          .error = cluster::errc::no_eligible_allocation_nodes,
        };
        roundtrip_test(data);
    }
    {
        cluster::finish_reallocation_request data{
          .id = tests::random_named_int<model::node_id>(),
        };
        roundtrip_test(data);
    }
    {
        cluster::finish_reallocation_reply data{
          .error = cluster::errc::no_eligible_allocation_nodes,
        };
        roundtrip_test(data);
    }
    {
        cluster::feature_action_request data{
          .action = random_feature_update_action(),
        };
        roundtrip_test(data);
    }
    {
        cluster::feature_action_response data{
          .error = cluster::errc::no_eligible_allocation_nodes,
        };
        roundtrip_test(data);
    }
    {
        cluster::partitions_filter data = random_partitions_filter();
        roundtrip_test(data);
    }
    {
        cluster::node_report_filter data{
          .include_partitions = cluster::include_partitions_info(
            tests::random_bool()),
          .ntp_filters = random_partitions_filter(),
        };
        roundtrip_test(data);
    }
    {
        storage::disk data{
          .path = random_generators::gen_alphanum_string(
            random_generators::get_int(20)),
          .free = random_generators::get_int<uint64_t>(),
          .total = random_generators::get_int<uint64_t>(),
        };
        roundtrip_test(data);
    }
    {
        auto data = random_local_state();
        // local_state isn't adl encoded directly but is rather embedded
        // explicitly in the adl serialization of node_health_report's adl
        // encoding. with serde we are going to serialize the entire type tree,
        // but don't need to add adl encoding in this case.
        //
        // use a non-default value here for serde test. tests that use adl need
        roundtrip_test(data);
    }
    {
        cluster::partition_status data{
          .id = tests::random_named_int<model::partition_id>(),
          .term = tests::random_named_int<model::term_id>(),
          .leader_id = std::nullopt,
          .revision_id = tests::random_named_int<model::revision_id>(),
          .size_bytes = random_generators::get_int<size_t>(),
        };
        if (tests::random_bool()) {
            data.leader_id = tests::random_named_int<model::node_id>();
        }
        roundtrip_test(data);
    }
    {
        auto data = random_topic_status();
        roundtrip_test(data);
    }
    {
        auto data = random_drain_status();
        roundtrip_test(data);
    }
    {
        chunked_vector<cluster::topic_status> topics;
        for (auto i = 0, mi = random_generators::get_int(20); i < mi; ++i) {
            topics.push_back(random_topic_status());
        }
        cluster::node_health_report data(
          tests::random_named_int<model::node_id>(),
          random_local_state(),
          std::move(topics),
          random_drain_status());

        // Squash local_state to a form that ADL represents, since we will
        // test ADL roundtrip.
        data.local_state.cache_disk = std::nullopt;

        roundtrip_test(cluster::node_health_report_serde{data});
    }
    {
        chunked_vector<cluster::topic_status> topics;
        for (auto i = 0, mi = random_generators::get_int(20); i < mi; ++i) {
            topics.push_back(random_topic_status());
        }
        cluster::node_health_report report(
          tests::random_named_int<model::node_id>(),
          random_local_state(),
          std::move(topics),
          random_drain_status());

        // Squash to ADL-understood disk state
        report.local_state.cache_disk = report.local_state.data_disk;

        roundtrip_test(cluster::get_node_health_reply{
          .report = cluster::node_health_report_serde{report},
        });
        // try serde with non-default error code. adl doesn't encode error so
        // this is a serde only test.
        roundtrip_test(cluster::get_node_health_reply{
          .error = cluster::errc::error_collecting_health_report,
          .report = cluster::node_health_report_serde{report},
        });
    }
    {
        std::vector<model::node_id> nodes;
        for (auto i = 0, mi = random_generators::get_int(20); i < mi; ++i) {
            nodes.push_back(tests::random_named_int<model::node_id>());
        }
        cluster::cluster_report_filter data{
          .node_report_filter = cluster::
            node_report_filter{
              .include_partitions = cluster::include_partitions_info(tests::random_bool()),
              .ntp_filters = random_partitions_filter(),},
          .nodes = nodes,
        };
        roundtrip_test(data);
    }
    {
        std::vector<model::node_id> nodes;
        for (auto i = 0, mi = random_generators::get_int(20); i < mi; ++i) {
            nodes.push_back(tests::random_named_int<model::node_id>());
        }
        cluster::cluster_report_filter filter{
          .node_report_filter = cluster::
            node_report_filter{
              .include_partitions = cluster::include_partitions_info(tests::random_bool()),
              .ntp_filters = random_partitions_filter(),},
          .nodes = nodes,
        };
        cluster::get_cluster_health_request data{
          .filter = filter,
          .refresh = cluster::force_refresh(tests::random_bool()),
        };
        roundtrip_test(data);
    }
    { roundtrip_test(cluster::random_node_state()); }
    { roundtrip_test(random_cluster_health_report()); }
    {
        cluster::get_cluster_health_reply data{
          .error = cluster::errc::join_request_dispatch_error,
        };
        if (tests::random_bool()) {
            data.report = random_cluster_health_report();
        }
        roundtrip_test(std::move(data));
    }
    {
        cluster::topic_configuration_vector topics;
        for (auto i = 0, mi = random_generators::get_int(20); i < mi; ++i) {
            topics.push_back(random_topic_configuration());
        }
        cluster::create_topics_request data{
          .topics = std::move(topics),
          .timeout = random_timeout_clock_duration(),
        };
        // adl encoding for topic_configuration doesn't encode/decode to exact
        // equality, but also already existed prior to serde support being added
        // so only testing the serde case.
        roundtrip_test(std::move(data));
    }
    {
        auto data = random_partition_metadata();
        roundtrip_test(data);
    }
    {
        model::topic_metadata data{model::random_topic_namespace()};
        for (auto i = 0, mi = random_generators::get_int(10); i < mi; ++i) {
            data.partitions.push_back(random_partition_metadata());
        }
        roundtrip_test(data);
    }
    {
        cluster::create_topics_reply data;
        for (auto i = 0, mi = random_generators::get_int(10); i < mi; ++i) {
            data.results.push_back(cluster::topic_result{
              model::random_topic_namespace(),
              cluster::errc::source_topic_not_exists,
            });
        }
        for (auto i = 0, mi = random_generators::get_int(10); i < mi; ++i) {
            model::topic_metadata tm{model::random_topic_namespace()};
            for (auto i = 0, mi = random_generators::get_int(10); i < mi; ++i) {
                tm.partitions.push_back(random_partition_metadata());
            }
            data.metadata.push_back(tm);
        }
        for (auto i = 0, mi = random_generators::get_int(10); i < mi; ++i) {
            data.configs.push_back(random_topic_configuration());
        }
        // adl serialization doesn't preserve equality for topic_configuration.
        // serde serialization does and was added after support for adl so adl
        // semantics are preserved.
        roundtrip_test(std::move(data));
    }
    {
        raft::transfer_leadership_request data{
          .group = tests::random_named_int<raft::group_id>(),
        };
        if (tests::random_bool()) {
            data.target = tests::random_named_int<model::node_id>();
        }
        roundtrip_test(data);
    }
    {
        raft::transfer_leadership_reply data{
          .success = tests::random_bool(),
          .result = raft::errc::append_entries_dispatch_error,
        };
        roundtrip_test(data);
    }
    {
        raft::vnode data{
          tests::random_named_int<model::node_id>(),
          tests::random_named_int<model::revision_id>()};
        roundtrip_test(data);
    }
    {
        raft::timeout_now_request data{
          .target_node_id = raft::
            vnode{tests::random_named_int<model::node_id>(), tests::random_named_int<model::revision_id>()},
          .node_id = raft::
            vnode{tests::random_named_int<model::node_id>(), tests::random_named_int<model::revision_id>()},
          .group = tests::random_named_int<raft::group_id>(),
          .term = tests::random_named_int<model::term_id>(),
        };
        roundtrip_test(data);
    }
    {
        raft::timeout_now_reply data{
          .target_node_id = raft::
            vnode{tests::random_named_int<model::node_id>(), tests::random_named_int<model::revision_id>()},
          .term = tests::random_named_int<model::term_id>(),
          .result = raft::timeout_now_reply::status::failure,
        };
        roundtrip_test(data);
    }
    {
        const raft::install_snapshot_request orig{
          .target_node_id = raft::
            vnode{tests::random_named_int<model::node_id>(), tests::random_named_int<model::revision_id>()},
          .term = tests::random_named_int<model::term_id>(),
          .group = tests::random_named_int<raft::group_id>(),
          .node_id = raft::
            vnode{tests::random_named_int<model::node_id>(), tests::random_named_int<model::revision_id>()},
          .last_included_index = tests::random_named_int<model::offset>(),
          .file_offset = random_generators::get_int<uint64_t>(),
          .chunk = bytes_to_iobuf(
            random_generators::get_bytes(random_generators::get_int(1024))),
          .done = tests::random_bool(),
          .dirty_offset = tests::random_named_int<model::offset>(),
        };
        /*
         * manual adl/serde test to workaround iobuf being move-only
         */
        {
            raft::install_snapshot_request serde_in{
              .target_node_id = orig.target_node_id,
              .term = orig.term,
              .group = orig.group,
              .node_id = orig.node_id,
              .last_included_index = orig.last_included_index,
              .file_offset = orig.file_offset,
              .chunk = orig.chunk.copy(),
              .done = orig.done,
              .dirty_offset = orig.dirty_offset,
            };
            auto serde_out = serde::to_iobuf(std::move(serde_in));
            auto from_serde = serde::from_iobuf<raft::install_snapshot_request>(
              std::move(serde_out));

            BOOST_REQUIRE(orig == from_serde);
        }
    }
    {
        raft::install_snapshot_reply data{
          .target_node_id = raft::
            vnode{tests::random_named_int<model::node_id>(), tests::random_named_int<model::revision_id>()},
          .term = tests::random_named_int<model::term_id>(),
          .bytes_stored = random_generators::get_int<uint64_t>(),
          .success = tests::random_bool(),
        };
        roundtrip_test(data);
    }
    {
        raft::vote_request data{
          .node_id = raft::
            vnode{tests::random_named_int<model::node_id>(), tests::random_named_int<model::revision_id>()},
          .target_node_id = raft::
            vnode{tests::random_named_int<model::node_id>(), tests::random_named_int<model::revision_id>()},
          .group = tests::random_named_int<raft::group_id>(),
          .term = tests::random_named_int<model::term_id>(),
          .prev_log_index = tests::random_named_int<model::offset>(),
          .prev_log_term = tests::random_named_int<model::term_id>(),
          .leadership_transfer = tests::random_bool(),
        };
        roundtrip_test(data);
    }
    {
        raft::vote_reply data{
          .target_node_id = raft::
            vnode{tests::random_named_int<model::node_id>(), tests::random_named_int<model::revision_id>()},
          .term = tests::random_named_int<model::term_id>(),
          .granted = tests::random_bool(),
          .log_ok = tests::random_bool(),
        };
        roundtrip_test(data);
    }
    {
        raft::heartbeat_request data;

        // heartbeat request uses the first node/target_node for all of the
        // heartbeat meatdata entries. so here we arrange for that to be true in
        // the input data so that equality works as expected.
        const auto node_id = tests::random_named_int<model::node_id>();
        const auto target_node_id = tests::random_named_int<model::node_id>();

        for (auto i = 0, mi = random_generators::get_int(1, 20); i < mi; ++i) {
            raft::protocol_metadata meta{
              .group = tests::random_named_int<raft::group_id>(),
              .commit_index = tests::random_named_int<model::offset>(),
              .term = tests::random_named_int<model::term_id>(),
              .prev_log_index = tests::random_named_int<model::offset>(),
              .prev_log_term = tests::random_named_int<model::term_id>(),
              .last_visible_index = tests::random_named_int<model::offset>(),
            };
            meta.dirty_offset
              = meta.prev_log_index; // always true for heartbeats
            raft::heartbeat_metadata hm{
              .meta = meta,
              .node_id = raft::
                vnode{node_id, tests::random_named_int<model::revision_id>()},
              .target_node_id = raft::
                vnode{target_node_id, tests::random_named_int<model::revision_id>()},
            };
            data.heartbeats.push_back(hm);
        }

        // encoder will sort automatically. so for equality to work as expected
        // we use the same sorting for the input as the expected output.
        struct sorter_fn {
            constexpr bool operator()(
              const raft::heartbeat_metadata& lhs,
              const raft::heartbeat_metadata& rhs) const {
                return lhs.meta.commit_index < rhs.meta.commit_index;
            }
        };

        std::sort(data.heartbeats.begin(), data.heartbeats.end(), sorter_fn{});

        // serde round trip test async version
        {
            auto serde_in = data;
            iobuf serde_out;
            serde::write_async(serde_out, std::move(serde_in)).get();
            auto from_serde = serde::from_iobuf<raft::heartbeat_request>(
              std::move(serde_out));
            BOOST_REQUIRE(data == from_serde);
        }
    }
    {
        raft::heartbeat_reply data;

        // heartbeat reply uses the first node/target_node for all of the
        // reply meatdata entries. so here we arrange for that to be true in
        // the input data so that equality works as expected.
        const auto node_id = tests::random_named_int<model::node_id>();
        const auto target_node_id = tests::random_named_int<model::node_id>();

        for (auto i = 0, mi = random_generators::get_int(1, 20); i < mi; ++i) {
            raft::append_entries_reply reply{
              .target_node_id = raft::
                vnode{target_node_id, tests::random_named_int<model::revision_id>()},
              .node_id = raft::
                vnode{node_id, tests::random_named_int<model::revision_id>()},
              .group = tests::random_named_int<raft::group_id>(),
              .term = tests::random_named_int<model::term_id>(),
              .last_flushed_log_index
              = tests::random_named_int<model::offset>(),
              .last_dirty_log_index = tests::random_named_int<model::offset>(),
              .last_term_base_offset = tests::random_named_int<model::offset>(),
              .result = raft::reply_result::group_unavailable,
            };
            data.meta.push_back(reply);
        }

        // encoder will sort automatically. so for equality to work as expected
        // we use the same sorting for the input as the expected output.
        struct sorter_fn {
            constexpr bool operator()(
              const raft::append_entries_reply& lhs,
              const raft::append_entries_reply& rhs) const {
                return lhs.last_flushed_log_index < rhs.last_flushed_log_index;
            }
        };

        std::sort(data.meta.begin(), data.meta.end(), sorter_fn{});

        roundtrip_test(data);
    }
    {
        raft::protocol_metadata data{
          .group = tests::random_named_int<raft::group_id>(),
          .commit_index = tests::random_named_int<model::offset>(),
          .term = tests::random_named_int<model::term_id>(),
          .prev_log_index = tests::random_named_int<model::offset>(),
          .prev_log_term = tests::random_named_int<model::term_id>(),
          .last_visible_index = tests::random_named_int<model::offset>(),
          .dirty_offset = tests::random_named_int<model::offset>(),
        };
        roundtrip_test(data);
    }
    {
        const auto gold
          = model::test::make_random_batches(model::offset(0), 20).get();

        // make a copy of the source batches for later comparison because the
        // copy moved into the request will get eaten.
        ss::circular_buffer<model::record_batch> batches_in;
        for (const auto& batch : gold) {
            batches_in.push_back(batch.copy());
        }

        raft::protocol_metadata pmd{
          .group = tests::random_named_int<raft::group_id>(),
          .commit_index = tests::random_named_int<model::offset>(),
          .term = tests::random_named_int<model::term_id>(),
          .prev_log_index = tests::random_named_int<model::offset>(),
          .prev_log_term = tests::random_named_int<model::term_id>(),
          .last_visible_index = tests::random_named_int<model::offset>(),
          .dirty_offset = tests::random_named_int<model::offset>(),
        };

        raft::append_entries_request data{
          raft::vnode{
            tests::random_named_int<model::node_id>(),
            tests::random_named_int<model::revision_id>()},
          raft::vnode{
            tests::random_named_int<model::node_id>(),
            tests::random_named_int<model::revision_id>()},
          pmd,
          model::make_memory_record_batch_reader(std::move(batches_in)),
          raft::flush_after_append(tests::random_bool()),
        };

        // append_entries_request -> iobuf
        iobuf serde_out;
        const auto node_id = data.source_node();
        const auto target_node_id = data.target_node();
        const auto meta = data.metadata();
        const auto flush = data.is_flush_required();
        serde::write_async(serde_out, std::move(data)).get();

        // iobuf -> append_entries_request
        iobuf_parser serde_in(std::move(serde_out));
        auto from_serde
          = serde::read_async<raft::append_entries_request>(serde_in).get();

        BOOST_REQUIRE(from_serde.source_node() == node_id);
        BOOST_REQUIRE(from_serde.target_node() == target_node_id);
        BOOST_REQUIRE(from_serde.metadata() == meta);
        BOOST_REQUIRE(from_serde.is_flush_required() == flush);

        auto batches_from_serde = model::consume_reader_to_memory(
                                    std::move(from_serde).release_batches(),
                                    model::no_timeout)
                                    .get();
        BOOST_REQUIRE(gold.size() > 0);
        BOOST_REQUIRE(batches_from_serde.size() == gold.size());
        for (size_t i = 0; i < gold.size(); i++) {
            BOOST_REQUIRE(batches_from_serde[i] == gold[i]);
        }
    }
    {
        raft::append_entries_reply data{
          .target_node_id = raft::
            vnode{tests::random_named_int<model::node_id>(), tests::random_named_int<model::revision_id>()},
          .node_id = raft::
            vnode{tests::random_named_int<model::node_id>(), tests::random_named_int<model::revision_id>()},
          .group = tests::random_named_int<raft::group_id>(),
          .term = tests::random_named_int<model::term_id>(),
          .last_flushed_log_index = tests::random_named_int<model::offset>(),
          .last_dirty_log_index = tests::random_named_int<model::offset>(),
          .last_term_base_offset = tests::random_named_int<model::offset>(),
          .result = raft::reply_result::group_unavailable,
          .may_recover = tests::random_bool(),
        };
        roundtrip_test(data);
    }
    {
        cluster::cancel_node_partition_movements_request request{
          .node_id = tests::random_named_int<model::node_id>(),
          .direction = random_generators::random_choice(
            std::vector<cluster::partition_move_direction>{
              cluster::partition_move_direction::from_node,
              cluster::partition_move_direction::to_node,
              cluster::partition_move_direction::all}),
        };
        roundtrip_test(request);
    }
    {
        // Test schema ID validation topic_create
        auto key_validation = tests::random_bool();
        auto key_strategy = tests::random_subject_name_strategy();
        auto val_validation = tests::random_bool();
        auto val_strategy = tests::random_subject_name_strategy();

        cluster::topic_properties props{};

        props.record_key_schema_id_validation = tests::random_optional(
          [=] { return key_validation; });
        props.record_key_schema_id_validation_compat = tests::random_optional(
          [=] { return key_validation; });
        props.record_key_subject_name_strategy = tests::random_optional(
          [=] { return key_strategy; });
        props.record_key_subject_name_strategy_compat = tests::random_optional(
          [=] { return key_strategy; });
        props.record_value_schema_id_validation = tests::random_optional(
          [=] { return val_validation; });
        props.record_value_schema_id_validation_compat = tests::random_optional(
          [=] { return val_validation; });
        props.record_value_subject_name_strategy = tests::random_optional(
          [=] { return val_strategy; });
        props.record_value_subject_name_strategy_compat
          = tests::random_optional([=] { return val_strategy; });

        roundtrip_test(props);
    }
    {
        // Test schema ID validation incremental_topic_updates
        auto key_validation = tests::random_bool();
        auto key_strategy = tests::random_subject_name_strategy();
        auto val_validation = tests::random_bool();
        auto val_strategy = tests::random_subject_name_strategy();

        cluster::incremental_topic_updates updates;
        updates.record_key_schema_id_validation = random_property_update(
          tests::random_optional([=] { return key_validation; }));
        updates.record_key_schema_id_validation_compat = random_property_update(
          tests::random_optional([=] { return key_validation; }));
        updates.record_key_subject_name_strategy = random_property_update(
          tests::random_optional([=] { return key_strategy; }));
        updates.record_key_subject_name_strategy_compat
          = random_property_update(
            tests::random_optional([=] { return key_strategy; }));
        updates.record_value_schema_id_validation = random_property_update(
          tests::random_optional([=] { return val_validation; }));
        updates.record_value_schema_id_validation_compat
          = random_property_update(
            tests::random_optional([=] { return val_validation; }));
        updates.record_value_subject_name_strategy = random_property_update(
          tests::random_optional([=] { return val_strategy; }));
        updates.record_value_subject_name_strategy_compat
          = random_property_update(
            tests::random_optional([=] { return val_strategy; }));

        roundtrip_test(updates);
    }
}

SEASTAR_THREAD_TEST_CASE(cluster_property_kv_exchangable_with_pair) {
    using pairs_t = std::vector<std::pair<ss::sstring, ss::sstring>>;
    using kvs_t = std::vector<cluster::cluster_property_kv>;
    kvs_t kvs;
    pairs_t pairs;

    for (int i = 0; i < 10; ++i) {
        auto k = random_generators::gen_alphanum_string(
          random_generators::get_int(10, 20));
        auto v = random_generators::gen_alphanum_string(
          random_generators::get_int(10, 20));

        kvs.emplace_back(k, v);
        pairs.emplace_back(k, v);
    }

    auto serialized_pairs = reflection::to_iobuf(pairs);
    auto serialized_kvs = reflection::to_iobuf(kvs);

    auto deserialized_pairs_from_kvs = reflection::from_iobuf<pairs_t>(
      serialized_kvs.copy());

    auto deserialized_pairs_from_pairs = reflection::from_iobuf<pairs_t>(
      serialized_pairs.copy());

    auto deserialized_kvs_from_kvs = reflection::from_iobuf<kvs_t>(
      serialized_kvs.copy());

    auto deserialized_kvs_from_pairs = reflection::from_iobuf<kvs_t>(
      serialized_pairs.copy());

    BOOST_REQUIRE(deserialized_pairs_from_kvs == deserialized_pairs_from_pairs);
    BOOST_REQUIRE(deserialized_kvs_from_kvs == deserialized_kvs_from_pairs);
    for (auto i = 0; i < 10; ++i) {
        BOOST_REQUIRE_EQUAL(
          deserialized_pairs_from_kvs[i].first,
          deserialized_kvs_from_pairs[i].key);
        BOOST_REQUIRE_EQUAL(
          deserialized_pairs_from_kvs[i].second,
          deserialized_kvs_from_pairs[i].value);
        BOOST_REQUIRE_EQUAL(
          deserialized_pairs_from_pairs[i].second,
          deserialized_kvs_from_kvs[i].value);
        BOOST_REQUIRE_EQUAL(
          deserialized_pairs_from_pairs[i].second,
          deserialized_kvs_from_kvs[i].value);
    }
}
template<typename Cmd>
requires cluster::ControllerCommand<Cmd>
ss::future<model::record_batch> serialize_cmd(Cmd cmd) {
    return ss::do_with(
      iobuf{},
      iobuf{},
      [cmd = std::move(cmd)](iobuf& key_buf, iobuf& value_buf) mutable {
          auto value_f
            = reflection::async_adl<cluster::command_type>{}
                .to(value_buf, Cmd::type)
                .then([&value_buf, v = std::move(cmd.value)]() mutable {
                    return reflection::adl<typename Cmd::value_t>{}.to(
                      value_buf, std::move(v));
                });
          auto key_f = reflection::async_adl<typename Cmd::key_t>{}.to(
            key_buf, std::move(cmd.key));
          return ss::when_all_succeed(std::move(key_f), std::move(value_f))
            .discard_result()
            .then([&key_buf, &value_buf]() mutable {
                cluster::simple_batch_builder builder(
                  Cmd::batch_type, model::offset(0));
                builder.add_raw_kv(std::move(key_buf), std::move(value_buf));
                return std::move(builder).build();
            });
      });
}

template<typename Cmd, typename Key, typename Value>
void serde_roundtrip_cmd(Key key, Value value) {
    auto cmd = Cmd(std::move(key), std::move(value));
    auto batch = cluster::serde_serialize_cmd(cmd);
    auto deserialized = cluster::deserialize(
                          std::move(batch), cluster::make_commands_list<Cmd>())
                          .get();

    auto deserialized_cmd = std::get<Cmd>(deserialized);

    BOOST_REQUIRE(deserialized_cmd.key == cmd.key);
    if constexpr (std::equality_comparable<Value>) {
        BOOST_REQUIRE(deserialized_cmd.value == cmd.value);
    }
}

template<typename Cmd, typename Key, typename Value>
void adl_roundtrip_cmd(Key key, Value value) {
    auto cmd = Cmd(std::move(key), std::move(value));
    auto batch = serialize_cmd(cmd).get();
    auto deserialized = cluster::deserialize(
                          std::move(batch), cluster::make_commands_list<Cmd>())
                          .get();
    auto deserialized_cmd = std::get<Cmd>(deserialized);

    BOOST_REQUIRE(deserialized_cmd.key == cmd.key);

    if constexpr (std::equality_comparable<Value>) {
        BOOST_REQUIRE(deserialized_cmd.value == cmd.value);
    }
}

template<typename Cmd, typename Key, typename Value>
void roundtrip_cmd(Key key, Value value) {
    adl_roundtrip_cmd<Cmd>(key, value);
    serde_roundtrip_cmd<Cmd>(key, value);
}

SEASTAR_THREAD_TEST_CASE(commands_serialization_test) {
    for (int i = 0; i < 50; ++i) {
        serde_roundtrip_cmd<cluster::create_topic_cmd>(
          model::random_topic_namespace(),
          cluster::topic_configuration_assignment(
            random_topic_configuration(), random_partition_assignments()));

        roundtrip_cmd<cluster::delete_topic_cmd>(
          model::random_topic_namespace(), model::random_topic_namespace());

        roundtrip_cmd<cluster::move_partition_replicas_cmd>(
          model::random_ntp(),
          std::vector<model::broker_shard>{
            {model::node_id(10), 1}, {model::node_id(1), 2}});

        roundtrip_cmd<cluster::finish_moving_partition_replicas_cmd>(
          model::random_ntp(),
          std::vector<model::broker_shard>{
            {model::node_id(10), 1}, {model::node_id(1), 2}});

        roundtrip_cmd<cluster::update_topic_properties_cmd>(
          model::random_topic_namespace(), random_incremental_topic_updates());

        roundtrip_cmd<cluster::create_partition_cmd>(
          model::random_topic_namespace(),
          cluster::create_partitions_configuration_assignment(
            random_create_partitions_configuration(),
            random_partition_assignments()));

        roundtrip_cmd<cluster::create_user_cmd>(
          tests::random_named_string<security::credential_user>(),
          tests::random_credential());

        roundtrip_cmd<cluster::delete_user_cmd>(
          tests::random_named_string<security::credential_user>(), 0);

        roundtrip_cmd<cluster::update_user_cmd>(
          tests::random_named_string<security::credential_user>(),
          tests::random_credential());

        cluster::create_acls_cmd_data create_acls_data{};
        for (auto i = 0, mi = random_generators::get_int(1, 20); i < mi; ++i) {
            create_acls_data.bindings.push_back(tests::random_acl_binding());
        }

        roundtrip_cmd<cluster::create_acls_cmd>(std::move(create_acls_data), 0);

        cluster::delete_acls_cmd_data delete_acl_data;

        for (auto i = 0, mi = random_generators::get_int(1, 20); i < mi; ++i) {
            delete_acl_data.filters.push_back(
              tests::random_acl_binding_filter());
        }

        roundtrip_cmd<cluster::delete_acls_cmd>(std::move(delete_acl_data), 0);

        roundtrip_cmd<cluster::decommission_node_cmd>(
          tests::random_named_int<model::node_id>(), 0);

        roundtrip_cmd<cluster::recommission_node_cmd>(
          tests::random_named_int<model::node_id>(), 0);

        roundtrip_cmd<cluster::finish_reallocations_cmd>(
          tests::random_named_int<model::node_id>(), 0);

        roundtrip_cmd<cluster::maintenance_mode_cmd>(
          tests::random_named_int<model::node_id>(), 0);

        cluster::cluster_config_delta_cmd_data config_delta;
        for (auto i = 0, mi = random_generators::get_int(1, 20); i < mi; ++i) {
            config_delta.upsert.push_back(random_property_kv());
        }
        config_delta.remove = random_strings();

        roundtrip_cmd<cluster::cluster_config_delta_cmd>(
          tests::random_named_int<cluster::config_version>(),
          std::move(config_delta));

        cluster::cluster_config_status_cmd_data config_status;

        config_status.status.invalid = random_strings();
        config_status.status.restart = tests::random_bool();
        config_status.status.node = tests::random_named_int<model::node_id>();
        config_status.status.unknown = random_strings();
        config_status.status.version
          = tests::random_named_int<cluster::config_version>();

        roundtrip_cmd<cluster::cluster_config_status_cmd>(
          tests::random_named_int<model::node_id>(), std::move(config_status));

        cluster::feature_update_cmd_data feature_update_data;
        for (auto i = 0, mi = random_generators::get_int(1, 20); i < mi; ++i) {
            feature_update_data.actions.push_back(
              random_feature_update_action());
        }
        feature_update_data.logical_version
          = tests::random_named_int<cluster::cluster_version>();

        roundtrip_cmd<cluster::feature_update_cmd>(
          std::move(feature_update_data), 0);

        roundtrip_cmd<cluster::cancel_moving_partition_replicas_cmd>(
          model::random_ntp(),
          cluster::cancel_moving_partition_replicas_cmd_data(
            cluster::force_abort_update(tests::random_bool())));
    }
}
