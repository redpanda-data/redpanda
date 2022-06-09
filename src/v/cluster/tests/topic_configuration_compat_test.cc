// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tests/utils.h"
#include "cluster/types.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "reflection/adl.h"
#include "test_utils/randoms.h"
#include "test_utils/rpc.h"
#include "units.h"
#include "v8_engine/data_policy.h"

#include <seastar/core/loop.hh>
#include <seastar/testing/thread_test_case.hh>

#include <chrono>

using namespace std::chrono_literals; // NOLINT

namespace old {
struct topic_properties {
    std::optional<model::compression> compression;
    std::optional<model::cleanup_policy_bitflags> cleanup_policy_bitflags;
    std::optional<model::compaction_strategy> compaction_strategy;
    std::optional<model::timestamp_type> timestamp_type;
    std::optional<size_t> segment_size;
    tristate<size_t> retention_bytes{std::nullopt};
    tristate<std::chrono::milliseconds> retention_duration{std::nullopt};
};

struct topic_configuration {
    topic_configuration(model::ns n, model::topic t, int32_t count, int16_t rf)
      : tp_ns(std::move(n), std::move(t))
      , partition_count(count)
      , replication_factor(rf) {}

    model::topic_namespace tp_ns;
    // using signed integer because Kafka protocol defines it as signed int
    int32_t partition_count;
    // using signed integer because Kafka protocol defines it as signed int
    int16_t replication_factor;

    topic_properties properties;
};

struct create_topics_request {
    std::vector<topic_configuration> topics;
    model::timeout_clock::duration timeout;
};

struct create_topics_reply {
    std::vector<cluster::topic_result> results;
    std::vector<model::topic_metadata> metadata;
    std::vector<topic_configuration> configs;
};

struct topic_configuration_assignment {
    topic_configuration_assignment() = delete;

    topic_configuration_assignment(
      topic_configuration cfg, std::vector<cluster::partition_assignment> pas)
      : cfg(std::move(cfg))
      , assignments(std::move(pas)) {}

    topic_configuration cfg;
    std::vector<cluster::partition_assignment> assignments;
};

struct incremental_topic_updates_1 {
    static constexpr int8_t version = -1;
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

struct incremental_topic_updates_2 {
    static constexpr int8_t version = -2;
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

/**
 * Struct representing single topic properties update
 */
struct topic_properties_update_v1 {
    explicit topic_properties_update_v1(model::topic_namespace tp_ns)
      : tp_ns(std::move(tp_ns)) {}

    model::topic_namespace tp_ns;
    incremental_topic_updates_1 properties;
};
struct topic_properties_update_v2 {
    explicit topic_properties_update_v2(model::topic_namespace tp_ns)
      : tp_ns(std::move(tp_ns)) {}

    model::topic_namespace tp_ns;
    incremental_topic_updates_2 properties;
};

struct update_topic_properties_request {
    std::vector<topic_properties_update_v2> updates;
};

} // namespace old

namespace reflection {

template<>
struct adl<old::topic_configuration> {
    void to(iobuf& out, old::topic_configuration&& t) {
        reflection::serialize(
          out,
          t.tp_ns,
          t.partition_count,
          t.replication_factor,
          t.properties.compression,
          t.properties.cleanup_policy_bitflags,
          t.properties.compaction_strategy,
          t.properties.timestamp_type,
          t.properties.segment_size,
          t.properties.retention_bytes,
          t.properties.retention_duration);
        // No recovery field
    }
    old::topic_configuration from(iobuf_parser& in) {
        auto ns = model::ns(adl<ss::sstring>{}.from(in));
        auto topic = model::topic(adl<ss::sstring>{}.from(in));
        auto partition_count = adl<int32_t>{}.from(in);
        auto rf = adl<int16_t>{}.from(in);

        auto cfg = old::topic_configuration(
          std::move(ns), std::move(topic), partition_count, rf);

        cfg.properties.compression
          = adl<std::optional<model::compression>>{}.from(in);
        cfg.properties.cleanup_policy_bitflags
          = adl<std::optional<model::cleanup_policy_bitflags>>{}.from(in);
        cfg.properties.compaction_strategy
          = adl<std::optional<model::compaction_strategy>>{}.from(in);
        cfg.properties.timestamp_type
          = adl<std::optional<model::timestamp_type>>{}.from(in);
        cfg.properties.segment_size = adl<std::optional<size_t>>{}.from(in);
        cfg.properties.retention_bytes = adl<tristate<size_t>>{}.from(in);
        cfg.properties.retention_duration
          = adl<tristate<std::chrono::milliseconds>>{}.from(in);
        // No recovery field

        return cfg;
    }
};

template<>
struct adl<old::create_topics_request> {
    void to(iobuf& out, old::create_topics_request&& r) {
        reflection::serialize(out, std::move(r.topics), r.timeout);
    }
    old::create_topics_request from(iobuf io) {
        return reflection::from_iobuf<old::create_topics_request>(
          std::move(io));
    }
    old::create_topics_request from(iobuf_parser& in) {
        using underlying_t = std::vector<old::topic_configuration>;
        auto configs = adl<underlying_t>().from(in);
        auto timeout = adl<model::timeout_clock::duration>().from(in);
        return old::create_topics_request{std::move(configs), timeout};
    }
};

template<>
struct adl<old::create_topics_reply> {
    void to(iobuf& out, old::create_topics_reply&& r) {
        reflection::serialize(
          out,
          std::move(r.results),
          std::move(r.metadata),
          std::move(r.configs));
    }

    old::create_topics_reply from(iobuf io) {
        return reflection::from_iobuf<old::create_topics_reply>(std::move(io));
    }

    old::create_topics_reply from(iobuf_parser& in) {
        auto results = adl<std::vector<cluster::topic_result>>().from(in);
        auto md = adl<std::vector<model::topic_metadata>>().from(in);
        auto cfg = adl<std::vector<old::topic_configuration>>().from(in);
        return old::create_topics_reply{
          std::move(results), std::move(md), std::move(cfg)};
    }
};

template<>
struct adl<old::topic_configuration_assignment> {
    void to(iobuf& b, old::topic_configuration_assignment&& assigned_cfg) {
        reflection::serialize(
          b, std::move(assigned_cfg.cfg), std::move(assigned_cfg.assignments));
    }
    old::topic_configuration_assignment from(iobuf_parser& in) {
        auto cfg = adl<old::topic_configuration>{}.from(in);
        auto assignments
          = adl<std::vector<cluster::partition_assignment>>{}.from(in);
        return old::topic_configuration_assignment(
          std::move(cfg), std::move(assignments));
    }
};

template<>
struct adl<old::incremental_topic_updates_1> {
    void to(iobuf& out, old::incremental_topic_updates_1&& t) {
        reflection::serialize(
          out,
          old::incremental_topic_updates_1::version,
          t.compression,
          t.cleanup_policy_bitflags,
          t.compaction_strategy,
          t.timestamp_type,
          t.segment_size,
          t.retention_bytes,
          t.retention_duration,
          t.data_policy);
    }

    old::incremental_topic_updates_1 from(iobuf_parser& in) {
        auto version = adl<int8_t>{}.from(in.peek(1));
        if (version < 0) {
            // Consume version from stream
            in.skip(1);
            vassert(
              version >= old::incremental_topic_updates_1::version,
              "topic_configuration version {} is not supported",
              version);
        } else {
            version = 0;
        }

        old::incremental_topic_updates_1 updates;
        updates.compression
          = adl<cluster::property_update<std::optional<model::compression>>>{}
              .from(in);
        updates.cleanup_policy_bitflags = adl<cluster::property_update<
          std::optional<model::cleanup_policy_bitflags>>>{}
                                            .from(in);
        updates.compaction_strategy = adl<cluster::property_update<
          std::optional<model::compaction_strategy>>>{}
                                        .from(in);
        updates.timestamp_type
          = adl<
              cluster::property_update<std::optional<model::timestamp_type>>>{}
              .from(in);
        updates.segment_size
          = adl<cluster::property_update<std::optional<size_t>>>{}.from(in);
        updates.retention_bytes
          = adl<cluster::property_update<tristate<size_t>>>{}.from(in);
        updates.retention_duration
          = adl<cluster::property_update<tristate<std::chrono::milliseconds>>>{}
              .from(in);
        if (version < 0) {
            updates.data_policy = adl<cluster::property_update<
              std::optional<v8_engine::data_policy>>>{}
                                    .from(in);
        }
        return updates;
    }
};

template<>
struct adl<old::incremental_topic_updates_2> {
    void to(iobuf& out, old::incremental_topic_updates_2&& t) {
        reflection::serialize(
          out,
          old::incremental_topic_updates_2::version,
          t.compression,
          t.cleanup_policy_bitflags,
          t.compaction_strategy,
          t.timestamp_type,
          t.segment_size,
          t.retention_bytes,
          t.retention_duration);
    }
    old::incremental_topic_updates_2 from(iobuf_parser& in) {
        auto version = adl<int8_t>{}.from(in.peek(1));
        if (version < 0) {
            // Consume version from stream
            in.skip(1);
            vassert(
              version >= old::incremental_topic_updates_2::version,
              "topic_configuration version {} is not supported",
              version);
        } else {
            version = 0;
        }

        old::incremental_topic_updates_2 updates;
        updates.compression
          = adl<cluster::property_update<std::optional<model::compression>>>{}
              .from(in);
        updates.cleanup_policy_bitflags = adl<cluster::property_update<
          std::optional<model::cleanup_policy_bitflags>>>{}
                                            .from(in);
        updates.compaction_strategy = adl<cluster::property_update<
          std::optional<model::compaction_strategy>>>{}
                                        .from(in);
        updates.timestamp_type
          = adl<
              cluster::property_update<std::optional<model::timestamp_type>>>{}
              .from(in);
        updates.segment_size
          = adl<cluster::property_update<std::optional<size_t>>>{}.from(in);
        updates.retention_bytes
          = adl<cluster::property_update<tristate<size_t>>>{}.from(in);
        updates.retention_duration
          = adl<cluster::property_update<tristate<std::chrono::milliseconds>>>{}
              .from(in);
        return updates;
    }
};

template<>
struct adl<old::topic_properties_update_v1> {
    void to(iobuf& out, old::topic_properties_update_v1&& r) {
        reflection::serialize(out, r.tp_ns, r.properties);
    }

    old::topic_properties_update_v1 from(iobuf_parser& parser) {
        auto tp_ns = adl<model::topic_namespace>{}.from(parser);
        old::topic_properties_update_v1 ret(std::move(tp_ns));
        ret.properties = adl<old::incremental_topic_updates_1>{}.from(parser);
        return ret;
    }
};

template<>
struct adl<old::topic_properties_update_v2> {
    void to(iobuf& out, old::topic_properties_update_v2&& r) {
        reflection::serialize(out, r.tp_ns, r.properties);
    }

    old::topic_properties_update_v2 from(iobuf_parser& parser) {
        auto tp_ns = adl<model::topic_namespace>{}.from(parser);
        old::topic_properties_update_v2 ret(std::move(tp_ns));
        ret.properties = adl<old::incremental_topic_updates_2>{}.from(parser);
        return ret;
    }
};

} // namespace reflection

template<typename T, typename N>
N serialize_upgrade_rpc(T&& t) {
    iobuf io = reflection::to_iobuf(std::forward<T>(t));
    iobuf_parser parser(std::move(io));
    return reflection::adl<N>{}.from(parser);
}

template<class CfgIn, class CfgOut>
void topic_config_roundtrip() {
    CfgIn cfg(model::ns("test"), model::topic{"a_topic"}, 3, 1);

    cfg.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::deletion
        | model::cleanup_policy_bitflags::compaction;
    cfg.properties.compaction_strategy = model::compaction_strategy::offset;
    cfg.properties.compression = model::compression::snappy;
    cfg.properties.segment_size = std::optional<size_t>(1_GiB);
    cfg.properties.retention_bytes = tristate<size_t>{};
    cfg.properties.retention_duration = tristate<std::chrono::milliseconds>(
      10h);
    if constexpr (std::is_same<CfgIn, cluster::topic_configuration>::value) {
        // Init new fields
        cfg.properties.recovery = true;
        cfg.properties.shadow_indexing = model::shadow_indexing_mode::archival;
    }

    auto d = serialize_upgrade_rpc<CfgIn, CfgOut>(std::move(cfg));

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

    if constexpr (std::is_same<CfgIn, cluster::topic_configuration>::value) {
        // Init new fields
        BOOST_REQUIRE(d.properties.recovery == true);
        BOOST_REQUIRE(
          d.properties.shadow_indexing
          == model::shadow_indexing_mode::archival);
    } else {
        BOOST_REQUIRE_EQUAL(false, d.properties.recovery.has_value());
        BOOST_REQUIRE_EQUAL(false, d.properties.shadow_indexing.has_value());
    }
}

SEASTAR_THREAD_TEST_CASE(topic_config_upgrade_rt_test) {
    topic_config_roundtrip<
      old::topic_configuration,
      cluster::topic_configuration>();
}

SEASTAR_THREAD_TEST_CASE(topic_config_uniform_rt_test) {
    topic_config_roundtrip<
      cluster::topic_configuration,
      cluster::topic_configuration>();
}

void topic_config_with_recovery_field_roundtrip(
  std::optional<bool> recovery_field,
  std::optional<model::shadow_indexing_mode> si_mode) {
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
    cfg.properties.recovery = recovery_field;
    cfg.properties.shadow_indexing = si_mode;

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
    BOOST_REQUIRE_EQUAL(recovery_field, d.properties.recovery);
    BOOST_REQUIRE_EQUAL(si_mode, d.properties.shadow_indexing);
}

SEASTAR_THREAD_TEST_CASE(topic_config_with_recovery_field_null_null_rt_test) {
    topic_config_with_recovery_field_roundtrip(std::nullopt, std::nullopt);
}

SEASTAR_THREAD_TEST_CASE(
  topic_config_with_recovery_field_true_archival_rt_test) {
    topic_config_with_recovery_field_roundtrip(
      true, model::shadow_indexing_mode::archival);
}

SEASTAR_THREAD_TEST_CASE(
  topic_config_with_recovery_field_true_shadow_indexing_rt_test) {
    topic_config_with_recovery_field_roundtrip(
      true, model::shadow_indexing_mode::fetch);
}

SEASTAR_THREAD_TEST_CASE(
  topic_config_with_recovery_field_false_disabled_rt_test) {
    topic_config_with_recovery_field_roundtrip(
      false, model::shadow_indexing_mode::disabled);
}

template<class ReqIn, class ReqOut>
void create_topics_request_roundtrip() {
    ReqIn req;
    using cfg_t = typename std::decay<decltype(req.topics.front())>::type;
    auto t1 = cfg_t(model::ns("default"), model::topic("tp-1"), 12, 3);
    auto t2 = cfg_t(model::ns("default"), model::topic("tp-2"), 6, 5);
    if constexpr (std::is_same<cfg_t, cluster::topic_configuration>::value) {
        t1.properties.recovery = false;
        t1.properties.shadow_indexing = model::shadow_indexing_mode::archival;
        t2.properties.recovery = true;
        t2.properties.shadow_indexing = model::shadow_indexing_mode::fetch;
    }
    req.topics = {t1, t2};
    req.timeout = std::chrono::seconds(10101);
    auto res = serialize_upgrade_rpc<ReqIn, ReqOut>(std::move(req));

    BOOST_CHECK(res.timeout == std::chrono::seconds(10101));
    BOOST_REQUIRE_EQUAL(res.topics[0].partition_count, 12);
    BOOST_REQUIRE_EQUAL(res.topics[0].replication_factor, 3);
    BOOST_REQUIRE_EQUAL(res.topics[0].tp_ns.ns, model::ns("default"));
    BOOST_REQUIRE_EQUAL(res.topics[0].tp_ns.tp, model::topic("tp-1"));
    if constexpr (std::is_same<cfg_t, cluster::topic_configuration>::value) {
        BOOST_REQUIRE_EQUAL(res.topics[0].properties.recovery.value(), false);
        BOOST_REQUIRE_EQUAL(
          res.topics[0].properties.shadow_indexing.value(),
          model::shadow_indexing_mode::archival);
    } else {
        BOOST_REQUIRE_EQUAL(
          res.topics[0].properties.recovery.has_value(), false);
        BOOST_REQUIRE_EQUAL(
          res.topics[0].properties.shadow_indexing.has_value(), false);
    }

    BOOST_REQUIRE_EQUAL(res.topics[1].partition_count, 6);
    BOOST_REQUIRE_EQUAL(res.topics[1].replication_factor, 5);
    BOOST_REQUIRE_EQUAL(res.topics[1].tp_ns.ns, model::ns("default"));
    BOOST_REQUIRE_EQUAL(res.topics[1].tp_ns.tp, model::topic("tp-2"));
    if constexpr (std::is_same<cfg_t, cluster::topic_configuration>::value) {
        BOOST_REQUIRE_EQUAL(res.topics[1].properties.recovery.value(), true);
        BOOST_REQUIRE_EQUAL(
          res.topics[1].properties.shadow_indexing.value(),
          model::shadow_indexing_mode::fetch);
    } else {
        BOOST_REQUIRE_EQUAL(
          res.topics[1].properties.recovery.has_value(), false);
        BOOST_REQUIRE_EQUAL(
          res.topics[1].properties.shadow_indexing.has_value(), false);
    }
}

SEASTAR_THREAD_TEST_CASE(create_topics_request_upgrade_rt_test) {
    create_topics_request_roundtrip<
      old::create_topics_request,
      cluster::create_topics_request>();
}

SEASTAR_THREAD_TEST_CASE(create_topics_request_uniform_rt_test) {
    create_topics_request_roundtrip<
      cluster::create_topics_request,
      cluster::create_topics_request>();
}

template<class ReqIn, class ReqOut>
void create_topics_reply_roundtrip() {
    ReqIn req;
    using cfg_t = typename std::decay<decltype(req.configs.front())>::type;
    auto t1 = cfg_t(model::ns("default"), model::topic("tp-1"), 12, 3);
    auto t2 = cfg_t(model::ns("default"), model::topic("tp-2"), 6, 5);
    if constexpr (std::is_same<cfg_t, cluster::topic_configuration>::value) {
        t1.properties.recovery = false;
        t1.properties.shadow_indexing = model::shadow_indexing_mode::archival;
        t2.properties.recovery = true;
        t2.properties.shadow_indexing = model::shadow_indexing_mode::fetch;
    }

    auto md1 = model::topic_metadata(
      model::topic_namespace(model::ns("test-ns"), model::topic("tp-1")));
    auto pmd1 = model::partition_metadata(model::partition_id(0));

    pmd1.leader_node = model::node_id(10);
    pmd1.replicas.push_back(model::broker_shard{model::node_id(10), 0});
    pmd1.replicas.push_back(model::broker_shard{model::node_id(12), 1});
    pmd1.replicas.push_back(model::broker_shard{model::node_id(13), 2});
    md1.partitions = {pmd1};

    req.results = {
      cluster::topic_result(
        model::topic_namespace(model::ns("default"), model::topic("tp-1")),
        cluster::errc::success),
      cluster::topic_result(
        model::topic_namespace(model::ns("default"), model::topic("tp-2")),
        cluster::errc::notification_wait_timeout)};
    req.metadata = {md1};
    req.configs = {t1, t2};

    auto res = serialize_upgrade_rpc<ReqIn, ReqOut>(std::move(req));

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
    BOOST_REQUIRE_EQUAL(res.configs[0].partition_count, 12);
    BOOST_REQUIRE_EQUAL(res.configs[0].replication_factor, 3);
    BOOST_REQUIRE_EQUAL(res.configs[0].tp_ns.ns, model::ns("default"));
    BOOST_REQUIRE_EQUAL(res.configs[0].tp_ns.tp, model::topic("tp-1"));
    if constexpr (std::is_same<cfg_t, cluster::topic_configuration>::value) {
        BOOST_REQUIRE_EQUAL(res.configs[0].properties.recovery.value(), false);
        BOOST_REQUIRE_EQUAL(
          res.configs[0].properties.shadow_indexing.value(),
          model::shadow_indexing_mode::archival);
    } else {
        BOOST_REQUIRE_EQUAL(
          res.configs[0].properties.recovery.has_value(), false);
        BOOST_REQUIRE_EQUAL(
          res.configs[0].properties.shadow_indexing.has_value(), false);
    }

    BOOST_REQUIRE_EQUAL(res.configs[1].partition_count, 6);
    BOOST_REQUIRE_EQUAL(res.configs[1].replication_factor, 5);
    BOOST_REQUIRE_EQUAL(res.configs[1].tp_ns.ns, model::ns("default"));
    BOOST_REQUIRE_EQUAL(res.configs[1].tp_ns.tp, model::topic("tp-2"));
    if constexpr (std::is_same<cfg_t, cluster::topic_configuration>::value) {
        BOOST_REQUIRE_EQUAL(res.configs[1].properties.recovery.value(), true);
        BOOST_REQUIRE_EQUAL(
          res.configs[1].properties.shadow_indexing.value(),
          model::shadow_indexing_mode::fetch);
    } else {
        BOOST_REQUIRE_EQUAL(
          res.configs[1].properties.recovery.has_value(), false);
        BOOST_REQUIRE_EQUAL(
          res.configs[1].properties.shadow_indexing.has_value(), false);
    }
}

SEASTAR_THREAD_TEST_CASE(create_topics_reply_upgrade_rt_test) {
    create_topics_reply_roundtrip<
      old::create_topics_reply,
      cluster::create_topics_reply>();
}

SEASTAR_THREAD_TEST_CASE(create_topics_reply_uniform_rt_test) {
    create_topics_reply_roundtrip<
      cluster::create_topics_reply,
      cluster::create_topics_reply>();
}

template<class ReqIn, class ReqOut>
void topic_configuration_assignment_roundtrip() {
    using cfg_t = decltype(ReqIn::cfg);
    auto tc = cfg_t(model::ns("default"), model::topic("tp-1"), 12, 3);
    if constexpr (std::is_same<cfg_t, cluster::topic_configuration>::value) {
        tc.properties.recovery = true;
        tc.properties.shadow_indexing = model::shadow_indexing_mode::archival;
    }
    auto p1 = cluster::partition_assignment{
      raft::group_id{42},
      model::partition_id{34},
      {
        model::broker_shard{.node_id = model::node_id{1}, .shard = 2},
        model::broker_shard{.node_id = model::node_id{3}, .shard = 4},
      },
    };
    auto p2 = cluster::partition_assignment{
      raft::group_id{51},
      model::partition_id{15},
      {
        model::broker_shard{.node_id = model::node_id{4}, .shard = 1},
        model::broker_shard{.node_id = model::node_id{5}, .shard = 2},
      },
    };

    ReqIn assign_cfg(tc, {p1, p2});

    auto res = serialize_upgrade_rpc<ReqIn, ReqOut>(std::move(assign_cfg));

    BOOST_REQUIRE_EQUAL(res.cfg.tp_ns.tp, model::topic("tp-1"));
    BOOST_REQUIRE_EQUAL(res.cfg.tp_ns.ns, model::ns("default"));
    BOOST_REQUIRE_EQUAL(res.cfg.partition_count, 12);
    BOOST_REQUIRE_EQUAL(res.cfg.replication_factor, 3);

    if constexpr (std::is_same<cfg_t, cluster::topic_configuration>::value) {
        BOOST_REQUIRE_EQUAL(res.cfg.properties.recovery.value(), true);
        BOOST_REQUIRE_EQUAL(
          res.cfg.properties.shadow_indexing.value(),
          model::shadow_indexing_mode::archival);
    } else {
        BOOST_REQUIRE_EQUAL(res.cfg.properties.recovery.has_value(), false);
        BOOST_REQUIRE_EQUAL(
          res.cfg.properties.shadow_indexing.has_value(), false);
    }
}

SEASTAR_THREAD_TEST_CASE(topics_configuration_assignment_upgrade_rt_test) {
    topic_configuration_assignment_roundtrip<
      old::topic_configuration_assignment,
      cluster::topic_configuration_assignment>();
}

SEASTAR_THREAD_TEST_CASE(topics_configuration_assignment_uniform_rt_test) {
    topic_configuration_assignment_roundtrip<
      cluster::topic_configuration_assignment,
      cluster::topic_configuration_assignment>();
}

template<class T>
T make_incremental_topic_properties() {
    T upd;
    upd.cleanup_policy_bitflags.op = cluster::incremental_update_operation::set;
    upd.cleanup_policy_bitflags.value
      = model::cleanup_policy_bitflags::deletion;
    upd.compaction_strategy.op = cluster::incremental_update_operation::set;
    upd.compaction_strategy.value = model::compaction_strategy::offset;
    upd.compression.op = cluster::incremental_update_operation::set;
    upd.compression.value = model::compression::zstd;
    upd.retention_bytes.op = cluster::incremental_update_operation::set;
    upd.retention_bytes.value = tristate<unsigned long>(12345);
    upd.segment_size.op = cluster::incremental_update_operation::set;
    upd.segment_size.value = 54321;
    upd.retention_duration.op = cluster::incremental_update_operation::set;
    upd.retention_duration.value = tristate<std::chrono::milliseconds>(10s);
    upd.timestamp_type.op = cluster::incremental_update_operation::set;
    upd.timestamp_type.value = model::timestamp_type::create_time;
    if constexpr (std::is_same<T, cluster::incremental_topic_updates>::value) {
        upd.shadow_indexing.op = cluster::incremental_update_operation::set;
        upd.shadow_indexing.value = model::shadow_indexing_mode::archival;
    }
    if constexpr (std::is_same<T, old::incremental_topic_updates_1>::value) {
        upd.data_policy.op = cluster::incremental_update_operation::set;
        upd.data_policy.value = v8_engine::data_policy("fn", "sn");
    }
    return upd;
}

template<class UpdIn, class UpdOut>
void compare_incremental_topic_updates(const UpdIn& upd, const UpdOut& res) {
    BOOST_REQUIRE(
      upd.cleanup_policy_bitflags.op == res.cleanup_policy_bitflags.op);
    BOOST_REQUIRE(
      upd.cleanup_policy_bitflags.value == res.cleanup_policy_bitflags.value);
    BOOST_REQUIRE(upd.compaction_strategy.op == res.compaction_strategy.op);
    BOOST_REQUIRE(
      upd.compaction_strategy.value == res.compaction_strategy.value);
    BOOST_REQUIRE(upd.compression.op == res.compression.op);
    BOOST_REQUIRE(upd.compression.value == res.compression.value);
    BOOST_REQUIRE(upd.retention_bytes.op == res.retention_bytes.op);
    BOOST_REQUIRE(upd.retention_bytes.value == res.retention_bytes.value);
    BOOST_REQUIRE(upd.segment_size.op == res.segment_size.op);
    BOOST_REQUIRE(upd.segment_size.value == res.segment_size.value);
    BOOST_REQUIRE(upd.retention_duration.op == res.retention_duration.op);
    BOOST_REQUIRE(upd.retention_duration.value == res.retention_duration.value);
    BOOST_REQUIRE(upd.timestamp_type.op == res.timestamp_type.op);
    BOOST_REQUIRE(upd.timestamp_type.value == res.timestamp_type.value);
    if constexpr (std::is_same<UpdIn, cluster::incremental_topic_updates>::
                    value) {
        BOOST_REQUIRE(upd.shadow_indexing.op == res.shadow_indexing.op);
        BOOST_REQUIRE(upd.shadow_indexing.value == res.shadow_indexing.value);
    }
}

template<class UpdIn, class UpdOut>
void incremental_topic_updates_roundtrip() {
    auto upd = make_incremental_topic_properties<UpdIn>();
    auto tmp = upd;
    auto res = serialize_upgrade_rpc<UpdIn, UpdOut>(std::move(tmp));
    compare_incremental_topic_updates(upd, res);
}

SEASTAR_THREAD_TEST_CASE(incremental_topic_updates_upgrade_rt_test_v1) {
    incremental_topic_updates_roundtrip<
      old::incremental_topic_updates_1,
      cluster::incremental_topic_updates>();
}

SEASTAR_THREAD_TEST_CASE(incremental_topic_updates_upgrade_rt_test_v2) {
    incremental_topic_updates_roundtrip<
      old::incremental_topic_updates_2,
      cluster::incremental_topic_updates>();
}

SEASTAR_THREAD_TEST_CASE(incremental_topic_updates_uniform_rt_test) {
    incremental_topic_updates_roundtrip<
      cluster::incremental_topic_updates,
      cluster::incremental_topic_updates>();
}

template<class UpdIn, class UpdOut>
void topic_properties_updates_roundtrip() {
    UpdIn upd(model::topic_namespace(model::ns("ns"), model::topic("topic")));
    upd.properties
      = make_incremental_topic_properties<decltype(upd.properties)>();

    auto tmp = upd;
    auto res = serialize_upgrade_rpc<UpdIn, UpdOut>(std::move(tmp));

    compare_incremental_topic_updates(upd.properties, res.properties);
}

SEASTAR_THREAD_TEST_CASE(topic_properties_updates_upgrade_rt_test_v1) {
    topic_properties_updates_roundtrip<
      old::topic_properties_update_v1,
      cluster::topic_properties_update>();
}

SEASTAR_THREAD_TEST_CASE(topic_properties_updates_upgrade_rt_test_v2) {
    topic_properties_updates_roundtrip<
      old::topic_properties_update_v2,
      cluster::topic_properties_update>();
}

SEASTAR_THREAD_TEST_CASE(topic_properties_updates_uniform_rt_test) {
    topic_properties_updates_roundtrip<
      cluster::topic_properties_update,
      cluster::topic_properties_update>();
}
