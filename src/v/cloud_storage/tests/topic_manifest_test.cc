/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "bytes/iostream.h"
#include "bytes/streambuf.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/topic_path_utils.h"
#include "cloud_storage/types.h"
#include "cluster/types.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "test_utils/randoms.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <fmt/ranges.h>

#include <chrono>
#include <limits>
#include <ratio>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>

using namespace cloud_storage;

namespace {
const remote_path_provider path_provider(std::nullopt, std::nullopt);
} // anonymous namespace

// update manifest, serialize, compare jsons

static const cluster::topic_configuration cfg{
  model::ns("cfg-test-namespace"), model::topic("cfg-test-topic"), 32, 3};

static constexpr std::string_view min_topic_manifest_json = R"json({
    "version": 1,
    "namespace": "test-namespace",
    "topic": "test-topic",
    "partition_count": 32,
    "replication_factor": 3,
    "revision_id": 0,
    "compression": null,
    "cleanup_policy_bitflags": null,
    "compaction_strategy": null,
    "timestamp_type": null,
    "segment_size": null
})json";

static constexpr std::string_view full_topic_manifest_json = R"json({
    "version": 1,
    "namespace": "full-test-namespace",
    "topic": "full-test-topic",
    "partition_count": 64,
    "replication_factor": 6,
    "revision_id": 1,
    "compression": "snappy",
    "cleanup_policy_bitflags": "compact,delete",
    "compaction_strategy": "offset",
    "timestamp_type": "LogAppendTime",
    "segment_size": 1234,
    "retention_bytes": 42342,
    "retention_duration": 36000000000
})json";

static constexpr std::string_view missing_partition_count = R"json({
    "version": 1,
    "namespace": "test-namespace",
    "namespace": "test-namespace1",
    "namespace": "test-namespace2",
    "topic": "test-topic",
    "replication_factor": 3,
    "revision_id": 0
})json";

static constexpr std::string_view wrong_version = R"json({
    "version": 99,
    "namespace": "full-test-namespace",
    "topic": "full-test-topic",
    "partition_count": 64,
    "replication_factor": 6,
    "revision_id": 1,
    "compression": "snappy",
    "cleanup_policy_bitflags": "compact,delete",
    "compaction_strategy": "offset",
    "timestamp_type": "LogAppendTime",
    "segment_size": 1234,
    "retention_bytes": 42342,
    "retention_duration": 36000000000
})json";

static constexpr std::string_view wrong_compaction_strategy = R"json({
    "version": 1,
    "namespace": "full-test-namespace",
    "topic": "full-test-topic",
    "partition_count": 64,
    "replication_factor": 6,
    "revision_id": 1,
    "compression": "snappy",
    "cleanup_policy_bitflags": "compact,delete",
    "compaction_strategy": "wrong_value",
    "timestamp_type": "LogAppendTime",
    "segment_size": 1234,
    "retention_bytes": 42342,
    "retention_duration": 36000000000
})json";

static constexpr std::string_view negative_properties_manifest = R"json({
    "version": 1,
    "namespace": "negative-test-namespace",
    "topic": "full-test-topic",
    "partition_count": 64,
    "replication_factor": 6,
    "revision_id": 1,
    "compression": "snappy",
    "cleanup_policy_bitflags": "compact,delete",
    "compaction_strategy": "offset",
    "timestamp_type": "LogAppendTime",
    "segment_size": -1234,
    "retention_bytes": -42342,
    "retention_duration": -36000000000
})json";

inline ss::input_stream<char> make_manifest_stream(std::string_view json) {
    iobuf i;
    i.append(json.data(), json.size());
    return make_iobuf_input_stream(std::move(i));
}

SEASTAR_THREAD_TEST_CASE(manifest_type_topic) {
    topic_manifest m;
    BOOST_REQUIRE(m.get_manifest_type() == manifest_type::topic);
}

SEASTAR_THREAD_TEST_CASE(create_topic_manifest_correct_path) {
    auto path = prefixed_topic_manifest_json_path(cfg.tp_ns);
    BOOST_REQUIRE_EQUAL(
      path,
      "50000000/meta/cfg-test-namespace/cfg-test-topic/topic_manifest.json");
    auto serde_path = prefixed_topic_manifest_bin_path(cfg.tp_ns);
    BOOST_REQUIRE_EQUAL(
      serde_path,
      "50000000/meta/cfg-test-namespace/cfg-test-topic/topic_manifest.bin");
}

SEASTAR_THREAD_TEST_CASE(update_topic_manifest_correct_path) {
    topic_manifest m;
    m.update(
       manifest_format::json, make_manifest_stream(min_topic_manifest_json))
      .get();
    auto path = m.get_manifest_path(path_provider);
    BOOST_REQUIRE_EQUAL(
      path, "70000000/meta/test-namespace/test-topic/topic_manifest.bin");
}

SEASTAR_THREAD_TEST_CASE(construct_serialize_update_same_object) {
    topic_manifest m(cfg, model::initial_revision_id(0));
    auto [is, size] = m.serialize().get();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    topic_manifest restored;
    restored.update(std::move(rstr)).get();

    BOOST_REQUIRE(m == restored);
}

SEASTAR_THREAD_TEST_CASE(update_serialize_update_same_object) {
    topic_manifest m;
    m.update(
       manifest_format::json, make_manifest_stream(min_topic_manifest_json))
      .get();
    auto [is, size] = m.serialize().get();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    topic_manifest restored;
    restored.update(std::move(rstr)).get();

    BOOST_REQUIRE(m == restored);
}

SEASTAR_THREAD_TEST_CASE(min_config_update_all_fields_correct) {
    topic_manifest m;
    m.update(
       manifest_format::json, make_manifest_stream(min_topic_manifest_json))
      .get();

    auto topic_config = m.get_topic_config().value();
    BOOST_REQUIRE_EQUAL(m.get_revision(), model::initial_revision_id(0));
    BOOST_REQUIRE_EQUAL(
      topic_config.tp_ns,
      model::topic_namespace(
        model::ns("test-namespace"), model::topic("test-topic")));
    BOOST_REQUIRE_EQUAL(topic_config.partition_count, 32);
    BOOST_REQUIRE_EQUAL(topic_config.replication_factor, 3);
    BOOST_REQUIRE(!topic_config.properties.compression);
    BOOST_REQUIRE(!topic_config.properties.cleanup_policy_bitflags);
    BOOST_REQUIRE(!topic_config.properties.compaction_strategy);
    BOOST_REQUIRE(!topic_config.properties.timestamp_type);
    BOOST_REQUIRE(!topic_config.properties.segment_size);
    BOOST_REQUIRE(topic_config.properties.retention_bytes.is_disabled());
    BOOST_REQUIRE(topic_config.properties.retention_duration.is_disabled());
}

SEASTAR_THREAD_TEST_CASE(full_config_update_all_fields_correct) {
    topic_manifest m;
    m.update(
       manifest_format::json, make_manifest_stream(full_topic_manifest_json))
      .get();

    auto topic_config = m.get_topic_config().value();
    BOOST_REQUIRE_EQUAL(m.get_revision(), model::initial_revision_id(1));
    BOOST_REQUIRE_EQUAL(
      topic_config.tp_ns,
      model::topic_namespace(
        model::ns("full-test-namespace"), model::topic("full-test-topic")));
    BOOST_REQUIRE_EQUAL(topic_config.partition_count, 64);
    BOOST_REQUIRE_EQUAL(topic_config.replication_factor, 6);
    BOOST_REQUIRE_EQUAL(
      topic_config.properties.compression.value(), model::compression::snappy);
    BOOST_REQUIRE_EQUAL(
      topic_config.properties.cleanup_policy_bitflags.value(),
      model::cleanup_policy_bitflags::deletion
        | model::cleanup_policy_bitflags::compaction);
    BOOST_REQUIRE_EQUAL(
      topic_config.properties.compaction_strategy.value(),
      model::compaction_strategy::offset);
    BOOST_REQUIRE_EQUAL(
      topic_config.properties.timestamp_type.value(),
      model::timestamp_type::append_time);
    BOOST_REQUIRE_EQUAL(topic_config.properties.segment_size.value(), 1234);
    BOOST_REQUIRE_EQUAL(topic_config.properties.retention_bytes.value(), 42342);
    BOOST_REQUIRE_EQUAL(
      topic_config.properties.retention_duration.value(),
      std::chrono::milliseconds(36000000000));

    // ensure that for v1 topic_manifest, tristates that were not serialized
    // keep their default value
    BOOST_REQUIRE_EQUAL(
      topic_config.properties.initial_retention_local_target_bytes,
      cluster::topic_properties{}.initial_retention_local_target_bytes);
}

SEASTAR_THREAD_TEST_CASE(topic_manifest_min_serialization) {
    auto min_cfg = cfg;
    min_cfg.properties.retention_bytes = tristate<size_t>(
      std::numeric_limits<size_t>::min());
    min_cfg.properties.retention_duration = tristate<std::chrono::milliseconds>(
      std::chrono::milliseconds::min());
    min_cfg.properties.segment_size = std::make_optional(
      std::numeric_limits<size_t>::min());

    topic_manifest m(min_cfg, model::initial_revision_id{0});
    iobuf buf;
    iobuf_ostreambuf obuf(buf);
    std::ostream os(&obuf);
    m.serialize_v1_json(os);

    auto rstr = make_iobuf_input_stream(std::move(buf));
    topic_manifest restored;
    restored.update(manifest_format::json, std::move(rstr)).get();
    BOOST_CHECK_EQUAL(m.get_revision(), restored.get_revision());
    auto restored_cfg = restored.get_topic_config().value();
    // as a safety net, negative values for retention_duration are converted to
    // disabled tristate (infinite retention)
    BOOST_CHECK(restored_cfg.properties.retention_duration.is_disabled());

    BOOST_CHECK_EQUAL(
      restored_cfg.properties.retention_bytes,
      min_cfg.properties.retention_bytes);
    BOOST_CHECK_EQUAL(
      restored_cfg.properties.segment_size, min_cfg.properties.segment_size);
}

SEASTAR_THREAD_TEST_CASE(topic_manifest_max_serialization) {
    auto max_cfg = cfg;
    max_cfg.properties.retention_bytes = tristate<size_t>(
      std::numeric_limits<size_t>::max());
    max_cfg.properties.retention_duration = tristate<std::chrono::milliseconds>(
      std::chrono::milliseconds::max());
    max_cfg.properties.segment_size = std::make_optional(
      std::numeric_limits<size_t>::max());
    topic_manifest m(max_cfg, model::initial_revision_id{0});
    iobuf buf;
    iobuf_ostreambuf obuf(buf);
    std::ostream os(&obuf);
    m.serialize_v1_json(os);

    auto rstr = make_iobuf_input_stream(std::move(buf));
    topic_manifest restored;
    restored.update(manifest_format::json, std::move(rstr)).get();
    BOOST_REQUIRE(m == restored);
}

SEASTAR_THREAD_TEST_CASE(missing_required_fields_throws) {
    topic_manifest m;
    BOOST_REQUIRE_EXCEPTION(
      m.update(
         manifest_format::json, make_manifest_stream(missing_partition_count))
        .get(),
      std::runtime_error,
      [](std::runtime_error ex) {
          return std::string(ex.what()).find(
                   "Missing _partition_count value in "
                   "parsed topic manifest")
                 != std::string::npos;
      });
}

SEASTAR_THREAD_TEST_CASE(wrong_version_throws) {
    topic_manifest m;
    BOOST_REQUIRE_EXCEPTION(
      m.update(manifest_format::json, make_manifest_stream(wrong_version))
        .get(),
      std::runtime_error,
      [](std::runtime_error ex) {
          return std::string(ex.what()).find(
                   "topic manifest version {99} is not supported")
                 != std::string::npos;
      });
}

SEASTAR_THREAD_TEST_CASE(wrong_compaction_strategy_throws) {
    topic_manifest m;
    BOOST_REQUIRE_EXCEPTION(
      m.update(
         manifest_format::json, make_manifest_stream(wrong_compaction_strategy))
        .get(),
      std::runtime_error,
      [](std::runtime_error ex) {
          return std::string(ex.what()).find("Invalid compaction_strategy: "
                                             "wrong_value")
                 != std::string::npos;
      });
}

SEASTAR_THREAD_TEST_CASE(full_update_serialize_update_same_object) {
    topic_manifest m;
    m.update(
       manifest_format::json, make_manifest_stream(full_topic_manifest_json))
      .get();

    auto [is, size] = m.serialize().get();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    topic_manifest restored;
    restored.update(std::move(rstr)).get();

    BOOST_REQUIRE(m == restored);
}

SEASTAR_THREAD_TEST_CASE(update_non_empty_manifest) {
    topic_manifest m(cfg, model::initial_revision_id(0));
    m.update(
       manifest_format::json, make_manifest_stream(full_topic_manifest_json))
      .get();
    auto [is, size] = m.serialize().get();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    topic_manifest restored;
    restored.update(std::move(rstr)).get();

    BOOST_REQUIRE(m == restored);
}

SEASTAR_THREAD_TEST_CASE(test_negative_property_manifest) {
    topic_manifest m(cfg, model::initial_revision_id(0));
    m.update(
       manifest_format::json,
       make_manifest_stream(negative_properties_manifest))
      .get();
    auto tp_cfg = m.get_topic_config();
    BOOST_REQUIRE(tp_cfg.has_value());
    BOOST_REQUIRE_EQUAL(64, tp_cfg->partition_count);
    BOOST_REQUIRE_EQUAL(6, tp_cfg->replication_factor);
    auto tp_props = tp_cfg->properties;

    // The unsigned types that were passed in negative values shouldn't be set.
    BOOST_REQUIRE(tp_props.retention_duration.is_disabled());
    BOOST_REQUIRE(tp_props.retention_bytes.is_disabled());
    BOOST_REQUIRE(!tp_props.segment_size.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_retention_ms_bytes_manifest) {
    // see issues/14325

    // check that serialization/deserialization of disabled tristate for
    // retention_bytes and retention_duration works as expected

    auto test_cfg = cfg;
    test_cfg.properties.retention_bytes = tristate<size_t>{disable_tristate};
    test_cfg.properties.retention_duration
      = tristate<std::chrono::milliseconds>{disable_tristate};

    auto m = topic_manifest{test_cfg, model::initial_revision_id{0}};

    auto serialized = m.serialize().get().stream;
    auto buf = iobuf{};
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(serialized, os).get();

    auto reconstructed = topic_manifest{};
    reconstructed.update(make_iobuf_input_stream(std::move(buf))).get();

    BOOST_REQUIRE(reconstructed.get_topic_config());
    auto reconstructed_props
      = reconstructed.get_topic_config().value().properties;
    BOOST_CHECK_MESSAGE(
      test_cfg.properties == reconstructed_props,
      fmt::format(
        "test_cfg: {} reconstructed_cfg: {}",
        test_cfg.properties,
        reconstructed_props));

    BOOST_CHECK(test_cfg == reconstructed.get_topic_config());
}

SEASTAR_THREAD_TEST_CASE(test_topic_manifest_serde_feature_table) {
    auto random_topic_configuration = cluster::topic_configuration{
      model::ns{"a_ns"}, model::topic{"a_topic"}, 20, 30};

    random_topic_configuration.properties = cluster::topic_properties{
      model::compression::gzip,
      std::nullopt,
      model::compaction_strategy::offset,
      std::nullopt,
      42,
      tristate<size_t>{},
      tristate<std::chrono::milliseconds>{disable_tristate},
      false,
      std::nullopt,
      true,
      "rrr",
      std::nullopt,
      std::nullopt,
      313,
      tristate<size_t>{disable_tristate},
      tristate<std::chrono::milliseconds>{std::chrono::milliseconds{20}},
      true,
      tristate<std::chrono::milliseconds>{},
      true,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      false,
      false,
      std::nullopt,
      std::nullopt,
      tristate<size_t>{},
      tristate<std::chrono::milliseconds>{std::chrono::milliseconds{55}},
      model::vcluster_id{xid{{1, 2, 4, 5, 6, 7, 8, 9}}},
      std::nullopt,
      std::nullopt,
      std::nullopt,
      false,
      std::nullopt,
      false,
    };

    auto random_initial_revision_id
      = tests::random_named_int<model::initial_revision_id>();

    // create serde image of random_topic_configuration, deserialize it through
    // topic_manifest and check that the result is equal
    auto manifest = topic_manifest{
      random_topic_configuration, random_initial_revision_id};
    BOOST_CHECK(manifest.get_revision() == random_initial_revision_id);
    BOOST_CHECK(
      manifest.get_manifest_path(path_provider)().extension() == ".bin");

    auto serialized_manifest = manifest.serialize().get().stream;

    auto reconstructed_serde_manifest = topic_manifest{};
    reconstructed_serde_manifest
      .update(manifest_format::serde, std::move(serialized_manifest))
      .get();
    BOOST_CHECK(
      reconstructed_serde_manifest.get_revision()
      == random_initial_revision_id);
    BOOST_CHECK(
      reconstructed_serde_manifest.get_topic_config()
      == random_topic_configuration);
}
