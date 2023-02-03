/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "bytes/iostream.h"
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/types.h"
#include "cluster/types.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <string>
#include <string_view>
#include <system_error>

using namespace cloud_storage;

// update manifest, serialize, compare jsons

static const manifest_topic_configuration cfg{
  .tp_ns = model::topic_namespace(
    model::ns("cfg-test-namespace"), model::topic("cfg-test-topic")),
  .partition_count = 32,
  .replication_factor = 3};

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
    "version": 2,
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
    topic_manifest m(cfg, model::initial_revision_id(0));
    auto path = m.get_manifest_path();
    BOOST_REQUIRE_EQUAL(
      path,
      "50000000/meta/cfg-test-namespace/cfg-test-topic/topic_manifest.json");
}

SEASTAR_THREAD_TEST_CASE(update_topic_manifest_correct_path) {
    topic_manifest m;
    m.update(make_manifest_stream(min_topic_manifest_json)).get();
    auto path = m.get_manifest_path();
    BOOST_REQUIRE_EQUAL(
      path, "70000000/meta/test-namespace/test-topic/topic_manifest.json");
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
    m.update(make_manifest_stream(min_topic_manifest_json)).get();
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
    m.update(make_manifest_stream(min_topic_manifest_json)).get();

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
    BOOST_REQUIRE(
      !topic_config.properties.retention_bytes.is_disabled()
      && !topic_config.properties.retention_bytes.has_optional_value());
    BOOST_REQUIRE(
      !topic_config.properties.retention_duration.is_disabled()
      && !topic_config.properties.retention_duration.has_optional_value());
}

SEASTAR_THREAD_TEST_CASE(full_config_update_all_fields_correct) {
    topic_manifest m;
    m.update(make_manifest_stream(full_topic_manifest_json)).get();

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
}

SEASTAR_THREAD_TEST_CASE(missing_required_fields_throws) {
    topic_manifest m;
    BOOST_REQUIRE_EXCEPTION(
      m.update(make_manifest_stream(missing_partition_count)).get(),
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
      m.update(make_manifest_stream(wrong_version)).get(),
      std::runtime_error,
      [](std::runtime_error ex) {
          return std::string(ex.what()).find(
                   "topic manifest version {2} is not supported")
                 != std::string::npos;
      });
}

SEASTAR_THREAD_TEST_CASE(wrong_compaction_strategy_throws) {
    topic_manifest m;
    BOOST_REQUIRE_EXCEPTION(
      m.update(make_manifest_stream(wrong_compaction_strategy)).get(),
      std::runtime_error,
      [](std::runtime_error ex) {
          return std::string(ex.what()).find(
                   "Failed to parse topic manifest "
                   "{\"30000000/meta/full-test-namespace/full-test-topic/"
                   "topic_manifest.json\"}: Invalid compaction_strategy: "
                   "wrong_value")
                 != std::string::npos;
      });
}

SEASTAR_THREAD_TEST_CASE(full_update_serialize_update_same_object) {
    topic_manifest m;
    m.update(make_manifest_stream(full_topic_manifest_json)).get();

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
    m.update(make_manifest_stream(full_topic_manifest_json)).get();
    auto [is, size] = m.serialize().get();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    topic_manifest restored;
    restored.update(std::move(rstr)).get();

    BOOST_REQUIRE(m == restored);
}
