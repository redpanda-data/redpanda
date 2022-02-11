/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/types.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

using namespace cloud_storage;

// update manifest, serialize, compare jsons

static const cluster::topic_configuration cfg(
  model::ns("cfg-test-namespace"),
  model::topic("cfg-test-topic"),
  /* partition_count = */ 32,
  /* replication_factor = */ 3);

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
    "retention_duration": 360000000
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
    auto [is, size] = m.serialize();
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
    auto [is, size] = m.serialize();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    topic_manifest restored;
    restored.update(std::move(rstr)).get();

    BOOST_REQUIRE(m == restored);
}

SEASTAR_THREAD_TEST_CASE(full_update_serialize_update_same_object) {
    topic_manifest m;
    m.update(make_manifest_stream(full_topic_manifest_json)).get();

    auto [is, size] = m.serialize();
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
    auto [is, size] = m.serialize();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    topic_manifest restored;
    restored.update(std::move(rstr)).get();

    BOOST_REQUIRE(m == restored);
}
