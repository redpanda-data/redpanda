/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cluster/cloud_metadata/cluster_manifest.h"

#include <seastar/testing/thread_test_case.hh>

using namespace cluster::cloud_metadata;

namespace {
ss::input_stream<char> make_manifest_stream(std::string_view json) {
    iobuf i;
    i.append(json.data(), json.size());
    return make_iobuf_input_stream(std::move(i));
}
} // anonymous namespace

static constexpr std::string_view simple_manifest_json = R"json({
    "version": 0,
    "compat_version": 0,
    "upload_time_since_epoch": 100,
    "cluster_uuid": "01234567-89ab-cdef-0123-456789abcdef",
    "metadata_id": 7,
    "controller_snapshot_offset": 42,
    "controller_snapshot_path": "cluster_metadata/01234567-89ab-cdef-0123-456789abcdef/0/controller.snapshot"
})json";

SEASTAR_THREAD_TEST_CASE(test_basic_serialization) {
    cluster_metadata_manifest manifest;
    manifest.update(make_manifest_stream(simple_manifest_json)).get();
    BOOST_CHECK_EQUAL(100, manifest.upload_time_since_epoch.count());
    auto uuid_str = "{01234567-89ab-cdef-0123-456789abcdef}";
    BOOST_CHECK_EQUAL(fmt::to_string(manifest.cluster_uuid), uuid_str);
    BOOST_CHECK_EQUAL(7, manifest.metadata_id());
    BOOST_CHECK_EQUAL(42, manifest.controller_snapshot_offset());
    BOOST_CHECK_EQUAL(
      "cluster_metadata/01234567-89ab-cdef-0123-456789abcdef/0/"
      "controller.snapshot",
      manifest.controller_snapshot_path);

    auto [is, size] = manifest.serialize().get();
    iobuf buf;
    auto os = make_iobuf_ref_output_stream(buf);
    ss::copy(is, os).get();

    auto rstr = make_iobuf_input_stream(std::move(buf));
    cluster_metadata_manifest restored;
    restored.update(std::move(rstr)).get();
    BOOST_REQUIRE_EQUAL(manifest, restored);

    cloud_storage::remote_manifest_path path{
      "cluster_metadata/01234567-89ab-cdef-0123-456789abcdef/manifests/7/"
      "cluster_manifest.json"};
    BOOST_REQUIRE_EQUAL(path, manifest.get_manifest_path());
}

static constexpr std::string_view simple_manifest_json_with_extra = R"json({
    "version": 0,
    "compat_version": 0,
    "upload_time_since_epoch": 100,
    "cluster_uuid": "01234567-89ab-cdef-0123-456789abcdef",
    "metadata_id": 7,
    "controller_snapshot_offset": 42,
    "controller_snapshot_path": "cluster_metadata/01234567-89ab-cdef-0123-456789abcdef/0/controller.snapshot",
    "foo": "bar"
})json";

SEASTAR_THREAD_TEST_CASE(test_extra_field) {
    cluster_metadata_manifest manifest;
    manifest.update(make_manifest_stream(simple_manifest_json)).get();

    cluster_metadata_manifest manifest_with_extra;
    manifest_with_extra
      .update(make_manifest_stream(simple_manifest_json_with_extra))
      .get();

    BOOST_CHECK_EQUAL(manifest, manifest_with_extra);
}

static constexpr std::string_view bad_compat_version_json = R"json({
    "version": 0,
    "compat_version": 999,
    "upload_time_since_epoch": 100,
    "cluster_uuid": "01234567-89ab-cdef-0123-456789abcdef",
    "metadata_id": 7,
    "controller_snapshot_offset": 42,
    "controller_snapshot_path": "cluster_metadata/01234567-89ab-cdef-0123-456789abcdef/0/controller.snapshot"
})json";

SEASTAR_THREAD_TEST_CASE(test_bad_compat_serialization) {
    cluster_metadata_manifest manifest;
    BOOST_REQUIRE_EXCEPTION(
      manifest.update(make_manifest_stream(bad_compat_version_json)).get(),
      std::runtime_error,
      [](auto& ex) {
          return std::string(ex.what()).find(
                   "Can't deserialize cluster manifest, supported version")
                 != std::string::npos;
      });
}

static constexpr std::string_view bad_uuid_manifest_json = R"json({
    "version": 0,
    "compat_version": 0,
    "upload_time_since_epoch": 100,
    "cluster_uuid": "1-877-kars4kids",
    "metadata_id": 7,
    "controller_snapshot_offset": 42,
    "controller_snapshot_path": "cluster_metadata/01234567-89ab-cdef-0123-456789abcdef/0/controller.snapshot"
})json";

SEASTAR_THREAD_TEST_CASE(test_bad_uuid) {
    cluster_metadata_manifest manifest;
    BOOST_REQUIRE_EXCEPTION(
      manifest.update(make_manifest_stream(bad_uuid_manifest_json)).get(),
      std::runtime_error,
      [](auto& ex) {
          return std::string(ex.what()).find(
                   "Failed to deserialize 'cluster_uuid' field")
                 != std::string::npos;
      });
}

static constexpr std::string_view empty_fields_manifest_json = R"json({
    "version": 0,
    "compat_version": 0
})json";

SEASTAR_THREAD_TEST_CASE(test_missing_fields_serialization) {
    cluster_metadata_manifest manifest;
    manifest.update(make_manifest_stream(empty_fields_manifest_json)).get();
    BOOST_CHECK_EQUAL(0, manifest.upload_time_since_epoch.count());
    BOOST_CHECK_EQUAL(model::cluster_uuid{}, manifest.cluster_uuid);
    BOOST_CHECK_EQUAL(cluster_metadata_id{}, manifest.metadata_id);
    BOOST_CHECK_EQUAL(model::offset{}, manifest.controller_snapshot_offset);
    BOOST_CHECK_EQUAL("", manifest.controller_snapshot_path);
}
