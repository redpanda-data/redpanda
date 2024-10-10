/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/remote_label.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/topic_mount_manifest.h"
#include "cloud_storage/types.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "test_utils/test.h"

using namespace cloud_storage;

namespace {
const remote_path_provider path_provider(std::nullopt, std::nullopt);
} // anonymous namespace

const ss::sstring test_uuid_str = "deadbeef-0000-0000-0000-000000000000";
const model::cluster_uuid test_uuid{uuid_t::from_string(test_uuid_str)};
const remote_label test_label{test_uuid};
const model::topic_namespace test_tp_ns{
  model::ns{"test_ns"}, model::topic{"test_tp"}};
const model::initial_revision_id rev_id{123};

TEST(topic_mount_manifest, manifest_type) {
    topic_mount_manifest m(test_label, test_tp_ns, rev_id);
    ASSERT_TRUE(m.get_manifest_type() == manifest_type::topic_mount);
}

TEST(topic_mount_manifest, serde) {
    topic_mount_manifest m(test_label, test_tp_ns, rev_id);
    auto serialized_manifest = m.serialize().get().stream;

    // Sanity check that the manifest contains the test label and
    // topic namespace.
    ASSERT_EQ(m.get_source_label(), test_label);
    ASSERT_EQ(m.get_tp_ns(), test_tp_ns);

    topic_mount_manifest reconstructed_m(
      remote_label{}, model::topic_namespace{}, model::initial_revision_id{});

    // Sanity check that the manifests differ.
    ASSERT_NE(reconstructed_m.get_source_label(), m.get_source_label());
    ASSERT_NE(reconstructed_m.get_tp_ns(), m.get_tp_ns());
    ASSERT_NE(
      reconstructed_m.get_manifest_path(path_provider),
      m.get_manifest_path(path_provider));

    reconstructed_m.update(std::move(serialized_manifest)).get();

    // Manifests should be identical after deserialization.
    ASSERT_EQ(reconstructed_m.get_source_label(), m.get_source_label());
    ASSERT_EQ(reconstructed_m.get_tp_ns(), m.get_tp_ns());
    ASSERT_EQ(reconstructed_m.get_revision_id(), m.get_revision_id());
    ASSERT_EQ(
      reconstructed_m.get_manifest_path(path_provider),
      m.get_manifest_path(path_provider));
}
