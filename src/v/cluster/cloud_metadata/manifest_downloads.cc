/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cluster/cloud_metadata/manifest_downloads.h"

#include "cloud_storage/remote.h"
#include "cluster/cloud_metadata/error_outcome.h"
#include "cluster/cloud_metadata/key_utils.h"
#include "cluster/logger.h"
#include "utils/uuid.h"

#include <boost/uuid/uuid_io.hpp>

namespace {

const std::regex cluster_metadata_manifest_prefix_expr{
  R"REGEX(cluster_metadata/[a-z0-9-]+/manifests/(\d+)/)REGEX"};

} // anonymous namespace

namespace cluster::cloud_metadata {

ss::future<cluster_manifest_result> download_highest_manifest_for_cluster(
  cloud_storage::remote& remote,
  const model::cluster_uuid& cluster_uuid,
  const cloud_storage_clients::bucket_name& bucket,
  retry_chain_node& retry_node) {
    // Download the manifest
    auto cluster_uuid_prefix = cluster_manifests_prefix(cluster_uuid) + "/";
    vlog(
      clusterlog.trace, "Listing objects with prefix {}", cluster_uuid_prefix);
    auto list_res = co_await remote.list_objects(
      bucket,
      retry_node,
      cloud_storage_clients::object_key(cluster_uuid_prefix),
      '/');
    if (list_res.has_error()) {
        vlog(
          clusterlog.debug, "Error downloading manifest {}", list_res.error());
        co_return error_outcome::list_failed;
    }
    // Examine the metadata IDs for this cluster.
    // Results take the form:
    // cluster_metadata/<cluster_uuid>/manifests/<meta_id>/
    auto& manifest_prefixes = list_res.value().common_prefixes;
    cluster_metadata_manifest manifest;
    if (manifest_prefixes.empty()) {
        vlog(
          clusterlog.debug,
          "No manifests found for cluster {}",
          cluster_uuid());
        co_return error_outcome::no_matching_metadata;
    }
    for (const auto& prefix : manifest_prefixes) {
        vlog(
          clusterlog.trace, "Prefix found for {}: {}", cluster_uuid(), prefix);
    }
    // Find the manifest with the highest metadata ID.
    cluster_metadata_id highest_meta_id{};
    for (const auto& prefix : manifest_prefixes) {
        std::smatch matches;
        std::string p = prefix;
        // E.g. cluster_metadata/<cluster_uuid>/manifests/3/
        const auto matches_manifest_expr = std::regex_match(
          p.cbegin(), p.cend(), matches, cluster_metadata_manifest_prefix_expr);
        if (!matches_manifest_expr) {
            continue;
        }
        const auto& meta_id_str = matches[1].str();
        cluster_metadata_id meta_id;
        try {
            meta_id = cluster_metadata_id(std::stoi(meta_id_str.c_str()));
        } catch (...) {
            vlog(
              clusterlog.debug,
              "Ignoring invalid metadata ID: {}",
              meta_id_str);
            continue;
        }
        highest_meta_id = std::max(highest_meta_id, meta_id);
    }
    if (highest_meta_id == cluster_metadata_id{}) {
        vlog(
          clusterlog.debug,
          "No manifests with valid metadata IDs found for cluster {}",
          cluster_uuid());
        co_return error_outcome::no_matching_metadata;
    }

    // Deserialize the manifest.
    auto manifest_res = co_await remote.download_manifest(
      bucket,
      cluster_manifest_key(cluster_uuid, highest_meta_id),
      manifest,
      retry_node);
    if (manifest_res != cloud_storage::download_result::success) {
        vlog(
          clusterlog.debug, "Manifest download failed with {}", manifest_res);
        co_return error_outcome::download_failed;
    }
    vlog(
      clusterlog.trace,
      "Downloaded manifest for {} from {}: {}",
      cluster_uuid(),
      bucket(),
      manifest);
    co_return manifest;
}

} // namespace cluster::cloud_metadata
