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
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cloud_metadata/error_outcome.h"
#include "cluster/cloud_metadata/key_utils.h"
#include "cluster/logger.h"
#include "utils/uuid.h"

#include <boost/uuid/uuid_io.hpp>

namespace {

const std::regex cluster_metadata_manifest_prefix_expr{
  R"REGEX(cluster_metadata/[a-z0-9-]+/manifests/(\d+)/)REGEX"};

const std::regex cluster_metadata_manifest_expr{
  R"REGEX(cluster_metadata/([a-z0-9-]+)/manifests/(\d+)/cluster_manifest.json)REGEX"};

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
        vassert(
          matches.size() >= 2,
          "Unexpected size of regex match",
          matches.size());
        const auto& meta_id_str = matches[1].str();
        cluster_metadata_id meta_id;
        try {
            meta_id = cluster_metadata_id(std::stol(meta_id_str.c_str()));
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
    auto manifest_res = co_await remote.download_manifest_json(
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

namespace {
bool is_offsets_snapshot_path(
  const ss::sstring& object, const cluster_metadata_manifest& m) {
    for (const auto& paths : m.offsets_snapshots_by_partition) {
        for (const auto& p : paths) {
            if (p == object) {
                return true;
            }
        }
    }
    return false;
}
} // anonymous namespace

ss::future<std::list<ss::sstring>> list_orphaned_by_manifest(
  cloud_storage::remote& remote,
  const model::cluster_uuid& cluster_uuid,
  const cloud_storage_clients::bucket_name& bucket,
  const cluster_metadata_manifest& manifest,
  retry_chain_node& retry_node) {
    auto uuid_prefix = cluster_uuid_prefix(cluster_uuid) + "/";
    vlog(clusterlog.trace, "Listing objects with prefix {}", uuid_prefix);
    auto list_res = co_await remote.list_objects(
      bucket, retry_node, cloud_storage_clients::object_key(uuid_prefix));
    if (list_res.has_error()) {
        vlog(
          clusterlog.debug,
          "Error listing under {}: {}",
          uuid_prefix,
          list_res.error());
        co_return std::list<ss::sstring>{};
    }
    std::list<ss::sstring> ret;
    for (auto& item : list_res.value().contents) {
        if (
          item.key == ss::sstring{manifest.get_manifest_path()()}
          || item.key == manifest.controller_snapshot_path
          || is_offsets_snapshot_path(item.key, manifest)) {
            vlog(clusterlog.trace, "Ignoring expected object: {}", item.key);
            continue;
        }
        vlog(clusterlog.trace, "Found orphaned object: {}", item.key);
        ret.emplace_back(std::move(item.key));
    }
    co_return ret;
}

ss::future<cluster_manifest_result> download_highest_manifest_in_bucket(
  cloud_storage::remote& remote,
  const cloud_storage_clients::bucket_name& bucket,
  retry_chain_node& retry_node,
  std::optional<model::cluster_uuid> ignore_uuid) {
    // Look for unique cluster UUIDs for which we have metadata.
    constexpr auto cluster_prefix = "cluster_metadata/";
    vlog(clusterlog.trace, "Listing objects with prefix {}", cluster_prefix);
    auto list_res = co_await remote.list_objects(
      bucket,
      retry_node,
      cloud_storage_clients::object_key(cluster_prefix),
      std::nullopt);
    if (list_res.has_error()) {
        vlog(clusterlog.debug, "Error downloading manifest", list_res.error());
        co_return error_outcome::list_failed;
    }
    // Examine all cluster metadata in this bucket.
    auto& cluster_metadata_items = list_res.value().contents;
    if (cluster_metadata_items.empty()) {
        vlog(clusterlog.debug, "No manifests found in bucket {}", bucket());
        co_return error_outcome::no_matching_metadata;
    }

    // Look through those that look like cluster metadata manifests and find
    // the one with the highest metadata ID. This will be the returned to the
    // caller.
    model::cluster_uuid uuid_with_highest_meta_id{};
    cluster_metadata_id highest_meta_id{};
    for (const auto& item : cluster_metadata_items) {
        std::smatch matches;
        std::string k = item.key;
        const auto matches_manifest_expr = std::regex_match(
          k.cbegin(), k.cend(), matches, cluster_metadata_manifest_expr);
        if (!matches_manifest_expr) {
            continue;
        }
        const auto& cluster_uuid_str = matches[1].str();
        const auto& meta_id_str = matches[2].str();
        cluster_metadata_id meta_id{};
        model::cluster_uuid cluster_uuid{};
        try {
            meta_id = cluster_metadata_id(std::stoi(meta_id_str.c_str()));
        } catch (...) {
            vlog(
              clusterlog.debug,
              "Ignoring invalid metadata ID: {}",
              meta_id_str);
            continue;
        }
        try {
            auto u = boost::lexical_cast<uuid_t::underlying_t>(
              cluster_uuid_str);
            std::vector<uint8_t> uuid_vec{u.begin(), u.end()};
            cluster_uuid = model::cluster_uuid(std::move(uuid_vec));
        } catch (...) {
            vlog(
              clusterlog.debug,
              "Ignoring invalid cluster UUID: {}",
              cluster_uuid_str);
            continue;
        }
        if (ignore_uuid == cluster_uuid) {
            continue;
        }
        if (meta_id > highest_meta_id) {
            highest_meta_id = meta_id;
            uuid_with_highest_meta_id = cluster_uuid;
        }
    }
    if (highest_meta_id == cluster_metadata_id{}) {
        vlog(clusterlog.debug, "No valid manifests in bucket {}", bucket());
        co_return error_outcome::no_matching_metadata;
    }
    cluster_metadata_manifest manifest;
    auto manifest_res = co_await remote.download_manifest_json(
      bucket,
      cluster_manifest_key(uuid_with_highest_meta_id, highest_meta_id),
      manifest,
      retry_node);
    if (manifest_res != cloud_storage::download_result::success) {
        vlog(
          clusterlog.debug, "Manifest download failed with {}", manifest_res);
        co_return error_outcome::download_failed;
    }
    co_return manifest;
}

} // namespace cluster::cloud_metadata
