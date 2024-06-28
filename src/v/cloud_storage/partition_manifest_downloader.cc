// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "cloud_storage/partition_manifest_downloader.h"

#include "cloud_storage/types.h"

namespace cloud_storage {

partition_manifest_downloader::partition_manifest_downloader(
  const cloud_storage_clients::bucket_name bucket,
  const remote_path_provider& path_provider,
  const model::ntp& ntp,
  model::initial_revision_id rev,
  remote& remote)
  : bucket_(bucket)
  , remote_path_provider_(path_provider)
  , ntp_(ntp)
  , rev_(rev)
  , remote_(remote) {}

ss::future<result<find_partition_manifest_outcome, error_outcome>>
partition_manifest_downloader::download_manifest(
  retry_chain_node& parent_retry,
  ss::lowres_clock::time_point deadline,
  model::timestamp_clock::duration backoff,
  partition_manifest* manifest) {
    retry_chain_node retry_node(deadline, backoff, &parent_retry);
    co_return co_await download_manifest(retry_node, manifest);
}

ss::future<result<find_partition_manifest_outcome, error_outcome>>
partition_manifest_downloader::download_manifest(
  retry_chain_node& retry_node, partition_manifest* manifest) {
    auto bin_path = remote_manifest_path{
      remote_path_provider_.partition_manifest_path(ntp_, rev_)};
    auto bin_res = co_await remote_.download_manifest_bin(
      bucket_, bin_path, *manifest, retry_node);
    if (bin_res == download_result::success) {
        co_return find_partition_manifest_outcome::success;
    }
    if (bin_res != download_result::notfound) {
        co_return error_outcome::manifest_download_error;
    }
    auto json_str = remote_path_provider_.partition_manifest_path_json(
      ntp_, rev_);
    if (!json_str.has_value()) {
        co_return find_partition_manifest_outcome::no_matching_manifest;
    }
    auto json_path = remote_manifest_path{*json_str};
    auto json_res = co_await remote_.download_manifest_json(
      bucket_, json_path, *manifest, retry_node);
    if (json_res == download_result::success) {
        co_return find_partition_manifest_outcome::success;
    }
    if (json_res == download_result::notfound) {
        co_return find_partition_manifest_outcome::no_matching_manifest;
    }
    co_return error_outcome::manifest_download_error;
}

ss::future<result<find_partition_manifest_outcome, error_outcome>>
partition_manifest_downloader::manifest_exists(retry_chain_node& retry_node) {
    const auto bin_path = remote_manifest_path{
      remote_path_provider_.partition_manifest_path(ntp_, rev_)};
    auto bin_res = co_await remote_.object_exists(
      bucket_,
      cloud_storage_clients::object_key{bin_path},
      retry_node,
      existence_check_type::manifest);
    if (bin_res == download_result::success) {
        co_return find_partition_manifest_outcome::success;
    }
    if (bin_res != download_result::notfound) {
        co_return error_outcome::manifest_download_error;
    }

    const auto json_path_str
      = remote_path_provider_.partition_manifest_path_json(ntp_, rev_);
    if (!json_path_str.has_value()) {
        co_return find_partition_manifest_outcome::no_matching_manifest;
    }
    const auto json_path = remote_manifest_path{json_path_str.value()};
    auto json_res = co_await remote_.object_exists(
      bucket_,
      cloud_storage_clients::object_key{json_path},
      retry_node,
      existence_check_type::manifest);
    if (json_res == download_result::success) {
        co_return find_partition_manifest_outcome::success;
    }
    if (json_res != download_result::notfound) {
        co_return error_outcome::manifest_download_error;
    }
    co_return find_partition_manifest_outcome::no_matching_manifest;
}

} // namespace cloud_storage
