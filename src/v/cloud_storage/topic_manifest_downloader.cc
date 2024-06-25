// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/topic_manifest_downloader.h"

#include "base/outcome.h"
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/topic_path_utils.h"
#include "cloud_storage/types.h"
#include "container/fragmented_vector.h"
#include "hashing/xx.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/lowres_clock.hh>

namespace cloud_storage {

namespace {

const std::regex topic_manifest_suffix_regex{
  R"REGEX([A-Za-z0-9-]/[a-z0-9-]+/topic_manifest.bin)REGEX"};

} // anonymous namespace

topic_manifest_downloader::topic_manifest_downloader(
  const cloud_storage_clients::bucket_name bucket,
  const std::optional<remote_label> remote_label,
  const model::topic_namespace topic,
  remote& remote)
  : bucket_(bucket)
  , remote_label_(remote_label)
  , topic_(topic)
  , remote_(remote) {}

ss::future<result<find_topic_manifest_outcome, error_outcome>>
topic_manifest_downloader::download_manifest(
  retry_chain_node& parent_retry,
  ss::lowres_clock::time_point deadline,
  model::timestamp_clock::duration backoff,
  topic_manifest* manifest) {
    retry_chain_node retry_node(deadline, backoff, &parent_retry);

    // First look for topic manifests with a label.
    ss::sstring remote_label_str{""};
    if (remote_label_.has_value()) {
        remote_label_str = fmt::format(
          "{}/", remote_label_.value().cluster_uuid);
    }
    const auto labeled_manifest_filter = fmt::format(
      "meta/{}/{}/{}", topic_.ns(), topic_.tp(), remote_label_str);
    auto list_res = co_await remote_.list_objects(
      bucket_,
      retry_node,
      cloud_storage_clients::object_key{labeled_manifest_filter});
    if (list_res.has_error()) {
        co_return error_outcome::manifest_download_error;
    }
    // If there's more than one, callers will need to pass a label (or a more
    // specific one).
    chunked_vector<ss::sstring> list_contents;
    for (auto& entry : list_res.value().contents) {
        if (entry.key.ends_with("topic_manifest.bin")) {
            list_contents.emplace_back(entry.key);
        }
    }
    if (list_contents.size() > 1) {
        co_return find_topic_manifest_outcome::multiple_matching_manifests;
    }
    // If there's exactly one, presume it's the one we care about. Since
    // labeled manifests are newer, they take precedence over prefixed
    // manifests.
    if (list_contents.size() == 1) {
        const remote_manifest_path labeled_manifest(list_contents[0]);
        auto manifest_res = co_await remote_.download_manifest_bin(
          bucket_, labeled_manifest, *manifest, retry_node);
        if (manifest_res == cloud_storage::download_result::success) {
            co_return find_topic_manifest_outcome::success;
        }
        // Regardless of the outcome (i.e. even not-found), return an error. If
        // we had a list result but it was deleted, something is suspicious, so
        // don't proceed.
        co_return error_outcome::manifest_download_error;
    }
    // If we had been supplied a label but we don't see a matching manifest,
    // don't look for other manifests. The label is a signal that we are only
    // looking for labeled manifests.
    if (remote_label_.has_value()) {
        co_return find_topic_manifest_outcome::no_matching_manifest;
    }

    // Then look for prefixed binary manifests. If we find one, return it:
    // since they're newer than JSON manifests, they take precedence.
    const remote_manifest_path prefixed_bin_path(
      prefixed_topic_manifest_bin_path(topic_));
    auto bin_manifest_res = co_await remote_.download_manifest_bin(
      bucket_, prefixed_bin_path, *manifest, retry_node);
    if (bin_manifest_res == cloud_storage::download_result::success) {
        co_return find_topic_manifest_outcome::success;
    }
    if (bin_manifest_res != cloud_storage::download_result::notfound) {
        co_return error_outcome::manifest_download_error;
    }

    // Finally, look for prefixed json topic manifest.
    const remote_manifest_path prefixed_json_path(
      prefixed_topic_manifest_json_path(topic_));
    auto json_manifest_res = co_await remote_.download_manifest_json(
      bucket_, prefixed_json_path, *manifest, retry_node);
    if (json_manifest_res == cloud_storage::download_result::success) {
        co_return find_topic_manifest_outcome::success;
    }
    if (json_manifest_res != cloud_storage::download_result::notfound) {
        co_return error_outcome::manifest_download_error;
    }
    co_return find_topic_manifest_outcome::no_matching_manifest;
}

} // namespace cloud_storage
