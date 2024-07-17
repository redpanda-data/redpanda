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
#include "base/vlog.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/topic_path_utils.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/client.h"
#include "container/fragmented_vector.h"
#include "hashing/xx.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>

#include <absl/container/btree_set.h>

namespace cloud_storage {

namespace {
bool bin_manifest_filter(
  const cloud_storage_clients::client::list_bucket_item& i) {
    return i.key.ends_with("topic_manifest.bin");
};
} // namespace

std::ostream& operator<<(std::ostream& os, find_topic_manifest_outcome o) {
    switch (o) {
    case find_topic_manifest_outcome::success:
        os << "find_topic_manifest_outcome::success";
        break;
    case find_topic_manifest_outcome::no_matching_manifest:
        os << "find_topic_manifest_outcome::no_matching_manifest";
        break;
    case find_topic_manifest_outcome::multiple_matching_manifests:
        os << "find_topic_manifest_outcome::multiple_matching_manifests";
        break;
    }
    return os;
}

topic_manifest_downloader::topic_manifest_downloader(
  const cloud_storage_clients::bucket_name bucket,
  std::optional<ss::sstring> hint,
  const model::topic_namespace topic,
  remote& remote)
  : bucket_(bucket)
  , label_hint_(std::move(hint))
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
    ss::sstring remote_label_str = label_hint_.value_or("");
    const auto labeled_manifest_filter = fmt::format(
      "{}/{}", labeled_topic_manifest_root(topic_), remote_label_str);
    auto list_res = co_await remote_.list_objects(
      bucket_,
      retry_node,
      cloud_storage_clients::object_key{labeled_manifest_filter},
      std::nullopt,
      bin_manifest_filter);
    if (list_res.has_error()) {
        vlog(
          cst_log.error,
          "Labeled topic manifest download resulted in listing error with "
          "prefix '{}': {}",
          labeled_manifest_filter,
          list_res.error());
        co_return error_outcome::manifest_download_error;
    }
    // If there's more than one, callers will need to pass a label (or a more
    // specific one).
    auto list_contents = std::move(list_res.value().contents);
    if (list_contents.size() > 1) {
        static constexpr size_t max_to_print = 10;
        const size_t num_to_print = std::min(
          list_contents.size(), max_to_print);
        vlog(
          cst_log.info,
          "Labeled topic manifest download resulted in {} matching "
          "manifests with prefix '{}', printing first {}",
          list_contents.size(),
          remote_label_str,
          num_to_print);
        size_t num_printed = 0;
        for (const auto& item : list_contents) {
            if (num_printed == num_to_print) {
                break;
            }
            vlog(
              cst_log.info,
              "Match for hint '{}': {}",
              remote_label_str,
              item.key);
            ++num_printed;
        }
        co_return find_topic_manifest_outcome::multiple_matching_manifests;
    }
    // If there's exactly one, presume it's the one we care about. Since
    // labeled manifests are newer, they take precedence over prefixed
    // manifests.
    if (list_contents.size() == 1) {
        const auto labeled_manifest = remote_manifest_path{
          list_contents[0].key};
        auto manifest_res = co_await remote_.download_manifest_bin(
          bucket_, labeled_manifest, *manifest, retry_node);
        if (manifest_res == cloud_storage::download_result::success) {
            co_return find_topic_manifest_outcome::success;
        }
        // Regardless of the outcome (i.e. even not-found), return an error. If
        // we had a list result but it was deleted, something is suspicious, so
        // don't proceed.
        vlog(
          cst_log.error,
          "Labeled topic manifest download result with path {}: {}",
          labeled_manifest,
          manifest_res);
        co_return error_outcome::manifest_download_error;
    }

    // Then look for prefixed binary manifests. If we find one, return it:
    // since they're newer than JSON manifests, they take precedence.
    const remote_manifest_path prefixed_bin_path(
      prefixed_topic_manifest_bin_path(topic_));
    vlog(
      cst_log.info,
      "Labeled topic manifest download at {} resulted in no manifests, falling "
      "back on hash-prefixed binary manifest",
      labeled_manifest_filter);
    auto bin_manifest_res = co_await remote_.download_manifest_bin(
      bucket_, prefixed_bin_path, *manifest, retry_node);
    if (bin_manifest_res == cloud_storage::download_result::success) {
        co_return find_topic_manifest_outcome::success;
    }
    if (bin_manifest_res != cloud_storage::download_result::notfound) {
        vlog(
          cst_log.error,
          "Prefixed topic manifest download result with path {}: {}",
          prefixed_bin_path,
          bin_manifest_res);
        co_return error_outcome::manifest_download_error;
    }
    vlog(
      cst_log.info,
      "Prefixed topic manifest download at {} resulted in no manifests, "
      "falling back on hash-prefixed JSON manifest",
      prefixed_bin_path);

    // Finally, look for prefixed json topic manifest.
    const remote_manifest_path prefixed_json_path(
      prefixed_topic_manifest_json_path(topic_));
    auto json_manifest_res = co_await remote_.download_manifest_json(
      bucket_, prefixed_json_path, *manifest, retry_node);
    if (json_manifest_res == cloud_storage::download_result::success) {
        co_return find_topic_manifest_outcome::success;
    }
    if (json_manifest_res != cloud_storage::download_result::notfound) {
        vlog(
          cst_log.error,
          "Prefixed topic manifest download result with path {}: {}",
          prefixed_json_path,
          json_manifest_res);
        co_return error_outcome::manifest_download_error;
    }
    vlog(
      cst_log.info,
      "Prefixed topic manifest download at {} resulted in no manifests",
      prefixed_json_path);
    co_return find_topic_manifest_outcome::no_matching_manifest;
}

namespace {
using list_outcome_t
  = std::vector<cloud_storage_clients::client::list_bucket_item>;
ss::future<result<list_outcome_t, error_outcome>> find_prefixed_manifest_paths(
  remote& remote,
  cloud_storage_clients::bucket_name bucket,
  retry_chain_node& parent_retry,
  ss::lowres_clock::time_point deadline,
  model::timestamp_clock::duration backoff,
  const ss::sstring& hash_prefix) {
    vassert(
      hash_prefix.size() == 8,
      "Expected prefix [0-9a-f]0000000, got {}",
      hash_prefix);
    retry_chain_node retry(deadline, backoff, &parent_retry);
    auto prefixed_list_res = co_await remote.list_objects(
      bucket,
      retry,
      cloud_storage_clients::object_key{fmt::format("{}/", hash_prefix)});
    if (prefixed_list_res.has_error()) {
        vlog(
          cst_log.error,
          "Finding prefixed topic manifest resulted in listing error under "
          "prefix {}: {}",
          hash_prefix,
          prefixed_list_res.error());
        co_return error_outcome::manifest_download_error;
    }
    co_return prefixed_list_res.value().contents;
}
} // namespace

ss::future<result<find_topic_manifest_outcome, error_outcome>>
topic_manifest_downloader::find_manifests(
  remote& remote,
  cloud_storage_clients::bucket_name bucket,
  retry_chain_node& parent_retry,
  ss::lowres_clock::time_point deadline,
  model::timestamp_clock::duration backoff,
  std::optional<tp_ns_filter_t> tp_filter,
  chunked_vector<topic_manifest>* manifests) {
    retry_chain_node retry(deadline, backoff, &parent_retry);
    absl::btree_set<model::topic_namespace> topics;
    const auto maybe_add_topic =
      [&topics,
       &tp_filter](const std::optional<model::topic_namespace>& topic) {
          if (!topic.has_value()) {
              return;
          }
          if (!tp_filter.has_value() || tp_filter.value()(*topic)) {
              topics.emplace(*topic);
          }
      };
    {
        // First, collect labeled topic manifests that match out filter.
        const auto labeled_list_res = co_await remote.list_objects(
          bucket,
          retry,
          cloud_storage_clients::object_key{
            fmt::format("{}/", labeled_topic_manifests_root())});
        if (labeled_list_res.has_error()) {
            vlog(
              cst_log.error,
              "Finding topic manifest resulted in listing error with prefix "
              "'{}': {}",
              labeled_topic_manifests_root(),
              labeled_list_res.error());
            co_return error_outcome::manifest_download_error;
        }
        for (const auto& item : labeled_list_res.value().contents) {
            auto tp_ns = tp_ns_from_labeled_path(item.key);
            maybe_add_topic(tp_ns);
        }
        // End scope to free memory.
    }
    // Next, collect prefixed topic manifests that match out filter.
    static const auto prefixed_roots = prefixed_topic_manifests_roots();
    std::vector<ss::future<result<list_outcome_t, error_outcome>>> futs;
    futs.reserve(prefixed_roots.size());
    for (const auto& root : prefixed_roots) {
        futs.push_back(find_prefixed_manifest_paths(
          remote, bucket, parent_retry, deadline, backoff, root));
    }
    auto prefixed_res = co_await ss::when_all_succeed(futs.begin(), futs.end());
    bool has_prefix_list_error = false;
    for (const auto& r : prefixed_res) {
        if (r.has_error()) {
            vlog(
              cst_log.error,
              "Finding topic manifest resulted in listing error for prefixed "
              "roots: {}",
              r.error());
            has_prefix_list_error = true;
            continue;
        }
        for (const auto& item : r.value()) {
            auto tp_ns = tp_ns_from_prefixed_path(item.key);
            maybe_add_topic(tp_ns);
        }
    }
    if (has_prefix_list_error) {
        co_return error_outcome::manifest_download_error;
    }
    // Use the manifest downloader to look for the filtered manifests.
    chunked_vector<topic_manifest> m;
    m.reserve(topics.size());
    for (const auto& tp : topics) {
        topic_manifest tm;
        topic_manifest_downloader dl(bucket, /*hint=*/std::nullopt, tp, remote);
        // Not the most optimal since the downloader will check multiple paths,
        // even though we looked at paths above, but this is nice and tidy.
        auto res = co_await dl.download_manifest(
          parent_retry, deadline, backoff, &tm);
        if (res.has_error()) {
            vlog(
              cst_log.error,
              "Finding topic manifest for {} resulted in download error: {}",
              tp,
              res.error());
            co_return res.error();
        }
        if (res.value() != find_topic_manifest_outcome::success) {
            vlog(
              cst_log.info,
              "Finding topic manifest for {} resulted in download outcome: {}",
              tp,
              res.value());
            co_return res.value();
        }
        m.push_back(std::move(tm));
    }
    *manifests = std::move(m);
    co_return find_topic_manifest_outcome::success;
}

} // namespace cloud_storage
