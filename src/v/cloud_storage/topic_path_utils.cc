// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "cloud_storage/topic_path_utils.h"

#include "cloud_storage/remote_label.h"
#include "hashing/xx.h"
#include "model/fundamental.h"

#include <regex>

namespace cloud_storage {

namespace {
const std::regex prefixed_manifest_path_expr{
  R"REGEX(\w+/meta/([^/]+)/([^/]+)/topic_manifest\.(json|bin))REGEX"};

const std::regex labeled_manifest_path_expr{
  R"REGEX(meta/([^/]+)/([^/]+)/[^/]+/\d+/topic_manifest\.bin)REGEX"};
} // namespace

ss::sstring labeled_topic_manifests_root() { return "meta"; }

chunked_vector<ss::sstring> prefixed_topic_manifests_roots() {
    constexpr static auto hex_chars = std::string_view{"0123456789abcdef"};
    chunked_vector<ss::sstring> roots;
    roots.reserve(hex_chars.size());
    for (char c : hex_chars) {
        roots.emplace_back(fmt::format("{}0000000", c));
    }
    return roots;
}

ss::sstring labeled_topic_manifest_root(const model::topic_namespace& topic) {
    return fmt::format(
      "{}/{}/{}", labeled_topic_manifests_root(), topic.ns(), topic.tp());
}

ss::sstring labeled_topic_manifest_prefix(
  const remote_label& label, const model::topic_namespace& topic) {
    return fmt::format(
      "{}/{}", labeled_topic_manifest_root(topic), label.cluster_uuid());
}

ss::sstring labeled_topic_manifest_path(
  const remote_label& label,
  const model::topic_namespace& topic,
  model::initial_revision_id rev) {
    return fmt::format(
      "{}/{}/topic_manifest.bin",
      labeled_topic_manifest_prefix(label, topic),
      rev());
}

ss::sstring labeled_topic_lifecycle_marker_path(
  const remote_label& label,
  const model::topic_namespace& topic,
  model::initial_revision_id rev) {
    return fmt::format(
      "{}/{}_lifecycle.bin",
      labeled_topic_manifest_prefix(label, topic),
      rev());
}

ss::sstring
prefixed_topic_manifest_prefix(const model::topic_namespace& topic) {
    constexpr uint32_t bitmask = 0xF0000000;
    auto path = fmt::format("{}/{}", topic.ns(), topic.tp());
    uint32_t hash = bitmask & xxhash_32(path.data(), path.size());
    return fmt::format("{:08x}/meta/{}/{}", hash, topic.ns(), topic.tp());
}

ss::sstring
prefixed_topic_manifest_bin_path(const model::topic_namespace& topic) {
    return fmt::format(
      "{}/topic_manifest.bin", prefixed_topic_manifest_prefix(topic));
}

ss::sstring
prefixed_topic_manifest_json_path(const model::topic_namespace& topic) {
    return fmt::format(
      "{}/topic_manifest.json", prefixed_topic_manifest_prefix(topic));
}

ss::sstring prefixed_topic_lifecycle_marker_path(
  const model::topic_namespace& topic, model::initial_revision_id rev) {
    return fmt::format(
      "{}/{}_lifecycle.bin", prefixed_topic_manifest_prefix(topic), rev());
}

std::optional<model::topic_namespace>
tp_ns_from_labeled_path(const std::string& path) {
    std::smatch matches;
    const auto is_topic_manifest = std::regex_match(
      path.cbegin(), path.cend(), matches, labeled_manifest_path_expr);
    if (!is_topic_manifest) {
        return std::nullopt;
    }
    const auto& ns = matches[1].str();
    const auto& tp = matches[2].str();
    return model::topic_namespace{model::ns{ns}, model::topic{tp}};
}

std::optional<model::topic_namespace>
tp_ns_from_prefixed_path(const std::string& path) {
    std::smatch matches;
    const auto is_topic_manifest = std::regex_match(
      path.cbegin(), path.cend(), matches, prefixed_manifest_path_expr);
    if (!is_topic_manifest) {
        return std::nullopt;
    }
    const auto& ns = matches[1].str();
    const auto& tp = matches[2].str();
    return model::topic_namespace{model::ns{ns}, model::topic{tp}};
}

} // namespace cloud_storage
