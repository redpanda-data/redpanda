// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/topic_mount_manifest_path.h"

#include <regex>

namespace {
// Topic manifest path format to be used with fmt library.
// The format is `migration/{namespace}/{label}/{topic}/{rev}`.
constexpr std::string_view topic_mount_manifest_path_fmt
  = "migration/{}/{}/{}/{}";

// Topic manifest path regex expression for parsing a path formatted with
// topic_mount_manifest_path_fmt.
constexpr std::string_view topic_mount_manifest_path_expr
  = R"REGEX(migration/([^/]+)/([^/]+)/([^/]+)/([^/]+))REGEX";

static_assert(
  topic_mount_manifest_path_fmt.starts_with(
    cloud_storage::topic_mount_manifest_path::prefix()),
  "topic_mount_manifest_path_fmt must start with migration prefix");

static_assert(
  topic_mount_manifest_path_expr.starts_with(
    cloud_storage::topic_mount_manifest_path::prefix()),
  "topic_mount_manifest_path_expr must start with migration prefix");

const std::regex topic_mount_manifest_path_re{
  topic_mount_manifest_path_expr.data(), topic_mount_manifest_path_expr.size()};

} // namespace

namespace cloud_storage {

topic_mount_manifest_path::topic_mount_manifest_path(
  model::cluster_uuid cluster_uuid,
  model::topic_namespace tp_ns,
  model::initial_revision_id rev)
  : _cluster_uuid(cluster_uuid)
  , _tp_ns(std::move(tp_ns))
  , _rev(rev) {}

topic_mount_manifest_path::operator ss::sstring() const {
    return fmt::format(
      topic_mount_manifest_path_fmt, _cluster_uuid, _tp_ns.ns, _tp_ns.tp, _rev);
}

std::optional<topic_mount_manifest_path>
topic_mount_manifest_path::parse(const std::string_view path) {
    std::match_results<std::string_view::const_iterator> matches;
    const auto valid = std::regex_match(
      path.cbegin(), path.cend(), matches, topic_mount_manifest_path_re);
    if (!valid) {
        return std::nullopt;
    }
    uuid_t label{};
    try {
        label = uuid_t::from_string(
          std::string_view{matches[1].first, matches[1].second});
    } catch (...) {
        return std::nullopt;
    }
    const auto& ns = matches[2].str();
    const auto& tp = matches[3].str();

    model::initial_revision_id rev;
    try {
        size_t processed_chars = 0;
        rev = model::initial_revision_id(
          std::stoll(matches[4].str(), &processed_chars));
        if (processed_chars != matches[4].str().length()) {
            return std::nullopt;
        }
    } catch (...) {
        return std::nullopt;
    }

    return topic_mount_manifest_path(
      model::cluster_uuid{label},
      model::topic_namespace{model::ns(ns), model::topic(tp)},
      rev);
}
} // namespace cloud_storage
