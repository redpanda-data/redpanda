// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "cloud_storage/remote_path_provider.h"

#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/partition_path_utils.h"
#include "cloud_storage/remote_label.h"
#include "cloud_storage/segment_path_utils.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/topic_mount_manifest.h"
#include "cloud_storage/topic_path_utils.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"

#include <utility>

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

remote_path_provider::remote_path_provider(
  std::optional<remote_label> label,
  std::optional<model::topic_namespace> topic_namespace_override)
  : label_(label)
  , _topic_namespace_override(std::move(topic_namespace_override)) {}

remote_path_provider remote_path_provider::copy() const {
    remote_path_provider ret(label_, _topic_namespace_override);
    return ret;
}

ss::sstring remote_path_provider::topic_manifest_prefix(
  const model::topic_namespace& topic) const {
    const auto& tp_ns = _topic_namespace_override.value_or(topic);
    if (label_.has_value()) {
        return labeled_topic_manifest_prefix(*label_, tp_ns);
    }
    return prefixed_topic_manifest_prefix(tp_ns);
}

ss::sstring remote_path_provider::topic_manifest_path(
  const model::topic_namespace& topic, model::initial_revision_id rev) const {
    const auto& tp_ns = _topic_namespace_override.value_or(topic);
    if (label_.has_value()) {
        return labeled_topic_manifest_path(*label_, tp_ns, rev);
    }
    return prefixed_topic_manifest_bin_path(tp_ns);
}

std::optional<ss::sstring> remote_path_provider::topic_manifest_path_json(
  const model::topic_namespace& topic) const {
    if (label_.has_value()) {
        return std::nullopt;
    }
    const auto& tp_ns = _topic_namespace_override.value_or(topic);
    return prefixed_topic_manifest_json_path(tp_ns);
}

ss::sstring remote_path_provider::partition_manifest_prefix(
  const model::ntp& ntp, model::initial_revision_id rev) const {
    std::optional<model::ntp> ntp_override;
    if (_topic_namespace_override.has_value()) {
        ntp_override = model::ntp(
          _topic_namespace_override->ns,
          _topic_namespace_override->tp,
          ntp.tp.partition);
    }
    const auto& maybe_overridden_ntp = ntp_override.value_or(ntp);
    if (label_.has_value()) {
        return labeled_partition_manifest_prefix(
          *label_, maybe_overridden_ntp, rev);
    }
    return prefixed_partition_manifest_prefix(maybe_overridden_ntp, rev);
}

ss::sstring remote_path_provider::partition_manifest_path(
  const partition_manifest& manifest) const {
    return fmt::format(
      "{}/{}",
      partition_manifest_prefix(manifest.get_ntp(), manifest.get_revision_id()),
      manifest.get_manifest_filename());
}

ss::sstring remote_path_provider::partition_manifest_path(
  const model::ntp& ntp, model::initial_revision_id rev) const {
    return fmt::format(
      "{}/{}",
      partition_manifest_prefix(ntp, rev),
      partition_manifest::filename());
}

std::optional<ss::sstring> remote_path_provider::partition_manifest_path_json(
  const model::ntp& ntp, model::initial_revision_id rev) const {
    if (label_.has_value()) {
        return std::nullopt;
    }
    std::optional<model::ntp> ntp_override;
    if (_topic_namespace_override.has_value()) {
        ntp_override = model::ntp(
          _topic_namespace_override->ns,
          _topic_namespace_override->tp,
          ntp.tp.partition);
    }
    const auto& maybe_overridden_ntp = ntp_override.value_or(ntp);
    return prefixed_partition_manifest_json_path(maybe_overridden_ntp, rev);
}

ss::sstring remote_path_provider::spillover_manifest_path(
  const partition_manifest& stm_manifest,
  const spillover_manifest_path_components& c) const {
    return fmt::format(
      "{}/{}",
      partition_manifest_prefix(
        stm_manifest.get_ntp(), stm_manifest.get_revision_id()),
      spillover_manifest::filename(c));
}

ss::sstring remote_path_provider::topic_mount_manifest_path(
  const topic_mount_manifest& manifest) const {
    return ss::sstring(cloud_storage::topic_mount_manifest_path(
      manifest.get_source_label().cluster_uuid,
      manifest.get_tp_ns(),
      manifest.get_revision_id()));
}

ss::sstring remote_path_provider::segment_path(
  const model::ntp& ntp,
  model::initial_revision_id rev,
  const segment_meta& segment) const {
    const auto segment_name = partition_manifest::generate_remote_segment_name(
      segment);
    std::optional<model::ntp> ntp_override;
    if (_topic_namespace_override.has_value()) {
        ntp_override = model::ntp(
          _topic_namespace_override->ns,
          _topic_namespace_override->tp,
          ntp.tp.partition);
    }
    const auto& maybe_overridden_ntp = ntp_override.value_or(ntp);
    if (label_.has_value()) {
        return labeled_segment_path(
          *label_,
          maybe_overridden_ntp,
          rev,
          segment_name,
          segment.archiver_term);
    }
    return prefixed_segment_path(
      maybe_overridden_ntp, rev, segment_name, segment.archiver_term);
}

ss::sstring remote_path_provider::segment_path(
  const partition_manifest& manifest, const segment_meta& segment) const {
    return segment_path(
      manifest.get_ntp(), manifest.get_revision_id(), segment);
}

ss::sstring remote_path_provider::topic_lifecycle_marker_path(
  const model::topic_namespace& topic, model::initial_revision_id rev) const {
    const auto& tp_ns = _topic_namespace_override.value_or(topic);
    if (label_.has_value()) {
        return labeled_topic_lifecycle_marker_path(*label_, tp_ns, rev);
    }
    return prefixed_topic_lifecycle_marker_path(tp_ns, rev);
}

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
    } catch (const std::exception& e) {
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
    } catch (const std::exception& e) {
        return std::nullopt;
    }

    return topic_mount_manifest_path(
      model::cluster_uuid{label},
      model::topic_namespace{model::ns(ns), model::topic(tp)},
      rev);
}

} // namespace cloud_storage
