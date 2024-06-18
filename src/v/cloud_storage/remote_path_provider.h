// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cloud_storage/fwd.h"
#include "cloud_storage/remote_label.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/core/sstring.hh>

#include <optional>

namespace cloud_storage {

class remote_path_provider {
public:
    explicit remote_path_provider(std::optional<remote_label> label);

    // Prefix of the topic manifest path. This can be used to filter objects to
    // find topic manifests.
    ss::sstring
    topic_manifest_prefix(const model::topic_namespace& topic) const;

    // Topic manifest path.
    ss::sstring topic_manifest_path(
      const model::topic_namespace& topic, model::initial_revision_id) const;

    // Prefix of the partition manifest path. This can be used to filter
    // objects to find partition or spillover manifests.
    ss::sstring partition_manifest_prefix(
      const model::ntp& ntp, model::initial_revision_id) const;

    // Partition manifest path. The returned path is expected to be used as the
    // path of STM manifest (i.e. not a spillover manifest).
    ss::sstring partition_manifest_path(
      const model::ntp& ntp, model::initial_revision_id) const;

    // Partition manifest path.
    // NOTE: also accepts subclasses of partition manifest, e.g. spillover
    // manifests.
    ss::sstring
    partition_manifest_path(const partition_manifest& manifest) const;

    // The JSON path of a partition manifest, if supported by `label_`.
    // E.g., when a label is supplied, partition manifests are expected to not
    // be written as JSON, and this will return std::nullopt.
    std::optional<ss::sstring> partition_manifest_path_json(
      const model::ntp& ntp, model::initial_revision_id) const;

    // Spillover manifest path.
    ss::sstring spillover_manifest_path(
      const partition_manifest& stm_manifest,
      const spillover_manifest_path_components& c) const;

    // Segment paths.
    ss::sstring segment_path(
      const partition_manifest& manifest, const segment_meta& segment) const;
    ss::sstring segment_path(
      const model::ntp& ntp,
      model::initial_revision_id rev,
      const segment_meta& segment) const;

private:
    std::optional<remote_label> label_;
};

} // namespace cloud_storage
