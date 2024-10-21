// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/seastarx.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <string_view>

namespace cloud_storage {

/// Encapsulates the logic for constructing and parsing the path of a topic
/// mount manifest.
class topic_mount_manifest_path {
    static inline constexpr std::string_view path_prefix = "migration/";

public:
    explicit topic_mount_manifest_path(
      model::cluster_uuid cluster_uuid,
      model::topic_namespace tp_ns,
      model::initial_revision_id rev);

    explicit operator ss::sstring() const;

    [[nodiscard]] static std::optional<topic_mount_manifest_path>
    parse(const std::string_view);

    [[nodiscard]] static constexpr inline std::string_view prefix() {
        return path_prefix;
    }

    [[nodiscard]] const model::cluster_uuid& cluster_uuid() const noexcept {
        return _cluster_uuid;
    }

    [[nodiscard]] const model::topic_namespace& tp_ns() const noexcept {
        return _tp_ns;
    }

    [[nodiscard]] const model::initial_revision_id rev() const noexcept {
        return _rev;
    }

    friend bool operator==(
      const topic_mount_manifest_path& lhs,
      const topic_mount_manifest_path& rhs)
      = default;

private:
    model::cluster_uuid _cluster_uuid;
    model::topic_namespace _tp_ns;
    model::initial_revision_id _rev;
};

} // namespace cloud_storage
