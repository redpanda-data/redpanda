// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cloud_topics/dl_overlay.h"
#include "cloud_topics/dl_version.h"
#include "container/fragmented_vector.h"
#include "serde/envelope.h"

namespace experimental::cloud_topics {

struct dl_snapshot_id
  : serde::
      envelope<dl_snapshot_id, serde::version<0>, serde::compat_version<0>> {
    dl_snapshot_id() noexcept = default;

    explicit dl_snapshot_id(dl_version version) noexcept
      : version(version) {}

    auto serde_fields() { return std::tie(version); }

    bool operator==(const dl_snapshot_id& other) const noexcept = default;

    /// Version for which the snapshot is created.
    dl_version version;
};

struct dl_snapshot_payload
  : serde::checksum_envelope<
      dl_snapshot_id,
      serde::version<0>,
      serde::compat_version<0>> {
    /// Version for which the snapshot is created.
    dl_snapshot_id id;

    /// Overlays visible at the snapshot version.
    fragmented_vector<dl_overlay> overlays;
};

}; // namespace experimental::cloud_topics

template<>
struct fmt::formatter<experimental::cloud_topics::dl_snapshot_id>
  : fmt::formatter<std::string_view> {
    auto format(
      const experimental::cloud_topics::dl_snapshot_id&,
      fmt::format_context& ctx) const -> decltype(ctx.out());
};
