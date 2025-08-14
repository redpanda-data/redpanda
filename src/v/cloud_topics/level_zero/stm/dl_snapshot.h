/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_zero/stm/dl_version.h"
#include "container/chunked_vector.h"
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
      dl_snapshot_payload,
      serde::version<0>,
      serde::compat_version<0>> {
    /// Version for which the snapshot is created.
    dl_snapshot_id id;
};

}; // namespace experimental::cloud_topics

template<>
struct fmt::formatter<experimental::cloud_topics::dl_snapshot_id>
  : fmt::formatter<std::string_view> {
    auto format(
      const experimental::cloud_topics::dl_snapshot_id&,
      fmt::format_context& ctx) const -> decltype(ctx.out());
};
