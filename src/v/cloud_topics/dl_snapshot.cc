// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/dl_snapshot.h"

auto fmt::formatter<experimental::cloud_topics::dl_snapshot_id>::format(
  const experimental::cloud_topics::dl_snapshot_id& id,
  fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", id.version);
}
