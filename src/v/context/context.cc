// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "context/context.h"

namespace context {

system_clock::time_point wall_deadline(const context_ref ctx) noexcept {
    auto internal_deadline = ctx.deadline();

    if (internal_deadline == context::no_deadline) {
        return system_clock::time_point::max();
    }

    auto remaining = internal_deadline - clock::now();
    auto sys_tp = lowres_system_clock::now() + remaining;

    return system_clock::time_point{
      std::chrono::duration_cast<system_clock::duration>(
        sys_tp.time_since_epoch())};
}

} // namespace context
