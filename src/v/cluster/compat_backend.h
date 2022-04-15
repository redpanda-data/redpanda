/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/commands.h"
#include "cluster/errc.h"

#include <seastar/core/future.hh>

namespace cluster {
/**
 *
 * This is a backend for mux_stm that consumes and ignores future-version
 * message types, enabling us to start and run an older redpanda with
 * a controller log that contains newer-versioned structures.
 */
class compat_backend {
public:
    compat_backend() noexcept {}

    /**
     * Ignore the message, it is not meaningful to this version of Redpanda
     */
    ss::future<std::error_code> apply_update(model::record_batch) {
        return ss::make_ready_future<std::error_code>(cluster::errc::success);
    }

    bool is_batch_applicable(const model::record_batch& b) {
        return b.header().type == model::record_batch_type::feature_update;
    }

private:
    static constexpr auto accepted_commands
      = make_commands_list<feature_update_cmd>();
};

} // namespace cluster