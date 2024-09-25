
/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "raft/errc.h"
#include "raft/tests/raft_fixture.h"

namespace raft {
template<>
struct raft_fixture::retry_policy<std::error_code> {
    static std::error_code timeout_error() {
        return raft::make_error_code(raft::errc::timeout);
    }
    static bool should_retry(const std::error_code& err) {
        if (err.category() == raft::error_category()) {
            return retry_policy<raft::errc>::should_retry(
              raft::errc(err.value()));
        } else {
            return false;
        }
    }
};
} // namespace raft
