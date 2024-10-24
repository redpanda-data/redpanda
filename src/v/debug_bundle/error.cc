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

#include "error.h"

#include <system_error>

namespace debug_bundle {
namespace {
struct error_category final : std::error_category {
    const char* name() const noexcept override { return "debug_bundle"; }

    std::string message(int ev) const override {
        switch (static_cast<error_code>(ev)) {
        case error_code::success:
            return "success";
        case error_code::debug_bundle_process_running:
            return "debug bundle process currently running";
        case error_code::debug_bundle_process_not_running:
            return "debug bundle process not currently running";
        case error_code::invalid_parameters:
            return "invalid parameters";
        case error_code::process_failed:
            return "process failed";
        case error_code::internal_error:
            return "internal system error";
        case error_code::job_id_not_recognized:
            return "job id not recognized";
        case error_code::debug_bundle_process_never_started:
            return "debug bundle process was never started";
        case error_code::rpk_binary_not_present:
            return "rpk binary not present";
        case error_code::debug_bundle_expired:
            return "debug bundle expired and was removed";
        }

        return "(unknown error code)";
    }
};

const error_category debug_bundle_error_category{};
} // namespace

std::error_code make_error_code(error_code e) noexcept {
    return {static_cast<int>(e), debug_bundle_error_category};
}

} // namespace debug_bundle
