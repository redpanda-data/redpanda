/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "panda_link/errc.h"

namespace panda_link {
namespace {
struct error_category final : std::error_category {
    const char* name() const noexcept override { return "panda_link"; }

    std::string message(int ev) const override {
        switch (static_cast<errc>(ev)) {
        case errc::success:
            return "success";
        case errc::panda_link_does_not_exist:
            return "panda_link_does_not_exist";
        case errc::panda_link_already_exists:
            return "panda_link_already_exists";
        case errc::invalid_configuration:
            return "invalid_configuration";
        case errc::internal_server_error:
            return "internal_server_error";
        case errc::unknown_error:
            return "unknown_error";
        }
        return "(unknown error code)";
    }
};

const error_category panda_link_error_category{};
} // namespace

std::error_code make_error_code(errc e) noexcept {
    return {static_cast<int>(e), panda_link_error_category};
}
} // namespace panda_link
