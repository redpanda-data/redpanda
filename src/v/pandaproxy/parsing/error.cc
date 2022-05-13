/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "error.h"

namespace pandaproxy::parse {

namespace {

struct error_category final : std::error_category {
    const char* name() const noexcept override { return "pandaproxy::parse"; }
    std::string message(int ev) const override {
        switch (static_cast<error_code>(ev)) {
        case error_code::empty_param:
            return "empty_param";
        case error_code::invalid_param:
            return "invalid_param";
        case error_code::not_acceptable:
            return "not_acceptable";
        case error_code::unsupported_media_type:
            return "unsupported_media_type";
        }
        return "(unrecognized error)";
    }
};

const error_category ppp_error_category{};

}; // namespace

std::error_code make_error_code(error_code ec) {
    return {static_cast<int>(ec), ppp_error_category};
}

} // namespace pandaproxy::parse
