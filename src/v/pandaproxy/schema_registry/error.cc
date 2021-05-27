/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "error.h"

#include "pandaproxy/error.h"
#include "pandaproxy/schema_registry/error.h"

namespace pandaproxy::schema_registry {

namespace {

struct error_category final : std::error_category {
    const char* name() const noexcept override {
        return "pandaproxy::schema_registry";
    }
    std::string message(int ev) const override {
        switch (static_cast<error_code>(ev)) {
        case error_code::schema_id_not_found:
            return "Schema not found";
        case error_code::schema_invalid:
            return "Invalid schema";
        case error_code::subject_not_found:
            return "Subject not found";
        case error_code::subject_version_not_found:
            return "Subject version not found";
        }
        return "(unrecognized error)";
    }
    // TODO(Ben): Determine how best to use default_error_condition between
    // pandaproxy/rest and pandaproxy/schema_registry
    std::error_condition
    default_error_condition(int ec) const noexcept override {
        switch (static_cast<error_code>(ec)) {
        case error_code::schema_id_not_found:
        case error_code::subject_not_found:
        case error_code::subject_version_not_found:
            return reply_error_code::topic_not_found; // 40401
        case error_code::schema_invalid:
            return reply_error_code::unprocessable_entity;
        }
        return {};
    }
};

const error_category pps_error_category{};

}; // namespace

std::error_code make_error_code(error_code e) {
    return {static_cast<int>(e), pps_error_category};
}

} // namespace pandaproxy::schema_registry
