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
        case error_code::schema_empty:
            return "Empty schema";
        case error_code::schema_incompatible:
            return "Schema being registered is incompatible with an earlier "
                   "schema for subject";
        case error_code::schema_version_invalid:
            return "The specified version is not a valid version id. Allowed "
                   "values are between [1, 2^31-1] and the string \"latest\"";
        case error_code::subject_not_found:
            return "Subject not found";
        case error_code::subject_version_not_found:
            return "Subject version not found";
        case error_code::subject_soft_deleted:
            return "Subject was soft deleted.Set permanent=true to delete "
                   "permanently";
        case error_code::subject_not_deleted:
            return "Subject not deleted before being permanently deleted";
        case error_code::subject_version_soft_deleted:
            return "Version was soft deleted.Set permanent=true to delete "
                   "permanently";
        case error_code::subject_version_not_deleted:
            return "Version not deleted before being permanently deleted";
        case error_code::compatibility_not_found:
            return "Subject does not have subject-level compatibility "
                   "configured";
        case error_code::subject_version_operaton_not_permitted:
            return "Overwrite new schema is not permitted.";
        case error_code::subject_version_has_references:
            return "One or more references exist to the schema";
        case error_code::subject_version_schema_id_already_exists:
            return "Schema already registered with another id";
        case error_code::subject_schema_invalid:
            return "Error while looking up schema under subject";
        case error_code::write_collision:
            return "Too many retries on write collision";
        case error_code::topic_parse_error:
            return "Unexpected data found in topic";
        case error_code::compatibility_level_invalid:
            return "Invalid compatibility level. Valid values are none, "
                   "backward, forward, full, backward_transitive, "
                   "forward_transitive, and full_transitive";
        }
        return "(unrecognized error)";
    }
    // TODO(Ben): Determine how best to use default_error_condition between
    // pandaproxy/rest and pandaproxy/schema_registry
    std::error_condition
    default_error_condition(int ec) const noexcept override {
        switch (static_cast<error_code>(ec)) {
        case error_code::subject_not_found:
            return reply_error_code::topic_not_found; // 40401
        case error_code::subject_version_not_found:
            return reply_error_code::partition_not_found; // 40402
        case error_code::schema_id_not_found:
            return reply_error_code::consumer_instance_not_found; // 40403
        case error_code::subject_soft_deleted:
            return reply_error_code::subject_soft_deleted; // 40404
        case error_code::subject_not_deleted:
            return reply_error_code::subject_not_deleted; // 40405
        case error_code::subject_version_soft_deleted:
            return reply_error_code::subject_version_soft_deleted; // 40406
        case error_code::subject_version_not_deleted:
            return reply_error_code::subject_version_not_deleted; // 40407
        case error_code::compatibility_not_found:
            return reply_error_code::compatibility_not_found; // 40408
        case error_code::subject_schema_invalid:
            return reply_error_code::internal_server_error; // 500
        case error_code::write_collision:
            return reply_error_code::write_collision; // 50301
        case error_code::schema_invalid:
            return reply_error_code::unprocessable_entity;
        case error_code::schema_empty:
            return reply_error_code::schema_empty; // 42201
        case error_code::schema_version_invalid:
            return reply_error_code::schema_version_invalid; // 42202
        case error_code::subject_version_operaton_not_permitted:
            return reply_error_code::
              subject_version_operaton_not_permitted; // 42205
        case error_code::subject_version_has_references:
            return reply_error_code::subject_version_has_references; // 42206
        case error_code::subject_version_schema_id_already_exists:
            return reply_error_code::
              subject_version_schema_id_already_exists; // 42207
        case error_code::schema_incompatible:
            return reply_error_code::conflict; // 409
        case error_code::topic_parse_error:
            return reply_error_code::zookeeper_error; // 50001
        case error_code::compatibility_level_invalid:
            return reply_error_code::compatibility_level_invalid; // 42203
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
