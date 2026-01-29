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

#include "redpanda/admin/services/shadow_link/err.h"

#include "base/vassert.h"
#include "serde/protobuf/rpc.h"

namespace admin {
void handle_error(cluster_link::errc err, ss::sstring info) {
    switch (err) {
    case cluster_link::errc::success:
        vunreachable("Unexpected success code in handle_error");
    case cluster_link::errc::invalid_task_state_change:
    case cluster_link::errc::task_not_running:
    case cluster_link::errc::task_already_running:
    case cluster_link::errc::failed_to_start_task:
    case cluster_link::errc::task_already_registered_on_link:
    case cluster_link::errc::task_creation_failed:
    case cluster_link::errc::rpc_error:
    case cluster_link::errc::link_creation_failed:
    case cluster_link::errc::topic_does_not_exist:
    case cluster_link::errc::topic_metadata_stale:
    case cluster_link::errc::failed_to_stop_task:
    case cluster_link::errc::failed_to_pause_task:
        throw serde::pb::rpc::internal_exception(std::move(info));
    case cluster_link::errc::failed_to_connect_to_remote_cluster:
    case cluster_link::errc::remote_cluster_does_not_support_required_api:
    case cluster_link::errc::link_connection_failed:
    case cluster_link::errc::service_not_ready:
    case cluster_link::errc::service_shutting_down:
        throw serde::pb::rpc::unavailable_exception(std::move(info));
    case cluster_link::errc::cluster_link_disabled:
    case cluster_link::errc::link_has_active_shadow_topics:
    case cluster_link::errc::license_required:
    case cluster_link::errc::link_unsupported_api_version:
    case cluster_link::errc::link_cluster_unreachable:
    case cluster_link::errc::link_broker_unreachable:
    case cluster_link::errc::link_broker_verification_failed:
    case cluster_link::errc::link_verification_unknown_error:
        throw serde::pb::rpc::failed_precondition_exception(std::move(info));
    case cluster_link::errc::link_id_not_found:
    case cluster_link::errc::topic_not_being_mirrored:
        throw serde::pb::rpc::not_found_exception(std::move(info));
    case cluster_link::errc::invalid_configuration:
        throw serde::pb::rpc::invalid_argument_exception(std::move(info));
    case cluster_link::errc::topic_already_mirrored:
    case cluster_link::errc::topic_mirrored_by_other_link:
        throw serde::pb::rpc::already_exists_exception(std::move(info));
    case cluster_link::errc::link_limit_reached:
        throw serde::pb::rpc::resource_exhausted_exception(std::move(info));
    }
}
} // namespace admin
