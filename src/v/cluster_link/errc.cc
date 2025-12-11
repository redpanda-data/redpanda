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

#include "cluster_link/errc.h"

#include "proto/redpanda/core/admin/v2/error_reason.pb.h"

namespace cluster_link {
namespace {
struct error_category final : public std::error_category {
    const char* name() const noexcept override { return "cluster_link"; }

    std::string message(int ev) const override {
        switch (static_cast<errc>(ev)) {
        case errc::success:
            return "success";
        case errc::invalid_task_state_change:
            return "invalid task state change";
        case errc::task_not_running:
            return "task not running";
        case errc::task_already_running:
            return "task already running";
        case errc::failed_to_start_task:
            return "failed to start task";
        case errc::task_already_registered_on_link:
            return "task already registered on link";
        case errc::failed_to_connect_to_remote_cluster:
            return "failed to connect to remote cluster";
        case errc::remote_cluster_does_not_support_required_api:
            return "remote cluster does not support required API";
        case errc::link_id_not_found:
            return "link ID not found";
        case errc::link_connection_failed:
            return "link connection failed";
        case errc::task_creation_failed:
            return "task creation failed";
        case errc::invalid_configuration:
            return "invalid configuration";
        case errc::rpc_error:
            return "Internal RPC error";
        case errc::cluster_link_disabled:
            return "Cluster link feature is disabled";
        case errc::topic_already_mirrored:
            return "Topic is already being mirrored";
        case errc::topic_mirrored_by_other_link:
            return "Topic is being mirrored by another link";
        case errc::topic_not_being_mirrored:
            return "Topic is not being mirrored";
        case errc::service_shutting_down:
            return "service shutting down";
        case errc::link_limit_reached:
            return "shadow link limit reached";
        case errc::link_creation_failed:
            return "link creation failed";
        case errc::link_has_active_shadow_topics:
            return "link has active shadow topics";
        case errc::topic_does_not_exist:
            return "topic does not exist";
        case errc::topic_metadata_stale:
            return "topic metadata is stale";
        case errc::license_required:
            return "license required for operation";
        case errc::service_not_ready:
            return "shadow linking service is not ready";
        case errc::link_unsupported_api_version:
            return "unsupported API version on remote cluster";
        case cluster_link::errc::link_cluster_unreachable:
            return "unable to reach link cluster, no broker is reachable.";
        case errc::link_broker_unreachable:
            return "unable to reach link broker";
        case errc::link_broker_verification_failed:
            return "link broker verification failed";
        case errc::link_verification_unknown_error:
            return "link verification unknown error";
        case errc::failed_to_stop_task:
            return "failed to stop task";
        case errc::failed_to_pause_task:
            return "failed to pause task";
        }

        return "(unknown error code)";
    }
};

const error_category cluster_link_error_category{};
} // namespace

std::error_code make_error_code(errc ec) noexcept {
    return {static_cast<int>(ec), cluster_link_error_category};
}

const std::error_category& error_category() noexcept {
    return cluster_link_error_category;
}

// Helper function to map errc to protobuf Reason enum
redpanda::core::admin::v2::Reason errc_to_reason(errc ec) {
    using redpanda::core::admin::v2::Reason;

    switch (ec) {
    case errc::success:
        return Reason::REASON_UNSPECIFIED;
    case errc::link_unsupported_api_version:
    case errc::remote_cluster_does_not_support_required_api:
        return Reason::REASON_SHADOW_LINK_API_VERSION_UNSUPPORTED;
    case errc::link_broker_unreachable:
        return Reason::REASON_SHADOW_LINK_BROKER_UNREACHABLE;
    case errc::link_cluster_unreachable:
        return Reason::REASON_SHADOW_LINK_CLUSTER_UNREACHABLE;
    case errc::link_broker_verification_failed:
        return Reason::REASON_SHADOW_LINK_BROKER_VERIFICATION_FAILED;
    case errc::link_connection_failed:
        return Reason::REASON_SHADOW_LINK_CONNECTION_FAILED;
    case errc::link_verification_unknown_error:
        return Reason::REASON_SHADOW_LINK_VERIFICATION_UNKNOWN_ERROR;
    case errc::link_creation_failed:
        return Reason::REASON_SHADOW_LINK_CREATION_FAILED;
    case errc::link_id_not_found:
        return Reason::REASON_SHADOW_LINK_NOT_FOUND;
    case errc::service_shutting_down:
        return Reason::REASON_SHADOW_LINK_SERVICE_SHUTTING_DOWN;
    case errc::rpc_error:
        return Reason::REASON_SHADOW_LINK_RPC_ERROR;
    case errc::invalid_configuration:
        return Reason::REASON_SHADOW_LINK_INVALID_CONFIGURATION;
    case errc::topic_already_mirrored:
        return Reason::REASON_SHADOW_TOPIC_ALREADY_MIRRORED;
    case errc::topic_mirrored_by_other_link:
        return Reason::REASON_SHADOW_TOPIC_MIRRORED_BY_OTHER_LINK;
    case errc::topic_not_being_mirrored:
        return Reason::REASON_SHADOW_TOPIC_NOT_MIRRORED;
    case errc::link_has_active_shadow_topics:
        return Reason::REASON_SHADOW_LINK_HAS_ACTIVE_TOPICS;
    case errc::topic_does_not_exist:
        return Reason::REASON_SHADOW_TOPIC_DOES_NOT_EXIST;
    case errc::topic_metadata_stale:
        return Reason::REASON_SHADOW_TOPIC_METADATA_STALE;
    case errc::invalid_task_state_change:
        return Reason::REASON_SHADOW_LINK_INVALID_TASK_STATE;
    case errc::task_not_running:
        return Reason::REASON_SHADOW_LINK_TASK_NOT_RUNNING;
    case errc::task_already_running:
        return Reason::REASON_SHADOW_LINK_TASK_ALREADY_RUNNING;
    case errc::failed_to_start_task:
        return Reason::REASON_SHADOW_LINK_TASK_START_FAILED;
    case errc::task_creation_failed:
        return Reason::REASON_SHADOW_LINK_TASK_CREATION_FAILED;
    case errc::task_already_registered_on_link:
        return Reason::REASON_SHADOW_LINK_TASK_ALREADY_REGISTERED;
    case errc::failed_to_stop_task:
        return Reason::REASON_SHADOW_LINK_TASK_STOP_FAILED;
    case errc::failed_to_pause_task:
        return Reason::REASON_SHADOW_LINK_TASK_PAUSE_FAILED;
    case errc::cluster_link_disabled:
        return Reason::REASON_SHADOW_LINK_FEATURE_DISABLED;
    case errc::license_required:
        return Reason::REASON_SHADOW_LINK_LICENSE_REQUIRED;
    case errc::link_limit_reached:
        return Reason::REASON_SHADOW_LINK_LIMIT_REACHED;
    case errc::service_not_ready:
        return Reason::REASON_SHADOW_LINK_SERVICE_NOT_READY;
    case errc::failed_to_connect_to_remote_cluster:
        return Reason::REASON_SHADOW_LINK_REMOTE_API_UNSUPPORTED;
    }
    return Reason::REASON_UNSPECIFIED;
}

// Helper function to create error_info with reason and metadata
serde::pb::rpc::error_info make_error_info(
  errc ec,
  chunked_hash_map<ss::sstring, ss::sstring> metadata) {
    serde::pb::rpc::error_info info;

    // Map errc to Reason enum and convert to string
    auto reason_enum = errc_to_reason(ec);
    info.reason = ss::sstring(
      redpanda::core::admin::v2::Reason_Name(reason_enum));

    // Set domain to redpanda.com/core
    info.domain = ss::sstring(serde::pb::rpc::error_info::redpanda_core_domain);

    // Move metadata
    info.metadata = std::move(metadata);

    return info;
}

err_info::err_info(errc ec)
  : _ec(ec)
  , _msg(make_error_code(_ec).message()) {}

err_info::err_info(errc ec, std::string msg)
  : _ec(ec)
  , _msg(std::move(msg)) {}

err_info::err_info(
  errc ec, std::string msg, std::optional<serde::pb::rpc::error_info> info)
  : _ec(ec)
  , _msg(std::move(msg))
  , _error_info(std::move(info)) {}

errc err_info::code() const noexcept { return _ec; }

const std::string& err_info::message() const noexcept { return _msg; }

const std::optional<serde::pb::rpc::error_info>&
err_info::info() const noexcept {
    return _error_info;
}

} // namespace cluster_link
