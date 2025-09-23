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

err_info::err_info(errc ec)
  : _ec(ec)
  , _msg(make_error_code(_ec).message()) {}

err_info::err_info(errc ec, std::string msg)
  : _ec(ec)
  , _msg(std::move(msg)) {}

errc err_info::code() const noexcept { return _ec; }

const std::string& err_info::message() const noexcept { return _msg; }

} // namespace cluster_link
