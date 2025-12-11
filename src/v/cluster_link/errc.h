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

#pragma once

#include "base/outcome.h"
#include "serde/protobuf/rpc.h"

#include <optional>
#include <system_error>

namespace cluster_link {
enum class errc : int {
    success = 0,
    invalid_task_state_change,
    task_not_running,
    task_already_running,
    failed_to_start_task,
    task_already_registered_on_link,
    failed_to_connect_to_remote_cluster,
    remote_cluster_does_not_support_required_api,
    link_id_not_found,
    link_connection_failed,
    task_creation_failed,
    invalid_configuration,
    rpc_error,
    cluster_link_disabled,
    topic_already_mirrored,
    topic_mirrored_by_other_link,
    topic_not_being_mirrored,
    service_shutting_down,
    link_limit_reached,
    link_creation_failed,
    link_has_active_shadow_topics,
    topic_does_not_exist,
    topic_metadata_stale,
    license_required,
    service_not_ready,
    link_unsupported_api_version,
    link_cluster_unreachable,
    link_broker_unreachable,
    link_broker_verification_failed,
    link_verification_unknown_error,
    failed_to_stop_task,
    failed_to_pause_task,
};

std::error_code make_error_code(errc) noexcept;

const std::error_category& error_category() noexcept;

// Helper function to create error_info with reason and metadata
serde::pb::rpc::error_info make_error_info(
  errc ec,
  chunked_hash_map<ss::sstring, ss::sstring> metadata = {});

class err_info {
public:
    explicit err_info(errc ec);
    err_info(errc, std::string);
    err_info(errc, std::string, std::optional<serde::pb::rpc::error_info>);

    errc code() const noexcept;
    const std::string& message() const noexcept;
    const std::optional<serde::pb::rpc::error_info>& info() const noexcept;

private:
    errc _ec;
    std::string _msg;
    std::optional<serde::pb::rpc::error_info> _error_info;
};

template<typename T>
using cl_result = result<T, err_info>;
} // namespace cluster_link

namespace std {
template<>
struct is_error_code_enum<cluster_link::errc> : true_type {};
} // namespace std
