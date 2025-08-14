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
};

std::error_code make_error_code(errc) noexcept;

const std::error_category& error_category() noexcept;

class err_info {
public:
    explicit err_info(errc ec);
    err_info(errc, std::string);
    errc code() const noexcept;
    const std::string& message() const noexcept;

private:
    errc _ec;
    std::string _msg;
};

template<typename T>
using result = result<T, err_info>;
} // namespace cluster_link

namespace std {
template<>
struct is_error_code_enum<cluster_link::errc> : true_type {};
} // namespace std
