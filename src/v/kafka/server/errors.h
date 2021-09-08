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
#pragma once
#include "cluster/errc.h"
#include "kafka/protocol/errors.h"

namespace kafka {

constexpr error_code map_topic_error_code(cluster::errc code) {
    switch (code) {
    case cluster::errc::success:
        return error_code::none;
    case cluster::errc::topic_invalid_config:
        return error_code::invalid_config;
    case cluster::errc::topic_invalid_partitions:
        return error_code::invalid_partitions;
    case cluster::errc::topic_invalid_replication_factor:
        return error_code::invalid_replication_factor;
    case cluster::errc::notification_wait_timeout:
        return error_code::request_timed_out;
    case cluster::errc::not_leader_controller:
        return error_code::not_controller;
    case cluster::errc::topic_already_exists:
        return error_code::topic_already_exists;
    case cluster::errc::topic_not_exists:
        return error_code::unknown_topic_or_partition;
    case cluster::errc::timeout:
        return error_code::request_timed_out;
    case cluster::errc::invalid_topic_name:
        return error_code::invalid_topic_exception;
    case cluster::errc::missing_materialized_source_topic:
        return error_code::invalid_config;
    default:
        return error_code::unknown_server_error;
    }
}

} // namespace kafka
