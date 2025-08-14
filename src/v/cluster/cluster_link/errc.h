/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include <fmt/format.h>

#include <system_error>

namespace cluster::cluster_link {
enum class errc : int16_t {
    success = 0, // must be 0
    does_not_exist,
    invalid_create,
    invalid_update,
    limit_exceeded,
    service_error,
    timeout,
    not_leader_controller,
    replication_error,
    feature_disabled,
    rpc_error,
    throttling_quota_exceeded,
    topic_already_being_mirrored,
    topic_being_mirrored_by_other_link,
    topic_not_being_mirrored,
    mirror_topic_name_invalid,
    uuid_conflict,
    bootstrap_servers_empty,
    tls_configuration_invalid,
    link_name_invalid,
    topic_filter_invalid,
    topic_property_excluded_from_mirroring,
};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final;

    std::string message(int c) const final;
};

const std::error_category& error_category() noexcept;

std::error_code make_error_code(errc e) noexcept;
} // namespace cluster::cluster_link

namespace std {
template<>
struct is_error_code_enum<cluster::cluster_link::errc> : true_type {};
} // namespace std

template<>
struct fmt::formatter<cluster::cluster_link::errc>
  : fmt::formatter<std::string_view> {
    auto format(cluster::cluster_link::errc e, fmt::format_context& ctx) const
      -> decltype(ctx.out());
};
