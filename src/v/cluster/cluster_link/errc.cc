/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cluster_link/errc.h"

namespace cluster::cluster_link {
const char* errc_category::name() const noexcept {
    return "cluster::cluster_link";
}

std::string errc_category::message(int c) const {
    switch (static_cast<errc>(c)) {
    case errc::success:
        return "Success";
    case errc::does_not_exist:
        return "Cluster link does not exist";
    case errc::invalid_create:
        return "Invalid cluster link configuration for create";
    case errc::invalid_update:
        return "Invalid cluster link configuration for update";
    case errc::limit_exceeded:
        return "Cluster link limit exceeded";
    case errc::service_error:
        return "Service error with cluster link in controller table";
    case errc::timeout:
        return "Timeout occurred while processing request";
    case errc::not_leader_controller:
        return "This node is not raft-0 leader. i.e. is not leader controller";
    case errc::replication_error:
        return "Unable to replicate given state across cluster nodes";
    case errc::feature_disabled:
        return "Requested feature is disabled";
    case errc::rpc_error:
        return "RPC error occurred while processing cluster link request";
    case errc::throttling_quota_exceeded:
        return "Request declined due to exceeded requests quotas";
    case errc::topic_already_being_mirrored:
        return "Topic is already being mirrored by a cluster link";
    case errc::topic_being_mirrored_by_other_link:
        return "Topic is being mirrored by a different cluster link";
    case errc::topic_not_being_mirrored:
        return "Topic is not being mirrored by the cluster link";
    case errc::mirror_topic_name_invalid:
        return "Mirror topic name is invalid";
    case errc::uuid_conflict:
        return "Provided UUID on cluster link update command does not match "
               "current UUID";
    case errc::bootstrap_servers_empty:
        return "Bootstrap servers cannot be empty";
    case errc::tls_configuration_invalid:
        return "TLS configuration is invalid";
    case errc::link_name_invalid:
        return "Provided cluster link name is invalid";
    case errc::topic_filter_invalid:
        return "Topic filter is invalid";
    case errc::topic_property_excluded_from_mirroring:
        return "Topic property is excluded from mirroring";
    }
    return "cluster::cluster_link::unknown";
}

const std::error_category& error_category() noexcept {
    static errc_category e;
    return e;
}

std::error_code make_error_code(errc e) noexcept {
    return std::error_code(static_cast<int>(e), error_category());
}
} // namespace cluster::cluster_link

auto fmt::formatter<cluster::cluster_link::errc>::format(
  cluster::cluster_link::errc e, fmt::format_context& ctx) const
  -> decltype(ctx.out()) {
    switch (e) {
    case cluster::cluster_link::errc::success:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::success");
    case cluster::cluster_link::errc::does_not_exist:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::does_not_exist");
    case cluster::cluster_link::errc::invalid_create:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::invalid_create");
    case cluster::cluster_link::errc::invalid_update:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::invalid_update");
    case cluster::cluster_link::errc::limit_exceeded:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::limit_exceeded");
    case cluster::cluster_link::errc::service_error:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::service_error");
    case cluster::cluster_link::errc::timeout:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::timeout");
    case cluster::cluster_link::errc::not_leader_controller:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::not_leader_controller");
    case cluster::cluster_link::errc::replication_error:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::replication_error");
    case cluster::cluster_link::errc::feature_disabled:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::feature_disabled");
    case cluster::cluster_link::errc::rpc_error:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::rpc_error");
    case cluster::cluster_link::errc::throttling_quota_exceeded:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::throttling_quota_exceeded");
    case cluster::cluster_link::errc::topic_already_being_mirrored:
        return fmt::format_to(
          ctx.out(),
          "cluster::cluster_link::errc::topic_already_being_mirrored");
    case cluster::cluster_link::errc::topic_being_mirrored_by_other_link:
        return fmt::format_to(
          ctx.out(),
          "cluster::cluster_link::errc::topic_being_mirrored_by_other_link");
    case cluster::cluster_link::errc::topic_not_being_mirrored:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::topic_not_being_mirrored");
    case cluster::cluster_link::errc::mirror_topic_name_invalid:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::mirror_topic_name_invalid");
    case cluster::cluster_link::errc::uuid_conflict:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::uuid_conflict");
    case cluster::cluster_link::errc::bootstrap_servers_empty:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::bootstrap_servers_empty");
    case cluster::cluster_link::errc::tls_configuration_invalid:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::tls_configuration_invalid");
    case cluster::cluster_link::errc::link_name_invalid:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::link_name_invalid");
    case cluster::cluster_link::errc::topic_filter_invalid:
        return fmt::format_to(
          ctx.out(), "cluster::cluster_link::errc::topic_filter_invalid");
    case cluster::cluster_link::errc::topic_property_excluded_from_mirroring:
        return fmt::format_to(
          ctx.out(),
          "cluster::cluster_link::errc::topic_property_excluded_from_"
          "mirroring");
    }
}
