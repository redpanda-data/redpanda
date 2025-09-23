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

#include "cluster/fwd.h"
#include "cluster_link/fwd.h"
#include "proto/redpanda/core/admin/v2/shadow_link.proto.h"
#include "redpanda/admin/proxy/client.h"

#include <seastar/core/sharded.hh>

namespace admin {
class shadow_link_service_impl : public proto::admin::shadow_link_service {
public:
    shadow_link_service_impl(
      admin::proxy::client proxy_client,
      ss::sharded<cluster_link::service>* service,
      ss::sharded<cluster::metadata_cache>* md_cache);

    ss::future<proto::admin::create_shadow_link_response> create_shadow_link(
      serde::pb::rpc::context,
      proto::admin::create_shadow_link_request) override;

    ss::future<proto::admin::delete_shadow_link_response> delete_shadow_link(
      serde::pb::rpc::context,
      proto::admin::delete_shadow_link_request) override;

    ss::future<proto::admin::get_shadow_link_response> get_shadow_link(
      serde::pb::rpc::context, proto::admin::get_shadow_link_request) override;

    ss::future<proto::admin::list_shadow_links_response> list_shadow_links(
      serde::pb::rpc::context,
      proto::admin::list_shadow_links_request) override;

    ss::future<proto::admin::update_shadow_link_response> update_shadow_link(
      serde::pb::rpc::context,
      proto::admin::update_shadow_link_request) override;

    ss::future<proto::admin::fail_over_response> fail_over(
      serde::pb::rpc::context, proto::admin::fail_over_request) override;

private:
    /**
     * @brief Returns a node to redirect the message to
     *
     * @param ntp The ntp to find the leader for
     * @return std::optional<model::node_id> Returns std::nullopt if no
     * direction necessary, otherwise the node to redirect to
     * @throws serde::pb::rpc::unavailable_exception if no leader is found for
     * @p ntp
     */
    std::optional<model::node_id> redirect_to(const model::ntp& ntp);

private:
    admin::proxy::client _proxy_client;

    ss::sharded<cluster_link::service>* _service;
    ss::sharded<cluster::metadata_cache>* _md_cache;
};
} // namespace admin
