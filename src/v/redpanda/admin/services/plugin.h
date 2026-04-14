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

#include "proto/redpanda/core/admin/v2/plugin.proto.h"
#include "redpanda/admin/proxy/client.h"
#include "transform/fwd.h"

#include <seastar/core/sharded.hh>

namespace admin {

class plugin_service_impl : public proto::admin::plugin_service {
public:
    plugin_service_impl(
      admin::proxy::client proxy_client,
      ss::sharded<transform::service>* transform_service);

    // transforms resource
    ss::future<proto::admin::create_transform_response> create_transform(
      serde::pb::rpc::context,
      proto::admin::create_transform_request) override;

    ss::future<proto::admin::get_transform_response> get_transform(
      serde::pb::rpc::context,
      proto::admin::get_transform_request) override;

    ss::future<proto::admin::list_transforms_response> list_transforms(
      serde::pb::rpc::context,
      proto::admin::list_transforms_request) override;

    ss::future<proto::admin::update_transform_response> update_transform(
      serde::pb::rpc::context,
      proto::admin::update_transform_request) override;

    ss::future<proto::admin::delete_transform_response> delete_transform(
      serde::pb::rpc::context,
      proto::admin::delete_transform_request) override;

    // binaries resource
    ss::future<proto::admin::create_binary_response> create_binary(
      serde::pb::rpc::context,
      proto::admin::create_binary_request) override;

    ss::future<proto::admin::list_binaries_response> list_binaries(
      serde::pb::rpc::context,
      proto::admin::list_binaries_request) override;

    ss::future<proto::admin::delete_binary_response> delete_binary(
      serde::pb::rpc::context,
      proto::admin::delete_binary_request) override;

private:
    admin::proxy::client _proxy_client;
    ss::sharded<transform::service>* _transform_service;
};

} // namespace admin
