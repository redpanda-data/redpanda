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

#include "redpanda/admin/services/shadow_link/shadow_link.h"

#include "serde/protobuf/rpc.h"

namespace admin {
ss::future<proto::admin::shadow_link>
shadow_link_service_impl::create_shadow_link(
  proto::admin::create_shadow_link_request) {
    throw serde::pb::rpc::unimplemented_exception();
}

ss::future<proto::admin::delete_shadow_link_response>
shadow_link_service_impl::delete_shadow_link(
  proto::admin::delete_shadow_link_request) {
    throw serde::pb::rpc::unimplemented_exception();
}

ss::future<proto::admin::shadow_link> shadow_link_service_impl::get_shadow_link(
  proto::admin::get_shadow_link_request) {
    throw serde::pb::rpc::unimplemented_exception();
}

ss::future<proto::admin::list_shadow_links_response>
shadow_link_service_impl::list_shadow_links(
  proto::admin::list_shadow_links_request) {
    throw serde::pb::rpc::unimplemented_exception();
}

ss::future<proto::admin::shadow_link>
shadow_link_service_impl::update_shadow_link(
  proto::admin::update_shadow_link_request) {
    throw serde::pb::rpc::unimplemented_exception();
}

ss::future<proto::admin::shadow_link>
shadow_link_service_impl::fail_over(proto::admin::fail_over_request) {
    throw serde::pb::rpc::unimplemented_exception();
}
} // namespace admin
