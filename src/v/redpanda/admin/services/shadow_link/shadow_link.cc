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

#include "cluster_link/service.h"
#include "redpanda/admin/services/shadow_link/converter.h"
#include "serde/protobuf/rpc.h"

namespace admin {
ss::logger sllog("shadow_link_service");
namespace {

template<typename T>
T handle_error(cluster_link::result<T> result) {
    if (result.has_value()) {
        return std::move(result).assume_value();
    }
    auto info = result.assume_error();
    switch (info.code()) {
    case cluster_link::errc::success:
        vassert(false, "Unexpected success code in handle_error");
    case cluster_link::errc::invalid_task_state_change:
    case cluster_link::errc::task_not_running:
    case cluster_link::errc::task_already_running:
    case cluster_link::errc::failed_to_start_task:
    case cluster_link::errc::task_already_registered_on_link:
    case cluster_link::errc::task_creation_failed:
    case cluster_link::errc::rpc_error:
        throw serde::pb::rpc::internal_exception(info.message());
    case cluster_link::errc::failed_to_connect_to_remote_cluster:
    case cluster_link::errc::remote_cluster_does_not_support_required_api:
    case cluster_link::errc::link_connection_failed:
    case cluster_link::errc::cluster_link_disabled:
        throw serde::pb::rpc::unavailable_exception(info.message());
    case cluster_link::errc::link_id_not_found:
        throw serde::pb::rpc::not_found_exception(info.message());
    case cluster_link::errc::invalid_configuration:
        throw serde::pb::rpc::invalid_argument_exception(info.message());
    case cluster_link::errc::topic_already_mirrored:
    case cluster_link::errc::topic_mirrored_by_other_link:
    case cluster_link::errc::topic_not_being_mirrored:
        throw serde::pb::rpc::already_exists_exception(info.message());
    }
}

ss::sstring iobuf_to_string(iobuf buf) {
    iobuf_parser parser{std::move(buf)};
    return parser.read_string_unsafe(parser.bytes_left());
}

template<typename T>
ss::future<ss::sstring> request_to_json_string(const T& req) {
    return req.to_json().then(
      [](iobuf buf) { return iobuf_to_string(std::move(buf)); });
}
} // namespace

shadow_link_service_impl::shadow_link_service_impl(
  ss::sharded<cluster_link::service>* service)
  : _service(service) {}

ss::future<proto::admin::shadow_link>
shadow_link_service_impl::create_shadow_link(
  proto::admin::create_shadow_link_request req) {
    auto json_str = co_await request_to_json_string(req);
    vlog(sllog.info, "create_shadow_link: {}", json_str);
    auto md = convert_create_to_metadata(std::move(req));
    auto resp = handle_error(
      co_await _service->local().create_cluster_link(std::move(md)));

    co_return metadata_to_shadow_link(std::move(resp));
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
