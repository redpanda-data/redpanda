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

#include "cluster/metadata_cache.h"
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
    case cluster_link::errc::link_creation_failed:
        throw serde::pb::rpc::internal_exception(info.message());
    case cluster_link::errc::failed_to_connect_to_remote_cluster:
    case cluster_link::errc::remote_cluster_does_not_support_required_api:
    case cluster_link::errc::link_connection_failed:
    case cluster_link::errc::service_shutting_down:
        throw serde::pb::rpc::unavailable_exception(info.message());
    case cluster_link::errc::cluster_link_disabled:
    case cluster_link::errc::link_has_active_shadow_topics:
        throw serde::pb::rpc::failed_precondition_exception(info.message());
    case cluster_link::errc::link_id_not_found:
        throw serde::pb::rpc::not_found_exception(info.message());
    case cluster_link::errc::invalid_configuration:
        throw serde::pb::rpc::invalid_argument_exception(info.message());
    case cluster_link::errc::topic_already_mirrored:
    case cluster_link::errc::topic_mirrored_by_other_link:
    case cluster_link::errc::topic_not_being_mirrored:
        throw serde::pb::rpc::already_exists_exception(info.message());
    case cluster_link::errc::link_limit_reached:
        throw serde::pb::rpc::resource_exhausted_exception(info.message());
    }
}

const std::vector<serde::pb::field_mask::path> disallowed_paths_in_update = {
  {"configurations", "client_options", "bootstrap_servers"},
  {"configurations", "client_options", "tls_settings"}};

bool is_path_disallowed(
  const serde::pb::field_mask::path_view& path,
  const std::vector<serde::pb::field_mask::path>& disallowed_paths) {
    for (const auto& disallowed : disallowed_paths) {
        if (
          path.size() >= disallowed.size()
          && std::ranges::equal(
            disallowed, path | std::views::take(disallowed.size()))) {
            return true;
        }
    }
    return false;
}
} // namespace

shadow_link_service_impl::shadow_link_service_impl(
  admin::proxy::client proxy_client,
  ss::sharded<cluster_link::service>* service,
  ss::sharded<cluster::metadata_cache>* md_cache)
  : _proxy_client(std::move(proxy_client))
  , _service(service)
  , _md_cache(md_cache) {}

ss::future<proto::admin::create_shadow_link_response>
shadow_link_service_impl::create_shadow_link(
  serde::pb::rpc::context, proto::admin::create_shadow_link_request req) {
    vlog(sllog.trace, "create_shadow_link: {}", req);

    auto md = convert_create_to_metadata(std::move(req));
    auto get_resp = _service->local().get_cluster_link(md.name);
    if (get_resp.has_value()) {
        throw serde::pb::rpc::already_exists_exception(
          ssx::sformat("Shadow link with name {} already exists", md.name));
    }

    auto resp = handle_error(
      co_await _service->local().upsert_cluster_link(std::move(md)));

    proto::admin::create_shadow_link_response sl_resp;
    sl_resp.set_shadow_link(metadata_to_shadow_link(std::move(resp)));

    co_return sl_resp;
}

ss::future<proto::admin::delete_shadow_link_response>
shadow_link_service_impl::delete_shadow_link(
  serde::pb::rpc::context, proto::admin::delete_shadow_link_request req) {
    vlog(sllog.trace, "delete_shadow_link: {}", req);

    handle_error(
      co_await _service->local().delete_cluster_link(
        cluster_link::model::name_t{req.get_name()}));

    proto::admin::delete_shadow_link_response dsl_resp;
    co_return dsl_resp;
}

ss::future<proto::admin::get_shadow_link_response>
shadow_link_service_impl::get_shadow_link(
  serde::pb::rpc::context, proto::admin::get_shadow_link_request req) {
    vlog(sllog.trace, "get_shadow_link: {}", req);

    auto resp = handle_error(_service->local().get_cluster_link(
      cluster_link::model::name_t{req.get_name()}));
    proto::admin::get_shadow_link_response get_resp;
    get_resp.set_shadow_link(metadata_to_shadow_link(std::move(resp)));
    co_return get_resp;
}

ss::future<proto::admin::list_shadow_links_response>
shadow_link_service_impl::list_shadow_links(
  serde::pb::rpc::context, proto::admin::list_shadow_links_request req) {
    vlog(sllog.trace, "list_shadow_links: {}", req);

    auto resp = handle_error(_service->local().list_cluster_links());

    proto::admin::list_shadow_links_response list_resp;
    chunked_vector<proto::admin::shadow_link> links;
    links.reserve(resp.size());
    for (auto& md : resp) {
        links.emplace_back(metadata_to_shadow_link(std::move(md)));
    }

    list_resp.set_shadow_links(std::move(links));

    co_return list_resp;
}

ss::future<proto::admin::update_shadow_link_response>
shadow_link_service_impl::update_shadow_link(
  serde::pb::rpc::context ctx, proto::admin::update_shadow_link_request req) {
    vlog(sllog.trace, "update_shadow_link: {}", req);
    auto redirect_node = redirect_to(model::controller_ntp);
    if (redirect_node) {
        vlog(
          sllog.debug,
          "Redirecting to leader of {}: {}",
          model::controller_ntp,
          *redirect_node);
        co_return co_await _proxy_client
          .make_client_for_node<proto::admin::shadow_link_service_client>(
            *redirect_node)
          .update_shadow_link(ctx, std::move(req));
    }

    auto link_name = cluster_link::model::name_t{
      req.get_shadow_link().get_name()};

    auto current_link = handle_error(
      _service->local().get_cluster_link(link_name));

    if (std::ranges::any_of(req.get_update_mask().paths, [](const auto& path) {
            return is_path_disallowed(path, disallowed_paths_in_update);
        })) {
        throw serde::pb::rpc::invalid_argument_exception(
          ssx::sformat(
            "Attempted to update disallowed fields in shadow link {}",
            link_name));
    }

    auto update_cmd = create_update_cluster_link_config_cmd(
      std::move(req), std::move(current_link));

    auto updated_md = handle_error(
      co_await _service->local().update_cluster_link(
        std::move(link_name), std::move(update_cmd)));

    proto::admin::update_shadow_link_response resp;
    resp.set_shadow_link(metadata_to_shadow_link(std::move(updated_md)));

    co_return resp;
}

ss::future<proto::admin::fail_over_response>
shadow_link_service_impl::fail_over(
  serde::pb::rpc::context, proto::admin::fail_over_request) {
    throw serde::pb::rpc::unimplemented_exception();
}

std::optional<model::node_id>
shadow_link_service_impl::redirect_to(const model::ntp& ntp) {
    auto leader_node = _md_cache->local().get_leader_id(ntp);
    if (!leader_node) {
        throw serde::pb::rpc::unavailable_exception{
          ssx::sformat("Partition {} does not have a leader", ntp)};
    }

    if (*leader_node == _proxy_client.self_node_id()) {
        return std::nullopt;
    }
    return *leader_node;
}
} // namespace admin
