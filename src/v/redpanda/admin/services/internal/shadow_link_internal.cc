/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "redpanda/admin/services/internal/shadow_link_internal.h"

#include "cluster_link/model/types.h"
#include "cluster_link/service.h"
#include "model/namespace.h"
#include "redpanda/admin/services/shadow_link/err.h"
#include "redpanda/admin/services/utils.h"

namespace admin::internal {
namespace {
cluster_link::model::mirror_topic_status
proto_to_model(proto::admin::shadow_topic_state state) {
    switch (state) {
    case proto::admin::shadow_topic_state::unspecified:
        throw serde::pb::rpc::invalid_argument_exception(
          "Cannot convert unspecified shadow topic state");
    case proto::admin::shadow_topic_state::active:
        return cluster_link::model::mirror_topic_status::active;
    case proto::admin::shadow_topic_state::faulted:
        return cluster_link::model::mirror_topic_status::failed;
    case proto::admin::shadow_topic_state::paused:
        return cluster_link::model::mirror_topic_status::paused;
    case proto::admin::shadow_topic_state::failing_over:
        return cluster_link::model::mirror_topic_status::failing_over;
    case proto::admin::shadow_topic_state::failed_over:
        return cluster_link::model::mirror_topic_status::failed_over;
    case proto::admin::shadow_topic_state::promoting:
        return cluster_link::model::mirror_topic_status::promoting;
    case proto::admin::shadow_topic_state::promoted:
        return cluster_link::model::mirror_topic_status::promoted;
    }
}
} // namespace
ss::logger sllog("shadow_link_internal_service");
using proto::admin::internal::shadow_link::
  force_update_shadow_topic_state_request;
using proto::admin::internal::shadow_link::
  force_update_shadow_topic_state_response;
using proto::admin::internal::shadow_link::remove_shadow_topic_request;
using proto::admin::internal::shadow_link::remove_shadow_topic_response;

shadow_link_internal_service_impl::shadow_link_internal_service_impl(
  admin::proxy::client proxy_client,
  ss::sharded<cluster_link::service>* service,
  ss::sharded<cluster::metadata_cache>* md_cache)
  : _proxy_client(std::move(proxy_client))
  , _service(service)
  , _md_cache(md_cache) {}

ss::future<remove_shadow_topic_response>
shadow_link_internal_service_impl::remove_shadow_topic(
  serde::pb::rpc::context ctx, remove_shadow_topic_request req) {
    vlog(sllog.trace, "remove_shadow_topic: {}", req);
    auto redirect_node = redirect_to(model::controller_ntp);

    if (redirect_node) {
        vlog(
          sllog.debug,
          "Redirecting to leader of {}: {}",
          model::controller_ntp,
          *redirect_node);
        co_return co_await _proxy_client
          .make_client_for_node<proto::admin::internal::shadow_link::
                                  shadow_link_internal_service_client>(
            redirect_node.value())
          .remove_shadow_topic(std::move(ctx), std::move(req));
    }

    auto _ = handle_error(
      co_await _service->local().delete_shadow_topic_from_shadow_link(
        cluster_link::model::name_t{req.get_shadow_link_name()},
        model::topic{req.get_shadow_topic_name()}));

    co_return remove_shadow_topic_response{};
}

ss::future<force_update_shadow_topic_state_response>
shadow_link_internal_service_impl::force_update_shadow_topic_state(
  serde::pb::rpc::context ctx, force_update_shadow_topic_state_request req) {
    vlog(sllog.trace, "force_update_shadow_topic_state: {}", req);
    auto redirect_node = redirect_to(model::controller_ntp);

    if (redirect_node) {
        vlog(
          sllog.debug,
          "Redirecting to leader of {}: {}",
          model::controller_ntp,
          *redirect_node);
        co_return co_await _proxy_client
          .make_client_for_node<proto::admin::internal::shadow_link::
                                  shadow_link_internal_service_client>(
            redirect_node.value())
          .force_update_shadow_topic_state(std::move(ctx), std::move(req));
    }

    auto _ = handle_error(
      co_await _service->local().update_mirror_topic_status(
        cluster_link::model::name_t{req.get_shadow_link_name()},
        model::topic{req.get_shadow_topic_name()},
        proto_to_model(req.get_new_state()),
        true /* force_update */));

    co_return force_update_shadow_topic_state_response{};
}

std::optional<model::node_id>
shadow_link_internal_service_impl::redirect_to(const model::ntp& ntp) {
    return utils::redirect_to_leader(
      _md_cache->local(), ntp, _proxy_client.self_node_id());
}

} // namespace admin::internal
