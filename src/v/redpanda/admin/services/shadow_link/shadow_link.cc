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
#include "redpanda/admin/services/shadow_link/err.h"
#include "redpanda/admin/services/utils.h"
#include "serde/protobuf/rpc.h"

namespace admin {
ss::logger sllog("shadow_link_service");

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
          .create_shadow_link(serde::pb::rpc::context{}, std::move(req));
    }
    vlog(sllog.info, "create_shadow_link: {}", req);
    auto md = convert_create_to_metadata(std::move(req));
    auto get_resp = _service->local().get_cluster_link(md.name);
    if (get_resp.has_value()) {
        throw serde::pb::rpc::already_exists_exception(
          ssx::sformat("Shadow link with name {} already exists", md.name));
    }

    auto resp = handle_error(
      co_await _service->local().upsert_cluster_link(std::move(md)));

    proto::admin::create_shadow_link_response sl_resp;
    sl_resp.set_shadow_link(metadata_to_shadow_link(resp, {}));

    co_return sl_resp;
}

ss::future<proto::admin::delete_shadow_link_response>
shadow_link_service_impl::delete_shadow_link(
  serde::pb::rpc::context ctx, proto::admin::delete_shadow_link_request req) {
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
          .delete_shadow_link(ctx, std::move(req));
    }
    vlog(sllog.info, "delete_shadow_link: {}", req);
    handle_error(
      co_await _service->local().delete_cluster_link(
        cluster_link::model::name_t{req.get_name()}, req.get_force()));

    proto::admin::delete_shadow_link_response dsl_resp;
    co_return dsl_resp;
}

ss::future<proto::admin::get_shadow_link_response>
shadow_link_service_impl::get_shadow_link(
  serde::pb::rpc::context ctx, proto::admin::get_shadow_link_request req) {
    vlog(sllog.trace, "get_shadow_link: {}", req);
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
          .get_shadow_link(ctx, std::move(req));
    }

    proto::admin::get_shadow_link_response get_resp;
    get_resp.set_shadow_link(
      co_await build_shadow_link(cluster_link::model::name_t{req.get_name()}));
    co_return get_resp;
}

ss::future<proto::admin::list_shadow_links_response>
shadow_link_service_impl::list_shadow_links(
  serde::pb::rpc::context, proto::admin::list_shadow_links_request req) {
    vlog(sllog.trace, "list_shadow_links: {}", req);
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
          .list_shadow_links(serde::pb::rpc::context{}, std::move(req));
    }

    auto resp = handle_error(_service->local().list_cluster_links());

    proto::admin::list_shadow_links_response list_resp;
    chunked_vector<proto::admin::shadow_link> links;
    links.reserve(resp.size());
    for (auto& md : resp) {
        auto status_report = handle_error(
          co_await _service->local().shadow_link_report(md->name));
        links.emplace_back(
          metadata_to_shadow_link(md, std::move(status_report)));
    }

    list_resp.set_shadow_links(std::move(links));

    co_return list_resp;
}

ss::future<proto::admin::update_shadow_link_response>
shadow_link_service_impl::update_shadow_link(
  serde::pb::rpc::context ctx, proto::admin::update_shadow_link_request req) {
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

    vlog(sllog.info, "update_shadow_link: {}", req);
    auto link_name = cluster_link::model::name_t{
      req.get_shadow_link().get_name()};

    auto current_link = handle_error(
      _service->local().get_cluster_link(link_name));

    auto update_cmd = create_update_cluster_link_config_cmd(
      std::move(req), std::move(current_link));

    auto updated_md = handle_error(
      co_await _service->local().update_cluster_link(
        link_name, std::move(update_cmd)));

    auto status_report = handle_error(
      co_await _service->local().shadow_link_report(link_name));

    proto::admin::update_shadow_link_response resp;
    resp.set_shadow_link(
      metadata_to_shadow_link(std::move(updated_md), std::move(status_report)));

    co_return resp;
}

ss::future<proto::admin::fail_over_response>
shadow_link_service_impl::fail_over(
  serde::pb::rpc::context ctx, proto::admin::fail_over_request req) {
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
          .fail_over(ctx, std::move(req));
    }
    vlog(sllog.info, "fail_over_request: {}", req);
    auto link_name = cluster_link::model::name_t{req.get_name()};
    auto current_link = handle_error(
      _service->local().get_cluster_link(link_name));
    auto status_report = handle_error(
      co_await _service->local().shadow_link_report(link_name));
    const auto& failover_topic = req.get_shadow_topic_name();
    proto::admin::fail_over_response resp;
    if (failover_topic.empty()) {
        // failover the entire link
        auto result = handle_error(
          co_await _service->local().failover_link_topics(
            std::move(link_name)));
        resp.set_shadow_link(
          metadata_to_shadow_link(std::move(result), std::move(status_report)));
    } else {
        // failover the specific shadow topic
        auto topic = model::topic{failover_topic};
        auto result = handle_error(
          co_await _service->local().update_mirror_topic_status(
            std::move(link_name),
            std::move(topic),
            cluster_link::model::mirror_topic_status::failing_over));
        resp.set_shadow_link(
          metadata_to_shadow_link(std::move(result), std::move(status_report)));
    }
    co_return resp;
}

ss::future<proto::admin::get_shadow_topic_response>
shadow_link_service_impl::get_shadow_topic(
  serde::pb::rpc::context ctx, proto::admin::get_shadow_topic_request req) {
    vlog(sllog.trace, "get_shadow_topic: {}", req);
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
          .get_shadow_topic(ctx, std::move(req));
    }
    auto resp = handle_error(_service->local().get_cluster_link(
      cluster_link::model::name_t{req.get_shadow_link_name()}));

    const auto& it = resp->state.mirror_topics.find(
      model::topic_view{req.get_name()});
    if (it == resp->state.mirror_topics.end()) {
        throw serde::pb::rpc::not_found_exception(
          ssx::sformat(
            "Shadow topic {} not found on link {}",
            req.get_name(),
            req.get_shadow_link_name()));
    }

    auto shadow_link_report = handle_error(
      co_await _service->local().shadow_link_report(
        cluster_link::model::name_t{req.get_shadow_link_name()}));

    vlog(sllog.trace, "shadow link report: {}", shadow_link_report);

    proto::admin::get_shadow_topic_response get_resp;
    get_resp.set_shadow_topic(
      model_to_shadow_topic(it->first, it->second, shadow_link_report));

    vlog(sllog.trace, "get_shadow_topic response: {}", get_resp);
    co_return get_resp;
}

ss::future<proto::admin::list_shadow_topics_response>
shadow_link_service_impl::list_shadow_topics(
  serde::pb::rpc::context ctx, proto::admin::list_shadow_topics_request req) {
    vlog(sllog.trace, "list_shadow_topics: {}", req);
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
          .list_shadow_topics(ctx, std::move(req));
    }

    auto resp = handle_error(_service->local().get_cluster_link(
      cluster_link::model::name_t{req.get_shadow_link_name()}));
    auto shadow_link_report = handle_error(
      co_await _service->local().shadow_link_report(
        cluster_link::model::name_t{req.get_shadow_link_name()}));

    chunked_vector<proto::admin::shadow_topic> shadow_topics;
    shadow_topics.reserve(resp->state.mirror_topics.size());
    std::ranges::transform(
      resp->state.mirror_topics,
      std::back_inserter(shadow_topics),
      [&shadow_link_report](const auto& p) {
          return model_to_shadow_topic(p.first, p.second, shadow_link_report);
      });

    proto::admin::list_shadow_topics_response list_resp;
    list_resp.set_shadow_topics(std::move(shadow_topics));

    vlog(sllog.trace, "list_shadow_topics response: {}", list_resp);
    co_return list_resp;
}

std::optional<model::node_id>
shadow_link_service_impl::redirect_to(const model::ntp& ntp) {
    return utils::redirect_to_leader(
      _md_cache->local(), ntp, _proxy_client.self_node_id());
}

ss::future<proto::admin::shadow_link>
shadow_link_service_impl::build_shadow_link(
  cluster_link::model::name_t shadow_link_name) {
    auto md = handle_error(
      _service->local().get_cluster_link(shadow_link_name));
    auto link_status = handle_error(
      co_await _service->local().shadow_link_report(shadow_link_name));

    co_return metadata_to_shadow_link(std::move(md), std::move(link_status));
}
} // namespace admin
