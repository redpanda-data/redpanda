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

#include "cloud_storage/configuration.h"
#include "cluster/cluster_recovery_manager.h"
#include "cluster/cluster_recovery_table.h"
#include "cluster/controller.h"
#include "json/validator.h"
#include "model/fundamental.h"
#include "redpanda/admin/api-doc/shadow_indexing.json.hh"
#include "redpanda/admin/server.h"
#include "redpanda/admin/util.h"
#include "utils/uuid.h"

#include <optional>
#include <string_view>

namespace {

json::validator make_initialize_cluster_recovery_validator() {
    const std::string_view schema = R"({
  "type": "object",
  "properties": {
    "cluster_uuid_override": {
      "type": "string",
      "format": "uuid"
    }
  },
  "additionalProperties": false
})";
    return json::validator(schema);
}

} // namespace

ss::future<std::unique_ptr<ss::http::reply>>
admin_server::initialize_cluster_recovery(
  std::unique_ptr<ss::http::request> request,
  std::unique_ptr<ss::http::reply> reply) {
    static thread_local auto body_validator(
      make_initialize_cluster_recovery_validator());

    std::optional<model::cluster_uuid> cluster_uuid_override;

    auto doc = co_await parse_optional_json_body(request.get());
    if (doc.has_value()) {
        admin::apply_validator(body_validator, doc.value());

        if (doc.value().HasMember("cluster_uuid_override")) {
            cluster_uuid_override = model::cluster_uuid(
              uuid_t::from_string(
                (doc.value())["cluster_uuid_override"].GetString()));
            if (!cluster_uuid_override.has_value()) {
                throw ss::httpd::bad_request_exception("Invalid UUID format");
            }
        }
    }

    reply->set_content_type("json");

    if (config::node().recovery_mode_enabled()) {
        throw ss::httpd::bad_request_exception(
          "Cluster restore is not available, recovery mode enabled");
    }
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*request, model::controller_ntp);
    }
    auto& bucket_property = cloud_storage::configuration::get_bucket_config();
    if (!bucket_property.is_overriden() || !bucket_property().has_value()) {
        throw ss::httpd::bad_request_exception(
          "Cluster recovery is not available. Missing bucket property");
    }
    cloud_storage_clients::bucket_name bucket(bucket_property().value());
    auto result = ss::httpd::shadow_indexing_json::init_recovery_result{};
    auto error_res
      = co_await _controller->get_cluster_recovery_manager().invoke_on(
        cluster::cluster_recovery_manager::shard,
        [bucket, cluster_uuid_override](auto& mgr) {
            return mgr.initialize_recovery(bucket, cluster_uuid_override);
        });
    if (error_res.has_error()) {
        switch (error_res.error()) {
        case cluster::cluster_recovery_manager::errc::success:
            vunreachable("unreachable");
        case cluster::cluster_recovery_manager::errc::unknown:
            throw ss::httpd::base_exception{
              ssx::sformat(
                "Error starting cluster recovery request. Check logs for "
                "details."),
              ss::http::reply::status_type::internal_server_error};
        case cluster::cluster_recovery_manager::errc::not_a_leader:
            throw co_await redirect_to_leader(*request, model::controller_ntp);
        case cluster::cluster_recovery_manager::errc::misconfigured:
            throw ss::httpd::base_exception{
              "Cluster is misconfigured for recovery. Check logs for details.",
              ss::http::reply::status_type::bad_request};
        case cluster::cluster_recovery_manager::errc::no_matching_metadata:
            throw ss::httpd::base_exception{
              "Error starting cluster recovery request: No matching metadata",
              ss::http::reply::status_type::internal_server_error};
        case cluster::cluster_recovery_manager::errc::already_in_progress:
            throw ss::httpd::base_exception{
              "Recovery already active",
              ss::http::reply::status_type::conflict};
        }
    }

    result.status = "Recovery initialized";
    reply->set_status(ss::http::reply::status_type::accepted, result.to_json());
    co_return reply;
}

ss::future<ss::json::json_return_type>
admin_server::get_cluster_recovery(std::unique_ptr<ss::http::request> req) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }
    ss::httpd::shadow_indexing_json::cluster_recovery_status ret;
    ret.state = "inactive";

    auto latest_recovery
      = _controller->get_cluster_recovery_table().local().current_recovery();
    if (
      !latest_recovery.has_value()
      || latest_recovery.value().get().stage
           == cluster::recovery_stage::complete) {
        co_return ret;
    }
    auto& recovery = latest_recovery.value().get();
    ret.state = ssx::sformat("{}", recovery.stage);
    if (recovery.error_msg.has_value()) {
        ret.error = recovery.error_msg.value();
    }
    co_return ret;
}
