/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/server/usage_manager.h"
#include "redpanda/admin/api-doc/usage.json.hh"
#include "redpanda/admin/server.h"

namespace {
ss::json::json_return_type raw_data_to_usage_response(
  const std::vector<kafka::usage_window>& total_usage, bool include_open) {
    std::vector<ss::httpd::usage_json::usage_response> resp;
    resp.reserve(total_usage.size());
    for (size_t i = (include_open ? 0 : 1); i < total_usage.size(); ++i) {
        resp.emplace_back();
        const auto& e = total_usage[i];
        resp.back().begin_timestamp = e.begin;
        resp.back().end_timestamp = e.end;
        resp.back().open = e.is_open();
        resp.back().kafka_bytes_received_count = e.u.bytes_received;
        resp.back().kafka_bytes_sent_count = e.u.bytes_sent;
        if (e.u.bytes_cloud_storage) {
            resp.back().cloud_storage_bytes_gauge = *e.u.bytes_cloud_storage;
        } else {
            resp.back().cloud_storage_bytes_gauge = -1;
        }
    }
    if (include_open && !resp.empty()) {
        /// Handle case where client does not want to observe
        /// value of 0 for open buckets end timestamp
        auto& e = resp.at(0);
        vassert(e.open(), "Bucket not open when expecting to be");
        e.end_timestamp = std::chrono::duration_cast<std::chrono::seconds>(
                            ss::lowres_system_clock::now().time_since_epoch())
                            .count();
    }
    return resp;
}
} // namespace

void admin_server::register_usage_routes() {
    register_route<user>(
      ss::httpd::usage_json::get_usage,
      [this](std::unique_ptr<ss::http::request> req) {
          if (!config::shard_local_cfg().enable_usage()) {
              throw ss::httpd::bad_request_exception(
                "Usage tracking is not enabled");
          }
          bool include_open = false;
          auto include_open_str = req->get_query_param("include_open_bucket");
          vlog(
            adminlog.info,
            "Request to observe usage info, include_open_bucket={}",
            include_open_str);
          if (!include_open_str.empty()) {
              include_open = str_to_bool(include_open_str);
          }
          return _usage_manager
            .invoke_on(
              kafka::usage_manager::usage_manager_main_shard,
              [](kafka::usage_manager& um) { return um.get_usage_stats(); })
            .then([include_open](auto total_usage) {
                return raw_data_to_usage_response(total_usage, include_open);
            });
      });
}
