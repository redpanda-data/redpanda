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

#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "redpanda/admin/api-doc/partition.json.hh"
#include "redpanda/admin/server.h"

void admin_server::register_kafka_routes() {
    register_route<superuser>(
      ss::httpd::partition_json::kafka_transfer_leadership,
      [this](std::unique_ptr<ss::http::request> req) {
          return kafka_transfer_leadership_handler(std::move(req));
      });
}

ss::future<ss::json::json_return_type>
admin_server::kafka_transfer_leadership_handler(
  std::unique_ptr<ss::http::request> req) {
    auto ntp = parse_ntp_from_request(req->param);

    std::optional<model::node_id> target;
    if (auto node = req->get_query_param("target"); !node.empty()) {
        try {
            target = model::node_id(std::stoi(node));
        } catch (...) {
            throw ss::httpd::bad_param_exception(
              fmt::format("Target node id must be an integer: {}", node));
        }
        if (*target < 0) {
            throw ss::httpd::bad_param_exception(
              fmt::format("Invalid target node id {}", *target));
        }
    }

    vlog(
      adminlog.info,
      "Leadership transfer request for leader of topic-partition {} to node {}",
      ntp,
      target);

    auto shard = _shard_table.local().shard_for(ntp);
    if (!shard) {
        // This node is not a member of the raft group, redirect.
        throw co_await redirect_to_leader(*req, ntp);
    }

    co_return co_await _partition_manager.invoke_on(
      *shard,
      [ntp = std::move(ntp), target, this, req = std::move(req)](
        cluster::partition_manager& pm) mutable {
          auto partition = pm.get(ntp);
          if (!partition) {
              throw ss::httpd::not_found_exception();
          }
          auto r = raft::transfer_leadership_request{
            .group = partition->group(),
            .target = target,
          };
          return partition->transfer_leadership(r).then(
            [this, ntp, req = std::move(req)](auto err) {
                return throw_on_error(*req, err, ntp).then([] {
                    return ss::json::json_return_type(ss::json::json_void());
                });
            });
      });
}
