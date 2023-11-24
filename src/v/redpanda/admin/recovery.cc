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
#include "cluster/migrations/tx_manager_migrator.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "redpanda/admin/api-doc/recovery.json.hh"
#include "redpanda/admin/server.h"

#include <seastar/json/json_elements.hh>

#include <system_error>

void admin_server::register_recovery_mode_routes() {
    vassert(
      _tx_manager_migrator != nullptr,
      "Tx manager migrator is expected to exits in recovery mode");

    register_route<superuser>(
      ss::httpd::recovery_json::migrate_tx_manager,
      [this](std::unique_ptr<ss::http::request>) {
          return _tx_manager_migrator.get()
            ->migrate(
              config::shard_local_cfg().transaction_coordinator_partitions())
            .then([](std::error_code ec) {
                if (ec) {
                    throw ss::httpd::base_exception(
                      fmt::format("Migration error: {}", ec.message()),
                      ss::http::reply::status_type::service_unavailable);
                }
                return ss::json::json_return_type(ss::json::json_void());
            });
      });
}
