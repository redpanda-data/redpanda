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
#include "cluster/controller_stm.h"
#include "cluster/migrations/tx_manager_migrator.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "redpanda/admin/api-doc/recovery.json.hh"
#include "redpanda/admin/server.h"

#include <seastar/core/smp.hh>
#include <seastar/json/json_elements.hh>

#include <system_error>

void admin_server::register_recovery_mode_routes() {
    vassert(
      _tx_manager_migrator != nullptr,
      "Tx manager migrator is expected to exits in recovery mode");

    register_route<superuser>(
      ss::httpd::recovery_json::migrate_tx_manager,
      [this](std::unique_ptr<ss::http::request>) {
          return ss::smp::submit_to(0, [this] {
              return _tx_manager_migrator.get()->migrate().then(
                [](std::error_code ec) {
                    if (ec) {
                        throw ss::httpd::base_exception(
                          fmt::format("Migration error: {}", ec.message()),
                          ss::http::reply::status_type::service_unavailable);
                    }
                    return ss::json::json_return_type(ss::json::json_void());
                });
          });
      });

    register_route<auth_level::user>(
      ss::httpd::recovery_json::get_tx_manager_migration_status,
      [this](std::unique_ptr<ss::http::request>) {
          return ss::smp::submit_to(cluster::controller_stm_shard, [this] {
              auto status = _tx_manager_migrator->get_status();
              ss::httpd::recovery_json::tx_manager_migration_status ret;
              ret.in_progress = status.migration_in_progress;
              ret.required = status.migration_required;
              return ss::json::json_return_type(ret);
          });
      });
}
