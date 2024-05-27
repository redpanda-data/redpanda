/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/data_migration_service_handler.h"

#include "cluster/data_migration_frontend.h"
#include "cluster/data_migration_types.h"
#include "cluster/errc.h"

namespace cluster {

data_migrations_service_handler::data_migrations_service_handler(
  ss::scheduling_group sc,
  ss::smp_service_group ssg,
  ss::sharded<data_migration_frontend>& frontend)
  : data_migrations_service(sc, ssg)
  , _frontend(frontend) {}

ss::future<create_data_migration_reply>
data_migrations_service_handler::create_data_migration(
  create_data_migration_request request, ::rpc::streaming_context&) {
    return _frontend.local()
      .create_migration(
        std::move(request.migration),
        data_migration_frontend::can_dispatch_to_leader::no)
      .then([](result<data_migration_id> result) {
          create_data_migration_reply reply;
          if (result.has_error()) {
              reply.ec = map_error_code(result.error());
              return reply;
          }
          reply.ec = errc::success;
          reply.id = result.value();
          return reply;
      });
}

ss::future<update_data_migration_state_reply>
data_migrations_service_handler::update_data_migration_state(
  update_data_migration_state_request request, ::rpc::streaming_context&) {
    return _frontend.local()
      .update_migration_state(
        request.id,
        request.state,
        data_migration_frontend::can_dispatch_to_leader::no)
      .then([](std::error_code ec) {
          return update_data_migration_state_reply{.ec = map_error_code(ec)};
      });
}

ss::future<remove_data_migration_reply>
data_migrations_service_handler::remove_data_migration(
  remove_data_migration_request request, ::rpc::streaming_context&) {
    return _frontend.local()
      .remove_migration(
        request.id, data_migration_frontend::can_dispatch_to_leader::no)
      .then([](std::error_code ec) {
          return remove_data_migration_reply{.ec = map_error_code(ec)};
      });
}

cluster::errc
data_migrations_service_handler::map_error_code(std::error_code ec) {
    if (ec.category() == cluster::error_category()) {
        return cluster::errc(ec.value());
    }

    return cluster::errc::replication_error;
}

} // namespace cluster
