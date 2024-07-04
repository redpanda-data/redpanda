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

namespace cluster::data_migrations {

service_handler::service_handler(
  ss::scheduling_group sc,
  ss::smp_service_group ssg,
  ss::sharded<frontend>& frontend,
  ss::sharded<irpc_frontend>& irpc_frontend)
  : data_migrations_service(sc, ssg)
  , _frontend(frontend)
  , _irpc_frontend(irpc_frontend) {}

ss::future<create_migration_reply> service_handler::create_migration(
  create_migration_request request, ::rpc::streaming_context&) {
    return _frontend.local()
      .create_migration(
        std::move(request.migration), frontend::can_dispatch_to_leader::no)
      .then([](result<id> result) {
          create_migration_reply reply;
          if (result.has_error()) {
              reply.ec = map_error_code(result.error());
              return reply;
          }
          reply.ec = errc::success;
          reply.id = result.value();
          return reply;
      });
}

ss::future<update_migration_state_reply>
service_handler::update_migration_state(
  update_migration_state_request request, ::rpc::streaming_context&) {
    return _frontend.local()
      .update_migration_state(
        request.id, request.state, frontend::can_dispatch_to_leader::no)
      .then([](std::error_code ec) {
          return update_migration_state_reply{.ec = map_error_code(ec)};
      });
}

ss::future<remove_migration_reply> service_handler::remove_migration(
  remove_migration_request request, ::rpc::streaming_context&) {
    return _frontend.local()
      .remove_migration(request.id, frontend::can_dispatch_to_leader::no)
      .then([](std::error_code ec) {
          return remove_migration_reply{.ec = map_error_code(ec)};
      });
}

ss::future<check_ntp_states_reply> service_handler::check_ntp_states(
  check_ntp_states_request request, ::rpc::streaming_context&) {
    return _irpc_frontend.local().check_ntp_states(std::move(request));
}

cluster::errc service_handler::map_error_code(std::error_code ec) {
    if (ec.category() == cluster::error_category()) {
        return cluster::errc(ec.value());
    }

    return cluster::errc::replication_error;
}

} // namespace cluster::data_migrations
