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
#pragma once

#include "cluster/data_migration_rpc_service.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"

#include <system_error>

namespace cluster {

class data_migrations_service_handler : public data_migrations_service {
public:
    explicit data_migrations_service_handler(
      ss::scheduling_group,
      ss::smp_service_group,
      ss::sharded<data_migration_frontend>&);

    ss::future<create_data_migration_reply> create_data_migration(
      create_data_migration_request, ::rpc::streaming_context&) final;

    ss::future<update_data_migration_state_reply> update_data_migration_state(
      update_data_migration_state_request, ::rpc::streaming_context&) final;

    ss::future<remove_data_migration_reply> remove_data_migration(
      remove_data_migration_request, ::rpc::streaming_context&) final;

private:
    static cluster::errc map_error_code(std::error_code);

    ss::sharded<data_migration_frontend>& _frontend;
};

} // namespace cluster
