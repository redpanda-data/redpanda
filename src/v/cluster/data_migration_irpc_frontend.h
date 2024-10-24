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

#include "cluster/data_migration_types.h"
#include "cluster/fwd.h"
#include "features/fwd.h"
#include "ssx/single_sharded.h"

#include <seastar/core/sharded.hh>

namespace cluster::data_migrations {

class irpc_frontend : public ss::peering_sharded_service<irpc_frontend> {
public:
    irpc_frontend(
      ss::sharded<features::feature_table>&, ssx::single_sharded<backend>&);

    ss::future<check_ntp_states_reply>
    check_ntp_states(check_ntp_states_request&& req);

private:
    ss::sharded<features::feature_table>& _features;
    ssx::single_sharded<backend>& _backend;
};

} // namespace cluster::data_migrations
