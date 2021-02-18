/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/fwd.h"
#include "cluster/id_allocator_service.h"

#include <seastar/core/sharded.hh>

namespace cluster {

class id_allocator final : public id_allocator_service {
public:
    id_allocator(
      ss::scheduling_group,
      ss::smp_service_group,
      ss::sharded<cluster::id_allocator_frontend>&);

    virtual ss::future<allocate_id_reply>
    allocate_id(allocate_id_request&&, rpc::streaming_context&) final;

private:
    ss::sharded<cluster::id_allocator_frontend>& _id_allocator_frontend;
};
} // namespace cluster
