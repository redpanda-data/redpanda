// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/id_allocator.h"

#include "cluster/id_allocator_frontend.h"
#include "cluster/logger.h"
#include "cluster/types.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"

#include <seastar/core/sharded.hh>

namespace cluster {

id_allocator::id_allocator(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<cluster::id_allocator_frontend>& id_allocator_frontend)
  : id_allocator_service(sg, ssg)
  , _id_allocator_frontend(id_allocator_frontend) {}

ss::future<allocate_id_reply>
id_allocator::allocate_id(allocate_id_request&& req, rpc::streaming_context&) {
    return _id_allocator_frontend.local().do_allocate_id(req.timeout);
}

} // namespace cluster
