// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/requests/init_producer_id_request.h"

#include "cluster/topics_frontend.h"
#include "kafka/groups/group_manager.h"
#include "kafka/groups/group_router.h"
#include "kafka/logger.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "utils/remote.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>

namespace kafka {

ss::future<response_ptr> init_producer_id_api::process(
  request_context&& ctx, [[maybe_unused]] ss::smp_service_group g) {
    return ss::do_with(std::move(ctx), [](request_context& ctx) {
        vlog(klog.trace, "processing init_producer_id");

        init_producer_id_request request;
        request.decode(ctx.reader(), ctx.header().version);

        return ctx.id_allocator_frontend()
          .allocate_id(config::shard_local_cfg().create_topic_timeout_ms())
          .then([&ctx](cluster::allocate_id_reply r) {
              init_producer_id_response reply;

              if (r.ec == cluster::errc::success) {
                  reply.data.producer_id = kafka::producer_id(r.id);
                  reply.data.producer_epoch = 0;
                  vlog(
                    klog.trace,
                    "allocated pid {} with epoch {}",
                    reply.data.producer_id,
                    reply.data.producer_epoch);
              } else {
                  vlog(klog.warn, "failed to allocate pid");
                  reply.data.error_code = error_code::broker_not_available;
              }

              return ctx.respond(std::move(reply));
          });
    });
}

} // namespace kafka
