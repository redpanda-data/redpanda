/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "kafka/protocol/produce.h"
#include "kafka/server/handlers/handler.h"

namespace kafka {

using produce_handler = two_phase_handler<produce_api, 0, 7>;

struct partition_produce_stages {
    ss::future<> dispatched;
    ss::future<produce_response::partition> produced;
};

struct produce_ctx {
    request_context rctx;
    produce_request request;
    produce_response response;
    ss::smp_service_group ssg;

    produce_ctx(
      request_context&& rctx,
      produce_request&& request,
      produce_response&& response,
      ss::smp_service_group ssg)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , response(std::move(response))
      , ssg(ssg) {}
};

/*
 * Unit Tests Exposure
 */
namespace testing {

/**
 * Exposed for testing/benchmarking only.
 */
kafka::partition_produce_stages produce_single_partition(
  produce_ctx& octx,
  produce_request::topic& topic,
  produce_request::partition& part);

} // namespace testing
} // namespace kafka
