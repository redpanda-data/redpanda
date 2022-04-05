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

struct produce_handler {
    using api = produce_api;
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(7);
    static process_result_stages handle(request_context, ss::smp_service_group);
};

} // namespace kafka
