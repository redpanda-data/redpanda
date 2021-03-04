/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "kafka/protocol/api_versions.h"
#include "kafka/server/handlers/handler.h"

namespace kafka {

struct api_versions_handler : public handler<api_versions_api, 0, 2> {
    static ss::future<response_ptr>
    handle(request_context&&, ss::smp_service_group);

    static api_versions_response handle_raw(request_context& ctx);
};

} // namespace kafka
