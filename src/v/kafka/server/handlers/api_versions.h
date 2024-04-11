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
#include "container/fragmented_vector.h"
#include "kafka/protocol/api_versions.h"
#include "kafka/server/handlers/handler.h"

namespace kafka {

struct api_versions_handler
  : public single_stage_handler<api_versions_api, 0, 3> {
    static constexpr api_version min_flexible = api_version(3);

    static ss::future<response_ptr>
      handle(request_context, ss::smp_service_group);

    static api_versions_response handle_raw(request_context& ctx);
};

chunked_vector<api_versions_response_key> get_supported_apis();

} // namespace kafka
