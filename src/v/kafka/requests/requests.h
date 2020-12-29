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

#include "kafka/requests/request_context.h"
#include "kafka/types.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/smp.hh>

namespace kafka {

// clang-format off
CONCEPT(
// A Kafka request.
template<typename T>
concept KafkaRequest = requires (T request, request_context&& ctx, ss::smp_service_group g) {
    { T::name } -> std::convertible_to<const char*>;
    { T::key } -> std::convertible_to<const api_key&>;
    { T::min_supported } -> std::convertible_to<const api_version&>;
    { T::max_supported } -> std::convertible_to<const api_version&>;
    { T::process(std::move(ctx), g) } -> std::same_as<ss::future<response_ptr>>;
};
)
// clang-format on

} // namespace kafka
