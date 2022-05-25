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
#include "kafka/protocol/types.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "kafka/types.h"

#include <seastar/core/smp.hh>

namespace kafka {

template<
  typename RequestApi,
  api_version::type MinSupported,
  api_version::type MaxSupported>
struct handler {
    using api = RequestApi;
    static constexpr api_version min_supported = api_version(MinSupported);
    static constexpr api_version max_supported = api_version(MaxSupported);
    static ss::future<response_ptr>
      handle(request_context, ss::smp_service_group);
};

// clang-format off
template<typename T>
concept KafkaApiHandler = requires (T h, request_context&& ctx, ss::smp_service_group g) {
    KafkaApi<typename T::api>;
    { T::min_supported } -> std::convertible_to<const api_version&>;
    { T::max_supported } -> std::convertible_to<const api_version&>;
    { T::handle(std::move(ctx), g) } -> std::same_as<ss::future<response_ptr>>;
};
template<typename T>
concept KafkaApiTwoPhaseHandler = requires (T h, request_context&& ctx, ss::smp_service_group g) {
    KafkaApi<typename T::api>;
    { T::min_supported } -> std::convertible_to<const api_version&>;
    { T::max_supported } -> std::convertible_to<const api_version&>;
    { T::handle(std::move(ctx), g) } -> std::same_as<process_result_stages>;
};
// clang-format on

} // namespace kafka
