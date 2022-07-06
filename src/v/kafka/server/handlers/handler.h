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

using memory_estimate_fn = size_t(size_t);

template<
  typename RequestApi,
  api_version::type MinSupported,
  api_version::type MaxSupported,
  memory_estimate_fn MemEstimator = default_memory_estimate>
struct handler {
    using api = RequestApi;
    static constexpr api_version min_supported = api_version(MinSupported);
    static constexpr api_version max_supported = api_version(MaxSupported);
    static ss::future<response_ptr>
      handle(request_context, ss::smp_service_group);
    /**
     * See any_handler_t::memory_estimate for a description of this function.
     */
    static size_t memory_estimate(size_t request_size) {
        return MemEstimator(request_size);
    }
};

template<typename T>
concept KafkaApiHandler = KafkaApi<typename T::api> && requires(
  T h, request_context&& ctx, ss::smp_service_group g) {
    { T::min_supported } -> std::convertible_to<const api_version&>;
    { T::max_supported } -> std::convertible_to<const api_version&>;
    { T::handle(std::move(ctx), g) } -> std::same_as<ss::future<response_ptr>>;
};
template<typename T>
concept KafkaApiTwoPhaseHandler = KafkaApi<typename T::api> && requires(
  T h, request_context&& ctx, ss::smp_service_group g) {
    { T::min_supported } -> std::convertible_to<const api_version&>;
    { T::max_supported } -> std::convertible_to<const api_version&>;
    { T::handle(std::move(ctx), g) } -> std::same_as<process_result_stages>;
};

template<typename T>
concept KafkaApiHandlerAny = KafkaApiHandler<T> || KafkaApiTwoPhaseHandler<T>;

} // namespace kafka
