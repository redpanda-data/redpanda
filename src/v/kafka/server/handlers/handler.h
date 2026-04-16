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
#include "kafka/server/fwd.h"
#include "kafka/server/logger.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"

#include <seastar/core/smp.hh>

namespace kafka {

using memory_estimate_fn = size_t(size_t, connection_context&);
using scheduling_group_provider_fn
  = std::optional<ss::scheduling_group>(const connection_context&);

constexpr size_t
default_estimate_adaptor(size_t request_size, connection_context&) {
    return default_memory_estimate(request_size);
}

constexpr std::optional<ss::scheduling_group>
default_scheduling_group_provider(const connection_context&) {
    return std::nullopt;
}

// Whether a handler should have a per-handler latency histogram registered.
// Only enable for very important common methods.
enum class latency_hist : bool { no = false, yes = true };

/**
 * Handlers are generally specializations of this template, via one of the
 * two aliases (handler or two_phase_hander) declared below, though it is
 * not strictly necessary (only conforming to one of the two KafkaApi*
 * concepts is needed).
 *
 * The benefit of this template is that it takes care of the most of the
 * handler boilerplate.
 */
template<
  typename RequestApi,
  api_version::type MinSupported,
  api_version::type MaxSupported,
  typename HandleRetType,
  memory_estimate_fn MemEstimator,
  scheduling_group_provider_fn SgProvider,
  latency_hist LatencyHistogram>
struct handler_template {
    using api = RequestApi;
    static constexpr api_version min_supported = api_version(MinSupported);
    static constexpr api_version max_supported = api_version(MaxSupported);
    static constexpr bool has_latency_histogram = LatencyHistogram
                                                  == latency_hist::yes;

    static HandleRetType handle(request_context, ss::smp_service_group);

    /**
     * See handler_interface::memory_estimate for a description of this
     * function.
     */
    static size_t
    memory_estimate(size_t request_size, connection_context& conn_ctx) {
        return MemEstimator(request_size, conn_ctx);
    }

    static std::optional<ss::scheduling_group>
    scheduling_group_override(const connection_context& conn_ctx) {
        return SgProvider(conn_ctx);
    }

    static void log_request(
      const request_header& header, const typename api::request_type& request) {
        vlog(
          kwire.trace,
          "[client_id: {}] handling {} v{} request {}",
          header.client_id,
          api::name,
          header.version(),
          request);
    }
};

/**
 * A single-stage handler implements the entire request handling in the initial
 * stage which occurs before any subsequent request is processed.
 */
template<
  typename RequestApi,
  api_version::type MinSupported,
  api_version::type MaxSupported,
  memory_estimate_fn MemEstimator = default_estimate_adaptor,
  scheduling_group_provider_fn SgProvider = default_scheduling_group_provider,
  latency_hist LatencyHistogram = latency_hist::no>
using single_stage_handler = handler_template<
  RequestApi,
  MinSupported,
  MaxSupported,
  ss::future<response_ptr>,
  MemEstimator,
  SgProvider,
  LatencyHistogram>;

/**
 * A two-stage handler has an initial stage which happens before any other
 * request can start processing (as in a single-stage handler) but then also has
 * a second stage which is processed in the background allowing other requests
 * on the same connection to start their handler. Responses are still sent in
 * order, but processing is out-of-order.
 */
template<
  typename RequestApi,
  api_version::type MinSupported,
  api_version::type MaxSupported,
  memory_estimate_fn MemEstimator = default_estimate_adaptor,
  scheduling_group_provider_fn SgProvider = default_scheduling_group_provider,
  latency_hist LatencyHistogram = latency_hist::no>
using two_phase_handler = handler_template<
  RequestApi,
  MinSupported,
  MaxSupported,
  process_result_stages,
  MemEstimator,
  SgProvider,
  LatencyHistogram>;

template<typename T>
concept KafkaApiHandler
  = KafkaApi<typename T::api>
    && requires(T h, request_context&& ctx, ss::smp_service_group g) {
           { T::min_supported } -> std::convertible_to<const api_version&>;
           { T::max_supported } -> std::convertible_to<const api_version&>;
           {
               T::handle(std::move(ctx), g)
           } -> std::same_as<ss::future<response_ptr>>;
       };
template<typename T>
concept KafkaApiTwoPhaseHandler
  = KafkaApi<typename T::api>
    && requires(T h, request_context&& ctx, ss::smp_service_group g) {
           { T::min_supported } -> std::convertible_to<const api_version&>;
           { T::max_supported } -> std::convertible_to<const api_version&>;
           {
               T::handle(std::move(ctx), g)
           } -> std::same_as<process_result_stages>;
       };

template<typename T>
concept KafkaApiHandlerAny = KafkaApiHandler<T> || KafkaApiTwoPhaseHandler<T>;

} // namespace kafka
