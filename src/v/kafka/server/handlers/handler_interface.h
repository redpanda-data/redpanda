/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "kafka/server/fwd.h"
#include "kafka/server/response.h"

namespace kafka {
/**
 * @brief Runtime polymorphic handler type.
 *
 * Allows access to all kafka request handling implementations though a
 * type erased interface. This avoids the need to bring every handler
 * type into scope and make everything that touches the handler a template
 * function on the handler type.
 *
 */
struct handler_interface {
    /**
     * @brief The minimum supported API version, inclusive.
     */
    virtual api_version min_supported() const = 0;

    /**
     * @brief The maximum supported API version, inclusive.
     */
    virtual api_version max_supported() const = 0;

    /**
     * @brief The name of the API method.
     */
    virtual const char* name() const = 0;

    /**
     * @brief The API key associated with the method.
     */
    virtual api_key key() const = 0;

    /**
     * @brief Estimates the memory used to process the request.
     *
     * Returns an esimate of the memory needed to process a request. This is
     * used to block the request until sufficient memory is available using the
     * "memory units" semaphore. Ideally this should be a conservative request
     * (i.e., a possible overestimate in cases where the memory use may vary
     * significantly) as the result of a too-small estimate may be an
     * out-of-memory condition, while a too-large estimate will "merely" reduce
     * performance.
     *
     * Unfortunately, this estimate happens early in the decoding process, after
     * only the request size and header has been read, so handlers don't have
     * as much information as they may like to make this decision. The
     * connection_context for the associated connection is passed to give access
     * to global state which may be useful in making the estimate.
     */
    virtual size_t
    memory_estimate(size_t request_size, connection_context& conn_ctx) const
      = 0;

    /**
     * @brief Handles the request.
     *
     * Invokes the request handler with the given request context
     * (which will be moved from) and smp_service_groups.
     *
     * The result stages objects contains futures for both the initial
     * dispatch phase, and the find response. For API methods which
     * are implemented a single phase, the same type is returned, but
     * the response future will complete as soon as the dispatch one does.
     *
     * @return process_result_stages representing the future completion of
     * the handler.
     */
    virtual process_result_stages
    handle(request_context&&, ss::smp_service_group) const
      = 0;

    virtual ~handler_interface() = default;
};

/**
 * @brief Pointer to a handler.
 *
 * Most code will use handler objects, which are simply pointers
 * to handlers, generally const objects with static storage duration
 * obtained from handler_for_key.
 */
using handler = const handler_interface*;

/**
 * @brief Return a handler for the given key, if any.
 *
 * Returns a pointer to a constant singleton handler for the given
 * key, or an empty optional if no such handler exists. The contained
 * any_hanlder is guaranteed to be non-null if the optional as a value.
 *
 * This method looks up the handler in a table populated by all handlers
 * in kafka::request_types.
 *
 * @param key the API key for the handler
 * @return std::optional<handler> the handler, if any
 */
std::optional<handler> handler_for_key(api_key key) noexcept;

/**
 * Map a Kafka API name to it's API key.
 *
 * @param name The name, taken from API schemata.
 * @return std::optional<api_key> the key, if any.
 */
std::optional<api_key> api_name_to_key(std::string_view name) noexcept;

/**
 * @return The highest value of Kafka API key
 */
size_t max_api_key() noexcept;

} // namespace kafka
