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
#include "kafka/server/handlers/handler_interface.h"

#include "kafka/server/handlers/handlers.h"
#include "kafka/server/handlers/produce.h"
#include "kafka/server/response.h"

#include <optional>

namespace kafka {

/**
 * @brief Packages together basic information common to every handler.
 */
struct handler_info {
    handler_info(
      api_key key,
      const char* name,
      api_version min_api,
      api_version max_api,
      memory_estimate_fn* mem_estimate) noexcept
      : _key(key)
      , _name(name)
      , _min_api(min_api)
      , _max_api(max_api)
      , _mem_estimate(mem_estimate) {}

    api_key _key;
    const char* _name;
    api_version _min_api, _max_api;
    memory_estimate_fn* _mem_estimate;
};

/**
 * @brief Creates a type-erased handler implementation given info and a handle
 * method.
 *
 * There are only two variants of this handler, for one and two pass
 * implementations.
 * This keeps the generated code duplication to a minimum, compared to
 * templating this on the handler type.
 *
 * @tparam is_two_pass true if the handler is two-pass
 */
template<bool is_two_pass>
struct handler_base final : public handler_interface {
    using single_pass_handler
      = ss::future<response_ptr>(request_context, ss::smp_service_group);
    using two_pass_handler
      = process_result_stages(request_context, ss::smp_service_group);
    using fn_type
      = std::conditional_t<is_two_pass, two_pass_handler, single_pass_handler>;

    handler_base(const handler_info& info, fn_type* handle_fn) noexcept
      : _info(info)
      , _handle_fn(handle_fn) {}

    api_version min_supported() const override { return _info._min_api; }
    api_version max_supported() const override { return _info._max_api; }

    api_key key() const override { return _info._key; }
    const char* name() const override { return _info._name; }

    size_t memory_estimate(
      size_t request_size, connection_context& conn_ctx) const override {
        return _info._mem_estimate(request_size, conn_ctx);
    }
    /**
     * Only handle varies with one or two pass, since one pass handlers
     * must pass through single_stage() to covert them to two-pass.
     */
    process_result_stages
    handle(request_context&& rc, ss::smp_service_group g) const override {
        if constexpr (is_two_pass) {
            return _handle_fn(std::move(rc), g);
        } else {
            return process_result_stages::single_stage(
              _handle_fn(std::move(rc), g));
        }
    }

private:
    handler_info _info;
    fn_type* _handle_fn;
};

/**
 * @brief Instance holder for the handler_base.
 *
 * Given a handler type H, exposes a static instance of the assoicated handler
 * base object.
 *
 * @tparam H the handler type.
 */
template<KafkaApiHandlerAny H>
struct handler_holder {
    static inline const handler_base<KafkaApiTwoPhaseHandler<H>> instance{
      handler_info{
        H::api::key,
        H::api::name,
        H::min_supported,
        H::max_supported,
        H::memory_estimate},
      H::handle};
};

template<typename... Ts>
constexpr auto make_lut(type_list<Ts...>) {
    constexpr int max_index = std::max({Ts::api::key...});
    static_assert(max_index < sizeof...(Ts) * 10, "LUT is too sparse");

    std::array<handler, max_index + 1> lut{};
    ((lut[Ts::api::key] = &handler_holder<Ts>::instance), ...);

    return lut;
}

static const auto& handlers() {
    static constexpr auto lut = make_lut(request_types{});
    return lut;
}

std::optional<handler> handler_for_key(kafka::api_key key) noexcept {
    const auto& lut = handlers();
    if (key >= (short)0 && key < (short)lut.size()) {
        // We have already checked the bounds above so it is safe to use []
        // instead of at()
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
        if (auto handler = lut[key]) {
            return handler;
        }
    }
    return std::nullopt;
}

std::optional<api_key> api_name_to_key(std::string_view name) noexcept {
    for (const auto& handler : handlers()) {
        if (handler && name == handler->name()) {
            return handler->key();
        }
    }
    return std::nullopt;
}

size_t max_api_key() noexcept { return max_api_key(request_types{}); }

} // namespace kafka
