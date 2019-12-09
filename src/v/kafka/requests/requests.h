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
concept KafkaRequest = requires (T request, request_context&& ctx, smp_service_group g) {
    { T::name } -> const char*;
    { T::key } -> api_key;
    { T::min_supported } -> api_version;
    { T::max_supported } -> api_version;
    { T::process(std::move(ctx), g) } -> future<response_ptr>;
};
)
// clang-format on

} // namespace kafka
