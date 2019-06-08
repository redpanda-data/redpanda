#include "redpanda/kafka/requests/headers.h"
#include "redpanda/kafka/requests/request_context.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

namespace kafka::requests {

seastar::logger kreq_log("init");

// clang-format off
CONCEPT(
// A Kafka request.
template<typename T>
concept KafkaRequest = requires (T request, request_context& ctx) {
    { T::key } -> api_key;
    { T::min_supported } -> api_version;
    { T::max_supported } -> api_version;
    { T::process(ctx) } -> seastar::future<response_ptr>;
};
)
// clang-format on

seastar::future<response_ptr> process_request(request_context& ctx) {
    // Eventually generate this with meta-classes.
    // Domain probe
    switch (ctx.header().key.value()) {
    };
    throw std::runtime_error(
      fmt::format("Unsupported API {}", ctx.header().key));
}

std::ostream& operator<<(std::ostream& os, const api_key& key) {
    return seastar::fmt_print(os, "{{api_key: {}}}", key.value());
}

std::ostream& operator<<(std::ostream& os, const api_version& version) {
    return seastar::fmt_print(os, "{{api_version: {}}}", version.value());
}

std::ostream& operator<<(std::ostream& os, const request_header& header) {
    seastar::fmt_print(
      os,
      "{{request_header: {}, {}, {{correlation_id: {}}}, ",
      header.key,
      header.version);
    if (header.client_id) {
        return seastar::fmt_print(os, "{{client_id: {}}}}}", header.client_id);
    }
    return os << "{no client_id}}";
}

} // namespace kafka::requests
