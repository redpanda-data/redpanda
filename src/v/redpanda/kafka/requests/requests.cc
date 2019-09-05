#include "redpanda/kafka/requests/api_versions_request.h"
#include "redpanda/kafka/requests/find_coordinator_request.h"
#include "redpanda/kafka/requests/headers.h"
#include "redpanda/kafka/requests/list_groups_request.h"
#include "redpanda/kafka/requests/metadata_request.h"
#include "redpanda/kafka/requests/offset_fetch_request.h"
#include "redpanda/kafka/requests/request_context.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

namespace kafka::requests {

logger kreq_log("kafka api");

// clang-format off
CONCEPT(
// A Kafka request.
template<typename T>
concept KafkaRequest = requires (T request, request_context& ctx, smp_service_group g) {
    { T::key } -> api_key;
    { T::min_supported } -> api_version;
    { T::max_supported } -> api_version;
    { T::process(ctx, g) } -> future<response_ptr>;
};
)
// clang-format on

template<typename... Ts>
struct type_list {};

template<typename... Requests>
CONCEPT(requires(KafkaRequest<Requests>, ...))
using make_request_types = type_list<Requests...>;

using request_types = make_request_types<
  metadata_request,
  offset_fetch_request,
  find_coordinator_request,
  list_groups_request,
  api_versions_request>;

future<response_ptr>
process_request(request_context& ctx, smp_service_group g) {
    // Eventually generate this with meta-classes.
    kreq_log.debug("Processing request for {}", ctx.header().key);
    switch (ctx.header().key.value()) {
    case api_versions_request::key.value():
        return api_versions_request::process(ctx, std::move(g));
    case metadata_request::key.value():
        return metadata_request::process(ctx, std::move(g));
    case list_groups_request::key.value():
        return list_groups_request::process(ctx, std::move(g));
    case find_coordinator_request::key.value():
        return find_coordinator_request::process(ctx, std::move(g));
    case offset_fetch_request::key.value():
        return offset_fetch_request::process(ctx, std::move(g));
    };
    throw std::runtime_error(
      fmt::format("Unsupported API {}", ctx.header().key));
}

std::ostream& operator<<(std::ostream& os, const api_key& key) {
    return fmt_print(os, "{{api_key: {}}}", key.value());
}

std::ostream& operator<<(std::ostream& os, const api_version& version) {
    return fmt_print(os, "{{api_version: {}}}", version.value());
}

std::ostream& operator<<(std::ostream& os, const request_header& header) {
    fmt_print(
      os,
      "{{request_header: {}, {}, {{correlation_id: {}}}, ",
      header.key,
      header.version);
    if (header.client_id) {
        return fmt_print(os, "{{client_id: {}}}}}", header.client_id);
    }
    return os << "{no client_id}}";
}

struct api_support {
    int16_t key;
    int16_t min_supported;
    int16_t max_supported;
};

template<typename RequestType>
static auto make_api_support() {
    return api_support{RequestType::key.value(),
                       RequestType::min_supported.value(),
                       RequestType::max_supported.value()};
}

template<typename... RequestTypes>
static void
serialize_apis(response_writer& writer, type_list<RequestTypes...>) {
    std::vector<api_support> apis;
    (apis.push_back(make_api_support<RequestTypes>()), ...);
    writer.write_array(apis, [](const auto& api, response_writer& wr) {
        wr.write(api.key);
        wr.write(api.min_supported);
        wr.write(api.max_supported);
    });
}

future<response_ptr>
api_versions_request::process(request_context& ctx, smp_service_group) {
    auto resp = std::make_unique<response>();
    // Unlike other request types, we handle ApiVersion requests
    // with higher versions than supported. We treat such a request
    // as if it were v0 and return a response using the v0 response
    // schema. The reason for this is that the client does not yet know what
    // versions a server supports when this request is sent, so instead of
    // assuming the lowest supported version, it can use the most recent
    // version and only fallback to the old version when necessary.
    if (ctx.header().version > max_supported) {
        resp->writer().write(errors::error_code::unsupported_version);
        return make_ready_future<response_ptr>(std::move(resp));
    }
    resp->writer().write(errors::error_code::none);
    serialize_apis(resp->writer(), request_types{});
    if (ctx.header().version > v0) {
        resp->writer().write(int32_t(0));
    }
    return make_ready_future<response_ptr>(std::move(resp));
}

} // namespace kafka::requests
