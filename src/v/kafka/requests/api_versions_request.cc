#include "kafka/requests/api_versions_request.h"

#include "kafka/requests/create_topics_request.h"
#include "kafka/requests/fetch_request.h"
#include "kafka/requests/find_coordinator_request.h"
#include "kafka/requests/heartbeat_request.h"
#include "kafka/requests/join_group_request.h"
#include "kafka/requests/leave_group_request.h"
#include "kafka/requests/list_groups_request.h"
#include "kafka/requests/list_offsets_request.h"
#include "kafka/requests/metadata_request.h"
#include "kafka/requests/offset_fetch_request.h"
#include "kafka/requests/produce_request.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/requests.h"
#include "kafka/requests/response.h"
#include "kafka/requests/response_writer.h"
#include "kafka/requests/sync_group_request.h"

namespace kafka {

void api_versions_request::encode(
  response_writer& writer, api_version version) {
    if (version >= api_version(3)) {
        writer.write(client_software_name);
        writer.write(client_software_version);
    }
}

void api_versions_request::decode(request_reader& reader, api_version version) {
    if (version >= api_version(3)) {
        client_software_name = reader.read_string();
        client_software_version = reader.read_string();
    }
}

void api_versions_response::encode(const request_context& ctx, response& resp) {
    auto& writer = resp.writer();
    auto version = ctx.header().version;

    writer.write(int16_t(error));
    writer.write_array(apis, [](const auto& api, response_writer& wr) {
        wr.write(int16_t(api.key()));
        wr.write(int16_t(api.min_version()));
        wr.write(int16_t(api.max_version()));
    });

    if (version >= api_version(1)) {
        writer.write(int32_t(throttle.count()));
    }
}

void api_versions_response::decode(iobuf buf, api_version version) {
    request_reader reader(std::move(buf));

    error = error_code{reader.read_int16()};
    apis = reader.read_array([](request_reader& reader) {
        return api{
          .key = api_key(reader.read_int16()),
          .min_version = api_version(reader.read_int16()),
          .max_version = api_version(reader.read_int16()),
        };
    });
    if (version >= api_version(1)) {
        throttle = std::chrono::milliseconds(reader.read_int32());
    }
}

template<typename... Ts>
struct type_list {};

template<typename... Requests>
CONCEPT(requires(KafkaRequest<Requests>, ...))
using make_request_types = type_list<Requests...>;

using request_types = make_request_types<
  produce_api,
  fetch_api,
  list_offsets_api,
  metadata_api,
  offset_fetch_api,
  find_coordinator_api,
  list_groups_api,
  api_versions_api,
  join_group_api,
  heartbeat_api,
  leave_group_api,
  sync_group_api,
  create_topics_api>;

template<typename RequestType>
static auto make_api() {
    return api_versions_response::api{
      RequestType::key, RequestType::min_supported, RequestType::max_supported};
}

template<typename... RequestTypes>
static std::vector<api_versions_response::api>
serialize_apis(type_list<RequestTypes...>) {
    std::vector<api_versions_response::api> apis;
    (apis.push_back(make_api<RequestTypes>()), ...);
    return apis;
}

std::vector<api_versions_response::api> get_supported_apis() {
    return serialize_apis(request_types{});
}

future<response_ptr>
api_versions_api::process(request_context&& ctx, smp_service_group) {
    // Unlike other request types, we handle ApiVersion requests
    // with higher versions than supported. We treat such a request
    // as if it were v0 and return a response using the v0 response
    // schema. The reason for this is that the client does not yet know what
    // versions a server supports when this request is sent, so instead of
    // assuming the lowest supported version, it can use the most recent
    // version and only fallback to the old version when necessary.
    api_versions_request request;
    request.decode(ctx.reader(), ctx.header().version);

    api_versions_response r;
    if (ctx.header().version > max_supported) {
        r.error = error_code::unsupported_version;
    } else if (!request.valid(ctx.header().version)) {
        r.error = error_code::invalid_request;
    } else {
        r.error = error_code::none;
    }

    if (
      r.error == error_code::none
      || r.error == error_code::unsupported_version) {
        r.apis = get_supported_apis();
    }

    auto resp = std::make_unique<response>();
    r.encode(ctx, *resp.get());
    return make_ready_future<response_ptr>(std::move(resp));
}

} // namespace kafka
