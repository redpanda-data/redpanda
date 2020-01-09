#include "kafka/requests/heartbeat_request.h"

#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "utils/remote.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>

namespace kafka {

void heartbeat_request::decode(request_context& ctx) {
    auto& reader = ctx.reader();
    auto version = ctx.header().version;

    group_id = kafka::group_id(reader.read_string());
    generation_id = kafka::generation_id(reader.read_int32());
    member_id = kafka::member_id(reader.read_string());
    if (version >= api_version(3)) {
        auto id = reader.read_nullable_string();
        if (id) {
            group_instance_id = kafka::group_instance_id(std::move(*id));
        }
    }
}

void heartbeat_request::encode(
  const request_context& ctx, response_writer& writer) {
    auto version = ctx.header().version;

    writer.write(group_id());
    writer.write(generation_id);
    writer.write(member_id());
    if (version >= api_version(3)) {
        writer.write(group_instance_id);
    }
}

void heartbeat_response::encode(const request_context& ctx, response& resp) {
    auto& writer = resp.writer();
    auto version = ctx.header().version;

    if (version >= api_version(1)) {
        writer.write(int32_t(throttle_time.count()));
    }
    writer.write(error);
}

ss::future<response_ptr>
heartbeat_api::process(request_context&& ctx, ss::smp_service_group g) {
    return ss::do_with(
      remote(std::move(ctx)), [g](remote<request_context>& remote_ctx) {
          auto& ctx = remote_ctx.get();
          heartbeat_request request;
          request.decode(ctx);
          return ctx.groups()
            .heartbeat(std::move(request))
            .then([&ctx](heartbeat_response&& reply) {
                auto resp = std::make_unique<response>();
                reply.encode(ctx, *resp.get());
                return ss::make_ready_future<response_ptr>(std::move(resp));
            });
      });
}

std::ostream& operator<<(std::ostream& o, const heartbeat_request& r) {
    return ss::fmt_print(
      o,
      "group={} gen={} member={} group_inst={}",
      r.group_id,
      r.generation_id,
      r.member_id,
      r.group_instance_id);
}

std::ostream& operator<<(std::ostream& o, const heartbeat_response& r) {
    return ss::fmt_print(o, "error={}", r.error);
}

} // namespace kafka
