#include "kafka/requests/leave_group_request.h"

#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "utils/remote.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>

namespace kafka {

void leave_group_request::decode(request_context& ctx) {
    auto& reader = ctx.reader();

    group_id = kafka::group_id(reader.read_string());
    member_id = kafka::member_id(reader.read_string());
}

void leave_group_request::encode(
  const request_context& ctx, response_writer& writer) {
    writer.write(group_id());
    writer.write(member_id());
}

void leave_group_response::encode(const request_context& ctx, response& resp) {
    auto& writer = resp.writer();
    auto version = ctx.header().version;

    if (version >= api_version(1)) {
        writer.write(int32_t(throttle_time.count()));
    }
    writer.write(error);
}

future<response_ptr>
leave_group_api::process(request_context&& ctx, smp_service_group g) {
    return do_with(
      remote(std::move(ctx)), [g](remote<request_context>& remote_ctx) {
          auto& ctx = remote_ctx.get();
          leave_group_request request;
          request.decode(ctx);
          return ctx.groups()
            .leave_group(std::move(request))
            .then([&ctx](leave_group_response&& reply) {
                auto resp = std::make_unique<response>();
                reply.encode(ctx, *resp.get());
                return make_ready_future<response_ptr>(std::move(resp));
            });
      });
}

std::ostream& operator<<(std::ostream& o, const leave_group_request& r) {
    return fmt_print(o, "group={} member={}", r.group_id, r.member_id);
}

std::ostream& operator<<(std::ostream& o, const leave_group_response& r) {
    return fmt_print(o, "error={}", r.error);
}

} // namespace kafka
