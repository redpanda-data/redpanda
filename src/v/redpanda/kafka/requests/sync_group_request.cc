#include "redpanda/kafka/requests/sync_group_request.h"

#include "model/metadata.h"
#include "redpanda/kafka/errors/errors.h"
#include "redpanda/kafka/requests/request_context.h"
#include "utils/remote.h"

#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka::requests {

void sync_group_request::decode(request_context& ctx) {
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
    assignments = reader.read_array([](request_reader& reader) {
        auto member = kafka::member_id(reader.read_string());
        auto assignment = reader.read_bytes();
        return member_assignment{member, assignment};
    });
}

void sync_group_response::encode(const request_context& ctx, response& resp) {
    auto& writer = resp.writer();
    auto version = ctx.header().version;

    if (version >= api_version(1)) {
        writer.write(int32_t(throttle_time.count()));
    }
    writer.write(error);
    writer.write(bytes_view(assignment));
}

future<response_ptr>
sync_group_request::process(request_context&& ctx, smp_service_group g) {
    return do_with(
      remote(std::move(ctx)), [g](remote<request_context>& remote_ctx) {
          auto& ctx = remote_ctx.get();
          sync_group_request request;
          request.decode(ctx);
          return ctx.groups()
            .sync_group(std::move(request))
            .then([&ctx](sync_group_response&& reply) {
                auto resp = std::make_unique<response>();
                reply.encode(ctx, *resp.get());
                return make_ready_future<response_ptr>(std::move(resp));
            });
      });
}

} // namespace kafka::requests
